//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.`
//  See the License for the specific language governing permissions and
//  limitations under the License.
//
// See accompanying file LICENSE or visit the Scribe site at:
// http://developers.facebook.com/scribe/
//
// @author Travis Crawford <travis@twitter.com>

#include "common.h"
#include "scribe_server.h"
#include "zk_client.h"

using namespace std;
using boost::lexical_cast;
using boost::shared_ptr;

// TODO(dvryaboy) pull out these constants
const std::string QUEUE_SIZE_KEY = "queue size";
const std::string MSGS_RECEIVED_KEY = "bytes received rate";
const int64_t MAX_INT64 = 0x43DFFFFFFFFFFFFF;

shared_ptr<ZKClient> g_ZKClient;

/*
 * Global Zookeeper watcher handles all callbacks.
 */
void watcher(zhandle_t *zzh, int type, int state,
    const char *path, void *watcherCtx) {

  // Zookeeper session established so attempt registration.
  if ((state == ZOO_CONNECTED_STATE) &&
      (type == ZOO_SESSION_EVENT)) {
    g_ZKClient->registerTask();
  }

  // Registration znode was deleted so attempt registration.
  else if ((state == ZOO_CONNECTED_STATE) &&
      (type == ZOO_DELETED_EVENT) &&
      (lexical_cast<string>(path) == g_ZKClient->zkFullRegistrationName)) {
    g_ZKClient->registerTask();
  }

  // This should never happen.
  else {
    LOG_DEBUG("Received unhandled watch callback: %s %d %d", path, type, state);
  }
}

ZKClient::ZKClient(scribeHandler* scribeHandlerObj_) {
  zh = NULL;
  scribeHandlerObj = scribeHandlerObj_;
  if (debug_level) {
    zoo_set_debug_level(ZOO_LOG_LEVEL_DEBUG);
  }
}

ZKClient::~ZKClient() {
  zookeeper_close(zh);
}

void ZKClient::connect() {
  LOG_DEBUG("Connecting to zookeeper.");
  zh = zookeeper_init(zkServer.c_str(), watcher, 10000, 0, 0, 0);
}

void ZKClient::disconnect() {
  LOG_DEBUG("Disconnecting from zookeeper.");
  zookeeper_close(zh);
  zh = NULL;
}

bool ZKClient::registerTask() {
  if (zkRegistrationPrefix.empty()) {
    return false;
  }

  LOG_OPER("Registering task in Zookeeper.");
  char hostname[1024];
  hostname[1023] = '\0';
  gethostname(hostname, 1023);
  zkRegistrationName = lexical_cast<string>(hostname) + ":" + lexical_cast<string>(scribeHandlerPort);
  zkFullRegistrationName = zkRegistrationPrefix + "/" + zkRegistrationName;

  static string contents = "";
  size_t index = zkRegistrationPrefix.find("/", 0);
  char tmp[0];

  // Prefixs are created as regular znodes.
  // TODO(wanli): skip this if the file already exists
  while (index < string::npos) {
    index = zkRegistrationPrefix.find("/", index+1);
    string prefix = zkRegistrationPrefix.substr(0, index);
    zoo_create(zh, prefix.c_str(), contents.c_str(), contents.length(),
        &ZOO_CREATOR_ALL_ACL, 0, tmp, sizeof(tmp));
  }

  // Register this scribe as an ephemeral node.
  int ret = zoo_create(zh, zkFullRegistrationName.c_str(), contents.c_str(),
      contents.length(), &ZOO_CREATOR_ALL_ACL,
      ZOO_EPHEMERAL, tmp, sizeof(tmp));

  if (ZOK == ret) {
    return true;
  } else if (ZNODEEXISTS == ret) {
    struct Stat stat;
    if (ZOK == zoo_exists(zh, zkFullRegistrationName.c_str(), 1, &stat)) {
      LOG_DEBUG("Set watch on znode that already exists: %s", zkFullRegistrationName.c_str());
      return true;
    } else {
      LOG_OPER("Failed setting watch on znode: %s", zkFullRegistrationName.c_str());
      return false;
    }
  }
  LOG_OPER("Registration failed for unknown reason: %s", zkFullRegistrationName.c_str());
  return false;
}

bool ZKClient::updateStatus(std::string& current_status) {
  int rc = zoo_set(zh, zkFullRegistrationName.c_str(), current_status.c_str(), current_status.length() + 1, -1);
  if (rc) {
    LOG_OPER("Error %d for writing %s to ZK file %s", rc, current_status.c_str(), zkFullRegistrationName.c_str());
  } else {
    LOG_OPER("Write %s to ZK file %s", current_status.c_str(), zkFullRegistrationName.c_str());
  }
  return rc == 0;
}

bool ZKClient::getAllHostsStatus(std::string& parentZnode, HostStatusMap* host_status_map) {
  struct String_vector children;
  if (zoo_get_children(zh, parentZnode.c_str(), 0, &children) != ZOK || children.count == 0) {
    return false;
  }
  char buffer[512];
  int allocated_buflen = sizeof(buffer);
  for (int i = 0; i < children.count; ++i) {
    int buflen = allocated_buflen;
    std::string zk_file_path = parentZnode + "/" + children.data[i];
    int rc = zoo_get(zh, zk_file_path.c_str(), 0, buffer, &buflen, NULL);
    if (rc) {
      LOG_OPER("Error %d for reading to ZK file %s", rc, zk_file_path.c_str());
    } else {
      string content = buffer;
      LOG_OPER("ZK file %s size: %ld content: %s", zk_file_path.c_str(), content.length(), content.c_str());
      (*host_status_map)[zk_file_path] = content;
    }
  }
  return true;
}

// Get the best host:port to send messages to at this time.
bool ZKClient::getRemoteScribe(std::string& parentZnode,
    string& remoteHost,
    unsigned long& remotePort) {
  bool ret = false;
  bool should_disconnect = false;
  if (zkServer.empty()) {
    LOG_OPER("No zookeeper server! Unable to discover remote scribes.");
    return false;
  } else if (NULL == zh) {
    connect();
    should_disconnect = true;
  } else if (!zoo_state(zh)) {
    LOG_OPER("No zookeeper connection state! Unable to discover remote scribes.");
    return false;
  }

  LOG_DEBUG("Getting the best remote scribe.");
  struct String_vector children;
  if (ZOK != zoo_get_children(zh, parentZnode.c_str(), 0, &children)) {
    ret = false;
  } else if (0 == children.count) {
    ret = false;
  } else {
    ret = selectScribeAggregator(parentZnode, remoteHost, remotePort);
  }

  if (should_disconnect) {
    disconnect();
  }
  return ret;
}

// TODO(dvryaboy) make this logic modular
bool ZKClient::selectScribeAggregator(string& parentZnode, string& remoteHost,
    unsigned long& remotePort) {
  host_counters_map_t host_counters_map;
  scribeHandlerObj->getCountersForAllHostsFromZooKeeper(parentZnode, host_counters_map);
  int max = 0, sum = 0;
  for (host_counters_map_t::iterator iter = host_counters_map.begin(); iter != host_counters_map.end(); iter++ ) {
    int measure = (iter->second.count(QUEUE_SIZE_KEY) != 0) ? (int) iter->second[QUEUE_SIZE_KEY] : 0;
    measure += (iter->second.count(MSGS_RECEIVED_KEY) != 0) ? (int) iter->second[MSGS_RECEIVED_KEY] : 0;
    if (measure > max) { max = measure; }
    // TODO(dvryaboy) remove debugging output, or wrap it in appropriate debug level
    LOG_OPER("ZK AGGREGATOR %s -->", iter->first.c_str());
    for (counter_map_t::iterator counterIter = iter->second.begin(); counterIter != iter->second.end(); counterIter++) {
      LOG_OPER("          %s --> %lld", counterIter->first.c_str(), counterIter->second);
    }
  }
  map<std::string, int> weight_map;
  //TODO (dvryaboy) make sum resilient to overflow
  for (host_counters_map_t::iterator iter = host_counters_map.begin(); iter != host_counters_map.end(); iter++ ) {
    int measure = (iter->second.count(QUEUE_SIZE_KEY) != 0) ? (int) iter->second[QUEUE_SIZE_KEY] : 0;
    measure += (iter->second.count(MSGS_RECEIVED_KEY) != 0) ? (int) iter->second[MSGS_RECEIVED_KEY] : 0;
    weight_map[iter->first] = (int) max - measure + 1;
    sum += weight_map[iter->first];
  }
  int r = rand() % sum;
  for (map<std::string, int>::iterator iter = weight_map.begin(); iter != weight_map.end(); iter++ ) {
    if ( (r -= iter->second) < 0 ) {
      size_t index = iter->first.find(":");
      remoteHost = iter->first.substr(parentZnode.length() + 1, index - parentZnode.length() - 1);
      string port = iter->first.substr(index+1, string::npos);
      remotePort = static_cast<unsigned long>(atol(port.c_str()));
      LOG_OPER("Selected %s, %ld", remoteHost.c_str(), remotePort);
      return true;
    }
  }
  return false;
}
