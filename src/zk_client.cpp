//  Copyright (c) 2007-2008 Facebook
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
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

ZKClient * g_ZKClient;

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

ZKClient::ZKClient(string& hostPort,
                   string& zk_registration_prefix,
                   unsigned long int handlerPort) {
  zkHostPort = hostPort;
  zkRegistrationPrefix = zk_registration_prefix;
  port = handlerPort;

  char hostname[1024];
  hostname[1023] = '\0';
  gethostname(hostname, 1023);
  zkRegistrationName = lexical_cast<string>(hostname) + ":" + lexical_cast<string>(port);
  zkFullRegistrationName = zkRegistrationPrefix + "/" + zkRegistrationName;

  if (debug_level) {
    zoo_set_debug_level(ZOO_LOG_LEVEL_DEBUG);
  }
}

ZKClient::~ZKClient() {
  zookeeper_close(zh);
}

bool ZKClient::connect() {
  zh = zookeeper_init(zkHostPort.c_str(), watcher, 10000, 0, 0, 0);
  return true; // ZK client retries on failure.
}

bool ZKClient::isRegistered() {
  return false;
}

bool ZKClient::registerTask() {
  static string delimiter = "/";
  static string contents = "";
  size_t index = zkRegistrationPrefix.find(delimiter, 0);
  char tmp[0];

  // Prefixs are created as regular znodes.
  while (index < string::npos) {
    index = zkRegistrationPrefix.find(delimiter, index+1);
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

// Get the best host:port to send messages to at this time.
bool ZKClient::getRemoteScribe(string& parentZnode,
                               string& remoteHost,
                               unsigned long& remotePort) {
  LOG_DEBUG("Getting the best remote scribe.");
  struct String_vector children;
  if (ZOK != zoo_get_children(zh, parentZnode.c_str(), 0, &children)) {
    return false;
  }

  if (0 == children.count) {
    return false;
  }

  // Choose a random scribe.
  srand(time(NULL));
  int remoteScribeIndex = rand() % children.count;

  string remoteScribeZnode = children.data[remoteScribeIndex];
  string delimiter = ":";
  size_t index = remoteScribeZnode.find(delimiter);

  remoteHost = remoteScribeZnode.substr(0, index);
  string port = remoteScribeZnode.substr(index+1, string::npos);
  remotePort = static_cast<unsigned long>(atol(port.c_str()));
  return true;
}
