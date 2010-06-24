/*
 * zk_agg_selector.cpp
 *
 *
 *
 *  Created on: Jun 23, 2010
 *      Author: Dmitriy V. Ryaboy
 */

#include "zk_agg_selector.h"
#include "zk_client.h"
#include "zk_status.h"

using namespace std;

// TODO(wanli): share this with zk_status.cpp
const string QUEUE_SIZE_KEY = "queue size";
const string MSGS_RECEIVED_KEY = "bytes received rate";

RandomAggSelector::RandomAggSelector(zhandle_t *zh)
 : zh_(zh) {
}

RandomAggSelector::~RandomAggSelector() {
}

bool RandomAggSelector::selectScribeAggregator(string& parentZnode,
    string& remoteHost,
    unsigned long& remotePort) {
  struct String_vector children;
  bool ret;
  if (ZOK != zoo_get_children(zh_, parentZnode.c_str(), 0, &children)) {
    ret = false;
  } else if (0 == children.count) {
    ret = false;
  } else {
    string remoteScribeZnode = children.data[rand() % children.count];
    size_t index = remoteScribeZnode.find(":");
    // TODO(wanli): refactor the following into a function
    remoteHost = remoteScribeZnode.substr(0, index);
    string port = remoteScribeZnode.substr(index+1, string::npos);
    remotePort = static_cast<unsigned long>(atol(port.c_str()));
    ret = true;
  }
  return ret;
}

MsgCounterAggSelector::MsgCounterAggSelector(boost::shared_ptr<ZKStatusReader> zkStatusReader, zhandle_t *zh)
 : zkStatusReader_(zkStatusReader),
   zh_(zh) {
}

MsgCounterAggSelector::~MsgCounterAggSelector() {
}

bool MsgCounterAggSelector::selectScribeAggregator(string& parentZnode,
    string& remoteHost,
    unsigned long& remotePort) {
  host_counters_map_t host_counters_map;
  zkStatusReader_->getCountersForAllHosts(parentZnode, host_counters_map);
  int max = 0, sum = 0;
  for (host_counters_map_t::iterator iter = host_counters_map.begin(); iter != host_counters_map.end(); iter++ ) {
    int measure = (iter->second.count(QUEUE_SIZE_KEY) != 0) ? (int) iter->second[QUEUE_SIZE_KEY] : 0;
    measure += (iter->second.count(MSGS_RECEIVED_KEY) != 0) ? (int) iter->second[MSGS_RECEIVED_KEY] : 0;
    if (measure > max) { max = measure; }
    LOG_DEBUG("ZK AGGREGATOR %s -->", iter->first.c_str());
    for (counter_map_t::iterator counterIter = iter->second.begin(); counterIter != iter->second.end(); counterIter++) {
      LOG_DEBUG("          %s --> %lld", counterIter->first.c_str(), counterIter->second);
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
  LOG_OPER("Couldn't find any aggregator");
  return false;
}

AggSelector* AggSelectorFactory::createAggSelector(
    boost::shared_ptr<ZKStatusReader> zkStatusReader, zhandle_t *zh, string& aggName) {
  if (aggName.compare("RandomAggSelector") == 0) {
    return new RandomAggSelector(zh);
  } else if (aggName.compare("MsgCounterAggSelector") == 0) {
    return new MsgCounterAggSelector(zkStatusReader, zh);
  } else {
    LOG_OPER("Fall back to random aggregator selector");
    return new RandomAggSelector(zh);
  }
}
