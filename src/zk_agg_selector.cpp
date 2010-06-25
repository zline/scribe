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

const string QUEUE_SIZE_KEY = "queue size";

RandomAggSelector::RandomAggSelector() {}

RandomAggSelector::~RandomAggSelector() {}

bool RandomAggSelector::selectScribeAggregator(host_counters_map_t hostCountersMap,
    string& remoteHost,
    unsigned long& remotePort) {
  if (hostCountersMap.size() == 0) {
    return false;
  } else {
    int randomInt = rand() % hostCountersMap.size();
    string remoteScribeZnode;
    for (host_counters_map_t::iterator iter = hostCountersMap.begin();
        iter != hostCountersMap.end() && randomInt >= 0; iter++ ) {
      remoteScribeZnode = iter->first;
      randomInt -= 1;
    }
    size_t index = remoteScribeZnode.find(":");
    size_t lastSlashIdx = remoteScribeZnode.find_last_of('/');
    remoteHost = remoteScribeZnode.substr(lastSlashIdx + 1, index - lastSlashIdx - 1);
    string port = remoteScribeZnode.substr(index+1, string::npos);
    remotePort = static_cast<unsigned long>(atol(port.c_str()));
    return true;
  }
}

MsgCounterAggSelector::MsgCounterAggSelector() {}

MsgCounterAggSelector::~MsgCounterAggSelector() {}

bool MsgCounterAggSelector::selectScribeAggregator(host_counters_map_t hostCountersMap,
    string& remoteHost,
    unsigned long& remotePort) {
  int max = 0, sum = 0;
  for (host_counters_map_t::iterator iter = hostCountersMap.begin(); iter != hostCountersMap.end(); iter++ ) {
    int measure = (iter->second.count(QUEUE_SIZE_KEY) != 0) ? (int) iter->second[QUEUE_SIZE_KEY] : 0;
    measure += (iter->second.count(RECEIVED_GOOD_RATE_KEY) != 0) ? (int) iter->second[RECEIVED_GOOD_RATE_KEY] : 0;
    if (measure > max) { max = measure; }
    LOG_DEBUG("ZK AGGREGATOR %s -->", iter->first.c_str());
    for (counter_map_t::iterator counterIter = iter->second.begin(); counterIter != iter->second.end(); counterIter++) {
      LOG_DEBUG("          %s --> %lld", counterIter->first.c_str(), counterIter->second);
    }
  }
  map<std::string, int> weight_map;
  for (host_counters_map_t::iterator iter = hostCountersMap.begin(); iter != hostCountersMap.end(); iter++ ) {
    int measure = (iter->second.count(QUEUE_SIZE_KEY) != 0) ? (int) iter->second[QUEUE_SIZE_KEY] : 0;
    measure += (iter->second.count(RECEIVED_GOOD_RATE_KEY) != 0) ? (int) iter->second[RECEIVED_GOOD_RATE_KEY] : 0;
    weight_map[iter->first] = (int) max - measure + 1;
    sum += weight_map[iter->first];
  }
  int r = rand() % sum;
  for (map<std::string, int>::iterator iter = weight_map.begin(); iter != weight_map.end(); iter++ ) {
    if ( (r -= iter->second) < 0 ) {
      size_t index = iter->first.find(":");
      size_t lastSlashIdx = iter->first.find_last_of('/');
      remoteHost = iter->first.substr(lastSlashIdx + 1, index - lastSlashIdx - 1);
      string port = iter->first.substr(index+1, string::npos);
      remotePort = static_cast<unsigned long>(atol(port.c_str()));
      LOG_OPER("Selected %s, %ld", remoteHost.c_str(), remotePort);
      return true;
    }
  }
  LOG_OPER("Couldn't find any aggregator");
  return false;
}

AggSelector* AggSelectorFactory::createAggSelector(string& aggName) {
  if (aggName.compare("RandomAggSelector") == 0) {
    LOG_OPER("Creating RandomAggSelector by request");
    return new RandomAggSelector();
  } else if (aggName.compare("MsgCounterAggSelector") == 0) {
    LOG_OPER("Creating MsgCounterAggSelector by request");
    return new MsgCounterAggSelector();
  } else {
    LOG_OPER("Creating RandomAggSelector by default");
    return new RandomAggSelector();
  }
}
