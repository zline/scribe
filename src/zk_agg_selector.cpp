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

using namespace std;

RandomAggSelector::RandomAggSelector() {}

RandomAggSelector::~RandomAggSelector() {}

bool RandomAggSelector::selectScribeAggregator(struct String_vector& hostNames,
    string& remoteHost,
    unsigned long& remotePort) {
  if (hostNames.count == 0) {
    LOG_DEBUG("No hosts in counters map!");
    return false;
  } else {
    int randomInt = rand() % hostNames.count;
    string remoteScribeZnode = hostNames.data[randomInt];
    size_t index = remoteScribeZnode.find(":");
    remoteHost = remoteScribeZnode.substr(0, index);
    string port = remoteScribeZnode.substr(index+1, string::npos);
    remotePort = static_cast<unsigned long>(atol(port.c_str()));
    LOG_DEBUG("Selected remote scribe %s:%lu", remoteHost.c_str(), remotePort);
    return true;
  }
}

AggSelector* AggSelectorFactory::createAggSelector(string& aggName) {
  if (aggName.compare("RandomAggSelector")) {
    LOG_OPER("Aggregator selector not specified. Creating RandomAggSelector by default");
  }
  return new RandomAggSelector();
}

