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

bool RandomAggSelector::selectScribeAggregator(ZKClient::HostNamesSet& hostNames,
    string& remoteHost,
    unsigned long& remotePort) {
  if (hostNames.size() == 0) {
    LOG_DEBUG("No hosts in counters map!");
    return false;
  } else {
    int randomInt = rand() % hostNames.size();
    string remoteScribeZnode;
    for (ZKClient::HostNamesSet::iterator iter = hostNames.begin();
        iter != hostNames.end() && randomInt >= 0; iter++ ) {
      remoteScribeZnode = *iter;
      randomInt -= 1;
    }
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

