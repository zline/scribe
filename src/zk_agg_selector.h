/*
 * zk_agg_selector.h
 *
 *  Created on: Jun 24, 2010
 *      Author: Dmitriy Ryaboy
 */

#ifndef ZK_AGG_SELECTOR_H_
#define ZK_AGG_SELECTOR_H_
#include "common.h"
#include "zk_client.h"
#include "scribe_server.h"
class scribeHandler;

class AggSelector {
public:
  virtual bool selectScribeAggregator(ZKClient::HostNamesSet& hostNames,
      std::string& _remoteHost,
      unsigned long& _remotePort) = 0;
};

class AggSelectorFactory {
public:
  static AggSelector* createAggSelector(std::string& aggName);
};

class RandomAggSelector : public AggSelector {
public:
  RandomAggSelector();
  virtual ~RandomAggSelector();
  bool selectScribeAggregator(ZKClient::HostNamesSet& hostNames, std::string& remoteHost,
      unsigned long& remotePort);
};

#endif /* ZK_AGG_SELECTOR_H_ */
