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

typedef std::map<std::string, int64_t> counter_map_t;
typedef std::map<std::string, counter_map_t> host_counters_map_t;

class AggSelector {
public:
  virtual bool selectScribeAggregator(host_counters_map_t host_counters_map,
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
  bool selectScribeAggregator(host_counters_map_t hostCountersMap, std::string& remoteHost,
      unsigned long& remotePort);
};

class MsgCounterAggSelector : public AggSelector {

public:
 MsgCounterAggSelector();
  virtual ~MsgCounterAggSelector();
  bool selectScribeAggregator(host_counters_map_t hostCountersMap, std::string& remoteHost,
      unsigned long& remotePort);
};

#endif /* ZK_AGG_SELECTOR_H_ */
