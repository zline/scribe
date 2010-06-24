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
  virtual bool selectScribeAggregator(std::string& parentZnode,
      std::string& _remoteHost,
      unsigned long& _remotePort) = 0;
};

class AggSelectorFactory {
public:
  static AggSelector* createAggSelector(scribeHandler *scribeHandlerObj, zhandle_t *zh, std::string& aggName);
};

class RandomAggSelector : public AggSelector {
private:
  scribeHandler *scribeHandlerObj_;
  zhandle_t *zh_;
public:
  RandomAggSelector(scribeHandler *scribeHandlerObj, zhandle_t *zh);
  bool selectScribeAggregator(std::string& parentZnode, std::string& remoteHost,
      unsigned long& remotePort);
};

class MsgCounterAggSelector : public AggSelector {
private:
  scribeHandler* scribeHandlerObj_;
  zhandle_t *zh_;
public:
  MsgCounterAggSelector(scribeHandler *scribeHandlerObj, zhandle_t *zh);
  bool selectScribeAggregator(std::string& parentZnode, std::string& remoteHost,
      unsigned long& remotePort);
};

#endif /* ZK_AGG_SELECTOR_H_ */
