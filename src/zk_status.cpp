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
// @author Wanli Yang <wanli@twitter.com>

#include "common.h"
#include <boost/tokenizer.hpp>
#include "zk_status.h"
#include "scribe_server.h"

using namespace std;
using boost::lexical_cast;
using boost::shared_ptr;

static string ReceivedGoodStatName = "received good";
static string ReceivedGoodRateStatName = "received good rate";

ZKStatusReader::ZKStatusReader(shared_ptr<ZKClient> zkClient)
 : zkClient_(zkClient) {
}

ZKStatusReader::~ZKStatusReader() {
}

void ZKStatusReader::getCounntersForAllHosts(
    string& parentZnode, HostCountersMap& _hostCountersMap) {
  ZKClient::HostStatusMap hostStatusMap;
  zkClient_->getAllHostsStatus(parentZnode, &hostStatusMap);

  // TODO(wanli): change this to LOG_DEBUG
  LOG_OPER("getCountersForAllHostsFromZooKeeper");

  for (ZKClient::HostStatusMap::iterator iter = hostStatusMap.begin();
       iter != hostStatusMap.end(); ++iter) {
    typedef boost::tokenizer<boost::char_separator<char> > tokenizer;
    boost::char_separator<char> sep("\n");
    tokenizer tokens(iter->second, sep);
    for (tokenizer::iterator tok_iter = tokens.begin();
         tok_iter != tokens.end(); ++tok_iter) {
      string::size_type pos = tok_iter->find_first_of(":");
      std::string counterName = tok_iter->substr(0, pos);
      std::string counterValue = tok_iter->substr(pos + 1);
      _hostCountersMap[iter->first][counterName] = atoll(counterValue.c_str());
      // TODO(wanli): change this to LOG_DEBUG
      LOG_OPER("set %s %s => %lld", iter->first.c_str(), counterName.c_str(),
               _hostCountersMap[iter->first][counterName]);
    }
  }
}

ZKStatusWriter::ZKStatusWriter(shared_ptr<ZKClient> zkClient,
                               scribeHandler *scribeHandler, int minUpdateInterval)
 : zkClient_(zkClient),
   scribeHandler_(scribeHandler),
   minUpdateInterval_(minUpdateInterval),
   lastWriteTime_(0),
   lastReceivedGood_(0) {
}

ZKStatusWriter::~ZKStatusWriter() {
}

void ZKStatusWriter::updateCounters() {
  time_t now;
  time(&now);
  if (now - lastWriteTime_ < minUpdateInterval_)
    return;
  scribeHandler_->incrementCounter("update counters to zookeeper");

  CounterMap counterMap;
  scribeHandler_->getCounters(counterMap);
  string allCountersString;
  char buffer[500];
  for (CounterMap::iterator iter = counterMap.begin();
       iter != counterMap.end(); ++iter) {
    sprintf(buffer, "%lld", iter->second);
    allCountersString += iter->first + ":" + buffer + "\n";
  }
  int64_t receivedGood = scribeHandler_->getCounter(ReceivedGoodStatName);
  int64_t receivedGoodRate = 0;
  if (lastReceivedGood_ > 0 && receivedGood > lastReceivedGood_) {
    receivedGoodRate =
        (receivedGood - lastReceivedGood_) / (now - lastWriteTime_);
  }
  scribeHandler_->setCounter(ReceivedGoodRateStatName, receivedGoodRate);
  lastReceivedGood_ = receivedGood;
  lastWriteTime_ = now;

  LOG_DEBUG("writeCountersToZooKeeper: %s", allCountersString.c_str());
  zkClient_->updateStatus(allCountersString);
}
