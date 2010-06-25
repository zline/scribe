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

#include <sstream>
#include <boost/tokenizer.hpp>
#include "common.h"
#include "zk_status.h"
#include "scribe_server.h"

using namespace std;
using boost::lexical_cast;
using boost::shared_ptr;

const string RECEIVED_GOOD_KEY = "received good";
const string RECEIVED_GOOD_RATE_KEY = "received good rate";
const string UPDATE_STATUS_TIMESTAMP_KEY = "update status timestamp";

ZKStatusReader::ZKStatusReader(ZKClient *zkClient)
 : zkClient_(zkClient) {
}

ZKStatusReader::~ZKStatusReader() {
}

void ZKStatusReader::getCountersForAllHosts(
    string& parentZnode, HostCountersMap& _hostCountersMap) {
  ZKClient::HostStatusMap hostStatusMap;
  zkClient_->getAllHostsStatus(parentZnode, &hostStatusMap);

  LOG_DEBUG("getCountersForAllHostsFromZooKeeper");

  typedef boost::tokenizer<boost::char_separator<char> > tokenizer;
  boost::char_separator<char> sep("\n");
  for (ZKClient::HostStatusMap::iterator iter = hostStatusMap.begin();
       iter != hostStatusMap.end(); ++iter) {
    tokenizer tokens(iter->second, sep);
    for (tokenizer::iterator tok_iter = tokens.begin();
         tok_iter != tokens.end(); ++tok_iter) {
      string::size_type pos = tok_iter->find_first_of(":");
      std::string counterName = tok_iter->substr(0, pos);
      std::string counterValue = tok_iter->substr(pos + 1);
      _hostCountersMap[iter->first][counterName] = atoll(counterValue.c_str());
      LOG_DEBUG("set %s %s => %lld", iter->first.c_str(), counterName.c_str(),
                _hostCountersMap[iter->first][counterName]);
    }
  }
}

ZKStatusWriter::ZKStatusWriter(shared_ptr<ZKClient> zkClient,
                               boost::shared_ptr<scribeHandler> scribeHandler)
 : zkClient_(zkClient),
   scribeHandler_(scribeHandler),
   lastWriteTime_(0),
   lastReceivedGood_(0) {
}

ZKStatusWriter::~ZKStatusWriter() {
}

void ZKStatusWriter::updateCounters() {
  time_t now;
  time(&now);
  if (now - lastWriteTime_ < static_cast<int>(scribeHandler_->updateStatusInterval))
    return;
  scribeHandler_->incrementCounter("update counters to zookeeper");

  CounterMap counterMap;
  scribeHandler_->getCounters(counterMap);
  ostringstream allCountersStream;
  for (CounterMap::iterator iter = counterMap.begin();
       iter != counterMap.end(); ++iter) {
    // skip all counters that contain ":" and the
    // RECEIVED_GOOD_RATE_KEY counter 
    if ((iter->first.find(":") == string::npos) &&
        (iter->first.compare(RECEIVED_GOOD_RATE_KEY) != 0)) {
      allCountersStream << iter->first << ":" << iter->second << "\n";
    }
  }
  int64_t receivedGood = scribeHandler_->getCounter(RECEIVED_GOOD_KEY);
  int64_t receivedGoodRate = 0;
  LOG_DEBUG("received good: %lld, lastReceivedGood_: %lld, duration: %ld",
            receivedGood, lastReceivedGood_, (now - lastWriteTime_));
  if (lastReceivedGood_ > 0 && receivedGood > lastReceivedGood_) {
    receivedGoodRate =
        (receivedGood - lastReceivedGood_) / (now - lastWriteTime_);
    if (receivedGoodRate == 0) receivedGoodRate = 1;
  }
  scribeHandler_->setCounter(RECEIVED_GOOD_RATE_KEY, receivedGoodRate);
  lastReceivedGood_ = receivedGood;
  lastWriteTime_ = now;
  // add "received good rate" and "publish timestamp"
  allCountersStream << RECEIVED_GOOD_RATE_KEY << ":" << receivedGoodRate << "\n";
  allCountersStream << UPDATE_STATUS_TIMESTAMP_KEY << ":" << now << "\n";

  string allCountersString = allCountersStream.str();
  zkClient_->updateStatus(allCountersString);
  LOG_DEBUG("writeCountersToZooKeeper: %s", allCountersString.c_str());
}
