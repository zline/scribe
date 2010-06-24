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
// @author Wanli Yang <wanli@twitter.com>

#ifndef ZK_STATUS_H
#define ZK_STATUS_H

#ifdef USE_ZOOKEEPER
#include "common.h"

class scribeHandler;
class ZKClient;

// counter name -> value map
typedef std::map<std::string, int64_t> CounterMap;
// host name -> counter map
typedef std::map<std::string, CounterMap> HostCountersMap;

// Read the ZKfiles under a parent znode and return the counters maps for all hosts.
class ZKStatusReader {
 public:
  ZKStatusReader(ZKClient *zkClient);
  virtual ~ZKStatusReader();
  void getCountersForAllHosts(std::string& parentZnode,
                               HostCountersMap& _hostCountersMap);
  
 private:
  ZKClient *zkClient_;
};

// Write counters map into the ZK file of a host
class ZKStatusWriter {
 public:
  ZKStatusWriter(boost::shared_ptr<ZKClient> zkClient,
                 boost::shared_ptr<scribeHandler> scribeHandler,
                 int minUpdateInterval);
  virtual ~ZKStatusWriter();
  void updateCounters();
  
 private:
  boost::shared_ptr<ZKClient> zkClient_;
  boost::shared_ptr<scribeHandler> scribeHandler_;
  int minUpdateInterval_;
  time_t lastWriteTime_;     // in seconds
  int64_t lastReceivedGood_;
};

#endif // USE_ZOOKEEPER
#endif // ZK_STATUS_H
