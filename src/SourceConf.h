/**
 * Copyright 2010 Twitter
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * @author Travis Crawford <travis@twitter.com>
 */

#ifndef SCRIBE_SOURCE_CONF_H
#define SCRIBE_SOURCE_CONF_H

#include "common.h"

class SourceConf {
 public:
  SourceConf();
  virtual ~SourceConf();
  void load(const std::string& filename);
  void getAllSources(std::vector<boost::property_tree::ptree>& _return);
  boost::property_tree::ptree pt;
};

#endif //!defined SCRIBE_SOURCE_CONF_H
