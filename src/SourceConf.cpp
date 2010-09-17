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

#include "SourceConf.h"
#include <boost/foreach.hpp>

using boost::property_tree::ptree;
using namespace std;

SourceConf::SourceConf() {}

SourceConf::~SourceConf() {}

void SourceConf::load(const string &filename) {
  LOG_DEBUG("Loading source configuration from <%s>", filename.c_str());
  read_xml(filename, pt);
}

void SourceConf::getAllSources(vector<ptree>& _return) {
  BOOST_FOREACH(ptree::value_type& node, pt.get_child("sources")) {
    _return.push_back(node.second);
  }
}
