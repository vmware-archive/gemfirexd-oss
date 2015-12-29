/*
 * Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

/**
 *  Created on: 7 Jun 2014
 *      Author: swale
 */

#ifndef CLIENTBASEIMPL_H_
#define CLIENTBASEIMPL_H_

#include "ClientBase.h"

/*
#ifdef _WINDOWS
template<typename K, typename V>
struct hash_map
{
  typedef std::map<K, V> type;
};
#else
#include <tr1/unordered_map>

template<typename K, typename V>
struct hash_map
{
  typedef std::tr1::unordered_map<K, V> type;
};
#endif
*/
#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>

template<typename K, typename V>
struct hash_map {
  typedef boost::unordered_map<K, V> type;
};

template<typename K>
struct hash_set {
  typedef boost::unordered_set<K> type;
};

#endif /* CLIENTBASEIMPL_H_ */
