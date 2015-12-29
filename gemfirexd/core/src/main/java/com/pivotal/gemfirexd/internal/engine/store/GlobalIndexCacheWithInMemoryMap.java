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

package com.pivotal.gemfirexd.internal.engine.store;

import java.util.HashMap;

import com.gemstone.gemfire.internal.cache.CacheMap;

/**
 * @author kneeraj
 *
 */
public class GlobalIndexCacheWithInMemoryMap implements CacheMap {

  private final HashMap<Object, Object> cache; 
  
  public GlobalIndexCacheWithInMemoryMap() {
    cache = new HashMap<Object, Object>();
  }

  @Override
  public int size() {
    return cache.size();
  }

  @Override
  public boolean isEmpty() {
    return cache.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return cache.containsKey(key);
  }

  @Override
  public Object get(Object key) {
    return cache.get(key);
  }

  @Override
  public Object put(Object key, Object value) {
    return cache.put(key, value);
  }

  @Override
  public Object remove(Object key) {
    return cache.remove(key);
  }

  @Override
  public void clear() {
    cache.clear();
  }

  @Override
  public void destroyCache() {
    cache.clear();
  }
}
