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

package com.gemstone.gemfire.cache.util;

import com.gemstone.gemfire.cache.CacheWriter;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.RegionEvent;

/**
 * Utility class that implements all methods in <code>CacheWriter</code>
 * with empty implementations. Applications can subclass this class and
 * only override the methods for the events of interest.
 *
 * @author Eric Zoerner
 *
 * @since 3.0
 */
public class CacheWriterAdapter<K,V> implements CacheWriter<K,V> {

  public void beforeCreate(EntryEvent<K,V> event) throws CacheWriterException {
  }

  public void beforeDestroy(EntryEvent<K,V> event) throws CacheWriterException {
  }

  public void beforeRegionDestroy(RegionEvent<K,V> event) throws CacheWriterException {
  }

  public void beforeRegionClear(RegionEvent<K,V> event) throws CacheWriterException {
  }

  public void beforeUpdate(EntryEvent<K,V> event) throws CacheWriterException {
  }

  public void close() {
  }

}
