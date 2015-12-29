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

package cacheworker;

import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import java.util.Properties;

/**
 * Simple CacheListener that prints caching events to System.out.
 *
 * @author Pivotal Software, Inc.
 * @since 6.6
 */
public class SimpleCacheListener<K,V> extends CacheListenerAdapter<K,V> implements Declarable {

  @Override
  public void afterCreate(EntryEvent<K,V> e) {
    System.out.println(this + " received afterCreate event for entry: " +
      e.getKey() + ", " + e.getNewValue());
  }

  @Override
  public void afterUpdate(EntryEvent<K,V> e) {
    System.out.println(this + " received afterUpdate event for entry: " +
      e.getKey() + ", " + e.getNewValue());
  }

  @Override
  public void afterDestroy(EntryEvent<K,V> e) {
    System.out.println(this + " received afterDestroy event for entry: " +
      e.getKey());
  }

  @Override
  public void afterInvalidate(EntryEvent<K,V> e) {
    System.out.println(this + " received afterInvalidate event for entry: " +
      e.getKey());
  }

  @Override
  public void init(Properties props) {
    System.out.println("Initialized SimpleCacheListener " + this);
  }

  /** Returns the name of this listener. */
  @Override
  public String toString() {
    return getClass().getName();
  }
}
