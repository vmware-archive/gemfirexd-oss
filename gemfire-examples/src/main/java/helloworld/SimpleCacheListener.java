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
package helloworld;

import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.RegionEvent;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import java.util.Properties;

public class SimpleCacheListener<K,V> extends CacheListenerAdapter<K,V> implements Declarable {

  public void afterCreate(EntryEvent<K,V> e) {
    System.out.println("    Received afterCreate event for entry: " +
      e.getKey() + ", " + e.getNewValue());
  }
  
  public void afterUpdate(EntryEvent<K,V> e) {
    System.out.println("    Received afterUpdate event for entry: " +
      e.getKey() + ", " + e.getNewValue());
  }
  
  public void afterDestroy(EntryEvent<K,V> e) {
    System.out.println("    Received afterDestroy event for entry: " +
      e.getKey());
  }

  public void afterInvalidate(EntryEvent<K,V> e) {
    System.out.println("    Received afterInvalidate event for entry: " +
      e.getKey());
  }

  public void afterRegionLive(RegionEvent e) {
    System.out.println("    Received afterRegionLive event, sent to durable clients after \nthe server has finished replaying stored events.  ");
  }

  public void init(Properties props) {
    // do nothing
  }

}

