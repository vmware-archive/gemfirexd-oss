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
package wan;

import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.EntryEvent;

import com.gemstone.gemfire.cache.util.CacheListenerAdapter;

import java.util.Properties;

/**
 * Class <code>WANCacheListener</code> is a <code>CacheListener</code> that logs
 * WAN events as they are received.
 */
public class WANCacheListener<K, V> extends CacheListenerAdapter<K, V> implements Declarable {

  /** Counter tracking the number of events received */
  protected int _numberOfEvents;

  /**
   * Processes an afterCreate event.
   * @param event The afterCreate <code>EntryEvent</code> received
   */
  @Override
  public void afterCreate(EntryEvent<K, V> event) {
    processEvent("afterCreate", event);
  }

  /**
   * Processes an afterUpdate event.
   * @param event The afterUpdate <code>EntryEvent</code> received
   */
  @Override
  public void afterUpdate(EntryEvent<K, V> event) {
    processEvent("afterUpdate", event);
  }

  /**
   * Processes an afterDestroy event.
   * @param event The afterDestroy <code>EntryEvent</code> received
   */
  @Override
  public void afterDestroy(EntryEvent<K, V> event) {
    processEvent("afterDestroy", event);
  }

  /**
   * Processes an afterInvalidate event.
   * @param event The afterInvalidate <code>EntryEvent</code> received
   */
  @Override
  public void afterInvalidate(EntryEvent<K, V> event) {
    processEvent("afterInvalidate", event);
  }

  /**
   * Initializes this <code>WANCacheListener</code>.
   * @param p The <code>Properties</code> with which to initialize this
   * <code>WANCacheListener</code>
   */
  public void init(Properties p) {
  }

  /**
   * Processes an event by incrementing the number of events received and logging
   * the event's key and new value.
   * @param operation The <code>EntryEvent</code>'s operation (afterCreate, afterUpdate,
   * afterDestroy, afterInvalidate)
   * @param event The <code>EntryEvent</code> received
   */
  protected void processEvent(String operation, EntryEvent<K, V> event) {
    System.out.println("WANCacheListener received " + operation + " event (" + ++_numberOfEvents + ") for region " + event.getRegion().getName() + ": " + event.getKey() + "->" + event.getNewValue());
  }
}
