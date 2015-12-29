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

package com.gemstone.gemfire.tutorial.storage;

import java.util.Properties;

import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;

/**
 * An implementation of GemFire CacheListener that just prints changes to
 * standard out.
 * 
 * This class extends CacheListenerAdapter so that we don't have to
 * implement all of the methods in CacheListener.
 * 
 * It implements Declarable so that it can be used in the GemFire xml configuration
 * file.
 * 
 * @author GemStone Systems, Inc.
 * 
 */
public class LoggingCacheListener extends CacheListenerAdapter implements
    Declarable {

  @Override
  public void afterCreate(EntryEvent event) {
    final String regionName = event.getRegion().getName();
    final Object key = event.getKey();
    final Object newValue = event.getNewValue();
    System.out.println("In region " + regionName + " created key " + key
        + " value " + newValue);
  }

  @Override
  public void afterDestroy(EntryEvent event) {
    final String regionName = event.getRegion().getName();
    final Object key = event.getKey();
    final Object newValue = event.getNewValue();
    System.out.println("In region " + regionName + " destroyed key " + key
        + " value " + newValue);
  }

  @Override
  public void afterUpdate(EntryEvent event) {
    final String regionName = event.getRegion().getName();
    final Object key = event.getKey();
    final Object newValue = event.getNewValue();
    final Object oldValue = event.getOldValue();
    System.out.println("In region " + regionName + " updated key " + key
        + " oldValue " + oldValue + "new value " + newValue);
  }

  public void init(Properties props) {
  }
}
