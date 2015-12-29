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
package com.gemstone.gemfire.internal.cache.wan;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.gemstone.gemfire.cache.asyncqueue.AsyncEvent;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventListener;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;

public class MyAsyncEventListener implements AsyncEventListener<Object, Object> {

  private final Map eventsMap;

  public MyAsyncEventListener() {
    this.eventsMap = new ConcurrentHashMap();
  }

  public boolean processEvents(List<AsyncEvent<Object, Object>> events) {
    for (AsyncEvent event : events) {
      if(this.eventsMap.containsKey(event.getKey())) {
        InternalDistributedSystem.getConnectedInstance().getLogWriter().fine("This is a duplicate event --> " + event.getKey());
      }
      this.eventsMap.put(event.getKey(), event.getDeserializedValue());
      
      InternalDistributedSystem.getConnectedInstance().getLogWriter().fine("Received an event --> " + event.getKey());
    }
    return true;
  }

  public Map getEventsMap() {
    return eventsMap;
  }

  public void close() {
  }
}