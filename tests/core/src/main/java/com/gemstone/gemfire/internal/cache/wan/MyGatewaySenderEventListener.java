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

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.cache.asyncqueue.AsyncEvent;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventListener;

public class MyGatewaySenderEventListener implements
    AsyncEventListener<Object, Object>, Serializable {
  String id = "MyGatewaySenderEventListener";
  /**
   * Creates a latency listener.
   */
  private final Map eventsMap;

  public MyGatewaySenderEventListener() {
    this.eventsMap = new HashMap();
  }

  /**
   * Processes events by recording their latencies.
   */
  public boolean processEvents(List<AsyncEvent<Object, Object>> events) {
    for (AsyncEvent event : events) {
      this.eventsMap.put(event.getKey(), event.getDeserializedValue());
    }
    return true;
  }

  public void close() {
  }

  public Map getEventsMap() {
    return this.eventsMap;
  }

  public void printMap() {
    System.out.println("Printing Map " + this.eventsMap);
  }
  
  @Override
  public boolean equals(Object obj){
    if(this == obj){
      return true;
    }
    if ( !(obj instanceof MyGatewaySenderEventListener) ) return false;
    MyGatewaySenderEventListener listener = (MyGatewaySenderEventListener)obj;
    return this.id.equals(listener.id);
  }
  
  @Override
  public String toString(){
    return id;
  }
}