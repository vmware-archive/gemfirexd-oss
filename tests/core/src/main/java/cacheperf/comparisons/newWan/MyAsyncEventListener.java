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
package cacheperf.comparisons.newWan; 

import hydra.Log;
import hydra.ProcessMgr;
import hydra.RemoteTestModule;

import java.util.List;

import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEvent;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventListener;

/** MyAsyncEventListener (AsyncEventListener)
 */
public class MyAsyncEventListener implements AsyncEventListener<Object, Object>, Declarable {

/** The process ID of the VM that created this listener */
public int whereIWasRegistered;

/** noArg constructor 
 */
public MyAsyncEventListener() {
   whereIWasRegistered = ProcessMgr.getProcessId();   
}

/**
 * Counts events based on operation type
 */
public boolean processEvents(List<AsyncEvent<Object, Object>> events) {
    //just throw it away
    return true;
}

public void init(java.util.Properties prop) {
   logCall("init(Properties)", null);
}

public void close() {
   logCall("close", null);
}

/** Log that a gateway event occurred.
 *
 *  @param event The event object that was passed to the event.
 */
public String logCall(String methodName, AsyncEvent event) {
   String aStr = toString(methodName, event);
   Log.getLogWriter().info(aStr);
   return aStr;
}


/** Return a string description of the GatewayEvent.
 *
 *  @param event The AsyncEvent object that was passed to the CqListener
 *
 *  @return A String description of the invoked GatewayEvent
 */
public String toString(String methodName, AsyncEvent event) {
   StringBuffer aStr = new StringBuffer();

   aStr.append("Invoked " + this.getClass().getName() + ": " + methodName + " in " + RemoteTestModule.getMyClientName());
   aStr.append(", whereIWasRegistered: " + whereIWasRegistered);

   if (event == null) {
     return aStr.toString();
   }
   aStr.append(", Event:" + event);
   return aStr.toString();
}  
}
