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
package cq; 

import hydra.*;
import util.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.query.*;

/** 
 *  Multi(CQ)Listener.
 *
 *  This listener can be assigned a name either at the time of instantiation
 *  or afterwards.  The Listener names are assigned via util.NameFactory.
 *
 *  Upon invocation of the listener callbacks, this listener logs the call
 *  (along with the details of the event) and adds it's assigned name to the
 *  InvokedListeners list in the BB.
 *
 *  @see ListenerBB.InvokedListeners
 *  @see util.NameFactory
 */
public class MultiListener extends util.AbstractListener implements CqListener, Declarable {

private boolean isCarefulValidation;  // serialExecution mode

/* name assigned to this MultiListener, e.g. MultiListener_XX */
private String name = "MultiListener";

/**
 * noArg contstructor 
 */
public MultiListener() {
   this.isCarefulValidation = TestConfig.tab().booleanAt(Prms.serialExecution, true);
}

/**
 *  Create a new listener and specify the assigned name for this listener
 */
public MultiListener(String name) {
   this.name = name;
   this.isCarefulValidation = TestConfig.tab().booleanAt(Prms.serialExecution);
}

/**
 *  Set method for name when instantiated with noArg constructor
 */
public void setName(String name) {
   this.name = name;
}

/**
 *  Get method for name 
 */
public String getName() {
   return this.name;
}

//=============================================================================
//  implementation of CQListener Methods
//=============================================================================

/**
 *  Handles the onEvent : logs call and adds the name of this listener to the 
 *  ListenerBB.InvokedListeners list (String) for the reporting cq
 */
public void onEvent(CqEvent event) {
   try {
      logCQEvent("onEvent", event);
      updateBB(event);
   } catch (Exception e) { 
      String s = "cq.MultiListener.onEvent() caught Exception " + e + " " + TestHelper.getStackTrace(e);
      throwException(s); 
   }
}

/**
 *  Handles the onError : throws Exception and writes to ListenerBB blackboard
 */
public void onError(CqEvent event) {
   try {
      logCQEvent("onError", event);
   } catch (Exception e) {
      String s = "cq.MultiListener.onError() caught Exception for key = " + event.getKey() + " and throwable " + event.getThrowable().toString() + " " + TestHelper.getStackTrace(e);
      throwException(s);
   }
}

public void close() {
   logCall("close", null);
}

/**
 *  Utility method to update the InvokedListeners list in the BB with the 
 *  name of this listener.
 */
private void updateBB(CqEvent event) {

  // We can only track the Listeners invoked in serialExecution mode
  if (!isCarefulValidation) {
     return;
  }

  // We only count the events in the targetVm (the VM performing the  
  // listener and entry operations
  if (ListenerTest.targetVm) {
     String key = ListenerBB.InvokedListeners + "_" + event.getCq().getName();
     String invokedList = (String)ListenerBB.getBB().getSharedMap().get(key);
     StringBuffer invoked = new StringBuffer(invokedList);
     invoked.append(this.name + ":");
     ListenerBB.getBB().getSharedMap().put(key, invoked.toString());
     ListenerBB.getBB().getSharedCounters().increment(ListenerBB.NUM_LISTENER_INVOCATIONS);
  }
}

/**
 * Utility method to write an Exception string to the Event Blackboard and
 * to also throw an exception containing the same string.
 *
 * @param errStr String to log, post to EventBB and throw
 * @throws TestException containing the passed in String
 *
 * @see util.TestHelper.checkForEventError
 */
protected void throwException(String errStr) {
   hydra.blackboard.SharedMap aMap = ListenerBB.getBB().getSharedMap();
   aMap.put(TestHelper.EVENT_ERROR_KEY, errStr);
   Log.getLogWriter().info(errStr);
   throw new TestException(errStr);
}



public void init(java.util.Properties prop) {
   logCall("init(Properties)", null);
}

}
