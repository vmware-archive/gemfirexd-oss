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
package management.operations.events.impl;

import static management.util.HydraUtil.logInfo;

import java.util.List;
import java.util.Map;

import management.operations.OperationEvent;
import management.operations.OperationPrms;
import management.operations.events.FunctionOperationEvents;

public class FunctionEvents extends AbstractEvents implements FunctionOperationEvents {

  public FunctionEvents(Map<String, List<OperationEvent>> eventMap) {
    super(eventMap);
    // TODO Auto-generated constructor stub
  }

  public FunctionEvents() {
    
  }

  @Override
  public void functionRegistered(String id) {
    if (OperationPrms.recordFunctionOps()) {
      OperationEvent event = new OperationEvent();
      event.name = EVENT_FUNCT_REGISTER;
      event.data = id;
      addEvent(event);
    }
    logInfo("FunctionOperations: Function registration completed for id : " + id);
  }

  @Override
  public void functionUnregistered(String id) {
    if (OperationPrms.recordFunctionOps()) {
      OperationEvent event = new OperationEvent();
      event.name = EVENT_FUNCT_UNREGISTER;
      event.data = id;
      addEvent(event);
    }
    logInfo("FunctionOperations: Function un-registration completed for id : " + id);
  }

  @Override
  public void functionExecuted(String id, Object result) {
    if (OperationPrms.recordFunctionOps()) {
      OperationEvent event = new OperationEvent();
      event.name = EVENT_FUNCT_EXEC;
      event.data = new Object[] { id, result };
      addEvent(event);
    }
    logInfo("FunctionOperations: Function execution completed for id : " + id);
  }
  
  public static List<String> getAllRegisteredFunction(){
    List list = AbstractEvents.getUniqueEventsAcrossAll(FunctionOperationEvents.EVENT_FUNCT_REGISTER);
    return list;
  }
  

}
