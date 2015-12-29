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
import management.operations.events.CQAndIndexOperationEvents;

public class CQAndIndexEvents extends AbstractEvents implements CQAndIndexOperationEvents {

  public CQAndIndexEvents(Map<String, List<OperationEvent>> eventMap) {
    super(eventMap);
    // TODO Auto-generated constructor stub
  }

  public CQAndIndexEvents(){
    
  }
  
  @Override
  public void cqCreated(String name, String query, String listeners) {
    if (OperationPrms.recordCqIndexOps()) {
      OperationEvent event = new OperationEvent();
      event.name = EVENT_CQ_CREATED;
      event.data = new Object[] { name, query };
      addEvent(event);
    }
    logInfo("CQIndexOperations: Cq create complete");
  }

  @Override
  public void cqStopped(String name, String query) {
    if (OperationPrms.recordCqIndexOps()) {
      OperationEvent event = new OperationEvent();
      event.name = EVENT_CQ_STOP;
      event.data = new Object[] { name, query };
      addEvent(event);
    }
    logInfo("CQIndexOperations: Cq stop complete");
  }

  @Override
  public void indexCreated(String name, String expression, String fromClause) {
    if (OperationPrms.recordCqIndexOps()) {
      OperationEvent event = new OperationEvent();
      event.name = EVENT_INDEX_CREATED;
      event.data = new Object[] { name, expression, fromClause };
      addEvent(event);
    }
    logInfo("CQIndexOperations: Index create complete");
  }

  @Override
  public void indexRemoved(String name) {
    if (OperationPrms.recordCqIndexOps()) {
      OperationEvent event = new OperationEvent();
      event.name = EVENT_INDEX_REMOVED;
      event.data = new Object[] { name };
      addEvent(event);
    }
    logInfo("CQIndexOperations: Index create complete");
  }

}
