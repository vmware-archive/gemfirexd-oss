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
import management.operations.events.TransactionOperationEvents;

public class TransactionEvents extends AbstractEvents implements TransactionOperationEvents {

  public TransactionEvents(Map<String, List<OperationEvent>> eventMap) {
    super(eventMap);
    // TODO Auto-generated constructor stub
  }

  @Override
  public void transactionStarted() {
    if (OperationPrms.recordTxOperations()) {
      OperationEvent event = new OperationEvent();
      event.name = EVENT_TX_STARTED;
      addEvent(event);
    }
    logInfo("TransactionOperations: Started");
  }

  @Override
  public void transactionCommited() {
    if (OperationPrms.recordTxOperations()) {
      OperationEvent event = new OperationEvent();
      event.name = EVENT_TX_COMMITED;
      addEvent(event);
    }
    logInfo("TransactionOperations: Commited Successfully");
  }

  @Override
  public void transactionRolledback() {
    if (OperationPrms.recordTxOperations()) {
      OperationEvent event = new OperationEvent();
      event.name = EVENT_TX_ROLLEDBACK;
      addEvent(event);
    }
    logInfo("TransactionOperations: Rolledback Successfully");
  }

}
