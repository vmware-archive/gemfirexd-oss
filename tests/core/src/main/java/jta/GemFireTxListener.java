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
package jta; 

import util.*;
import hydra.*;

import com.gemstone.gemfire.cache.*;

import java.sql.*;

import java.util.*;

/**
 * For use with GemFireTransactionManager in cacheCallback tests where no 
 * TxWriter is installed (so database commit must be done here).
 *
 * @author lhughes
 * @see TransactionListener
 */
public class GemFireTxListener extends util.AbstractListener implements TransactionListener {

/** Called after a successful commit of a transaction: logs and validates event
 *
 * @param event the TransactionEvent
 */
public void afterCommit(TransactionEvent event) {
  logTxEvent("afterCommit", event);

  try {
    Connection conn = GemFireTxCallback.getDBConnection();
    Log.getLogWriter().info("COMMITTING database tx ...");
    conn.commit();
    Log.getLogWriter().info("COMMITTED database tx.");
  } catch (Exception e) {
    throwException("afterCommit() caught " + e);
  }
}

/** Called after an explicit rollback of a transaction: logs and validates event
 *  rollsback database operation.
 *
 * @param event the TransactionEvent
 */
public void afterRollback(TransactionEvent event) {
  logTxEvent("afterRollback", event);

  try {
    Connection conn = GemFireTxCallback.getDBConnection();
    Log.getLogWriter().info("ROLLING BACK database tx ...");
    conn.rollback();
    Log.getLogWriter().info("ROLLED BACK database tx.");
  } catch (Exception e) {
    throwException("afterRollback() caught " + e);
  }
}

/** Called after an unsucessful commit operation: logs and validates event.
 *  rollsback database operation.
 *
 * @param event the TransactionEvent
 */
public void afterFailedCommit(TransactionEvent event) {
  logTxEvent("afterFailedCommit", event);

  try {
    Connection conn = GemFireTxCallback.getDBConnection();
    Log.getLogWriter().info("ROLLING BACK database tx ...");
    conn.rollback();
    Log.getLogWriter().info("ROLLED BACK database tx.");
  } catch (Exception e) {
    throwException("afterFailedCommit() caught " + e);
  }
}

/**  Called when the region containing this callback is destroyed, when the 
 *   cache is closed, or when a callback is removed from a region using an
 *   <code>AttributesMutator</code>.
 */
public void close() {
  Log.getLogWriter().info("TxListener: close()");
}

/** Utility method to log an Exception, place it into a wellknown location in the 
 *  EventBB and throw an exception.
 *
 *  @param errString string to be logged, placed on EventBB and include in
 *         Exception
 *  @throws TestException containing the given string
 *  @see TestHelper.checkForEventError
 */
protected void throwException(String errStr) {
      StringBuffer qualifiedErrStr = new StringBuffer();
      qualifiedErrStr.append("Exception reported in " + RemoteTestModule.getMyClientName() + "\n");
      qualifiedErrStr.append(errStr);
      errStr = qualifiedErrStr.toString();
     
      hydra.blackboard.SharedMap aMap = JtaBB.getBB().getSharedMap();
      aMap.put(TestHelper.EVENT_ERROR_KEY, errStr + " " + TestHelper.getStackTrace());         
      Log.getLogWriter().info(errStr);
      throw new TestException(errStr);
}

}

