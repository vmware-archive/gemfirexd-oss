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
 * For use with GemFireTransactionManager in multi VM cacheCallback tests.
 * This TransactionListener executes the updates to the database AND commits.
 *
 * @author lhughes
 * @see TransactionListener
 */
public class GemFireTxDBListener extends util.AbstractListener implements TransactionListener {

static String tableName = null;  // obtained from BB (unique oracle db name) for this test run

/** Called after a successful commit of a transaction: logs and validates event
 *
 * @param event the TransactionEvent
 */
public void afterCommit(TransactionEvent txEvent) {
  logTxEvent("afterCommit", txEvent);

  tableName = (String)JtaBB.getBB().getSharedMap().get(JtaBB.dbTableName);
  try {
    Connection conn = GemFireTxCallback.getDBConnection();

    List txEventList = txEvent.getEvents();
  
    // update database for all ops in TxEvent
    for (Iterator it = txEventList.iterator(); it.hasNext();) {
      EntryEvent event = (EntryEvent)it.next();
 
      com.gemstone.gemfire.cache.Operation eventOp = event.getOperation();
      if (eventOp.isCreate()) {
        afterCreate(event, conn);
      } else if (eventOp.isUpdate()) {
        afterUpdate(event, conn);
      } else if (eventOp.isDestroy()) {
        afterDestroy(event, conn);
      } else if (eventOp.isInvalidate()) {
        afterInvalidate(event, conn);
      }
    }

    // commit 
    Log.getLogWriter().info("COMMITTING database tx ...");
    conn.commit();
    Log.getLogWriter().info("COMMITTED database tx.");
  } catch (Exception e) {
    throwException("afterCommit() caught " + e);
  }
}

  public void afterCreate(EntryEvent event, Connection conn) throws CacheWriterException {
    String key = (String) event.getKey();
    String newValue = (String) event.getNewValue();

    try {
      String sql = "INSERT INTO " + tableName + " VALUES ('" + key + "','" + newValue + "')";

      int i = DBUtil.executeUpdate(sql,conn);
      Log.getLogWriter().info("rows updated for create = " + i);
    } catch(Exception e) {
      Log.getLogWriter().info("GemFireTxDBListener.afterCreate() caught " + TestHelper.getStackTrace(e));
      throw new CacheWriterException("GemFireTxDBListener.afterCreate() caught " + TestHelper.getStackTrace(e));
    }
  }

  public void afterUpdate(EntryEvent event, Connection conn) throws CacheWriterException {
    String key = (String) event.getKey();
    String newValue = (String) event.getNewValue();

    try {
      String sql = "UPDATE " + tableName + " SET name = '" + newValue + "' WHERE id = ('" + key + "')";
      int i = DBUtil.executeUpdate(sql,conn);
      Log.getLogWriter().info("rows updated = " + i);
    } catch(Exception e) {
      Log.getLogWriter().info("GemFireTxDBListener.afterUpdate() caught " + e);
      throw new CacheWriterException("GemFireTxDBListener.afterUpdate() caught " + e);
    }
  }

  public void afterDestroy(EntryEvent event, Connection conn) throws CacheWriterException {
    String key = (String) event.getKey();
    String sql = "DELETE FROM " + tableName + " WHERE id = ('" + key + "')";

    try {
      int i = DBUtil.executeUpdate(sql, conn);
      Log.getLogWriter().info("rows destroyed = " + i);
    } catch (Exception e) {
      Log.getLogWriter().info("GemFireTxDBListener.afterDestroy() caught " + TestHelper.getStackTrace(e));
      throw new CacheWriterException("GemFireTxDBListener.afterDestroy() caught " + TestHelper.getStackTrace(e));
    }
  }

/** An invalidate in the cache doesn't really need to operation on the database.
 *  It just means the next time the application attempts to access this entry
 *  through the Cache, it should do use the loader to get the value from the
 *  database.
 */
  public void afterInvalidate(EntryEvent event, Connection conn) throws CacheWriterException {
    String key = (String) event.getKey();
    String newValue = (String) event.getNewValue();
  }

/** Called after an explicit rollback of a transaction.
 *
 * @param event the TransactionEvent
 */
public void afterRollback(TransactionEvent event) {
  logTxEvent("afterRollback", event);

}

/** Called after an unsucessful commit operation: logs and validates event.
 *  rollsback database operation.
 *
 * @param event the TransactionEvent
 */
public void afterFailedCommit(TransactionEvent event) {
  logTxEvent("afterFailedCommit", event);
}

/**  Called when the region containing this callback is destroyed, when the 
 *   cache is closed, or when a callback is removed from a region using an
 *   <code>AttributesMutator</code>.
 */
public void close() {
  Log.getLogWriter().info("TxDBListener: close()");
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

