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

import java.util.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.naming.Context;
import javax.sql.DataSource;
import javax.transaction.UserTransaction;
import javax.transaction.RollbackException;

import com.gemstone.gemfire.cache.*;

import hydra.*;
import util.*;

/** CacheWriter to write entries to derby database
 */
public class GemFireDBWriter extends util.AbstractWriter implements CacheWriter {

  /** implement CacheWriter interface */
  public void beforeUpdate(EntryEvent event) throws CacheWriterException {
    logCall("beforeUpdate", event);

    String key = (String) event.getKey();
    String newValue = (String) event.getNewValue();
    String oldValue = (String) event.getOldValue();
        
    String tableName = (String)JtaBB.getBB().getSharedMap().get(JtaBB.dbTableName);
    try { 
      Connection conn = GemFireTxCallback.getDBConnection();
      String sql = "UPDATE " + tableName + " SET name = '" + newValue + "' WHERE id = ('" + key + "')";
      int i = DBUtil.executeUpdate(sql,conn);
      Log.getLogWriter().info("rows updated = " + i);
    } catch(Exception e) {
      Log.getLogWriter().info("GemFireDBWriter.beforeUpdate() caught " + e);
      throw new CacheWriterException("GemFireDBWriter.beforeUpdate() caught " + e);
    }    
  }

  public void beforeCreate(EntryEvent event) throws CacheWriterException {
    logCall("beforeCreate", event);

    // Don't process the LOCAL_LOAD_CREATES which occur when we 
    // load the cache from the database
    Operation op = event.getOperation();
    if (op.equals(Operation.LOCAL_LOAD_CREATE)) {
      return;
    }

    String key = (String) event.getKey();
    String newValue = (String) event.getNewValue();
    String oldValue = (String) event.getOldValue();

    String tableName = (String)JtaBB.getBB().getSharedMap().get(JtaBB.dbTableName);
    try { 
      Connection conn = GemFireTxCallback.getDBConnection();
      String sql = "UPDATE " + tableName + " SET name = '" + newValue + "' WHERE id = ('" + key + "')";
      int i = DBUtil.executeUpdate(sql,conn);
      Log.getLogWriter().info("rows updated for create = " + i);
    } catch(Exception e) {
      Log.getLogWriter().info("GemFireDBWriter.beforeCreate() caught " + TestHelper.getStackTrace(e));
      throw new CacheWriterException("GemFireDBWriter.beforeCreate() caught " + TestHelper.getStackTrace(e));
    }    
  }

  public void beforeDestroy(EntryEvent event) throws CacheWriterException {
    logCall("beforeDestroy", event);
    String key = (String) event.getKey();
    String newValue = (String) event.getNewValue();
    String oldValue = (String) event.getOldValue();
        
    String tableName = (String)JtaBB.getBB().getSharedMap().get(JtaBB.dbTableName);
    try {
      Connection conn = GemFireTxCallback.getDBConnection();
      String sql = "DELETE FROM " + tableName + " WHERE id = ('" + key + "')";
      int i = DBUtil.executeUpdate(sql, conn);
      Log.getLogWriter().info("rows destroyed = " + i);
    } catch (Exception e) {
      Log.getLogWriter().info("GemFireDBWriter.beforeDestroy() caught " + TestHelper.getStackTrace(e));
      throw new CacheWriterException("GemFireDBWriter.beforeDestroy() caught " + TestHelper.getStackTrace(e));
    }
  }

  public void beforeRegionDestroy(RegionEvent event) throws CacheWriterException {}
  public void beforeRegionClear(RegionEvent event) throws CacheWriterException {}
  public void close() {}

}
  
