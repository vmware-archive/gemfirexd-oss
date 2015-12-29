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
package sql.sqlDisk.dmlStatements;

import hydra.Log;

import java.sql.Connection;

import sql.SQLTest;
import sql.dmlStatements.TradeBuyOrdersDMLStmt;
import util.TestException;

public class TradeBuyOrdersDiscDMLStmt extends TradeBuyOrdersDMLStmt {
  public void insert(Connection dConn, Connection gConn, int size) {
    try {
      super.insert(dConn, gConn, size);
    } catch (TestException te) {
      if (isOfflineTest && (te.getMessage().contains("X0Z09") || te.getMessage().contains("X0Z08"))) {
        Log.getLogWriter().info("got expected PartitionOfflineException, continuing testing");
        if (dConn != null) rollback(dConn);
      } else if (isOfflineTest && !SQLTest.syncHAForOfflineTest && te.getMessage().contains("23503")) {
        //handle non sync HA case
        Log.getLogWriter().info("insert failed due to #43754, continuing testing");
        if (dConn != null) rollback(dConn);
      } else throw te;

    }
  }
  
  public void update(Connection dConn, Connection gConn, int size){   
    try {
      super.update(dConn, gConn, size);
    } catch (TestException te) {
      if (isOfflineTest && (te.getMessage().contains("X0Z09") || te.getMessage().contains("X0Z08"))) {
        Log.getLogWriter().info("got expected PartitionOfflineException, continuing testing");
        if (dConn != null) rollback(dConn);
      } else if (isOfflineTest && !SQLTest.syncHAForOfflineTest && te.getMessage().contains("23503")) {
        //handle non sync HA case, in certain condition this could happen when #43754 was
        //ignored. e.g. update with fk(sid) of 0, and the row was supposedly not inserted due to #43754
        Log.getLogWriter().info("insert failed due to #43754, continuing testing");
        if (dConn != null) rollback(dConn);
      }  else throw te;
    }
  }
  
  public void delete(Connection dConn, Connection gConn){
    try {
      super.delete(dConn, gConn);
    } catch (TestException te) {
      if (isOfflineTest && (te.getMessage().contains("X0Z09") || te.getMessage().contains("X0Z08"))) {
        Log.getLogWriter().info("got expected PartitionOfflineException, continuing testing");
        if (dConn != null) rollback(dConn);
      } else throw te;
    }
  }
  
  public void query(Connection dConn, Connection gConn){
    super.query(dConn, gConn);
  }
}
