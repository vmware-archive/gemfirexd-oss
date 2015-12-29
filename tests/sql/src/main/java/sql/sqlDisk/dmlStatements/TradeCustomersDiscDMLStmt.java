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
import hydra.MasterController;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sql.SQLHelper;
import sql.SQLTest;
import sql.dmlStatements.TradeCustomersDMLStmt;
import util.TestException;
import util.TestHelper;

public class TradeCustomersDiscDMLStmt extends TradeCustomersDMLStmt {

  public void insert(Connection dConn, Connection gConn, int size) {
    int[] cid = new int[size];
    String[] cust_name = new String[size];
    Date[] since = new Date[size];
    String[] addr = new String[size];
    List<SQLException> exList = new ArrayList<SQLException>();
    boolean success = false;
    getDataForInsert(cid, cust_name,since,addr, size); //get the data
    boolean[] gfxdOp = new boolean[1];
    gfxdOp[0] = true;

    if (dConn != null) {
      try {
          success = insertToDerbyTable(dConn, cid, cust_name,since, addr, size, exList);  //insert to derby table 
        int count = 0;
        while (!success) {
          if (count>=maxNumOfTries) {
            Log.getLogWriter().info("Could not get the lock to finish insert into derby, abort this operation");
            return;
          }
          MasterController.sleepForMs(rand.nextInt(retrySleepMs));
          exList .clear();
          success = insertToDerbyTable(dConn, cid, cust_name,since, addr, size, exList);  //retry insert to derby table                   
          count++;
        }

        insertToGFETable(gConn, cid, cust_name,since, addr, size, exList, gfxdOp);    //insert to gfe table 
        
        if (SQLTest.setTx && isHATest) { 
          commit(gConn); //commit gfxd first, so that if it failed we can roll back derby op

          if (!gfxdOp[0])
            rollback(dConn); //gemfirexd encounters Offline exception
          else
            commit(dConn);
          
        } else {    
          if (!gfxdOp[0])
            dConn.rollback(); //gemfirexd encounters Offline exception
          else
            dConn.commit(); //to commit and avoid rollback the successful operations
          
          gConn.commit(); //auto committed anyway
        }  
      } catch (SQLException se) {
        SQLHelper.printSQLException(se);
        throw new TestException ("insert to trade.customers fails\n" + TestHelper.getStackTrace(se));
      }  //for verification

    }
    else {
      try {
        insertToGFETable(gConn, cid, cust_name,since, addr, size);    //insert to gfe table 
      } catch (SQLException se) {
        if (isOfflineTest && (se.getSQLState().equals("X0Z09") || se.getSQLState().equals("X0Z08"))) {
          Log.getLogWriter().info("got expected PartitionOfflineException, continuing testing");
          return;
        }
        SQLHelper.printSQLException(se);
        throw new TestException ("insert to gfe trade.customers fails\n" + TestHelper.getStackTrace(se));
      }
    } //no verification for gfxd only case or user writer -- no writer invocation when exception is thrown
    
    //after bug fixed for existing tests, we may change this to apply for all tests 
    if (hasNetworth) {
      //also insert into networth table
      Log.getLogWriter().info("inserting into networth table");
      TradeNetworthDiscDMLStmt networth = new TradeNetworthDiscDMLStmt();
      networth.insert(dConn, gConn, size, cid);
      try {
        if (!SQLTest.setTx) {
          if (dConn!=null)        dConn.commit(); //to commit and avoid rollback the successful operations
          gConn.commit();
        } else {
          commit(gConn);
          if (dConn!=null) commit(dConn); //to see if commit in gfxd failed so derby op could be rolled back.
        }
      } catch (SQLException se) {
        SQLHelper.handleSQLException(se);
      }
    }    
  }
  
  //for gemfirexd checking
  protected void insertToGFETable(Connection conn, int[] cid, String[] cust_name,
      Date[] since, String[] addr, int size, List<SQLException> exceptions, 
      boolean[] success) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement(insert);
    int tid = getMyTid();
    int count = -1;
    Log.getLogWriter().info("Insert into gemfirexd, myTid is " + tid);
    for (int i=0 ; i<size ; i++) {
      try {
        count = insertToTable(stmt, cid[i], cust_name[i],since[i], addr[i], tid); 
        if (count != ((Integer)verifyRowCount.get(tid+"_insert"+i)).intValue()) {
          Log.getLogWriter().info("Gfxd insert has different row count from that of derby " +
            "derby inserted " + ((Integer)verifyRowCount.get(tid+"_insert"+i)).intValue() +
            " but gfxd inserted " + count);
        }
      } catch (SQLException se) {
        if (isOfflineTest && (se.getSQLState().equals("X0Z09") || se.getSQLState().equals("X0Z08"))) {
          Log.getLogWriter().info("got expected PartitionOfflineException, continuing testing");
          success[0] = false;
        } else SQLHelper.handleGFGFXDException(se, exceptions);  
      }
    }
  }
  
  public void update(Connection dConn, Connection gConn, int size){   
    try {
      super.update(dConn, gConn, size);
    } catch (TestException te) {
      if (isOfflineTest && (te.getMessage().contains("X0Z09") || te.getMessage().contains("X0Z08"))) {
        Log.getLogWriter().info("got expected PartitionOfflineException, continuing testing");
        if (dConn != null) rollback(dConn);
      } else throw te;
    }
  }
  
  public void delete(Connection dConn, Connection gConn){
    try {
      super.delete(dConn, gConn);
    } catch (TestException te) {
      if (isOfflineTest && (te.getMessage().contains("X0Z09") || te.getMessage().contains("X0Z08"))) {
        Log.getLogWriter().info("got expected PartitionOfflineException, continuing testing");
        if (dConn != null) rollback(dConn);
      }  else if (isOfflineTest && !SQLTest.syncHAForOfflineTest && te.getMessage().contains("23503")) {
        //handle non sync HA case
        Log.getLogWriter().info("delete failed due to #43754, continuing testing");
        if (dConn != null) rollback(dConn);
      } else throw te;
    }
  }
  
  public void query(Connection dConn, Connection gConn){
    super.query(dConn, gConn);
  } 

}
