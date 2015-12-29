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
package sql.dmlStatements.writer;

import hydra.Log;

import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;

import sql.SQLBB;
import sql.SQLHelper;
import sql.SQLTest;
import sql.dmlStatements.TradeCustomersDMLStmt;
import util.TestException;
import util.TestHelper;

public class TradeCustomersWriterDMLStmt extends TradeCustomersDMLStmt {
  public void insert(Connection dConn, Connection gConn, int size) {
    int[] cid = new int[size];
    String[] cust_name = new String[size];
    Date[] since = new Date[size];
    String[] addr = new String[size];
    getDataForInsert(cid, cust_name,since,addr, size); //get the data

    try {
      insertToGFETable(gConn, cid, cust_name,since, addr, size);    //insert to gfe table 
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);      
    }
       
    if (hasNetworth) {
      Log.getLogWriter().info("inserting into networth table");
      TradeNetworthWriterDMLStmt networth = new TradeNetworthWriterDMLStmt();
      networth.insert(null, gConn, size, cid);
      try {
        gConn.commit();
      } catch (SQLException se) {
        SQLHelper.handleSQLException(se);
      }
    }    
  }
  
  public void update(Connection dConn, Connection gConn, int size){   
    int[] cid = new int[size];
    int[] newCid = new int[size];
    String[] cust_name = new String[size];
    Date[] since = new Date[size];
    String[] addr = new String[size];     
    int[]  whichUpdate = new int[size];
    
    getDataForUpdate(gConn, newCid, cid, cust_name, since, addr, whichUpdate, size); //get the data
    updateGFETable(gConn, newCid,  cid, cust_name, since, addr, whichUpdate, size);  
  }
  
  public void delete(Connection dConn, Connection gConn){
    String cust_name = "name" + rand.nextInt((int)SQLBB.getBB().getSharedCounters().read(SQLBB.tradeCustomersPrimary));
    
    int cid = getCid(gConn); 
    int numOfNonUniqDelete = 1;  //how many delete statement is for non unique keys    
    int whichDelete = getWhichOne(numOfNonUniqDelete, delete.length);    

    deleteFromGFETable(gConn, cid, cust_name, whichDelete);
  }
}
