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

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Timestamp;

import sql.SQLBB;
import sql.dmlStatements.TradeTxHistoryDMLStmt;

public class TradeTxHistoryWriterDMLStmt extends TradeTxHistoryDMLStmt {
  public void insert(Connection dConn, Connection gConn, int size) {
    insert(dConn, gConn, size, false);
  }
  public void insert(Connection dConn, Connection gConn, int size, boolean isPut) {
    int[] cid = new int[size];
    int[] oid = new int[size];
    int[] sid = new int[size];
    int[] qty = new int[size];
    String[] type = new String[size];
    Timestamp[] time = new Timestamp[size];
    BigDecimal[] price = new BigDecimal[size];

    if (getDataForInsert(gConn, oid, cid, sid, qty, type, time, price, size))//get the data
    	insertToGFETable(gConn, oid, cid, sid, qty, type, time, price, size, isPut);
  }

  public void put(Connection dConn, Connection gConn, int size) {
    insert(dConn, gConn, size, true);
  }
  
  public void update(Connection dConn, Connection gConn, int size) {
    int[] sid = new int[size];
    int[] cid = new int[size];
    int[] oid = new int[size];
    String[] type = new String[size];
    int[] qty = new int[size];    
    int[]  whichUpdate = new int[size];

    if (getDataForUpdate(gConn, cid, sid, oid, qty, type, whichUpdate, size))
    	updateGFETable(gConn, cid, sid, oid, qty, type, whichUpdate, size);
  }
  
  public void delete(Connection dConn, Connection gConn) {
    int numOfNonUniqDelete = delete.length/2;  //how many delete statement is for non unique keys
    int whichDelete = getWhichOne(numOfNonUniqDelete, delete.length);
    int size = 2; //how many delete to be completed in this delete operation
    int[] cid = new int[size]; 
    int[] sid = new int[size];
    int[] oid = new int[size];
    int[] qty = new int[size];
    Timestamp[] time = new Timestamp[size]; 
    BigDecimal[] price = new BigDecimal[size];
    for (int i=0; i<size; i++) {
      oid[i] = rand.nextInt((int) SQLBB.getBB().getSharedCounters().read(SQLBB.tradeBuyOrdersPrimary)); //random instead of uniq
      qty[i] = getQty(); //the qty
      price[i] = getPrice(); //the price
      time[i] = new Timestamp(System.currentTimeMillis()-rand.nextInt(10*60*1000));       
    }
    
    int availSize;
    availSize = getDataForDelete(gConn, cid ,sid);    
    if(availSize == 0 || availSize == -1) return; //did not get the results
    
    deleteFromGFETable(gConn, whichDelete, cid, sid, oid, price, qty, time); 
  }
	
}
