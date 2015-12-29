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
import sql.dmlStatements.TradeSellOrdersDMLStmt;

public class TradeSellOrdersWriterDMLStmt extends TradeSellOrdersDMLStmt {
  public void insert(Connection dConn, Connection gConn, int size, int[] cid, int[] sid) {
    insert(dConn, gConn, size, cid, sid, false); 
  }
  public void insert(Connection dConn, Connection gConn, int size, int[] cid, int[] sid, boolean isPut) {
    int[] oid = new int[size];
    int[] qty = new int[size];
    String status = "open";
    Timestamp[] time = new Timestamp[size];
    BigDecimal[] ask = new BigDecimal[size];

    getDataForInsert(gConn, oid, cid, sid, qty, time, ask, size); //get the data
    insertToGFETable(gConn, oid, cid, sid, qty, status, time, ask, size, isPut);
  }

  public void put(Connection dConn, Connection gConn, int size, int[] cid, int[] sid) {
    insert(dConn, gConn, size, cid, sid, true);
  }
  
  public void update(Connection dConn, Connection gConn, int size) {
    int[] sid = new int[size];
    BigDecimal[] ask = new BigDecimal[size];
    Timestamp[] orderTime = new Timestamp[size];
    int[] cid = new int[size];
    int[] qty = new int[size];
    BigDecimal[] ask2 = new BigDecimal[size];    
    int[]  whichUpdate = new int[size];

    if (getDataForUpdate(gConn, cid, sid, qty, orderTime, ask, ask2, whichUpdate, size))
    	updateGFETable(gConn, cid, sid, qty, orderTime, ask, ask2, whichUpdate, size);

  }
  
  public void delete(Connection dConn, Connection gConn) {
    int numOfNonUniqDelete = delete.length/2;  //how many delete statement is for non unique keys
    int whichDelete = getWhichOne(numOfNonUniqDelete, delete.length);
    if (whichDelete == 3 && whichDelete != getMyTid()) whichDelete--; 
    int size = 2; //how many delete to be completed in this delete operation
    int[] cid = new int[size]; //only delete one record
    int[] sid = new int[size];
    int[] oid = new int[size];
    for (int i=0; i<size; i++) {
      oid[i] = rand.nextInt((int) SQLBB.getBB().getSharedCounters().read(SQLBB.tradeSellOrdersPrimary)); //random instead of uniq
    }
   
    int availSize;
    availSize = getDataForDelete(gConn, cid ,sid);
    
    if(availSize == 0) return; //did not get the results   

    deleteFromGFETable(gConn, whichDelete, cid, sid, oid); 

  }
}
