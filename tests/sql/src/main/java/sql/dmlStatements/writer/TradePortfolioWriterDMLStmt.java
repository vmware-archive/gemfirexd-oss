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

import java.math.BigDecimal;
import java.sql.Connection;

import sql.dmlStatements.TradePortfolioDMLStmt;

public class TradePortfolioWriterDMLStmt extends TradePortfolioDMLStmt {
  public void insert(Connection dConn, Connection gConn, int size) {
    int cid=0;
    cid = getCid(gConn);
    
    if (cid !=0)  //comment out this line to reproduce bug      
      insert(dConn, gConn, size, cid, false);
  }
  
  public void put(Connection dConn, Connection gConn, int size) {
    int cid=0;
    cid = getCid(gConn);
    
    if (cid !=0)  //comment out this line to reproduce bug      
      insert(dConn, gConn, size, cid, true);
  }
  
  public void insert(Connection dConn, Connection gConn, int size, int cid, boolean isPut) {
    int[] sid = new int[size];
    int[] qty = new int[size];
    BigDecimal[] sub = new BigDecimal[size];
    BigDecimal[] price = new BigDecimal[size];
    int availSize = 0;

    availSize = getDataFromResultSet(gConn, sid, qty, sub, price, size); //get the data
    insertToGFETable(gConn, cid, sid, qty, sub, availSize, isPut);
  }
  
  public void update(Connection dConn, Connection gConn, int size) {
    int numOfNonUniqUpdate = 6;  //how many update statement is for non unique keys
    int whichUpdate= getWhichOne(numOfNonUniqUpdate, update.length);
    
    int[] cid = new int[size];
    int[] sid = new int[size];
    int[] newCid = new int[size];
    int[] newSid = new int[size];
    int[] qty = new int[size];
    int[] availQty = new int[size];
    BigDecimal[] sub = new BigDecimal[size];
    BigDecimal[] price = new BigDecimal[size];;
    int availSize;
    
    
    
    if (isHATest && (whichUpdate == 0 || whichUpdate == 1)) {
        Log.getLogWriter().info("avoid x=x+1 in HA test for now, do not execute this update");
        return;
    } //avoid x=x+1 update in HA test for now	
 
    availSize = getDataForUpdate(gConn, cid, sid, qty, availQty, sub, newCid, newSid, price, size); //get the data;    
    updateGFETable(gConn, cid, sid, qty, availQty, sub, newCid, newSid, price, availSize, whichUpdate);

  }

  public void delete(Connection dConn, Connection gConn) {
    int numOfNonUniqDelete = delete.length/2;  //how many delete statement is for non unique keys
    int whichDelete = getWhichOne(numOfNonUniqDelete, delete.length);
    int[] cid = new int[1]; //only delete one record
    int[] sid = new int[1];
    int availSize;

    availSize= getDataForDelete(gConn, cid, sid, cid.length);    
    if(availSize == 0) return; //did not get the results
    
    deleteFromGFETable(gConn, whichDelete, cid, sid); 
  }

}
