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


import sql.SQLBB;
import sql.dmlStatements.TradeSecuritiesDMLStmt;

public class TradeSecuritiesWriterDMLStmt extends TradeSecuritiesDMLStmt {
  public void insert(Connection dConn, Connection gConn, int size) {
    insert(dConn, gConn, size, false);
  }
  public void put(Connection dConn, Connection gConn, int size) {
    insert(dConn, gConn, size, true);
  }
  public void insert(Connection dConn, Connection gConn, int size, boolean isPut) {
    int[] sec_id = new int[size];
    String[] symbol = new String[size];
    String[] exchange = new String[size];
    BigDecimal[] price = new BigDecimal[size];
    
    getDataForInsert(sec_id, symbol, exchange, price, size); //get the data
    insertToGFETable(gConn, sec_id, symbol, exchange, price, size, isPut);
  }

  public void update(Connection dConn, Connection gConn, int size) {
    int numOfNonUniqUpdate = 3;  //how many update statement is for non unique keys
    int whichUpdate= getWhichOne(numOfNonUniqUpdate, update.length);
    
    int[] sec_id = new int[size];
    String[] symbol = new String[size];
    String[] exchange = new String[size];
    BigDecimal[] price = new BigDecimal[size];
    
    getDataForUpdate(gConn, sec_id, symbol, exchange, price, size); //get the data
    
    
    updateGFETable(gConn, sec_id, symbol, exchange, price, size, whichUpdate);
  }
  
  public void delete(Connection dConn, Connection gConn) {
    int numOfNonUniqDelete = 1;  //how many delete statement is for non unique keys
    int whichDelete = getWhichOne(numOfNonUniqDelete, delete.length);
    
    String symbol = getSymbol();
    BigDecimal price = getPrice();
    String exchange = getExchange();
    int sec_id = rand.nextInt((int)SQLBB.getBB().getSharedCounters().read(SQLBB.tradeSecuritiesPrimary));   

    deleteFromGFETable(gConn, sec_id, symbol, price, exchange, whichDelete); 

  }

}
