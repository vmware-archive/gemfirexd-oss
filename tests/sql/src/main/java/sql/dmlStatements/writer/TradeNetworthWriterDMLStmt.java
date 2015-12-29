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

import sql.dmlStatements.TradeNetworthDMLStmt;


public class TradeNetworthWriterDMLStmt extends TradeNetworthDMLStmt {
  public void insert(Connection dConn, Connection gConn, int size, int[] cid) {
    BigDecimal[] cash = new BigDecimal[size];
    int[] loanLimit = new int[size];
    BigDecimal[] availLoan = new BigDecimal[size];
    getDataForInsert(cash, loanLimit, availLoan, size);
    BigDecimal securities = new BigDecimal(Integer.toString(0));

    insertToGFETable(gConn, cid, cash, securities, loanLimit, availLoan, size);
  }

  public void update(Connection dConn, Connection gConn, int size) {    
    int[] whichUpdate = new int[size];
    int[] cid = new int[size];
    BigDecimal[] availLoanDelta = new BigDecimal[size];
    BigDecimal[] sec = new BigDecimal[size];
    BigDecimal[] cashDelta = new BigDecimal[size];
    int[] newLoanLimit = new int[size]; 

    getDataForUpdate(gConn, cid, availLoanDelta, sec, cashDelta, newLoanLimit, whichUpdate, size); //get the data
    updateGFETable(gConn, cid, availLoanDelta, sec, cashDelta, newLoanLimit, whichUpdate, size);
  }
  
  public void delete(Connection dConn, Connection gConn) {
    int numOfNonUniqDelete = delete.length/2;  //how many delete statement is for non unique keys
    int whichDelete = getWhichOne(numOfNonUniqDelete, delete.length);
    int cid = getCid(gConn); 
    
    deleteFromGFETable(gConn, whichDelete, cid); 
  }
}
