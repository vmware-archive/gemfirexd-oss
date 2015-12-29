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
package sql.sqlTx.txTrigger;

import hydra.Log;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import sql.SQLHelper;

public class TxTriggerProcedure {
  static String insertSingleKeyTable = "create procedure trade.insertSingleKeyTableProc" +
  "(DP1 Varchar(20), DP2 Integer) " +
  "PARAMETER STYLE JAVA " +
  "LANGUAGE JAVA " +
  "MODIFIES SQL DATA " +
  "EXTERNAL NAME 'sql.sqlTx.txTrigger.TxTriggerProcedureTest.insertSingleKeyTable'";
  
  static String updateSingleKeyTable = "create procedure trade.updateSingleKeyTableProc" +
  "(DP1 Varchar(20), DP2 Integer) " +
  "PARAMETER STYLE JAVA " +
  "LANGUAGE JAVA " +
  "MODIFIES SQL DATA " +
  "EXTERNAL NAME 'sql.sqlTx.txTrigger.TxTriggerProcedureTest.updateSingleKeyTable'";
  
  static String deleteSingleKeyTable = "create procedure trade.deleteSingleKeyTableProc" +
  "(DP1 Varchar(20), DP2 Integer) " +
  "PARAMETER STYLE JAVA " +
  "LANGUAGE JAVA " +
  "MODIFIES SQL DATA " +
  "EXTERNAL NAME 'sql.sqlTx.txTrigger.TxTriggerProcedureTest.deleteSingleKeyTable'";
  
  static String insertPortfolioTable = "create procedure trade.insertPortfolioProc" +
  "(DP1 Varchar(20), DP2 Integer, DP3 Integer) " +
  "PARAMETER STYLE JAVA " +
  "LANGUAGE JAVA " +
  "MODIFIES SQL DATA " +
  "EXTERNAL NAME 'sql.sqlTx.txTrigger.TxTriggerProcedureTest.insertPortfolio'";
  
  static String updatePortfolioTable = "create procedure trade.updatePortfolioProc" +
  "(DP1 Varchar(20), DP2 Integer, DP3 Integer) " +
  "PARAMETER STYLE JAVA " +
  "LANGUAGE JAVA " +
  "MODIFIES SQL DATA " +
  "EXTERNAL NAME 'sql.sqlTx.txTrigger.TxTriggerProcedureTest.updatePortfolio'";
  
  static String deletePortfolioTable = "create procedure trade.deletePortfolioProc" +
  "(DP1 Varchar(20), DP2 Integer, DP3 Integer) " +
  "PARAMETER STYLE JAVA " +
  "LANGUAGE JAVA " +
  "MODIFIES SQL DATA " +
  "EXTERNAL NAME 'sql.sqlTx.txTrigger.TxTriggerProcedureTest.deletePortfolio'";
  
  static String insertTxhistoryTable = "create procedure trade.insertTxhistoryProc" +
  "(DP1 Varchar(20), DP2 Integer, DP3 Varchar(10)) " +
  "PARAMETER STYLE JAVA " +
  "LANGUAGE JAVA " +
  "MODIFIES SQL DATA " +
  "EXTERNAL NAME 'sql.sqlTx.txTrigger.TxTriggerProcedureTest.insertTxhistory'";
  
  static String updateTxhistoryTable = "create procedure trade.updateTxhistoryProc" +
  "(DP1 Varchar(20), DP2 Integer, DP3 Varchar(10)) " +
  "PARAMETER STYLE JAVA " +
  "LANGUAGE JAVA " +
  "MODIFIES SQL DATA " +
  "EXTERNAL NAME 'sql.sqlTx.txTrigger.TxTriggerProcedureTest.updateTxhistory'";
  
  static String deleteTxhistoryTable = "create procedure trade.deleteTxhistoryProc" +
  "(DP1 Varchar(20), DP2 Integer, DP3 Varchar(10)) " +
  "PARAMETER STYLE JAVA " +
  "LANGUAGE JAVA " +
  "MODIFIES SQL DATA " +
  "EXTERNAL NAME 'sql.sqlTx.txTrigger.TxTriggerProcedureTest.deleteTxhistory'";
  
  static String insertNetworthTable = "create procedure trade.insertNetworthProc" +
  "(DP1 Varchar(20), DP2 Integer) " +
  "PARAMETER STYLE JAVA " +
  "LANGUAGE JAVA " +
  "MODIFIES SQL DATA " +
  "EXTERNAL NAME 'sql.sqlTx.txTrigger.TxTriggerProcedureTest.insertNetworth'";
  
  protected static void createTriggerProcedure(Connection conn, String procedure) throws SQLException {
    Statement stmt = conn.createStatement();
    stmt.executeUpdate(procedure);    
    SQLWarning warning = stmt.getWarnings(); //test to see there is a warning
    if (warning != null) {
      SQLHelper.printSQLWarning(warning);
    } 
  }
  
  public static void createTriggerProcedures(Connection conn) {
    try {
      createTriggerProcedure(conn, insertSingleKeyTable);
      createTriggerProcedure(conn, updateSingleKeyTable);
      createTriggerProcedure(conn, deleteSingleKeyTable);
      Log.getLogWriter().info(insertSingleKeyTable);
      Log.getLogWriter().info(updateSingleKeyTable);
      Log.getLogWriter().info(deleteSingleKeyTable);
      
      createTriggerProcedure(conn, insertPortfolioTable);
      createTriggerProcedure(conn, updatePortfolioTable);
      createTriggerProcedure(conn, deletePortfolioTable);
      Log.getLogWriter().info(insertPortfolioTable);
      Log.getLogWriter().info(updatePortfolioTable);
      Log.getLogWriter().info(deletePortfolioTable);
      
      createTriggerProcedure(conn, insertTxhistoryTable);
      createTriggerProcedure(conn, updateTxhistoryTable);
      createTriggerProcedure(conn, deleteTxhistoryTable);
      Log.getLogWriter().info(insertTxhistoryTable);
      Log.getLogWriter().info(updateTxhistoryTable);
      Log.getLogWriter().info(deleteTxhistoryTable);
      
      createTriggerProcedure(conn, insertNetworthTable);
      Log.getLogWriter().info(insertNetworthTable);    
      
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
  }
  
}
