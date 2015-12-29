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
/**
 * 
 */
package sql.sqlutil;

import sql.dmlTxStatements.*;
import util.TestException;

/**
 * @author eshu
 *
 */
public class DMLTxStmtsFactory {
  public static final int TRADE_CUSTOMERS = 1;
  public static final int TRADE_SECURITIES = 2;
  public static final int TRADE_PORTFOLIO = 3;
  public static final int TRADE_NETWORTH = 4;
  public static final int TRADE_SELLORDERS = 5;
  public static final int TRADE_BUYORDERS = 6;
  public static final int TRADE_TXHISTORY = 7;
  
  public DMLTxStmtIF createDMLTxStmt(int whichTable) {
    DMLTxStmtIF dmlStmtIF=null;
    switch (whichTable) {
    case TRADE_CUSTOMERS:
      dmlStmtIF = new TradeCustomersDMLTxStmt();
      break;
    case TRADE_SECURITIES:
      dmlStmtIF = new TradeSecuritiesDMLTxStmt();
      break;
    case TRADE_PORTFOLIO:
      dmlStmtIF = new TradePortfolioDMLTxStmt();
      break;
    case TRADE_NETWORTH:
      dmlStmtIF = new TradeNetworthDMLTxStmt();
      break;
    case TRADE_SELLORDERS:
      dmlStmtIF = new TradeSellOrdersDMLTxStmt();
      break;
    case TRADE_BUYORDERS:
      dmlStmtIF = new TradeBuyOrdersDMLTxStmt();
      break;
    case TRADE_TXHISTORY:
      dmlStmtIF = new TradeTxHistoryDMLTxStmt();
      break;
    default:     
      throw new TestException("Unknown operation " + whichTable);    
    }
    return dmlStmtIF;
  }

  public static int getInt(String tableName) {
    int whichTable = -1;
    if (tableName.equalsIgnoreCase("trade.customers"))
      whichTable = TRADE_CUSTOMERS;
    else if (tableName.equalsIgnoreCase("trade.securities"))
      whichTable = TRADE_SECURITIES;
    else if (tableName.equalsIgnoreCase("trade.portfolio"))
      whichTable = TRADE_PORTFOLIO;
    else if (tableName.equalsIgnoreCase("trade.networth"))
      whichTable = TRADE_NETWORTH;
    else if (tableName.equalsIgnoreCase("trade.sellorders"))
      whichTable = TRADE_SELLORDERS;
    else if (tableName.equalsIgnoreCase("trade.buyorders"))
      whichTable = TRADE_BUYORDERS;
    else if (tableName.equalsIgnoreCase("trade.txhistory"))
      whichTable = TRADE_TXHISTORY;
    return whichTable;
  }
  
}
