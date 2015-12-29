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
package sql.sqlutil;

import sql.SQLTest;
import sql.dmlDistTxStatements.*;
import sql.dmlDistTxStatements.json.TradeCustomersDMLDistTxStmtJson;
import sql.dmlDistTxStatements.json.TradeNetworthDMLDistTxStmtJson;
import sql.dmlDistTxStatements.json.TradeSecuritiesDMLDistTxStmtJson;
import sql.dmlDistTxRRStatements.*;
import util.TestException;

public class DMLDistTxStmtsFactory {
  public static final int TRADE_CUSTOMERS = 1;
  public static final int TRADE_SECURITIES = 2;
  public static final int TRADE_PORTFOLIO = 3;
  public static final int TRADE_NETWORTH = 4;
  public static final int TRADE_SELLORDERS = 5;
  public static final int TRADE_BUYORDERS = 6;
  public static final int TRADE_TXHISTORY = 7;
  
  public DMLDistTxStmtIF createDMLDistTxStmt(int whichTable) {
    DMLDistTxStmtIF dmlStmtIF = null;
    switch (whichTable) {
      case TRADE_CUSTOMERS:
        if (SQLTest.hasJSON)
          dmlStmtIF = new TradeCustomersDMLDistTxStmtJson();
        else
          dmlStmtIF = new TradeCustomersDMLDistTxStmt();
        break;
      case TRADE_SECURITIES:
        if (SQLTest.hasJSON)
          dmlStmtIF = new TradeSecuritiesDMLDistTxStmtJson();
        else
          dmlStmtIF = new TradeSecuritiesDMLDistTxStmt();
        break;
      case TRADE_PORTFOLIO:
        dmlStmtIF = new TradePortfolioDMLDistTxStmt();
        break;
      case TRADE_NETWORTH:
        if (SQLTest.hasJSON)
          dmlStmtIF = new TradeNetworthDMLDistTxStmtJson();
        else
          dmlStmtIF = new TradeNetworthDMLDistTxStmt();
        break;
      case TRADE_SELLORDERS:
        dmlStmtIF = new TradeSellOrdersDMLDistTxStmt();
        break;
      case TRADE_BUYORDERS:
        // dmlStmtIF = new TradeBuyOrdersDMLDistTxStmt();
        break;
      case TRADE_TXHISTORY:
        // dmlStmtIF = new TradeTxHistoryDMLDistTxStmt();
        break;
      default:
        throw new TestException("Unknown operation " + whichTable);
    }
    return dmlStmtIF;
  }
  
  public DMLDistTxStmtIF createDMLDistTxRRStmt(int whichTable) {
    DMLDistTxStmtIF dmlStmtIF=null;
    switch (whichTable) {
    case TRADE_CUSTOMERS:
      dmlStmtIF = new TradeCustomersDMLDistTxRRStmt();
      break;
    case TRADE_SECURITIES:
      dmlStmtIF = new TradeSecuritiesDMLDistTxRRStmt();
      break;
    case TRADE_PORTFOLIO:
      dmlStmtIF = new TradePortfolioDMLDistTxRRStmt();
      break;
    case TRADE_NETWORTH:
      dmlStmtIF = new TradeNetworthDMLDistTxRRStmt();
      break;
    case TRADE_SELLORDERS:
      dmlStmtIF = new TradeSellOrdersDMLDistTxRRStmt();
      break;
    case TRADE_BUYORDERS:
      //dmlStmtIF = new TradeBuyOrdersDMLDistTxRRStmt();
      break;
    case TRADE_TXHISTORY:
      //dmlStmtIF = new TradeTxHistoryDMLDistTxRRStmt();
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
  
  public DMLDistTxStmtIF createNewTablesDMLDistTxStmt(int whichTable) {
    DMLDistTxStmtIF dmlStmtIF=null;
    switch (whichTable) {
    case TRADE_CUSTOMERS:
      dmlStmtIF = new TradeCustomersV1DMLDistTxStmt();
      break;
    case TRADE_SECURITIES:
      //dmlStmtIF = new TradeSecuritiesV1DMLDistTxStmt();
      break;
    case TRADE_PORTFOLIO:
      //dmlStmtIF = new TradePortfolioV1DMLDistTxStmt();
      break;
    case TRADE_NETWORTH:
      dmlStmtIF = new TradeNetworthV1DMLDistTxStmt();
      break;
    case TRADE_SELLORDERS:
      //dmlStmtIF = new TradeSellOrdersV1DMLDistTxStmt();
      break;
    case TRADE_BUYORDERS:
      //dmlStmtIF = new TradeBuyOrdersV1DMLDistTxStmt();
      break;
    case TRADE_TXHISTORY:
      //dmlStmtIF = new TradeTxHistoryV1DMLDistTxStmt();
      break;
    default:     
      throw new TestException("Unknown operation " + whichTable);    
    }
    return dmlStmtIF;
  }
  
  public DMLDistTxStmtIF createNewTablesDMLDistTxRRStmt(int whichTable) {
    DMLDistTxStmtIF dmlStmtIF=null;
    switch (whichTable) {
    case TRADE_CUSTOMERS:
      //dmlStmtIF = new TradeCustomersV1DMLDistTxRRStmt();
      break;
    case TRADE_SECURITIES:
      //dmlStmtIF = new TradeSecuritiesV1DMLDistTxRRStmt();
      break;
    case TRADE_PORTFOLIO:
      //dmlStmtIF = new TradePortfolioV1DMLDistTxRRStmt();
      break;
    case TRADE_NETWORTH:
     //dmlStmtIF = new TradeNetworthV1DMLDistTxRRStmt();
      break;
    case TRADE_SELLORDERS:
      //dmlStmtIF = new TradeSellOrdersV1DMLDistTxRRStmt();
      break;
    case TRADE_BUYORDERS:
      //dmlStmtIF = new TradeBuyOrdersV1DMLDistTxRRStmt();
      break;
    case TRADE_TXHISTORY:
      //dmlStmtIF = new TradeTxHistoryV1DMLDistTxRRStmt();
      break;
    default:     
      throw new TestException("Unknown operation " + whichTable);    
    }
    return dmlStmtIF;
  }

  public static int getNewTablesInt(String tableName) {
    int whichTable = -1;
    if (tableName.equalsIgnoreCase("trade.customersv1"))
      whichTable = TRADE_CUSTOMERS;
    else if (tableName.equalsIgnoreCase("trade.securitiesv1"))
      whichTable = TRADE_SECURITIES;
    else if (tableName.equalsIgnoreCase("trade.portfoliov1"))
      whichTable = TRADE_PORTFOLIO;
    else if (tableName.equalsIgnoreCase("trade.networthv1"))
      whichTable = TRADE_NETWORTH;
    else if (tableName.equalsIgnoreCase("trade.sellordersv1"))
      whichTable = TRADE_SELLORDERS;
    else if (tableName.equalsIgnoreCase("trade.buyordersv1"))
      whichTable = TRADE_BUYORDERS;
    else if (tableName.equalsIgnoreCase("trade.txhistoryv1"))
      whichTable = TRADE_TXHISTORY;
    return whichTable;
  }
}
