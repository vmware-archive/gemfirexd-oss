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

import sql.SQLTest;
import sql.dmlStatements.DMLStmtIF;
import sql.dmlStatements.EmpDepartmentDMLStmt;
import sql.dmlStatements.EmpEmployeesDMLStmt;
import sql.dmlStatements.TradeBuyOrdersDMLStmt;
import sql.dmlStatements.TradeCompaniesDMLStmt;
import sql.dmlStatements.TradeCustomerProfileDMLStmt;
import sql.dmlStatements.TradeCustomersDMLStmt;
import sql.dmlStatements.TradeNetworthDMLStmt;
import sql.dmlStatements.TradePortfolioDMLStmt;
import sql.dmlStatements.TradePortfolioV1DMLStmt;
import sql.dmlStatements.TradeSecuritiesDMLStmt;
import sql.dmlStatements.TradeSellOrdersDMLStmt;
import sql.dmlStatements.TradeSellOrdersDupDMLStmt;
import sql.dmlStatements.TradeTxHistoryDMLStmt;
import sql.dmlStatements.json.TradeBuyOrderDMLStmtJson;
import sql.dmlStatements.json.TradeCustomersDMLStmtJson;
import sql.dmlStatements.json.TradeNetworthDMLStmtJson;
import sql.dmlStatements.json.TradeSecuritiesDMLStmtJson;
import sql.dmlStatements.writer.TradeBuyOrdersWriterDMLStmt;
import sql.dmlStatements.writer.TradeCustomersWriterDMLStmt;
import sql.dmlStatements.writer.TradeNetworthWriterDMLStmt;
import sql.dmlStatements.writer.TradePortfolioV1WriterDMLStmt;
import sql.dmlStatements.writer.TradePortfolioWriterDMLStmt;
import sql.dmlStatements.writer.TradeSecuritiesWriterDMLStmt;
import sql.dmlStatements.writer.TradeSellOrdersWriterDMLStmt;
import sql.dmlStatements.writer.TradeTxHistoryWriterDMLStmt;
import sql.sqlDisk.dmlStatements.TradeBuyOrdersDiscDMLStmt;
import sql.sqlDisk.dmlStatements.TradeCompaniesDiscDMLStmt;
import sql.sqlDisk.dmlStatements.TradeCustomersDiscDMLStmt;
import sql.sqlDisk.dmlStatements.TradeNetworthDiscDMLStmt;
import sql.sqlDisk.dmlStatements.TradePortfolioDiscDMLStmt;
import sql.sqlDisk.dmlStatements.TradePortfolioV1DiscDMLStmt;
import sql.sqlDisk.dmlStatements.TradeSecuritiesDiscDMLStmt;
import sql.sqlDisk.dmlStatements.TradeSellOrdersDiscDMLStmt;
import sql.sqlDisk.dmlStatements.TradeTxHistoryDiscDMLStmt;
import util.TestException;

/**
 * @author eshu
 *
 */
public class DMLStmtsFactory {
  public static final int TRADE_CUSTOMERS = 1;
  public static final int TRADE_SECURITIES = 2;
  public static final int TRADE_PORTFOLIO = 3;
  public static final int TRADE_NETWORTH = 4;
  public static final int TRADE_SELLORDERS = 5;
  public static final int TRADE_BUYORDERS = 6;
  public static final int TRADE_TXHISTORY = 7;
  public static final int EMP_DEPARTMENT = 8;
  public static final int EMP_EMPLOYEES = 9;
  public static final int TRADE_CUSTOMERPROFILE = 10;
  public static final int TRADE_SELLORDERSDUP = 11;
  public static final int TRADE_COMPANIES = 12;
  public static final int TRADE_PORTFOLIOV1 = 13;

  public DMLStmtIF createDMLStmt(int whichTable) {
    DMLStmtIF dmlStmtIF;
    switch (whichTable) {
    case TRADE_CUSTOMERS:
      if (SQLTest.hasJSON) 
      dmlStmtIF = new TradeCustomersDMLStmtJson();
      else 
      dmlStmtIF = new TradeCustomersDMLStmt();
      break;
    case TRADE_SECURITIES:
      if (SQLTest.hasJSON) 
        dmlStmtIF = new TradeSecuritiesDMLStmtJson();
      else
      dmlStmtIF = new TradeSecuritiesDMLStmt();
      break;
    case TRADE_PORTFOLIO:
      dmlStmtIF = new TradePortfolioDMLStmt();
      break;
    case TRADE_NETWORTH:
      if (SQLTest.hasJSON) 
        dmlStmtIF = new TradeNetworthDMLStmtJson();
      else 
         dmlStmtIF = new TradeNetworthDMLStmt();
      break;
    case TRADE_SELLORDERS:
      dmlStmtIF = new TradeSellOrdersDMLStmt();
      break;
    case TRADE_BUYORDERS:
      if (SQLTest.hasJSON) 
        dmlStmtIF = new TradeBuyOrderDMLStmtJson();      
      else 
      dmlStmtIF = new TradeBuyOrdersDMLStmt();
      
      break;
    case TRADE_TXHISTORY:
      dmlStmtIF = new TradeTxHistoryDMLStmt();
      break;
    case EMP_DEPARTMENT:
      dmlStmtIF = new EmpDepartmentDMLStmt();
      break;
    case EMP_EMPLOYEES:
      dmlStmtIF = new EmpEmployeesDMLStmt();
      break;  
    case TRADE_CUSTOMERPROFILE:
    	dmlStmtIF = new TradeCustomerProfileDMLStmt();
    	break;
    case TRADE_SELLORDERSDUP:
      dmlStmtIF = new TradeSellOrdersDupDMLStmt();
      break;
    case TRADE_COMPANIES:
      dmlStmtIF = new TradeCompaniesDMLStmt();
      break;  
    case TRADE_PORTFOLIOV1:
      dmlStmtIF = new TradePortfolioV1DMLStmt();
      break;
    default:
      throw new TestException("Unknown operation " + whichTable);    
    }
    return dmlStmtIF;
  }  
  
  public DMLStmtIF createWriterDMLStmt(int whichTable) {
    DMLStmtIF dmlStmtIF;
    switch (whichTable) {
    case TRADE_CUSTOMERS:
      dmlStmtIF = new TradeCustomersWriterDMLStmt();
      break;
    case TRADE_SECURITIES:
      dmlStmtIF = new TradeSecuritiesWriterDMLStmt();
      break;
    case TRADE_PORTFOLIO:
      dmlStmtIF = new TradePortfolioWriterDMLStmt();
      break;
    case TRADE_NETWORTH:
      dmlStmtIF = new TradeNetworthWriterDMLStmt();
      break;
    case TRADE_SELLORDERS:
      dmlStmtIF = new TradeSellOrdersWriterDMLStmt();
      break;
    case TRADE_BUYORDERS:
      dmlStmtIF = new TradeBuyOrdersWriterDMLStmt();
      break;
    case TRADE_TXHISTORY:
      dmlStmtIF = new TradeTxHistoryWriterDMLStmt();
      break;
    case TRADE_PORTFOLIOV1:
      dmlStmtIF = new TradePortfolioV1WriterDMLStmt();
      break;
    default:     
      throw new TestException("Unknown operation " + whichTable);    
    }
    return dmlStmtIF;
  }
  
  public DMLStmtIF createDiscDMLStmt(int whichTable) {
    DMLStmtIF dmlStmtIF;
    switch (whichTable) {
    case TRADE_CUSTOMERS:
      dmlStmtIF = new TradeCustomersDiscDMLStmt();
      break;
    case TRADE_SECURITIES:
      dmlStmtIF = new TradeSecuritiesDiscDMLStmt();
      break;
    case TRADE_PORTFOLIO:
      dmlStmtIF = new TradePortfolioDiscDMLStmt();
      break;
    case TRADE_NETWORTH:
      dmlStmtIF = new TradeNetworthDiscDMLStmt();
      break;
    case TRADE_SELLORDERS:
      dmlStmtIF = new TradeSellOrdersDiscDMLStmt();
      break;
    case TRADE_BUYORDERS:
      dmlStmtIF = new TradeBuyOrdersDiscDMLStmt();
      break;
    case TRADE_TXHISTORY:
      dmlStmtIF = new TradeTxHistoryDiscDMLStmt();
      break;
    case TRADE_COMPANIES:
      dmlStmtIF = new TradeCompaniesDiscDMLStmt();
      break;
    case TRADE_PORTFOLIOV1:
      dmlStmtIF = new TradePortfolioV1DiscDMLStmt();
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
    else if (tableName.equalsIgnoreCase("emp.department"))
      whichTable = EMP_DEPARTMENT;
    else if (tableName.equalsIgnoreCase("emp.employees"))
      whichTable = EMP_EMPLOYEES;
    else if (tableName.equalsIgnoreCase("trade.customerprofile"))
    	whichTable = TRADE_CUSTOMERPROFILE;
    else if (tableName.equalsIgnoreCase("trade.sellordersdup"))
      whichTable = TRADE_SELLORDERSDUP;
    else if (tableName.equalsIgnoreCase("trade.companies"))
      whichTable = TRADE_COMPANIES;
    else if (tableName.equalsIgnoreCase("trade.portfoliov1"))
      whichTable = TRADE_PORTFOLIOV1;
    return whichTable;
  }
  
}
