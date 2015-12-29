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
import hydra.TestConfig;

import java.sql.Connection;
import java.sql.SQLException;

import sql.SQLHelper;
import sql.sqlTx.SQLTxPrms;

public class TxTriggers {
  //trade.monitor
  //tname varchar(20) not null, 
  //pk1 int not null, 
  //pk2 int not null,
  //insertCount int, 
  //updateCount int, " +
  //"deleteCount int
  static boolean callTriggerProcedure = TestConfig.tab().booleanAt(
      SQLTxPrms.callTriggerProcedrue, false);
  static boolean reproduce49947 = TestConfig.tab().booleanAt(
      SQLTxPrms.reproduce49947, false);
  
  static String insertCustomers = "create trigger trade.insertCustomers " +
  "after " +
  "insert " +
  "on trade.customers " +
  "referencing " +
  "new as inserted " +
  "for each row " +
  "insert into trade.monitor values " +
  "('customers', inserted.cid, -1, 1, 0, 0 )";
  
  static String updateCustomers = "create trigger trade.updateCustomers " +
  "after " +
  "update " +
  "on trade.customers " +
  "referencing " +
  "new as newupdate " +
  "for each row " +
  "update trade.monitor set updateCount = updateCount + 1 " +
  "where tname = 'customers' and pk1 = newupdate.cid ";
  
  static String deleteCustomers = "create trigger trade.deleteCustomers " +
  "after " +
  "delete " +
  "on trade.customers " +
  "referencing " +
  "old as deleted " +
  "for each row " +
  "update trade.monitor set deleteCount = deleteCount + 1 " +
  "where tname = 'customers' and pk1 = deleted.cid ";
  
  static String insertNetworth = "create trigger trade.insertNetworth " +
  "after " +
  "insert " +
  "on trade.networth " +
  "referencing " +
  "new as inserted " +
  "for each row " +
  "call trade.insertNetworthProc('networth', inserted.cid)";
  
  static String updateNetworth = "create trigger trade.updateNetworth " +
  "after " +
  "update " +
  "on trade.networth " +
  "referencing " +
  "old as updated " +
  "for each row " +
  "update trade.monitor set updateCount = updateCount + 1 " +
  "where tname = 'networth' and pk1 = updated.cid ";
  
  static String deleteNetworth = "create trigger trade.deleteNetworth " +
  "after " +
  "delete " +
  "on trade.networth " +
  "referencing " +
  "old as deleted " +
  "for each row " +
  "update trade.monitor set deleteCount = deleteCount + 1 " +
  "where tname = 'networth' and pk1 = deleted.cid ";
  
  static String insertSecurities = "create trigger trade.insertSecurities " +
  "after " +
  "insert " +
  "on trade.securities " +
  "referencing " +
  "new as inserted " +
  "for each row " +
  "insert into trade.monitor values " +
  "('securities', inserted.sec_id, -1, 1, 0, 0 )";
  
  static String updateSecurities = "create trigger trade.updateSecurities " +
  "after " +
  "update " +
  "on trade.securities " +
  "referencing " +
  "old as updated " +
  "for each row " +
  "update trade.monitor set updateCount = updateCount + 1 " +
  "where tname = 'securities' and pk1 = updated.sec_id ";
  
  static String deleteSecurities = "create trigger trade.deleteSecurities " +
  "after " +
  "delete " +
  "on trade.securities " +
  "referencing " +
  "old as deleted " +
  "for each row " +
  "update trade.monitor set deleteCount = deleteCount + 1 " +
  "where tname = 'securities' and pk1 = deleted.sec_id ";
  
  static String insertPortfolio = "create trigger trade.insertPortfolio " +
  "after " +
  "insert " +
  "on trade.portfolio " +
  "referencing " +
  "new as inserted " +
  "for each row " +
  "call trade.insertPortfolioProc('portfolio', inserted.cid, inserted.sid)";
  //could only use procedure call to avoid reinserted key case
  
  static String updatePortfolio = "create trigger trade.updatePortfolio " +
  "after " +
  "update " +
  "on trade.portfolio " +
  "referencing " +
  "old as updated " +
  "for each row " +
  "update trade.monitor set updateCount = updateCount + 1 " +
  "where tname = 'portfolio' and pk1 = updated.cid and pk2 = updated.sid ";
  
  static String deletePortfolio = "create trigger trade.deletePortfolio " +
  "after " +
  "delete " +
  "on trade.portfolio " +
  "referencing " +
  "old as deleted " +
  "for each row " +
  "update trade.monitor set deleteCount = deleteCount + 1 " +
  "where tname = 'portfolio' and pk1 = deleted.cid and pk2 = deleted.sid";
  
  static String insertSellorders = "create trigger trade.insertSellorders " +
  "after " +
  "insert " +
  "on trade.sellorders " +
  "referencing " +
  "new as inserted " +
  "for each row " +
  "insert into trade.monitor values " +
  "('sellorders', inserted.oid, -1, 1, 0, 0 )";
  
  static String updateSellorders = "create trigger trade.updateSellorders " +
  "after " +
  "update " +
  "on trade.sellorders " +
  "referencing " +
  "old as updated " +
  "for each row " +
  "update trade.monitor set updateCount = updateCount + 1 " +
  "where tname = 'sellorders' and pk1 = updated.oid ";
  
  static String deleteSellorders = "create trigger trade.deleteSellorders " +
  "after " +
  "delete " +
  "on trade.sellorders " +
  "referencing " +
  "old as deleted " +
  "for each row " +
  "update trade.monitor set deleteCount = deleteCount + 1 " +
  "where tname = 'sellorders' and pk1 = deleted.oid ";
  
  static String insertBuyorders = "create trigger trade.insertBuyorders " +
  "after " +
  "insert " +
  "on trade.buyorders " +
  "referencing " +
  "new as inserted " +
  "for each row " +
  "insert into trade.monitor values " +
  "('buyorders', inserted.oid, -1, 1, 0, 0 )";
  
  static String updateBuyorders = "create trigger trade.updateBuyorders " +
  "after " +
  "update " +
  "on trade.buyorders " +
  "referencing " +
  "old as updated " +
  "for each row " +
  "update trade.monitor set updateCount = updateCount + 1 " +
  "where tname = 'buyorders' and pk1 = updated.oid ";
  
  static String deleteBuyorders = "create trigger trade.deleteBuyorders " +
  "after " +
  "delete " +
  "on trade.buyorders " +
  "referencing " +
  "old as deleted " +
  "for each row " +
  "update trade.monitor set deleteCount = deleteCount + 1 " +
  "where tname = 'buyorders' and pk1 = deleted.oid ";
  
  static String insertTxhistory = "create trigger trade.insertTxhistory " +
  "after " +
  "insert " +
  "on trade.txhistory " +
  "referencing " +
  "new as inserted " +
  "for each row " +
  "call trade.insertTxhistoryProc('txhistory', inserted.oid, inserted.type)";
  //porcedure call
  
  static String updateTxhistory = "create trigger trade.updateTxhistory " +
  "after " +
  "update " +
  "on trade.txhistory " +
  "referencing " +
  "old as updated " +
  "for each row " +
  "call trade.updateTxhistoryProc('txhistory', updated.oid, updated.type) ";
  //procedure call
  
  static String deleteTxhistory = "create trigger trade.deleteTxhistory " +
  "after " +
  "delete " +
  "on trade.txhistory " +
  "referencing " +
  "old as deleted " +
  "for each row " +
  "call trade.deleteTxhistoryProc('txhistory', deleted.oid, deleted.type)";
  //procedure call
  
  
  //using procedures calls  
  static String insertCustomersP = "create trigger trade.insertCustomers " +
  "after " +
  "insert " +
  "on trade.customers " +
  "referencing " +
  "new as inserted " +
  "for each row " +
  "call trade.insertSingleKeyTableProc('customers', inserted.cid) " + 
  (reproduce49947 ? " on table trade.customers" : "");
  
  static String updateCustomersP = "create trigger trade.updateCustomers " +
  "after " +
  "update " +
  "on trade.customers " +
  "referencing " +
  "new as newupdate " +
  "for each row " +
  "call trade.updateSingleKeyTableProc('customers', newupdate.cid)";
  
  static String deleteCustomersP = "create trigger trade.deleteCustomers " +
  "after " +
  "delete " +
  "on trade.customers " +
  "referencing " +
  "old as deleted " +
  "for each row " +
  "call trade.deleteSingleKeyTableProc('customers', deleted.cid) ";
  
  static String insertNetworthP = "create trigger trade.insertNetworth " +
  "after " +
  "insert " +
  "on trade.networth " +
  "referencing " +
  "new as inserted " +
  "for each row " +
  "call trade.insertNetworthProc('networth', inserted.cid)";
  
  static String updateNetworthP = "create trigger trade.updateNetworth " +
  "after " +
  "update " +
  "on trade.networth " +
  "referencing " +
  "old as updated " +
  "for each row " +
  "call trade.updateSingleKeyTableProc('networth', updated.cid)";
  
  static String deleteNetworthP = "create trigger trade.deleteNetworth " +
  "after " +
  "delete " +
  "on trade.networth " +
  "referencing " +
  "old as deleted " +
  "for each row " +
  "call trade.deleteSingleKeyTableProc('networth', deleted.cid)";
  
  static String insertSecuritiesP = "create trigger trade.insertSecurities " +
  "after " +
  "insert " +
  "on trade.securities " +
  "referencing " +
  "new as inserted " +
  "for each row " +
  "call trade.insertSingleKeyTableProc('securities', inserted.sec_id)";
  
  static String updateSecuritiesP = "create trigger trade.updateSecurities " +
  "after " +
  "update " +
  "on trade.securities " +
  "referencing " +
  "old as updated " +
  "for each row " +
  "call trade.updateSingleKeyTableProc('securities', updated.sec_id) ";
  
  static String deleteSecuritiesP = "create trigger trade.deleteSecurities " +
  "after " +
  "delete " +
  "on trade.securities " +
  "referencing " +
  "old as deleted " +
  "for each row " +
  "call trade.deleteSingleKeyTableProc('securities', deleted.sec_id)";
  
  static String insertPortfolioP = "create trigger trade.insertPortfolio " +
  "after " +
  "insert " +
  "on trade.portfolio " +
  "referencing " +
  "new as inserted " +
  "for each row " +
  "call trade.insertPortfolioProc('portfolio', inserted.cid, inserted.sid)";
  
  static String updatePortfolioP = "create trigger trade.updatePortfolio " +
  "after " +
  "update " +
  "on trade.portfolio " +
  "referencing " +
  "old as updated " +
  "for each row " +
  "call trade.updatePortfolioProc('portfolio', updated.cid, updated.sid) ";
  
  static String deletePortfolioP = "create trigger trade.deletePortfolio " +
  "after " +
  "delete " +
  "on trade.portfolio " +
  "referencing " +
  "old as deleted " +
  "for each row " +
  "call trade.deletePortfolioProc('portfolio', deleted.cid, deleted.sid)";
  
  static String insertSellordersP = "create trigger trade.insertSellorders " +
  "after " +
  "insert " +
  "on trade.sellorders " +
  "referencing " +
  "new as inserted " +
  "for each row " +
  "call trade.insertSingleKeyTableProc('sellorders', inserted.oid)";
  
  static String updateSellordersP = "create trigger trade.updateSellorders " +
  "after " +
  "update " +
  "on trade.sellorders " +
  "referencing " +
  "old as updated " +
  "for each row " +
  "call trade.updateSingleKeyTableProc('sellorders', updated.oid) ";
  
  static String deleteSellordersP = "create trigger trade.deleteSellorders " +
  "after " +
  "delete " +
  "on trade.sellorders " +
  "referencing " +
  "old as deleted " +
  "for each row " +
  "call trade.deleteSingleKeyTableProc('sellorders', deleted.oid)";
  
  static String insertBuyordersP = "create trigger trade.insertBuyorders " +
  "after " +
  "insert " +
  "on trade.buyorders " +
  "referencing " +
  "new as inserted " +
  "for each row " +
  "call trade.insertSingleKeyTableProc('buyorders', inserted.oid)";
  
  static String updateBuyordersP = "create trigger trade.updateBuyorders " +
  "after " +
  "update " +
  "on trade.buyorders " +
  "referencing " +
  "old as updated " +
  "for each row " +
  "call trade.updateSingleKeyTableProc('buyorders', updated.oid) ";
  
  static String deleteBuyordersP = "create trigger trade.deleteBuyorders " +
  "after " +
  "delete " +
  "on trade.buyorders " +
  "referencing " +
  "old as deleted " +
  "for each row " +
  "call trade.deleteSingleKeyTableProc('buyorders', deleted.oid)";
  
  static String insertTxhistoryP = "create trigger trade.insertTxhistory " +
  "after " +
  "insert " +
  "on trade.txhistory " +
  "referencing " +
  "new as inserted " +
  "for each row " +
  "call trade.insertTxhistoryProc('txhistory', inserted.oid, inserted.type)";
  
  static String updateTxhistoryP = "create trigger trade.updateTxhistory " +
  "after " +
  "update " +
  "on trade.txhistory " +
  "referencing " +
  "old as updated " +
  "for each row " +
  "call trade.updateTxhistoryProc('txhistory', updated.oid, updated.type) ";
  
  static String deleteTxhistoryP = "create trigger trade.deleteTxhistory " +
  "after " +
  "delete " +
  "on trade.txhistory " +
  "referencing " +
  "old as deleted " +
  "for each row " +
  "call trade.deleteTxhistoryProc('txhistory', deleted.oid, deleted.type)";
  
  public static void createMonitorTriggers(Connection gConn) {
    try {
      if (!callTriggerProcedure) {
        Log.getLogWriter().info("creating following trigger: " + insertCustomers);
        gConn.createStatement().execute(insertCustomers);
        Log.getLogWriter().info("creating following trigger: " + updateCustomers);
        gConn.createStatement().execute(updateCustomers);
        Log.getLogWriter().info("creating following trigger: " + deleteCustomers);
        gConn.createStatement().execute(deleteCustomers);
        
        Log.getLogWriter().info("creating following trigger: " + insertNetworth);
        gConn.createStatement().execute(insertNetworth);
        Log.getLogWriter().info("creating following trigger: " + updateNetworth);
        gConn.createStatement().execute(updateNetworth);
        Log.getLogWriter().info("creating following trigger: " + deleteNetworth);
        gConn.createStatement().execute(deleteNetworth);
        
        Log.getLogWriter().info("creating following trigger: " + insertPortfolio);
        gConn.createStatement().execute(insertPortfolio);
        Log.getLogWriter().info("creating following trigger: " + updatePortfolio);
        gConn.createStatement().execute(updatePortfolio);
        Log.getLogWriter().info("creating following trigger: " + deletePortfolio);
        gConn.createStatement().execute(deletePortfolio);
        
        Log.getLogWriter().info("creating following trigger: " + insertSecurities);
        gConn.createStatement().execute(insertSecurities);
        Log.getLogWriter().info("creating following trigger: " + updateSecurities);
        gConn.createStatement().execute(updateSecurities);
        Log.getLogWriter().info("creating following trigger: " + deleteSecurities);
        gConn.createStatement().execute(deleteSecurities);
        
        Log.getLogWriter().info("creating following trigger: " + insertSellorders);
        gConn.createStatement().execute(insertSellorders);
        Log.getLogWriter().info("creating following trigger: " + updateSellorders);
        gConn.createStatement().execute(updateSellorders);
        Log.getLogWriter().info("creating following trigger: " + deleteSellorders);
        gConn.createStatement().execute(deleteSellorders);
        
        Log.getLogWriter().info("creating following trigger: " + insertBuyorders);
        gConn.createStatement().execute(insertBuyorders);
        Log.getLogWriter().info("creating following trigger: " + updateBuyorders);
        gConn.createStatement().execute(updateBuyorders);
        Log.getLogWriter().info("creating following trigger: " + deleteBuyorders);
        gConn.createStatement().execute(deleteBuyorders);
        
        /* non primary key case need to be reconsidered as multiple insert
         * could be performed concurrently without conflict
         * may only insert in the inittask.
         * */
        /*
        Log.getLogWriter().info("creating following trigger: " + insertTxhistory);
        gConn.createStatement().execute(insertTxhistory);
        Log.getLogWriter().info("creating following trigger: " + updateTxhistory);
        gConn.createStatement().execute(updateTxhistory);
        Log.getLogWriter().info("creating following trigger: " + deleteTxhistory);
        gConn.createStatement().execute(deleteTxhistory);
        */
      } else {  //using procedure call in trigger    
        Log.getLogWriter().info("creating following trigger: " + insertCustomersP);
        gConn.createStatement().execute(insertCustomersP);
        Log.getLogWriter().info("creating following trigger: " + updateCustomersP);
        gConn.createStatement().execute(updateCustomersP);
        Log.getLogWriter().info("creating following trigger: " + deleteCustomersP);
        gConn.createStatement().execute(deleteCustomersP);
        
        Log.getLogWriter().info("creating following trigger: " + insertNetworthP);
        gConn.createStatement().execute(insertNetworthP);
        Log.getLogWriter().info("creating following trigger: " + updateNetworthP);
        gConn.createStatement().execute(updateNetworthP);
        Log.getLogWriter().info("creating following trigger: " + deleteNetworthP);
        gConn.createStatement().execute(deleteNetworthP);
        
        Log.getLogWriter().info("creating following trigger: " + insertPortfolioP);
        gConn.createStatement().execute(insertPortfolioP);
        Log.getLogWriter().info("creating following trigger: " + updatePortfolioP);
        gConn.createStatement().execute(updatePortfolioP);
        Log.getLogWriter().info("creating following trigger: " + deletePortfolioP);
        gConn.createStatement().execute(deletePortfolioP);
        
        Log.getLogWriter().info("creating following trigger: " + insertSecuritiesP);
        gConn.createStatement().execute(insertSecuritiesP);
        Log.getLogWriter().info("creating following trigger: " + updateSecuritiesP);
        gConn.createStatement().execute(updateSecuritiesP);
        Log.getLogWriter().info("creating following trigger: " + deleteSecuritiesP);
        gConn.createStatement().execute(deleteSecuritiesP);
        
        Log.getLogWriter().info("creating following trigger: " + insertSellordersP);
        gConn.createStatement().execute(insertSellordersP);
        Log.getLogWriter().info("creating following trigger: " + updateSellordersP);
        gConn.createStatement().execute(updateSellordersP);
        Log.getLogWriter().info("creating following trigger: " + deleteSellordersP);
        gConn.createStatement().execute(deleteSellordersP);
        
        Log.getLogWriter().info("creating following trigger: " + insertBuyordersP);
        gConn.createStatement().execute(insertBuyordersP);
        Log.getLogWriter().info("creating following trigger: " + updateBuyordersP);
        gConn.createStatement().execute(updateBuyordersP);
        Log.getLogWriter().info("creating following trigger: " + deleteBuyordersP);
        gConn.createStatement().execute(deleteBuyordersP);
        
        /* non primary key case need to be reconsidered as multiple insert
         * could be performed concurrently without conflict
         * may only insert in the inittask.
         * */
        /*
        Log.getLogWriter().info("creating following trigger: " + insertTxhistoryP);
        gConn.createStatement().execute(insertTxhistoryP);
        Log.getLogWriter().info("creating following trigger: " + updateTxhistoryP);
        gConn.createStatement().execute(updateTxhistoryP);
        Log.getLogWriter().info("creating following trigger: " + deleteTxhistoryP);
        gConn.createStatement().execute(deleteTxhistoryP);
        */
      }
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
  }
}
