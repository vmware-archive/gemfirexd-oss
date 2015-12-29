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
package sql.tpce.tpcetxn;

import hydra.Log;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;

import sql.tpce.tpcedef.TPCETxnInput;
import sql.tpce.tpcedef.TPCETxnOutput;
import sql.tpce.tpcedef.input.TradeResultTxnInput;
import sql.tpce.tpcedef.output.TradeResultTxnOutput;
import util.TestException;

public class TPCETradeResult extends TPCETransaction {
  protected TradeResultTxnInput  trTxnInput = null;
  protected TradeResultTxnOutput trTxnOutput = null;
  protected Connection conn = null;
  
  protected static boolean isTicket41738Fixed = false;
  long trade_id;
  BigDecimal trade_price;
  
  //from frame1: may need to use frame1out to pass to frame2 to follow tpce spec.
  private long acct_id;
  private String type_id;
  private String symbol;
  private int trade_qty;
  private BigDecimal charge;
  private short is_lifo;
  private short trade_is_cash;
  
  private String type_name;
  private short type_is_sell;
  private short type_is_market;

  private int hs_qty;
  
  //from frame2:
  private long broker_id;
  private long cust_id;
  private short tax_status;
  private Timestamp trade_dts;
  
  BigDecimal buy_value = new BigDecimal("0.00");
  BigDecimal sell_value = new BigDecimal("0.00");
  
  //for frame3:
  BigDecimal tax_amount = new BigDecimal("0.00");
  
  //for frame4:
  BigDecimal comm_rate = new BigDecimal("0.00");
  private String s_name;
  
  //for frame5
  private BigDecimal comm_amount = new BigDecimal("0.00");
  private String st_completed_id = "CMPT";
  
  //for frame6
  private Date due_date;
  private BigDecimal se_amount;
  private BigDecimal acct_bal;
  
 
  private static final String selectTrade = "select T_CA_ID, T_TT_ID, T_S_SYMB, T_QTY, T_CHRG, T_LIFO, T_IS_CASH from TRADE where T_ID = ?";
  private static final String selectTradeType = "select TT_NAME, TT_IS_SELL, TT_IS_MRKT from TRADE_TYPE where TT_ID = ?";
  private static final String selectHoldingSummary = "select HS_QTY from HOLDING_SUMMARY where HS_CA_ID = ? and HS_S_SYMB = ?";
  private static final String selectHoldingSummaryTBLForLock = "select HS_QTY from HOLDING_SUMMARY_TBL where HS_CA_ID = ? and HS_S_SYMB = ? for update";
  
  private static final String selectCustomerAccount = "select CA_NAME, CA_B_ID, CA_C_ID, CA_TAX_ST from CUSTOMER_ACCOUNT where CA_ID = ?";
  
  //HOLDING_SUMMARY_TBL is used for join in gfxd, this could be used verify the hold_summary view
  private static final String insertHSTable = " insert into HOLDING_SUMMARY_TBL( HS_CA_ID, HS_S_SYMB, HS_QTY) values (?, ?, ?)";
  private static final String updateHSForSell = "update HOLDING_SUMMARY_TBL set HS_QTY = hs_qty - ? where HS_CA_ID = ? and HS_S_SYMB = ?";
  private static final String updateHSHoldLock = "update HOLDING_SUMMARY_TBL set HS_QTY = ? where HS_CA_ID = ? and HS_S_SYMB = ?";
  private static final String deleteHS = "delete from HOLDING_SUMMARY_TBL where HS_CA_ID = ? and HS_S_SYMB = ?";
  private static final String updateHSForBuy = "update HOLDING_SUMMARY_TBL set HS_QTY = hs_qty + ? where HS_CA_ID = ? and HS_S_SYMB = ?";
  
  //private static final String selectHoldingForUpdateDesc = "select H_T_ID, H_QTY, H_PRICE from HOLDING where H_CA_ID = ? and H_S_SYMB = ? order by H_DTS desc for update";
  //private static final String selectHoldingForUpdateAsc = "select H_T_ID, H_QTY, H_PRICE from HOLDING where H_CA_ID = ? and H_S_SYMB = ? order by H_DTS asc for update";
  //TODO: This is a temporarily work around for update is not supported with order by in gfxd
  //this work around is possible as test hold the holding-summary key lock already,
  //by either inserting a row when hs_qty is 0 or update the same hs_qty again.
  //no other concurrent trade result txns could work on holdings with same acct_id and symbol.
  //It seems no other txns do dml on holdings, so this work around should work.
  //
  //Once the resolution is ready, we need to use for update as this is needs to be tested
  private static final String selectHoldingForUpdateDesc = "select H_T_ID, H_QTY, H_PRICE from HOLDING where H_CA_ID = ? and H_S_SYMB = ? order by H_DTS desc ";
  private static final String selectHoldingForUpdateAsc = "select H_T_ID, H_QTY, H_PRICE from HOLDING where H_CA_ID = ? and H_S_SYMB = ? order by H_DTS asc ";
  
  private static final String insertHoldingHistory = " insert into HOLDING_HISTORY (HH_H_T_ID, HH_T_ID, HH_BEFORE_QTY, HH_AFTER_QTY) values (?, ?, ?, ?)";
  
  private static final String updateHoldingCurrentOf = "update HOLDING set H_QTY = ? where current of updateCursor";
  private static final String updateHoldingPK = "update HOLDING set H_QTY = ? where H_T_ID = ?"; 
  
  private static final String deleteHoldingCurrentOf = "delete from HOLDING where current of updateCursor";
  private static final String deleteHoldingPK = "delete from HOLDING where H_T_ID = ?";
  private static final String insertHolding = "insert into HOLDING (H_T_ID, H_CA_ID, H_S_SYMB, H_DTS, H_PRICE, H_QTY) values (? ,? ,? ,? ,? ,?)";
  
  private static final String selectTaxRate = "select sum(tx_rate) from taxrate where tx_id in (select cx_tx_id from customer_taxrate where cx_c_id = ?)";
  private static final String updateTaxAmount = "update TRADE set T_TAX = ? where T_ID = ?";
  
  private static final String selectSecurity = "select  S_EX_ID, S_NAME from SECURITY where S_SYMB = ?";
  private static final String selectCustomer = "select C_TIER from CUSTOMER where C_ID = ?";
  private static final String selectCommRate = "select CR_RATE from COMMISSION_RATE where CR_C_TIER = ? and CR_TT_ID = ? and CR_EX_ID = ? and CR_FROM_QTY <= ? and CR_TO_QTY >= ? Fetch first row only";

  private static final String updateTrade = "update TRADE set T_COMM = ?, T_DTS = ?, T_ST_ID = ?, T_TRADE_PRICE = ? where T_ID = ?";
  private static final String insertTradeHistory = "insert into TRADE_HISTORY(TH_T_ID, TH_DTS, TH_ST_ID) values (?, ?, ?)";
  private static final String updateBroker = "update BROKER set B_COMM_TOTAL = B_COMM_TOTAL + ?, B_NUM_TRADES = B_NUM_TRADES + 1 where B_ID = ?";
  
  private static final String insertSettlement = "insert into SETTLEMENT ( SE_T_ID, SE_CASH_TYPE, SE_CASH_DUE_DATE, SE_AMT) values ( ?, ?, ?, ?)";
  private static final String updateCustomerAccount = "update CUSTOMER_ACCOUNT set CA_BAL = CA_BAL + ? where CA_ID = ?";
  private static final String insertCashTransaction = "insert into CASH_TRANSACTION ( CT_DTS, CT_T_ID, CT_AMT, CT_NAME ) values ( ?, ?, ?, ?)";
  private static final String selectCABalance = "select CA_BAL from CUSTOMER_ACCOUNT where CA_ID = ?";
  
  public TPCETradeResult() {
    
  }
  
  @Override
  public TPCETxnOutput runTxn(TPCETxnInput txnInput, Connection conn)
      throws SQLException {
    trTxnInput = (TradeResultTxnInput)txnInput;
    trTxnOutput = new TradeResultTxnOutput();
    this.conn = conn;
    

    invokeFrame1();
    invokeFrame2();

    if ((tax_status == 1 || tax_status == 2) 
        && sell_value.compareTo(buy_value) == 1){
      invokeFrame3();
      if (tax_amount.compareTo(new BigDecimal("0.00")) != 1) {
        trTxnOutput.setStatus(-831);
      }
    }

    invokeFrame4();
    if (comm_rate.compareTo(new BigDecimal("0.00")) != 1) {
      trTxnOutput.setStatus(-841);
    }


    comm_amount = comm_rate.divide(new BigDecimal("100.0")).multiply(
        trade_price.multiply(new BigDecimal(String.valueOf(trade_qty))));
    invokeFrame5();
    
    
    Calendar c = Calendar.getInstance();
    c.setTime(new Date(trade_dts.getTime()));
    c.add(Calendar.DATE, 2);
    due_date = new Date(c.getTimeInMillis());
    
    if (type_is_sell == 1) {
      se_amount = trade_price.multiply(new BigDecimal(String.valueOf(trade_qty)))
        .subtract(charge).subtract(comm_amount);
    } else {
      se_amount = new BigDecimal("-1").multiply(trade_price.
          multiply(new BigDecimal(String.valueOf(trade_qty))).
          add(charge).add(comm_amount));

    }
    if (tax_status == 1) {
      se_amount = se_amount.subtract(tax_amount);
    }
    invokeFrame6();
    trTxnOutput.setAcctId(acct_id);
    trTxnOutput.setAcctBal(acct_bal);

    return trTxnOutput;
  }

  protected void invokeFrame1() throws SQLException{  
    trade_id = trTxnInput.getTradeID();
    //selectTrade = "select T_CA_ID, T_TT_ID, T_S_SYMB, T_QTY, T_CHRG, T_LIFO, T_IS_CASH from TRADE where T_ID = ?";
    PreparedStatement ps = conn.prepareStatement(selectTrade);
    ps.setLong(1, trade_id);
    ResultSet rs = ps.executeQuery();
    if (rs.next()) {
      acct_id = rs.getLong("T_CA_ID");
      type_id = rs.getString("T_TT_ID");
      symbol = rs.getString("T_S_SYMB");
      trade_qty = rs.getInt("T_QTY");
      charge = rs.getBigDecimal("T_CHRG");
      is_lifo = rs.getShort("T_IS_CASH");
      trade_is_cash = rs.getShort("T_IS_CASH");
      
      
      
      if (rs.next()) {
        trTxnOutput.setStatus(-811);
        throw new TestException ( selectTrade + " has more than 1 row " +
            "in result set for T_ID = " + trade_id);
      }
    } else {
      trTxnOutput.setStatus(-811);
      throw new TestException ( selectTrade + " does not get single row " +
          "in result set for T_ID = " + trade_id);
    }
    
    rs.close();
    
    //selectTradeType = "select TT_NAME, TT_IS_SELL, TT_IS_MRKT from TRADE_TYPE where TT_ID = ?";
    
    ps = conn.prepareStatement(selectTradeType);
    ps.setString(1, type_id);
    rs = ps.executeQuery();
    if (rs.next()) {
      type_name = rs.getString("TT_NAME");
      type_is_sell = rs.getShort("TT_IS_SELL");
      type_is_market = rs.getShort("TT_IS_MRKT");
      
      if (logDML) {
        Log.getLogWriter().info(selectTradeType + " gets TT_NAME: " + type_name +
            " TT_IS_SELL: " + type_is_sell + " TT_IS_MRKT: " + type_is_market );
        
      }
    
      if (rs.next()) {
        trTxnOutput.setStatus(-811);
        throw new TestException ( selectTradeType + " has more than 1 row " +
          "in result set for TT_ID = " + type_id);
      }
    } else {
      trTxnOutput.setStatus(-811);
      throw new TestException ( selectTradeType + " does not get single row " +
      "in result set for TT_ID = " + type_id);
    }
    
    rs.close();

    /*
    //select HS_QTY from HOLDING_SUMMARY where HS_CA_ID = ? and HS_S_SYMB = ?   
    //TODO, may need to use FOR UPDATE on HOLDLING_SUMMARY_TBL to prevent another trade result updates
    //HOLDING (VIEW HOLDING_SUMMARY relies on) table concurrently. and change the select 
    //to perform on HOLDING_SUMMARY_TBL
    //may move this select (for update) to frame2
    ps = conn.prepareStatement(selectHoldingSummary);
    ps.setLong(1, acct_id);
    ps.setString(2, symbol);
    rs = ps.executeQuery();
    if (rs.next()) {
      hs_qty = rs.getInt("HS_QTY");
    
      if (rs.next()) {
        trTxnOutput.setStatus(-811);
        throw new TestException ( selectHoldingSummary + " has more than 1 row " +
          "in result set for HS_CA_ID = " + acct_id + " and HS_S_SYMB = " + symbol );
      }
    } else {
      hs_qty = 0;
    }
    if (logDML) {
      Log.getLogWriter().info(selectHoldingSummary + " gets hs_qty " + hs_qty);
      
    }
    rs.close();
    */
    
    
    //TPC-E indicate trade-result txn should prevent phantom read, but gfxd does not 
    //support serializable. Will make change in the test to work around this issue
    //
    //instead, will insert into holding_summary_table a row if hs_qty is 0
    //or update the same qty again if none 0, so that no other concurrent
    //modification will succeed.
    PreparedStatement psLock = null;
    ResultSet rsLock = null;
    try {
      psLock = conn.prepareStatement(selectHoldingSummaryTBLForLock); //prevent concurrent write
      psLock.setLong(1, acct_id);
      psLock.setString(2, symbol);
      rsLock = psLock.executeQuery();
    } catch (SQLException se) {
      if (logDML && se.getSQLState().equals("X0Z02")) {
        Log.getLogWriter().info(selectHoldingSummaryTBLForLock + " gets expected conflict exception");
      }
      throw se;
    }
    if (rsLock.next()) {
      hs_qty = rsLock.getInt("HS_QTY");
      if (logDML) { 
        Log.getLogWriter().info(selectHoldingSummaryTBLForLock + " get HS_QTY: " + hs_qty
            + " for HS_CA_ID = " + acct_id + " and HS_S_SYMB = "  + symbol);
      }
      
      if (rsLock.next()) {
        trTxnOutput.setStatus(-811);
        throw new TestException ( selectHoldingSummary + " has more than 1 row " +
          "in result set for HS_CA_ID = " + acct_id + " and HS_S_SYMB = " + symbol );
      }
    } else {
      hs_qty = 0;
    }
    if (logDML) {
      Log.getLogWriter().info(selectHoldingSummaryTBLForLock + " gets hs_qty " + hs_qty);      
    }
    
    if (hs_qty == 0) {
      try {
        ps = conn.prepareStatement(insertHSTable);
        ps.setLong(1, acct_id);
        ps.setString(2, symbol);
        ps.setInt(3, hs_qty); //inserts 0 here
      
        int count = ps.executeUpdate();
        
        if (logDML) {
          Log.getLogWriter().info(insertHSTable + " inserts hs_qty as 0 to be " +
          		"updated in frame2 -- with HS_CA_ID = " + acct_id + 
              " HS_S_SYMB = " + symbol + " HS_QTY = " + hs_qty);
        }
        if (count != 1) {
          throw new TestException(insertHSTable  + " should insert 1 row " +
                   "but inserted " + count + " row(s)");
        }
      } catch (SQLException se) {
        if (se.getSQLState().equals("23505")) {
          Log.getLogWriter().info("should occur rarely -- the window is another insert " +
          		"occurs just after the select for update, test will allow this");
          //throw conflict exception instead, so that this txn will be retried.
          throw new SQLException("throw conflict exception instead for retry " +
          		"as gemfirexd does not support isolation Serializable", "X0Z02");
        } else throw se;
      }
    } else {
      ps = conn.prepareStatement(updateHSHoldLock);          
      ps.setLong(2, acct_id);
      ps.setString(3, symbol);
      ps.setInt(1, hs_qty);
   
      int count = ps.executeUpdate();
     
      if (logDML) {
        Log.getLogWriter().info(updateHSHoldLock + " does not change any hs_qty but" +
        		" to hold the row lock to prevent other concrrent update -- with HS_CA_ID = " + acct_id + 
            " HS_S_SYMB = " + symbol + " HS_QTY = " + hs_qty);
      }
      
      if (count != 1) {
        throw new TestException(updateHSHoldLock + " should update 1 row " +
            "but updated " + count + " row(s)");
      }  
    }

    rsLock.close();
    psLock.close();
  }
  
  protected void invokeFrame2() throws SQLException {
    trade_price = trTxnInput.getTradePrice();
    long hold_id;
    BigDecimal hold_price;
    trade_dts = new Timestamp(System.currentTimeMillis());


    int needed_qty = trade_qty;    
    int hold_qty = 0;
    ResultSet updatableRs = null;
    
    boolean useBatchInsert = true;
    boolean useBatchDelete = true;
    
    //selectCustomerAccount = "select CA_B_ID, CA_C_ID, CA_TAX_ST from CUSTOMER_ACCOUNT where CA_ID = ?";
    PreparedStatement ps = conn.prepareStatement(selectCustomerAccount);
    ps.setLong(1, acct_id);
    ResultSet rs = ps.executeQuery();
    if (rs.next()) {
      broker_id = rs.getInt("CA_B_ID");
      cust_id = rs.getInt("CA_C_ID");
      tax_status = rs.getShort("CA_TAX_ST");
      if (rs.next()) {
        trTxnOutput.setStatus(-821);
        throw new TestException ( selectCustomerAccount + " has more than 1 row " +
            "in result set for CA_ID = " + acct_id);
      }
    } else {
      trTxnOutput.setStatus(-821);
      throw new TestException ( selectCustomerAccount + " does not get single row " +
          "in result set for CA_ID = " + acct_id);
    }
    
    rs.close();
    ps.close();
    
    // Determine if sell or buy order
    if (type_is_sell  == 1) {
      /* inserts in frame1 now, will use update for hs_qty == 0 case as well)
      if (hs_qty == 0) {
      // no prior holdings exist, but one will be inserted
      //insert into HOLDING_SUMMARY_TBL( HS_CA_ID, HS_S_SYMB, HS_QTY) values (acct_id, symbol, -trade_qty)        
        ps = conn.prepareStatement(insertHSTable);
        ps.setLong(1, acct_id);
        ps.setString(2, symbol);
        ps.setInt(3, -trade_qty);
      
        int count = ps.executeUpdate();
        
        if (logDML) {
          Log.getLogWriter().info(insertHSTable + " with HS_CA_ID = " + acct_id + 
              " HS_S_SYMB = " + symbol + " HS_QTY = " + -trade_qty);
        }
        
        if (count != 1) {
          throw new TestException(insertHSTable  + " should insert 1 row " +
                   "but inserted " + count + " row(s)");
          //test need to make sure no other tx insert into hstable at same time as well. 
          //need to feed correct data, so that either only the thread process same ca and symbol in a batch
          //or no same ca and symbol in a batch to cause the issue.
          //or catch conflict exception here and do a retry, using flag 
        }    
        ps.close();
      } 
      else { */
        if (hs_qty != trade_qty) {
        // update HOLDING_SUMMARY (tbl) set HS_QTY = hs_qty - trade_qty where HS_CA_ID = acct_id and HS_S_SYMB = symbol
          ps = conn.prepareStatement(updateHSForSell);          
          ps.setLong(2, acct_id);
          ps.setString(3, symbol);
          ps.setInt(1, trade_qty);
       
          int count = ps.executeUpdate();
         
          if (logDML) {
            Log.getLogWriter().info(updateHSForSell + " with HS_CA_ID = " + acct_id + 
                " HS_S_SYMB = " + symbol + " HS_QTY = HS_QTY - " + trade_qty);
          }
          if (count != 1) {
            throw new TestException(updateHSForSell + " should update 1 row " +
                "but updated " + count + " row(s)");
           //test need to make sure no other tx insert into hstable at same time as well. 
           //need to feed correct data, so that either only one thread process same ca and symbol in a batch
           //or no same ca and symbol in a batch to cause the issue.
           //or catch conflict exception here and do a retry, using flag to check if conflict possible
          }    
          ps.close();
        } else { //if (hs_qty == trade_qty)
          //delete from
          //HOLDING_SUMMARY (tbl)
          //where
          //HS_CA_ID = acct_id and
          //HS_S_SYMB = symbol
          //}
          ps = conn.prepareStatement(deleteHS);          
          ps.setLong(1, acct_id);
          ps.setString(2, symbol);       
          int count = ps.executeUpdate();
         
          if (logDML) {
            Log.getLogWriter().info(deleteHS + " for HS_CA_ID = " + acct_id + 
                " HS_S_SYMB = " + symbol);
          }
          if (count != 1) {
            throw new TestException(deleteHS + " should delete 1 row " +
                "but deleteed " + count + " row(s)");
          }    
          ps.close();
        }
      //}
      
      // Sell Trade:
      // First look for existing holdings, H_QTY > 0
     if (hs_qty > 0) {
       if (is_lifo == 1) {
       // Could return 0, 1 or many rows
       //select H_T_ID, H_QTY, H_PRICE from HOLDING where H_CA_ID = acct_id and H_S_SYMB = symbol order by H_DTS desc for update
         ps = conn.prepareStatement(selectHoldingForUpdateDesc);       
       } else {
       // Could return 0, 1 or many rows
       //select H_T_ID, H_QTY, H_PRICE from HOLDING where H_CA_ID = acct_id and H_S_SYMB = symbol order by H_DTS asc for update
         ps = conn.prepareStatement(selectHoldingForUpdateAsc);
       }
       ps.setLong(1, acct_id);
       ps.setString(2, symbol);
       updatableRs = ps.executeQuery();
       int numOfInsertHH = 0;
       int numOfDeleteHolding = 0;
       PreparedStatement ps_insertHH = conn.prepareStatement(insertHoldingHistory);
       String updateHolding = isTicket41738Fixed  ? updateHoldingCurrentOf : updateHoldingPK; 
       PreparedStatement ps_updateHolding = isTicket41738Fixed  ? 
           conn.prepareStatement(updateHolding, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
           : conn.prepareStatement(updateHolding);
       if (isTicket41738Fixed) ps_updateHolding.setCursorName("updateCursor");
       
       String deleteHolding = isTicket41738Fixed  ? deleteHoldingCurrentOf : deleteHoldingPK; 
       PreparedStatement ps_deleteHolding = isTicket41738Fixed  ? 
           conn.prepareStatement(deleteHolding, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
           : conn.prepareStatement(deleteHolding);
       if (isTicket41738Fixed) ps_deleteHolding.setCursorName("updateCursor");
       
       while (updatableRs.next() && needed_qty != 0) {
         hold_id = updatableRs.getLong("H_T_ID");
         hold_qty = updatableRs.getInt("H_QTY");
         hold_price = updatableRs.getBigDecimal("H_PRICE");
             
         if (hold_qty > needed_qty) {//Selling some of the holdings
           //insert into HOLDING_HISTORY (HH_H_T_ID, HH_T_ID, HH_BEFORE_QTY, HH_AFTER_QTY) 
           //values (
           //hold_id, // H_T_ID of original trade
           //trade_id, // T_ID current trade
           //hold_qty, // H_QTY now
           //hold_qty - needed_qty // H_QTY after update
           //)       
           ps_insertHH.setLong(1, hold_id);
           ps_insertHH.setLong(2, trade_id);
           ps_insertHH.setInt(3, hold_qty);
           ps_insertHH.setInt(4, hold_qty - needed_qty);
           
           if (logDML) {
             Log.getLogWriter().info(insertHoldingHistory + " with HH_H_T_ID = " + hold_id + 
                 " HH_T_ID = " + trade_id + " HH_BEFORE_QTY = " + hold_qty +
                 " HH_AFTER_QTY = " + (hold_qty - needed_qty));
           }
           
           if (useBatchInsert) {
             ps_insertHH.addBatch();
             if (logDML) {
               Log.getLogWriter().info("add to batch insert");
             }
           }
           else {
             int count = ps_insertHH.executeUpdate();
             if (count != 1) {
               throw new TestException(insertHoldingHistory + " should insert 1 row " +
                   "but inserted " + count + " row(s)");
             }              
           }
           
           numOfInsertHH++; //how many inserts into holding history
           
           //update the holding table
           //update
           //HOLDING
           //set
           //H_QTY = hold_qty - needed_qty
           //where
           //current of hold_list
           ps_updateHolding.setInt(1, hold_qty - needed_qty);
           if (!isTicket41738Fixed) ps_updateHolding.setLong(2, hold_id);
           int count = ps_updateHolding.executeUpdate();
           
           if (logDML) {
             Log.getLogWriter().info(updateHolding + " with H_QTY  = " + 
                 hold_id + " - " + needed_qty + 
                 " for H_T_ID  = " + hold_id);
           } 
           
           if (count != 1) {
             throw new TestException(updateHolding + " should update 1 row " +
                "but updated " + count + " row(s)");
           } 
                      
           buy_value = buy_value.add(hold_price.multiply(
               new BigDecimal (String.valueOf(needed_qty))));
           sell_value = sell_value.add(trade_price.multiply(
               new BigDecimal (String.valueOf(needed_qty))));
           needed_qty = 0;
           
         } else {
           //values (hold_id, trade_id, hold_qty, 0)
           //hold_id, // H_T_ID original trade
           //trade_id, // T_ID current trade
           //hold_qty, // H_QTY now
           //0 // H_QTY after delete
           ps_insertHH.setLong(1, hold_id);
           ps_insertHH.setLong(2, trade_id);
           ps_insertHH.setInt(3, hold_qty);
           ps_insertHH.setInt(4, 0);
           
           if (logDML) {
             Log.getLogWriter().info(insertHoldingHistory + " with HH_H_T_ID = " + hold_id + 
                 " HH_T_ID = " + trade_id + " HH_BEFORE_QTY = " + hold_qty +
                 " HH_AFTER_QTY = " + 0);
           }
           
           if (useBatchInsert) {
             ps_insertHH.addBatch();
             if (logDML) {
               Log.getLogWriter().info("add to batch insert");
             }
           }
           else {
             int count = ps_insertHH.executeUpdate();
             if (count != 1) {
               throw new TestException(insertHoldingHistory + " should insert 1 row " +
                   "but inserted " + count + " row(s)");
             }   
           }
           numOfInsertHH++; //how many inserts into holding history
           
           //TODO numOfInsertHH could be potentially used to verify history rows being inserted.
           
           //delete from holding
           //delete from
           //HOLDING
           //where
           //current of hold_list
           
           if (!isTicket41738Fixed) ps_deleteHolding.setLong(1, hold_id);
           numOfDeleteHolding++;
           //TODO could be used with BB to verify holding table num of rows
           
           if (logDML) {
             Log.getLogWriter().info( deleteHolding + " with H_T_ID = " + hold_id);
           }
           
           
           if (useBatchDelete) {
             ps_deleteHolding.addBatch();
             if (logDML) {
               Log.getLogWriter().info("add to batch insert");
             }
           }
           else {
             int count = ps_deleteHolding.executeUpdate();
             if (count != 1) {
               throw new TestException(deleteHolding + " should delete 1 row " +
                   "but inserted " + count + " row(s)");
             }              
           }
              
           buy_value = buy_value.add(hold_price.multiply(
               new BigDecimal (String.valueOf(hold_qty))));
           sell_value = sell_value.add(trade_price.multiply(
               new BigDecimal (String.valueOf(hold_qty))));
           needed_qty = needed_qty - hold_qty;
         }
         
       }
       
       if (useBatchInsert) {
         int[] counts = ps_insertHH.executeBatch(); 
         //add batch insert case here
         if (counts.length != numOfInsertHH) {
           throw new TestException(insertHoldingHistory + " does not return correct number of" +
               " statement executed, should execute " + numOfInsertHH + " of statements but " +
               counts.length + " statements are executed");
         }
         for (int count: counts) {
           if (count != 1) {
             throw new TestException(insertHoldingHistory + " should insert 1 row " +
                 "for each statement in a batch but one of the statmement inserted " + count + " row(s)");
           }
         }
       }
       
       if (useBatchDelete) {
         //add batch delete case here
         int[] counts = ps_deleteHolding.executeBatch();
         
         if (counts.length != numOfDeleteHolding) {
           throw new TestException(deleteHolding + " does not return correct number of" +
              " statement executed, should execute " + numOfDeleteHolding + " of statements but " +
              counts.length + " statements are executed");
         }
         for (int count: counts) {
           if (count != 1) {
             throw new TestException(deleteHolding + " should delete 1 row " +
                 "for each statement in a batch but one of the statmement deleted " + count + " row(s)");
           }
         }
       }
       
       updatableRs.close(); //close hold_list
       ps.close(); 
     }
       
     // Sell Short:
     // If needed_qty > 0 then customer has sold all existing
     // holdings and customer is selling short.
     // A new HOLDING
     // record will be created with H_QTY set to the negative
     // number of needed shares.        
     if (needed_qty > 0) {
       //insert into
       //HOLDING_HISTORY (
       //HH_H_T_ID,
       //HH_T_ID,
       //HH_BEFORE_QTY,
       //HH_AFTER_QTY
       //)
       //values (
       //trade_id,
       // T_ID current is original trade
       // trade_id, // T_ID current trade
       //0, // H_QTY before
       //(-1) * needed_qty // H_QTY after insert
       //)
       PreparedStatement ps_insertHH = conn.prepareStatement(insertHoldingHistory);
       ps_insertHH.setLong(1, trade_id);
       ps_insertHH.setLong(2, trade_id);
       ps_insertHH.setInt(3, 0);
       ps_insertHH.setInt(4, (-1) * needed_qty);
      
       int count = ps_insertHH.executeUpdate();
       
       if (logDML) {
         Log.getLogWriter().info(insertHoldingHistory + " selling short with HH_H_T_ID = " + trade_id + 
             " HH_T_ID = " + trade_id + " HH_BEFORE_QTY = " + 0 +
             " HH_AFTER_QTY = " + ((-1) * needed_qty));
       }  
       if (count != 1) {
         throw new TestException(insertHoldingHistory + " should insert 1 row " +
            "but inserted " + count + " row(s)");
       } 
       //insert into
       //HOLDING (
       //H_T_ID,
       //H_CA_ID,
       //H_S_SYMB,
       //H_DTS,
       //H_PRICE,
       //H_QTY
       //)
       //values (
       //trade_id, // H_T_ID
       //acct_id, // H_CA_ID
       //symbol, // H_S_SYMB
       //trade_dts, // H_DTS
       //trade_price, // H_PRICE
       //(-1) * needed_qty //* H_QTY
       //)
       ps = conn.prepareStatement(insertHolding);
       ps.setLong(1, trade_id);
       ps.setLong(2, acct_id);
       ps.setString(3, symbol);
       ps.setTimestamp(4, trade_dts);
       ps.setBigDecimal(5, trade_price);
       ps.setInt(6, (-1) * needed_qty);
       
       count = ps.executeUpdate();
       
       if (logDML) {
         Log.getLogWriter().info(insertHolding + " with H_T_ID = " + trade_id + 
             " H_CA_ID = " + acct_id + " H_S_SYMB = " + symbol +
             " H_DTS = " + trade_dts + " H_PRICE = " + trade_price +
             " H_QTY = " + ((-1) * needed_qty));
       }  
       if (count != 1) {
         throw new TestException(insertHolding + " should insert 1 row " +
            "but inserted " + count + " row(s)");
       }       
     }      
    } else { // The trade is a BUY
      /* inserts in frame1 now, will use update for hs_qty == 0 case as well)
      if (hs_qty == 0) {
      // no prior holdings exist, but one will be inserted
        ps = conn.prepareStatement(insertHSTable);
        ps.setLong(1, acct_id);
        ps.setString(2, symbol);
        ps.setInt(3, trade_qty);
      
        int count = ps.executeUpdate();
        
        if (logDML) {
          Log.getLogWriter().info(insertHSTable + " with HS_CA_ID = " + acct_id + 
              " HS_S_SYMB = " + symbol + " HS_QTY = " + trade_qty);
        }
        
        if (count != 1) {
          throw new TestException(insertHSTable  + " should insert 1 row " +
                   "but inserted " + count + " row(s)");
          //test need to make sure no other tx insert into hstable at same time as well. 
          //need to feed correct data, so that either only the thread process same ca and symbol in a batch
          //or no same ca and symbol in a batch to cause the issue.
          //or catch conflict exception here and do a retry, using certain flag 
        }    
        ps.close();
      } else { */
        if (-hs_qty != trade_qty) {
          // update HOLDING_SUMMARY (tbl) set HS_QTY = hs_qty + trade_qty where HS_CA_ID = acct_id and HS_S_SYMB = symbol
          ps = conn.prepareStatement(updateHSForBuy);          
          ps.setInt(1, trade_qty);
          ps.setLong(2, acct_id);
          ps.setString(3, symbol);          
       
          int count = ps.executeUpdate();
         
          if (logDML) {
            Log.getLogWriter().info(updateHSForBuy + " with HS_CA_ID = " + acct_id + 
                " HS_S_SYMB = " + symbol + " HS_QTY = HS_QTY + " + trade_qty);
          }
          if (count != 1) {
            throw new TestException(updateHSForSell + " should update 1 row " +
                "but updated " + count + " row(s)");
           //test need to make sure no other tx update hstable at same time as well. 
           //need to feed correct data, so that either only one thread process same ca and symbol in a batch
           //or no same ca and symbol in a batch to cause the issue.
           //or catch conflict exception here and do a retry, using flag to check if conflict possible
          }    
          ps.close();
        } else { //if (-hs_qty = trade_qty) then
          //delete from
          //HOLDING_SUMMARY
          //where
          //HS_CA_ID = acct_id and
          //HS_S_SYMB = symbol
          //}
          ps = conn.prepareStatement(deleteHS);          
          ps.setLong(1, acct_id);
          ps.setString(2, symbol);       
          int count = ps.executeUpdate();
         
          if (logDML) {
            Log.getLogWriter().info(deleteHS + " for HS_CA_ID = " + acct_id + 
                " HS_S_SYMB = " + symbol);
          }
          if (count != 1) {
            throw new TestException(deleteHS + " should delete 1 row " +
                "but deleteed " + count + " row(s)");
          }    
          ps.close();
        }
      //}
        
        
      // Short Cover:
      // First look for existing negative holdings, H_QTY < 0,
      // which indicates a previous short sell. The buy trade
      // will cover the short sell.
      
      if (hs_qty < 0) {
        if (is_lifo == 1) {
        // Could return 0, 1 or many rows
        //select H_T_ID, H_QTY, H_PRICE from HOLDING where H_CA_ID = acct_id and H_S_SYMB = symbol order by H_DTS desc for update
          ps = conn.prepareStatement(selectHoldingForUpdateDesc);       
        } else {
        // Could return 0, 1 or many rows
        //select H_T_ID, H_QTY, H_PRICE from HOLDING where H_CA_ID = acct_id and H_S_SYMB = symbol order by H_DTS asc for update
          ps = conn.prepareStatement(selectHoldingForUpdateAsc);
        }
        ps.setLong(1, acct_id);
        ps.setString(2, symbol);
        updatableRs = ps.executeQuery();

        int numOfInsertHH = 0;
        int numOfDeleteHolding = 0;
        PreparedStatement ps_insertHH = conn.prepareStatement(insertHoldingHistory);
        String updateHolding = isTicket41738Fixed  ? updateHoldingCurrentOf : updateHoldingPK; 
        PreparedStatement ps_updateHolding = isTicket41738Fixed  ? 
            conn.prepareStatement(updateHolding, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
            : conn.prepareStatement(updateHolding);
        if (isTicket41738Fixed) ps_updateHolding.setCursorName("updateCursor");
        
        String deleteHolding = isTicket41738Fixed  ? deleteHoldingCurrentOf : deleteHoldingPK; 
        PreparedStatement ps_deleteHolding = isTicket41738Fixed  ? 
            conn.prepareStatement(deleteHolding, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
            : conn.prepareStatement(deleteHolding);
        if (isTicket41738Fixed) ps_deleteHolding.setCursorName("updateCursor");
        
        // Buy back securities to cover a short position.
        while (updatableRs.next() && needed_qty != 0) {
          hold_id = updatableRs.getLong("H_T_ID");
          hold_qty = updatableRs.getInt("H_QTY");
          hold_price = updatableRs.getBigDecimal("H_PRICE");
              
          if (hold_qty + needed_qty < 0) {// Buying back some of the Short Sell
            //insert into HOLDING_HISTORY (HH_H_T_ID, HH_T_ID, HH_BEFORE_QTY, HH_AFTER_QTY) 
            //values (
            //hold_id, // H_T_ID of original trade
            //trade_id, // T_ID current trade
            //hold_qty, // H_QTY now
            //hold_qty + needed_qty // H_QTY after update
            //)       
            ps_insertHH.setLong(1, hold_id);
            ps_insertHH.setLong(2, trade_id);
            ps_insertHH.setInt(3, hold_qty);
            ps_insertHH.setInt(4, hold_qty + needed_qty);
            
            if (logDML) {
              Log.getLogWriter().info(insertHoldingHistory + " with HH_H_T_ID = " + hold_id + 
                  " HH_T_ID = " + trade_id + " HH_BEFORE_QTY = " + hold_qty +
                  " HH_AFTER_QTY = " + (hold_qty + needed_qty));
            }
            
            if (useBatchInsert) {
              ps_insertHH.addBatch();
              if (logDML) {
                Log.getLogWriter().info("add to batch insert");
              }
            }
            else {
              int count = ps_insertHH.executeUpdate();
              if (count != 1) {
                throw new TestException(insertHoldingHistory + " should insert 1 row " +
                    "but inserted " + count + " row(s)");
              }              
            }
            
            numOfInsertHH++; //how many inserts into holding history
            
            //update the holding table
            //update
            //HOLDING
            //set
            //H_QTY = hold_qty + needed_qty
            //where
            //current of hold_list
            
            ps_updateHolding.setInt(1, hold_qty + needed_qty);
            if (!isTicket41738Fixed) ps_updateHolding.setLong(2, hold_id);
            int count = ps_updateHolding.executeUpdate();
            
            if (logDML) {
              Log.getLogWriter().info(updateHolding + " with H_QTY  = " + 
                  hold_id + " - " + needed_qty + 
                  " for H_T_ID  = " + hold_id);
            } 
            
            if (count != 1) {
              throw new TestException(updateHolding + " should update 1 row " +
                 "but updated " + count + " row(s)");
            } 
            //sell_value += needed_qty * hold_price
            //buy_value += needed_qty * trade_price           
            buy_value = buy_value.add(trade_price.multiply(
                new BigDecimal (String.valueOf(needed_qty))));
            sell_value = sell_value.add(hold_price.multiply(
                new BigDecimal (String.valueOf(needed_qty))));
            needed_qty = 0;
            
          } else { // Buying back all of the Short Sell
            //values (hold_id, trade_id, hold_qty, 0)
            //hold_id, // H_T_ID original trade
            //trade_id, // T_ID current trade
            //hold_qty, // H_QTY now
            //0 // H_QTY after delete
            ps_insertHH.setLong(1, hold_id);
            ps_insertHH.setLong(2, trade_id);
            ps_insertHH.setInt(3, hold_qty);
            ps_insertHH.setInt(4, 0);
            
            if (logDML) {
              Log.getLogWriter().info(insertHoldingHistory + " with HH_H_T_ID = " + hold_id + 
                  " HH_T_ID = " + trade_id + " HH_BEFORE_QTY = " + hold_qty +
                  " HH_AFTER_QTY = " + 0);
            }
            
            if (useBatchInsert) {
              ps_insertHH.addBatch();
              if (logDML) {
                Log.getLogWriter().info("add to batch insert");
              }
            }
            else {
              int count = ps_insertHH.executeUpdate();
              if (count != 1) {
                throw new TestException(insertHoldingHistory + " should insert 1 row " +
                    "but inserted " + count + " row(s)");
              }   
            }
            numOfInsertHH++; //how many inserts into holding history
            
            //TODO numOfInsertHH could be potentially used to verify history rows being inserted.
            
            //delete from holding
            //delete from
            //HOLDING
            //where
            //current of hold_list

            if (!isTicket41738Fixed) ps_deleteHolding.setLong(1, hold_id);
            numOfDeleteHolding++;
            //TODO could be used with BB to verify holding table num of rows
            
            if (logDML) {
              Log.getLogWriter().info( deleteHolding + " with H_T_ID = " + hold_id);
            }
            
            
            if (useBatchDelete) {
              ps_deleteHolding.addBatch();
              if (logDML) {
                Log.getLogWriter().info("add to batch insert");
              }
            }
            else {
              int count = ps_deleteHolding.executeUpdate();
              if (count != 1) {
                throw new TestException(deleteHolding + " should delete 1 row " +
                    "but inserted " + count + " row(s)");
              }              
            }
              
            // Make hold_qty positive for easy calculations
            hold_qty = -hold_qty;
            //sell_value += hold_qty * hold_price
            //buy_value += hold_qty * trade_price
            buy_value = buy_value.add(trade_price.multiply(
                new BigDecimal (String.valueOf(hold_qty))));
            sell_value = sell_value.add(hold_price.multiply(
                new BigDecimal (String.valueOf(hold_qty))));
            needed_qty = needed_qty - hold_qty;
          }
          
        }
        
        if (useBatchInsert) {
          int[] counts = ps_insertHH.executeBatch(); 
          //add batch insert case here
          if (counts.length != numOfInsertHH) {
            throw new TestException(insertHoldingHistory + " does not return correct number of" +
                " statement executed, should execute " + numOfInsertHH + " of statements but " +
                counts.length + " statements are executed");
          }
          for (int count: counts) {
            if (count != 1) {
              throw new TestException(insertHoldingHistory + " should insert 1 row " +
                  "for each statement in a batch but one of the statmement inserted " + count + " row(s)");
            }
          }
        }
        
        if (useBatchDelete) {
          //add batch delete case here
          int[] counts = ps_deleteHolding.executeBatch();
          
          if (counts.length != numOfDeleteHolding) {
            throw new TestException(deleteHolding + " does not return correct number of" +
               " statement executed, should execute " + numOfDeleteHolding + " of statements but " +
               counts.length + " statements are executed");
          }
          for (int count: counts) {
            if (count != 1) {
              throw new TestException(deleteHolding + " should delete 1 row " +
                  "for each statement in a batch but one of the statmement deleted " + count + " row(s)");
            }
          }
        }
        
        updatableRs.close(); //close hold_list
        ps.close(); 
      }
      
      // Buy Trade:
      // If needed_qty > 0, then the customer has covered all
      // previous Short Sells and the customer is buying new
      // holdings. A new HOLDING record will be created with
      // H_QTY set to the number of needed shares.
      if (needed_qty > 0) {
        //insert into
        //HOLDING_HISTORY (
        //HH_H_T_ID,
        //HH_T_ID,
        //HH_BEFORE_QTY,
        //HH_AFTER_QTY
        //)
        //values (
        //trade_id,
        // T_ID current is original trade
        // trade_id, // T_ID current trade
        //0, // H_QTY before
        //needed_qty // H_QTY after insert
        //)
        
        PreparedStatement ps_insertHH = conn.prepareStatement(insertHoldingHistory);
        ps_insertHH.setLong(1, trade_id);
        ps_insertHH.setLong(2, trade_id);
        ps_insertHH.setInt(3, 0);
        ps_insertHH.setInt(4, needed_qty);
       
        int count = ps_insertHH.executeUpdate();
        
        if (logDML) {
          Log.getLogWriter().info(insertHoldingHistory + " buying long with HH_H_T_ID = " + trade_id + 
              " HH_T_ID = " + trade_id + " HH_BEFORE_QTY = " + 0 +
              " HH_AFTER_QTY = " + needed_qty);
        }  
        if (count != 1) {
          throw new TestException(insertHoldingHistory + " should insert 1 row " +
             "but inserted " + count + " row(s)");
        } 
        //insert into
        //HOLDING (
        //H_T_ID,
        //H_CA_ID,
        //H_S_SYMB,
        //H_DTS,
        //H_PRICE,
        //H_QTY
        //)
        //values (
        //trade_id, // H_T_ID
        //acct_id, // H_CA_ID
        //symbol, // H_S_SYMB
        //trade_dts, // H_DTS
        //trade_price, // H_PRICE
        //needed_qty //* H_QTY
        //)
        
        ps = conn.prepareStatement(insertHolding);
        ps.setLong(1, trade_id);
        ps.setLong(2, acct_id);
        ps.setString(3, symbol);
        ps.setTimestamp(4, trade_dts);
        ps.setBigDecimal(5, trade_price);
        ps.setInt(6, needed_qty);
        
        count = ps.executeUpdate();
        
        if (logDML) {
          Log.getLogWriter().info(insertHolding + " with H_T_ID = " + trade_id + 
              " H_CA_ID = " + acct_id + " H_S_SYMB = " + symbol +
              " H_DTS = " + trade_dts + " H_PRICE = " + trade_price +
              " H_QTY = " + needed_qty);
        }  
        if (count != 1) {
          throw new TestException(insertHolding + " should insert 1 row " +
             "but inserted " + count + " row(s)");
        }       
      }
     
    }

  }
  
  protected void invokeFrame3() throws SQLException {
    BigDecimal tax_rates;
    PreparedStatement ps= conn.prepareStatement(selectTaxRate);
    ps.setLong(1, cust_id);     
    ResultSet rs = ps.executeQuery();
   
    if (rs.next()) {
      tax_rates = rs.getBigDecimal(1);
      if (logDML) Log.getLogWriter().info("tax rate is " + tax_rates);
      if (rs.next()) {
        throw new TestException ( selectTaxRate + " has more than 1 row " +
            "in result set for cx_c_id = " + cust_id);
      }
    } else {
      throw new TestException ( selectTaxRate + " does not get single row " +
          "in result set for cx_c_id = " + cust_id);
    }     
    rs.close();
    
    tax_amount = tax_rates.multiply(sell_value.subtract(buy_value));
    
    //update
    //TRADE
    //set
    //T_TAX = tax_amount
    //where
    //T_ID = trade_id
    ps = conn.prepareStatement(updateTaxAmount);
    ps.setBigDecimal(1, tax_amount);
    ps.setLong(2, trade_id);
    
    int count = ps.executeUpdate();
    
    if (logDML) {
      Log.getLogWriter().info(updateTaxAmount + " updates T_TAX = " + tax_amount + 
          " for T_ID = " + trade_id);
    }  
    if (count != 1) {
      throw new TestException(updateTaxAmount + " should updates only 1 row " +
         "but updated " + count + " row(s)");
    }  

  }
  
  protected void invokeFrame4() throws SQLException {
    String s_ex_id;
    short c_tier;
    //select
    //s_ex_id = S_EX_ID,
    //s_name
    //= S_NAME
    //from
    //SECURITY
    //where
    //S_SYMB = symbol
    PreparedStatement ps= conn.prepareStatement(selectSecurity);
    ps.setString(1, symbol);     
    ResultSet rs = ps.executeQuery();
   
    if (rs.next()) {
      s_ex_id = rs.getString("S_EX_ID");
      s_name = rs.getString(2); //S_NAME
      if (logDML) Log.getLogWriter().info(selectSecurity + " gets S_EX_ID: " + s_ex_id +
          " S_NAME: " + s_name + " for S_SYMB = " + symbol);
      if (rs.next()) {
        throw new TestException ( selectTaxRate + " has more than 1 row " +
            "in result set for S_SYMB = " + symbol);
      }
    } else {
      throw new TestException ( selectTaxRate + " does not get single row " +
          "in result set for S_SYMB = " + symbol);
    }     
    rs.close();
    ps.close();
    
    //select
    //c_tier = C_TIER
    //from
    //CUSTOMER
    //where
    //C_ID = cust_id
    
    ps= conn.prepareStatement(selectCustomer);
    ps.setLong(1, cust_id);     
    rs = ps.executeQuery();
   
    if (rs.next()) {
      c_tier = rs.getShort("C_TIER");

      if (logDML) Log.getLogWriter().info(selectCustomer + " gets C_TIER: " + c_tier +
          " S_NAME: " + s_name + " for C_ID = " + cust_id);
      if (rs.next()) {
        throw new TestException (selectCustomer + " has more than 1 row " +
            "in result set for C_ID = " + cust_id);
      }
    } else {
      throw new TestException (selectCustomer + " does not get single row " +
          "in result set for C_ID = " + cust_id);
    }     
    rs.close();
    ps.close();
    
    // Only want 1 commission rate row
    //select first 1 row
    //comm_rate = CR_RATE
    //from
    //COMMISSION_RATE
    //where
    //CR_C_TIER = c_tier and
    //CR_TT_ID = type_id and
    //CR_EX_ID = s_ex_id and
    //CR_FROM_QTY <= trade_qty and
    //CR_TO_QTY >= trade_qty

    ps= conn.prepareStatement(selectCommRate);
    ps.setShort(1, c_tier); 
    ps.setString(2, type_id);
    ps.setString(3, s_ex_id);
    ps.setInt(4, trade_qty);
    ps.setInt(5, trade_qty);
    rs = ps.executeQuery();
   
    if (rs.next()) {
      comm_rate = rs.getBigDecimal("CR_RATE");

      if (logDML) Log.getLogWriter().info(selectCommRate + " gets CR_RATE: " + comm_rate + 
          " for CR_C_TIER = " + c_tier + " CR_TT_ID = " + type_id + " CR_EX_ID = " + s_ex_id + 
          " CR_FROM_QTY <= " + trade_qty + " CR_TO_QTY >= " + trade_qty);
      if (rs.next()) {
        throw new TestException (selectCommRate + " has more than 1 row " +
            "in result set for CR_C_TIER = " + c_tier + " CR_TT_ID = " + type_id + " CR_EX_ID = " + s_ex_id + 
          " CR_FROM_QTY <= " + trade_qty + " CR_TO_QTY >= " + trade_qty);
      }
    } else {
      throw new TestException (selectCommRate + " does not get single row " +
          "in result set for CR_C_TIER = " + c_tier + " CR_TT_ID = " + type_id + " CR_EX_ID = " + s_ex_id + 
          " CR_FROM_QTY <= " + trade_qty + " CR_TO_QTY >= " + trade_qty);
    }     
    rs.close();
    ps.close();
  }

  protected void invokeFrame5() throws SQLException {
    //update
    //TRADE
    //set
    //T_COMM = comm_amount,
    //T_DTS = trade_dts,
    //T_ST_ID = st_completed_id,
    //T_TRADE_PRICE = trade_price
    //where
    //T_ID = trade_id
    
    PreparedStatement ps= conn.prepareStatement(updateTrade);
    ps.setBigDecimal(1, comm_amount);   
    ps.setTimestamp(2, trade_dts);
    ps.setString(3, st_completed_id);
    ps.setBigDecimal(4, trade_price);
    ps.setLong(5, trade_id);
    
    int count = ps.executeUpdate();
    
    if (logDML) {
      Log.getLogWriter().info(updateTrade + " updates T_COMM = " + comm_amount + 
          " T_DTS = " + trade_dts + " T_ST_ID = " + st_completed_id + 
          " T_TRADE_PRICE = " + trade_price + " for T_ID = " + trade_id);
    }  
    if (count != 1) {
      throw new TestException(updateTrade + " should updates only 1 row " +
         "but updated " + count + " row(s) for T_ID = " + trade_id);
    }  
    ps.close();
    
    //insert into
    //TRADE_HISTORY (
    //TH_T_ID,
    //TH_DTS,
    //TH_ST_ID
    //)
    //values (
    //trade_id,
    //trade_dts,
    //st_completed_id
    //)
    ps = conn.prepareStatement(insertTradeHistory);
    ps.setLong(1, trade_id); //TH_T_ID
    ps.setTimestamp(2, trade_dts); //TH_DTS
    ps.setString(3, st_completed_id); // TH_ST_ID
    
    count = ps.executeUpdate();
    
    if (logDML) {
      Log.getLogWriter().info(insertTradeHistory + " with TH_T_ID = " + trade_id + 
          " TH_DTS = " + trade_dts + " TH_ST_ID = " + st_completed_id);
    }
    
    if (count != 1)
      throw new TestException(insertTradeHistory + " should inserts 1 row " +
          "but inserted " + count + " row(s)");  
    
    ps.close();
    
    //update
    //BROKER
    //set
    //B_COMM_TOTAL = B_COMM_TOTAL + comm_amount,
    //B_NUM_TRADES = B_NUM_TRADES + 1
    //where
    //B_ID = broker_id
    ps = conn.prepareStatement(updateBroker);
    ps.setBigDecimal(1, comm_amount); 
    ps.setLong(2, broker_id); 
    
    count = ps.executeUpdate();
    
    if (logDML) {
      Log.getLogWriter().info(updateBroker + " set B_COMM_TOTAL = B_COMM_TOTAL + " + 
          comm_amount +  " B_NUM_TRADES = B_NUM_TRADES + 1 for B_ID = " + broker_id);
    }
    
    if (count != 1)
      throw new TestException(updateBroker + " should updates 1 row " +
          "but updated " + count + " row(s) for B_ID = " + broker_id);
    
    ps.close();
    
  }


  protected void invokeFrame6() throws SQLException {
    //Local Frame Variables
    String cash_type;
    if (trade_is_cash == 1) {
      cash_type = "Cash Account";
    } else {
      cash_type ="Margin";
    }
    
    //insert into
    //SETTLEMENT (
    //SE_T_ID,
    //SE_CASH_TYPE,
    //SE_CASH_DUE_DATE,
    //SE_AMT
    //)
    //values (
    //trade_id,
    //cash_type,
    //due_date,
    //se_amount
    //)
    
    PreparedStatement ps = conn.prepareStatement(insertSettlement);
    ps.setLong(1, trade_id); //SE_T_ID,
    ps.setString(2, cash_type); //SE_CASH_TYPE,
    ps.setDate(3, due_date); // SE_CASH_DUE_DATE
    ps.setBigDecimal(4, se_amount); // SE_AMT
    
    int count = ps.executeUpdate();
    
    if (logDML) {
      Log.getLogWriter().info(insertSettlement + " with SE_T_ID = " + trade_id + 
          " SE_CASH_TYPE = " + cash_type + " SE_CASH_DUE_DATE = " + due_date +
          " SE_AMT = " + se_amount);
    }
    
    if (count != 1)
      throw new TestException(insertSettlement + " with SE_T_ID = " + trade_id + 
          " SE_CASH_TYPE = " + cash_type + " SE_CASH_DUE_DATE = " + due_date +
          " SE_AMT = " + se_amount + " should inserts 1 row " +
          "but inserted " + count + " row(s)");  
    
    ps.close();
    
    if (trade_is_cash == 1){
      //update
      //CUSTOMER_ACCOUNT
      //set
      //CA_BAL = CA_BAL + se_amount
      //where
      //CA_ID = acct_id  
      ps = conn.prepareStatement(updateCustomerAccount);
      ps.setBigDecimal(1, se_amount); 
      ps.setLong(2, acct_id);
      //count = ps.executeUpdate();
      
      if (logDML) {
        Log.getLogWriter().info(updateCustomerAccount + " with CA_BAL = CA_BAL + " + 
            se_amount + " for CA_ID = " + acct_id );
      }
      
      count = ps.executeUpdate();

      if (count != 1)
        throw new TestException(updateCustomerAccount + " for CA_ID = " + acct_id + 
            " should inserts 1 row but inserted " + count + " row(s)");  
      
      ps.close();
    
      //insert into
      //CASH_TRANSACTION (
      //CT_DTS,
      //CT_T_ID,
      //CT_AMT,
      //CT_NAME
      //)
      //values (
      //trade_dts,
      //trade_id,
      //se_amount,
      //
      //)
      
      ps = conn.prepareStatement(insertCashTransaction);
      ps.setTimestamp(1, trade_dts); //CT_DTS,
      ps.setLong(2, trade_id); //CT_T_ID,
      ps.setBigDecimal(3, se_amount); // CT_AMT
      ps.setString(4, type_name + " " + trade_qty + " shares of " + s_name); // CT_NAME
      
      count = ps.executeUpdate();
      
      if (logDML) {
        Log.getLogWriter().info(insertCashTransaction + " with CT_DTS = " + trade_dts + 
            " CT_T_ID = " + trade_id + " CT_AMT = " + se_amount +
            " CT_NAME = " + type_name + " " + trade_qty + " shares of " + s_name);
      }
      
      if (count != 1)
        throw new TestException(insertCashTransaction + " with CT_DTS = " + trade_dts + 
          " CT_T_ID = " + trade_id + " CT_AMT = " + se_amount +
          " CT_NAME = " + type_name + " " + trade_qty + " shares of " + s_name + 
            " should inserts 1 row but inserted " + count + " row(s)");  
      
      ps.close();

    }
    
    //select
    //acct_bal = CA_BAL
    //from
    //CUSTOMER_ACCOUNT
    //where
    //CA_ID = acct_id
    ps= conn.prepareStatement(selectCABalance); 
    ps.setLong(1, acct_id);
    ResultSet rs = ps.executeQuery();
   
    if (rs.next()) {
      acct_bal = rs.getBigDecimal("CA_BAL");
      if (logDML) Log.getLogWriter().info(selectCABalance + " gets CA_BAL: " + acct_bal +
           " for SCA_ID = " + acct_id);
      if (rs.next()) {
        throw new TestException ( selectCABalance + " for SCA_ID = " + acct_id +
            " has more than 1 row " +
            "in result set for S_SYMB = " + symbol);
      }
    } else {
      throw new TestException ( selectCABalance + " for SCA_ID = " + acct_id +
          " does not get single row " +
          "in result set for S_SYMB = " + symbol);
    }     
    rs.close();
    ps.close();
    
    
    //commit transaction
    conn.commit();
    if (logDML) Log.getLogWriter().info("committed the trade_result_txn");
  }

}
