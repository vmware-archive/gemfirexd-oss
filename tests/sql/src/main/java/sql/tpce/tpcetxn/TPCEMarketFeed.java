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
import hydra.RemoteTestModule;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import java.sql.PreparedStatement;
import java.util.ArrayList;

import sql.SQLHelper;
import sql.tpce.TPCEBB;
import sql.tpce.TPCETest;
import sql.tpce.entity.TradeInfo;
import sql.tpce.tpcedef.TPCETxnInput;
import sql.tpce.tpcedef.TPCETxnOutput;
import sql.tpce.tpcedef.generator.MEE;
import sql.tpce.tpcedef.input.MarketFeedTxnInput;
import sql.tpce.tpcedef.output.MarketFeedTxnOutput;
import util.TestException;
import util.TestHelper;

public class TPCEMarketFeed extends TPCETransaction {
  protected MarketFeedTxnInput mfTxnInput = null;
  protected MarketFeedTxnOutput mfTxnOutput = null;
  protected Connection conn = null;

  private int num_updated;
  private int send_len;

  private BigDecimal[] price_quote;
  private int[] trade_qty;
  private String[] symbol;
  private int max_feed_len;

  private String type_stop_loss;
  private String type_limit_sell;
  private String type_limit_buy;
  private String status_submitted;

  private static String updateLastTrade = "update LAST_TRADE set LT_PRICE = ?, LT_VOL = LT_VOL + ?, LT_DTS = ? where LT_S_SYMB = ?";
  private static String selectTradeRequestForUpdate = "select TR_T_ID, TR_BID_PRICE, "
      + "TR_TT_ID, TR_QTY from TRADE_REQUEST -- GEMFIREXD-PROPERTIES index=i_tr_s_symb \n "
      + "where TR_S_SYMB = ? and ((TR_TT_ID = ? and TR_BID_PRICE >= ?) or "
      + "(TR_TT_ID = ? and TR_BID_PRICE <= ?) or (TR_TT_ID = ? and TR_BID_PRICE >= ?)) for update";
  // this select for update is not necessary as the update last_trade should
  // prevent any other
  // txns update on the trade_request table with the same symbol, but this is
  // going to be used
  // for delete current row (which is not supported yet in gfxd)

  // There is an issue here implementing the update: how do we guarantee the
  // last_trade updated (though data feed) is based on the time (now_dts)
  // it is possible for same symbol in last_trade (in different txnInputs)
  // a txn could update it first even though the timestamp is later
  //
  // in wait mode, the first txn may not be able to update a symbol and waiting
  // three txns processes txnInputs in serial from market feed, but concurrently
  // txn1 update symbol a, b, c
  // txn2 update symbol b, d, g, m
  // txn3 update symbol c
  //
  // assume txn1 updates a, but waits on txn2 to finish as it holds the lock for
  // b already
  // txn3 will be able to get lock on c before txn1 does and commit before txn1
  // does.
  // this could cause the now_dts updates in wrong order in last_trade based on
  // timestamp
  // will use new timestamp for each symbol/ticker iteration
  //
  // similarly with fail fast mode, two txns concurrently update symbols --
  // txn1 update symbol a, d, g
  // txn2 update symbol d, e, f
  //
  // it is possible for txn locks a, performs update and process trade_request
  // but fail get lock on d as txn2 gets the lock first on the row,
  // txn1 will gets conflict exception and retries,
  // txn2 will instead updates the late trade earlier than txn1 -- though
  // the now_dts will be in order, as the retries gets new now_dts
  // -- this is a logic issue from tpc-e
  // instead of gemfirexd issue, test will allow this to occur
  //

  private static String updateTrade = "update TRADE set T_DTS = ?, T_ST_ID = ? where T_ID = ?";
  // private static String deleteTradeRequest =
  // "    delete TRADE_REQUEST where current of request_list";

  private static String deleteTradeRequest = "delete from TRADE_REQUEST where TR_T_ID = ?"; // using PK based
  private static final String insertTradeHistory = "insert into TRADE_HISTORY(TH_T_ID, TH_DTS, TH_ST_ID) values (?, ?, ?)";

  @Override
  public TPCETxnOutput runTxn(TPCETxnInput txnInput, Connection conn)
      throws SQLException {
    mfTxnInput = (MarketFeedTxnInput) txnInput;
    mfTxnOutput = new MarketFeedTxnOutput();
    this.conn = conn;
    MEE mee = new MEE();

    this.price_quote = mfTxnInput.getPriceQuotes();
    this.symbol = mfTxnInput.getSymbol();
    this.trade_qty = mfTxnInput.getTradeQty();
    this.type_limit_buy = mfTxnInput.getLimitBuy();
    this.type_limit_sell = mfTxnInput.getLimitSell();
    this.type_stop_loss = mfTxnInput.getStopLoss();
    this.status_submitted = mfTxnInput.getStatusSubmitted();
    this.max_feed_len = symbol.length;
    if (logDML) {
      StringBuilder symbols = new StringBuilder();
      for (String symb: symbol) symbols.append(symb);
      Log.getLogWriter().info("this.symbol is assigned " + symbols.toString());
    }
        

    invokeFrame1(mee);
    
    if (num_updated < symbol.length) {
      mfTxnOutput.setStatus(-311);
      if (logDML) throw new TestException("Update last_trade should work as no " +
      		"symbol was deleted in the test run but for " + symbol.length + " of unique symbols, " +
      		"gfxd only updates " + num_updated + " in last_trade");
    }

    mfTxnOutput.setNumUpdated(num_updated);
    mfTxnOutput.setSendLen(send_len);

    return mfTxnOutput;
  }

  protected void invokeFrame1(MEE mee) throws SQLException {
    int rows_updated;
    int rows_updated_thisTicker;

    rows_updated = 0;
    PreparedStatement updateLastTradePs = conn
        .prepareStatement(updateLastTrade);
    PreparedStatement updateTradePs = conn.prepareStatement(updateTrade);
    PreparedStatement selectTrPs = conn
        .prepareStatement(selectTradeRequestForUpdate);
    PreparedStatement deleteTradeRequestPs = conn.prepareStatement(deleteTradeRequest);
    PreparedStatement insertTradeHistoryPs = conn.prepareStatement(insertTradeHistory);
    boolean retry;
    for (int i = 0; i < max_feed_len; i++) {
      retry = true;
      while (retry) {
        try {
          rows_updated_thisTicker = processTxnForSymbol(i, mee, updateLastTradePs,
              updateTradePs, selectTrPs, deleteTradeRequestPs, insertTradeHistoryPs);
          retry = false;
          rows_updated += rows_updated_thisTicker;
        } catch (SQLException se) {
          if (se.getSQLState().equals("X0Z02")) {
            if (logDML)
              Log.getLogWriter().info(
                  "will retry this op due to conflict exception X0Z02");
          } else {            
            //need to retry here for HA failure, as the each symbol process is committed 
            //from tpc-e logic, so retries is needed here to complete the whole txn.
            SQLHelper.handleSQLException(se); 
          }
            
        }
      }
    }
    num_updated = rows_updated;
  }

  protected int processTxnForSymbol(int i, MEE mee, PreparedStatement updateLastTradePs,
      PreparedStatement updateTradePs, PreparedStatement selectTrPs, 
      PreparedStatement deleteTradeRequestPs, PreparedStatement insertTradeHistoryPs) 
      throws SQLException {
    ArrayList<TradeInfo> tradeRequest = new ArrayList<TradeInfo>();

    // start transaction
    int rows_updated_thisTicker = 0;

    Timestamp now_dts = new Timestamp(System.currentTimeMillis());
    // use new timestamp for each symbol, so that write to last_trade will be
    // in order from now_dts value

    // update
    // LAST_TRADE
    // set
    // LT_PRICE = price_quote[i],
    // LT_VOL = LT_VOL + trade_qty[i],
    // LT_DTS = now_dts //current_timestamp
    // where
    // LT_S_SYMB = symbol[i]

    updateLastTradePs.setBigDecimal(1, price_quote[i]);
    updateLastTradePs.setInt(2, trade_qty[i]);
    updateLastTradePs.setTimestamp(3, now_dts);
    updateLastTradePs.setString(4, symbol[i]);

    int count = updateLastTradePs.executeUpdate();

    if (logDML) {
      Log.getLogWriter().info(
          updateLastTrade + " with LT_PRICE = " + price_quote[i]
              + " LT_VOL = LT_VOL + " + trade_qty[i] + " LT_DTS - " + now_dts
              + " for LT_S_SYMB = " + symbol[i]);
    }
    if (count != 1) {
      throw new TestException(updateLastTrade + " with LT_PRICE = "
          + price_quote[i] + " LT_VOL = LT_VOL + " + trade_qty[i]
          + " LT_DTS - " + now_dts + " for LT_S_SYMB = " + symbol[i]
          + " should update 1 row " + "but updated " + count + " row(s)");
      // test need to make sure no other tx insert into hstable at same time as
      // well.
      // need to feed correct data, so that either only one thread process same
      // ca and symbol in a batch
      // or no same ca and symbol in a batch to cause the issue.
      // or catch conflict exception here and do a retry, using flag to check if
      // conflict possible
    }
    rows_updated_thisTicker += count;

    processSelectTrForUpdate(i, updateTradePs, selectTrPs, now_dts, deleteTradeRequestPs, 
        insertTradeHistoryPs, tradeRequest);
    
    //close request_list
    conn.commit();
    if (TPCETest.logDML) Log.getLogWriter().info("committed market_feed_txn");
    
    send_len = send_len + tradeRequest.size();
    
    //send triggered trades to the Market Exchange Emulator
    //via the SendToMarket interface.
    //This should be done
    //after the related database changes have committed
    //For (j=0; j<rows_sent; j++)
    //{
    //SendToMarketFromFrame(TradeRequestBuffer[i].symbol,
    //TradeRequestBuffer[i].trade_id,
    //TradeRequestBuffer[i].price_quote,
    //TradeRequestBuffer[i].trade_qty,
    //TradeRequestBuffer[i].trade_type);
    //}
    
    //pass the tradeRequest to Market to process the those submitted orders
    
    //use this in init task for simple testing
    if (RemoteTestModule.getCurrentThread().getCurrentTask().getTaskTypeString().
            equalsIgnoreCase("INITTASK")) {
      for (TradeInfo tr: tradeRequest) {
        TPCEBB.getBB().addTradeId(tr.getTradeId());      
        TPCEBB.getBB().getSharedCounters().increment(TPCEBB.TradeIdsInsertedInInitTask);
        Log.getLogWriter().info("TPCEBB adds " + tr.getTradeId());
      }
    } else {
      mee.submitTradeToMarket(conn, tradeRequest);
    }
    
    
    return rows_updated_thisTicker;

  }

  protected void processSelectTrForUpdate(int i,
      PreparedStatement updateTradePs, PreparedStatement selectTrPs,
      Timestamp now_dts, PreparedStatement deleteTradeRequestPs, 
      PreparedStatement insertTradeHistoryPs, ArrayList<TradeInfo> tradeRequest)
  throws SQLException {
    BigDecimal req_price_quote;
    long req_trade_id;
    int req_trade_qty;
    String req_trade_type;

    // declare request_list cursor for
    // select
    // TR_T_ID,
    // TR_BID_PRICE,
    // TR_TT_ID,
    // TR_QTY
    // from
    // TRADE_REQUEST -- GEMFIREXD-PROPERTIES index=i_tr_s_symb \n
    // where
    // TR_S_SYMB = symbol[i] and (
    // (TR_TT_ID = type_stop_loss and
    // TR_BID_PRICE >= price_quote[i]) or
    // (TR_TT_ID = type_limit_sell and
    // TR_BID_PRICE <= price_quote[i]) or
    // (TR_TT_ID = type_limit_buy and
    // TR_BID_PRICE >= price_quote[i])
    // )
    try {
      selectTrPs.setString(1, symbol[i]);
      selectTrPs.setString(2, type_stop_loss);
      selectTrPs.setBigDecimal(3, price_quote[i]);
      selectTrPs.setString(4, type_limit_sell);
      selectTrPs.setBigDecimal(5, price_quote[i]);
      selectTrPs.setString(6, type_limit_buy);
      selectTrPs.setBigDecimal(7, price_quote[i]);

      ResultSet rs = selectTrPs.executeQuery();

      while (rs.next()) {
        req_trade_id = rs.getLong("TR_T_ID");
        req_price_quote = rs.getBigDecimal("TR_BID_PRICE");
        req_trade_type = rs.getString("TR_TT_ID");
        req_trade_qty = rs.getInt("TR_QTY");
      
        //update trade with date and status
        updateTradeTxn(updateTradePs, now_dts, req_trade_id);
        
        //delete current row in trade_request
        deleteTradeRequestTxn(deleteTradeRequestPs, req_trade_id);
        
        //insert into trade history with new status
        insertTradeHistoryTxn(insertTradeHistoryPs, req_trade_id, now_dts);
        
        /* TradeRequestBuffer[rows_sent].symbol = symbol[i]
         * TradeRequestBuffer[rows_sent].trade_id = req_trade_id
         * TradeRequestBuffer[rows_sent].price_quote = req_price_quote
         * TradeRequestBuffer[rows_sent].trade_qty = req_trade_qty
         * TradeRequestBuffer[rows_sent].trade_type = req_trade_type rows_sent =
         * rows_sent + 1 fetch from request_list into req_trade_id, req_price_quote,
         * req_trade_type, req_trade_qty } /* end of cursor fetch loop
         */
        TradeInfo tr = new TradeInfo();
        tr.setPriceQuotes(req_price_quote);
        tr.setSymbol(symbol[i]);
        tr.setTradeId(req_trade_id);
        tr.setTradeQty(req_trade_qty);
        tr.setTradeType(req_trade_type);
        tradeRequest.add(tr);
      }

    } catch (SQLException se) {
      if (se.getSQLState().equals("X0Z02") &&
          (!TPCETest.isClient || (TPCETest.isClient && TPCETest.useSyncCommit))) {

        throw new TestException(
            "Does not expect conflict exception here, as the"
                + " the txn already hold the lock for the symbol updated in last_trade table,"
                + " there should be no other txn could modify the same row for symbol "
                + symbol[i] + " in trade request table "
                + " the stack trace for the conflict exception is: "
                + TestHelper.getStackTrace(se));
      // other dml op on trade_request is insert in trade order txns, and should
      // not cause conflicts here
      // for thin client case, (see #48712) there is small window the committed row (insert from tradeorder) does not fully release the lock
      // TODO may need to use sync-commit connection to see which one has better 
      // perf result; use retry for now.
      // Actually the issue is most likely caused by batching ops in txn 
      // The last_trade is REPLICATE table, so the update only hold the lock 
      // on that particular node, further operations may cause conflict
      // so may need to add disable batching on servers to work around this (too many conflicts and retries) for thin client
     

      } else
        throw se;
    }
    
  }

  protected void updateTradeTxn(PreparedStatement updateTradePs,
      Timestamp now_dts, long req_trade_id) throws SQLException {
    try {
      // update
      // TRADE
      // set
      // T_DTS
      // = now_dts,
      // T_ST_ID = status_submitted
      // where
      // T_ID = req_trade_id
      updateTradePs.setTimestamp(1, now_dts);
      updateTradePs.setString(2, status_submitted);
      updateTradePs.setLong(3, req_trade_id);

      int count = updateTradePs.executeUpdate();
      if (logDML) {
        Log.getLogWriter().info(
            updateTrade + " with T_DTS = " + now_dts + " T_ST_ID = "
                + status_submitted + " for T_ID = " + req_trade_id);
      }
      if (count != 1) {
        throw new TestException(updateTrade + " with T_DTS = " + now_dts
            + " T_ST_ID = + " + status_submitted + " for T_ID = "
            + req_trade_id + " should update 1 row but updated " + count
            + " row(s)");
      }
    } catch (SQLException se) {
      if  (se.getSQLState().equals("X0Z02") &&
          (!TPCETest.isClient || (TPCETest.isClient && TPCETest.useSyncCommit)))
        throw new TestException(
            "Does not expect conflict exception here, as the "
                + "the trade table is only updated in market feed txn. No other txn should "
                + "be able to modify the trade_request table with the same symbol, "
                + "as the trade is inserted in trade order txn (if not committed, there should not be conflict) "
                + "or trade is updated in trade result txn but only for status became submitted, "
                + "or trade is updated in trade update txn (test will make sure update only on "
                + "submitted status when txnInput is configured in trade update txn}."
                + "The stack trace for the conflict exception is: "
                + TestHelper.getStackTrace(se));
      // other dml op on trade_request is insert in trade order txns, and
      // should not cause conflicts here
      // for thin client case, (see #48712) there is small window the committed row (insert from tradeorder) does not fully release the lock
      // TODO may need to use sync-commit connection to see which one has better 
      // perf result; use retry for now.
      else
        throw se;
    }

  }
  
  protected void deleteTradeRequestTxn(PreparedStatement ps, 
      long req_trade_id) throws SQLException {
    try {
      //delete TRADE_REQUEST where current of request_list
      //"delete from TRADE_REQUEST where TR_T_ID = ?"
      ps.setLong(1, req_trade_id);
      int count = ps.executeUpdate();
      
      if (logDML) {
        Log.getLogWriter().info(
            deleteTradeRequest + " for TR_T_ID = " + req_trade_id);
      }
      if (count != 1) {
        throw new TestException(deleteTradeRequest + " for TR_T_ID = " + req_trade_id +
            " should delete 1 row but deleted " + count
            + " row(s)");
      }
    } catch (SQLException se) {
      if (se.getSQLState().equals("X0Z02") &&
          (!TPCETest.isClient || (TPCETest.isClient && TPCETest.useSyncCommit)))
        throw new TestException(
            "Does not expect conflict exception here, as the lock should be " +
            "held by the select for update statement" + 
            "The stack trace for the conflict exception is: " + 
            TestHelper.getStackTrace(se));
      else
        throw se;
    }

  }
  
  protected void insertTradeHistoryTxn(PreparedStatement ps, long req_trade_id, 
      Timestamp now_dts) throws SQLException {
    //insert into TRADE_HISTORY(TH_T_ID, TH_DTS, TH_ST_ID) values (?, ?, ?)
    //insert into TRADE_HISTORY values ( TH_T_ID = req_trade_id, TH_DTS =
    //  now_dts, TH_ST_ID = status_submitted )
    try {
      ps = conn.prepareStatement(insertTradeHistory);
      ps.setLong(1, req_trade_id); //TH_T_ID
      ps.setTimestamp(2, now_dts); //TH_DTS
      ps.setString(3, status_submitted); // TH_ST_ID
      
      int count = ps.executeUpdate();
      
      if (logDML) {
        Log.getLogWriter().info(insertTradeHistory + " with TH_T_ID = " + req_trade_id + 
            " TH_DTS = " + now_dts + " TH_ST_ID = " + status_submitted);
      }
      
      if (count != 1)
        throw new TestException(insertTradeHistory + " with TH_T_ID = " + req_trade_id + 
            " TH_DTS = " + now_dts + " TH_ST_ID = " + status_submitted + " should inserts 1 row " +
            "but inserted " + count + " row(s)");  
    } catch (SQLException se) {
      if (se.getSQLState().equals("X0Z02"))
        throw new TestException(
            "Does not expect conflict exception here, as the lock should be " +
            "held by the select for update statement" + 
            "The stack trace for the conflict exception is: " + 
            TestHelper.getStackTrace(se));
      else
        throw se;
    }
  }

}
