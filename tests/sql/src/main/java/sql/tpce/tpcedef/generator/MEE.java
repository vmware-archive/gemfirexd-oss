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
package sql.tpce.tpcedef.generator;

import hydra.Log;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;

import sql.SQLHelper;
import sql.tpce.TPCEBB;
import sql.tpce.TPCETest;
import sql.tpce.entity.TradeInfo;
import sql.tpce.tpcedef.input.MarketFeedTxnInput;
import sql.tpce.tpcedef.input.TradeResultTxnInput;
import util.TestException;

public class MEE {
  //This implementation does not use TPC-E implementation, and is used for validating data
  
  private static boolean tradeToMarketWithDefaultId = TPCETest.tradeToMarketWithDefaultId;
  
  private static String insertTradeMarket = "insert into TRADE_Market ( TM_S_SYMB, " +
  		"TM_T_ID, TM_T_QTY, TM_T_BID_PRICE, TM_TT_ID, TM_STATUS, TM_ID) " +
  		"values (?, ?, ?, ?, ?, 0, default)";
  
  private static String insertTradeMarketWithoutDefaultId = "insert into TRADE_Market (TM_S_SYMB, " +
    "TM_T_ID, TM_T_QTY, TM_T_BID_PRICE, TM_TT_ID, TM_STATUS, TM_ID) " + 
    "values (?, ?, ?, ?, ?, 0, ?)";
  
  private static String selectTradeMarket = " select * from Trade_market where TM_status = 1 " +
  		"fetch first 10 rows only order by TM_ID for update";
  
  private static String selectTradeMarketWithHDFS = "select * from Trade_market order by TM_ID " +
      "fetch first 10 rows only for update"; //HDFS will using evict incoming on TM_STATUS becomes 1 (processed)
  
  private static String selectTradeMarketWithoutDefaultId = " select * from TRADE_Market where tm_id = ?";
  
  private static BigDecimal[] randomChange = {new BigDecimal("0.01"), new BigDecimal("0.02"),
    new BigDecimal("0.03"), new BigDecimal("0.04"), new BigDecimal("0.05")
  };
  
  public static final int mfCapacity = 10000;
  public static final int trCapacity = 100000;
  private static ArrayBlockingQueue<MarketFeedTxnInput> mfTxnInputsQueue = 
    new ArrayBlockingQueue<MarketFeedTxnInput>(mfCapacity);
  private static ArrayBlockingQueue<TradeResultTxnInput> trTxnInputsQueue = 
    new ArrayBlockingQueue<TradeResultTxnInput>(trCapacity);
  private static HashMap<String, ProcessedOrderQty> trackQtyForSymbol = new HashMap<String, ProcessedOrderQty>(); 
  
  //the trade sent to market will be added to trade_market table created for 
  //tracking which trades to be processed first
  //all perf tests will use test generated tradeToMarket Id instead of product generated ones
  //to avoid synchronization across the vms.
  public void submitTradeToMarket(Connection conn, ArrayList<TradeInfo> tradeRequest)
  throws SQLException {
    
    ArrayList<Long> tm_ids = new ArrayList<Long>();
    getTMIdsToInsert(tm_ids, tradeRequest.size());
    boolean retry = true;
    
    while (retry) {
      try {
        submitTradeToMarket(conn, tradeRequest, tm_ids);
        conn.commit(); //committing the trade_market insert for processing
        if (TPCETest.logDML) Log.getLogWriter().info("committed insert to trade_market");
        retry = false;
      } catch (SQLException se) {
        //need to handle HA here for retry
        SQLHelper.handleSQLException(se);
      }
    }
      
  }
  
  public void submitTradeToMarket(Connection conn, ArrayList<TradeInfo> tradeRequest, 
      ArrayList<Long> tm_ids) throws SQLException {
    
    String insert = tradeToMarketWithDefaultId? insertTradeMarket : insertTradeMarketWithoutDefaultId;
    
    PreparedStatement ps= conn.prepareStatement(insert);
    for (int i =0; i< tradeRequest.size(); i++) {      
      ps.setString(1, tradeRequest.get(i).getSymbol()); // TI_S_SYMB,
      ps.setLong(2, tradeRequest.get(i).getTradeId()); // TI_T_ID
      ps.setInt(3, tradeRequest.get(i).getTradeQty()); //TI_T_QTY
      ps.setBigDecimal(4, tradeRequest.get(i).getPriceQuote()); // TT_T_BID_PRICE
      ps.setString(5, tradeRequest.get(i).getTradeType()); //T_TT_ID
      
      if (!tradeToMarketWithDefaultId) {
        ps.setLong(6, tm_ids.get(i));
      }
       
      if (TPCETest.logDML) {
        Log.getLogWriter().info(insert + " in batch TM_S_SYMB: " + tradeRequest.get(i).getSymbol()
            + " TM_T_ID:" + tradeRequest.get(i).getTradeId() + " TM_T_QTY: " + tradeRequest.get(i).getTradeQty()
            + " TM_T_BID_PRICE: " + tradeRequest.get(i).getPriceQuote() + " T_TT_ID: " + tradeRequest.get(i).getTradeType()
            + (tradeToMarketWithDefaultId ? "" :" and TM_ID: " + tm_ids.get(i)));
      }
      ps.addBatch();
    }
    int[] counts = ps.executeBatch();
    for (int count: counts) {
      if (count != 1) throw new TestException (insert + " failed insert all rows");
    }
     
  }
  
  protected void getTMIdsToInsert(ArrayList<Long> list, int size) {
    //update tradeSentToMarket on bb
    long lastId = TPCEBB.getBB().getSharedCounters().add(TPCEBB.tradeSentToMarket, size);
    
    for (int i =1; i <= size; i++) {
      list.add(lastId - (size - i));
    }
  }
  
  
  //process trades send to market and generate TradeResultTxnInput and MarketFeedTxnInput
  //instead of synchronizing across all the vms, the test will only provide synchronization
  //in single vm, the time a thread when actually runs these txns may differs esp. across
  //the vms -- test validation needs to take this into consideration
  public void processTradesSubmitted(Connection conn) throws SQLException {
    //get tm_ids to be processed
    ArrayList<Long> tm_ids = new ArrayList<Long>(); 
    //will wait until new trades are available
    //though test should have enough trades available to process
    int size = 10;
    if (!tradeToMarketWithDefaultId) getTMIdsToProcess(tm_ids, size);
    
    boolean retry = true;
    HashMap<String, ArrayList<TradeInfo>> processedTrades = null;
    ArrayList<TradeResultTxnInput> trTxnInputs = null;
    ArrayList<String> processedSymbols = null;
    while (retry) {
      try {
        processedTrades= new HashMap<String, ArrayList<TradeInfo>>(); //retry will refresh this
        trTxnInputs = new ArrayList<TradeResultTxnInput>();
        processedSymbols = new ArrayList<String>(); 
        generateTrTxnInputs(conn, tm_ids, processedTrades, trTxnInputs, processedSymbols);
        retry = false;
      } catch (SQLException se) {
        //need to handle HA here for retry
        SQLHelper.handleSQLException(se);
      }
    }
    MarketFeedTxnInput mfTxnInput = getMfTxnInputs(processedTrades, processedSymbols);
    
    if (mfTxnInput == null || trTxnInputs == null || processedSymbols == null) {
      throw new TestException("TxnInputs should not be null at this time");
    }
    
    synchronized(mfTxnInputsQueue) {
      mfTxnInputsQueue.add(mfTxnInput);
    }
    
    synchronized(trTxnInputsQueue) {
      trTxnInputsQueue.addAll(trTxnInputs);
    }
  }
  
  protected void getTMIdsToProcess(ArrayList<Long> list, int size) {    
    //update tradeSentToMarket on bb

    long lastIdToProcess = TPCEBB.getBB().getSharedCounters().
      add(TPCEBB.tradeProcessedByMarket, size);
    long lastIdAvail = TPCEBB.getBB().getSharedCounters().read(TPCEBB.tradeSentToMarket);
    
    //wait until enough trades are sent to market 
    while (lastIdAvail < lastIdToProcess) {
      lastIdAvail = TPCEBB.getBB().getSharedCounters().read(TPCEBB.tradeSentToMarket);
    }
    
    for (int i =1; i <= size; i++) {
      list.add(lastIdToProcess - (size - i));
    }
  }
  
  public void generateTrTxnInputs(Connection conn, ArrayList<Long> tm_ids, 
      HashMap<String, ArrayList<TradeInfo>> processedTrades, 
      ArrayList<TradeResultTxnInput> trTxnInputs, 
      ArrayList<String> processedSymbols) throws SQLException {
    if (tradeToMarketWithDefaultId) {
      generateTrTxnInputsWithDefaultId(conn);
      return;
    } 
           
    generateTrTxnInputs(conn, trTxnInputs, tm_ids, processedTrades, processedSymbols);
       
  }
  
  protected void generateTrTxnInputsWithDefaultId(Connection conn) throws SQLException{
    //TODO to be implemented when testing HDFS etc, but may not ready
    //until order by supports with for update
    throw new TestException("Not ready for testing default id in Trade_Market table " +
    		"as order by with for update is not supported in gfxd yet");
    
  }
  
  protected void generateTrTxnInputs(Connection conn, ArrayList<TradeResultTxnInput> trTxnInputs,
      ArrayList<Long> ids, HashMap<String, ArrayList<TradeInfo>> processedTrades,
      ArrayList<String> processedSymbols) throws SQLException {
    TradeResultTxnInput trTxnInput;
    
    PreparedStatement ps = conn.prepareStatement(selectTradeMarketWithoutDefaultId);
    
    for (long id: ids) {
      trTxnInput = getTrTxnInput(ps, id, processedTrades, processedSymbols);
      trTxnInputs.add(trTxnInput); 
    }
    
  }
  
  protected TradeResultTxnInput getTrTxnInput(PreparedStatement ps, long id, 
      HashMap<String, ArrayList<TradeInfo>>  processedTrades, 
      ArrayList<String> processedSymbols) throws SQLException {
    TradeResultTxnInput trTxnInput = new TradeResultTxnInput();
    ps.setLong(1, id);
    ResultSet rs = ps.executeQuery();
    
    String symbol;
    long trade_id;
    int trade_qty;
    BigDecimal price;
    String trade_type;
    short tm_status;
   
    if (rs.next()) {
      trade_id = rs.getLong("TM_T_ID");
      symbol = rs.getString("TM_S_SYMB");
      trade_qty = rs.getInt("TM_T_QTY");
      price = rs.getBigDecimal("TM_T_BID_PRICE");
      trade_type = rs.getString("TM_TT_ID");
      tm_status = rs.getShort("TM_STATUS");
      
      if (tm_status != 0) {
        throw new TestException(selectTradeMarketWithoutDefaultId + " returns the row with " +
        		"TM_STATUS: " + tm_status );
      }
    } else {
      throw new TestException (selectTradeMarketWithoutDefaultId + " does not get result for " +
      		"TM_ID: " + id);
    }
    
    rs.close();
    
    TradeInfo trade = new TradeInfo();
    BigDecimal price_quote = getPrice(trade_type, price, symbol);
    
    trTxnInput.setTradeID(trade_id);
    trTxnInput.setTradePrice(price_quote);
    
    trade.setPriceQuotes(price_quote);
    trade.setSymbol(symbol);
    trade.setTradeId(trade_id);
    trade.setTradeQty(trade_qty);
    trade.setTradeType(trade_type);
    
    //no need to update TM_STATUS here for non default id case as tm_id is 
    //got from BB in tests.
   
    ArrayList<TradeInfo> trades = processedTrades.get(symbol);
    if (trades == null) {
      trades = new ArrayList<TradeInfo>();
      processedSymbols.add(symbol);
    } 
    trades.add(trade);
    processedTrades.put(symbol, trades);
 
    return trTxnInput;
    
  }
  
  protected MarketFeedTxnInput getMfTxnInputs(HashMap<String, ArrayList<TradeInfo>> processedTrades, 
      ArrayList<String> processedSymbols) {  
    MarketFeedTxnInput mfTxnInput = new MarketFeedTxnInput();
    mfTxnInput.setLimitBuy(TPCETest.type_limit_buy);
    mfTxnInput.setLimitSell(TPCETest.type_limit_sell);
    mfTxnInput.setStatusSubmitted(TPCETest.status_submitted);
    mfTxnInput.setStopLoss(TPCETest.type_stop_loss);
    
    int size = processedTrades.size();
    String[] symbols = new String[size];
    BigDecimal[] price = new BigDecimal[size];
    int[] qty = new int[size];
    
    String symbol;
    for (int i=0; i<size; i++) {
      symbol = processedSymbols.get(i);
      ArrayList<TradeInfo> trades = processedTrades.get(symbol);
      symbols[i] = symbol;
      getInfo(i, symbol, trades, price, qty);
  
    }
    
    mfTxnInput.setPriceQuotes(price);
    mfTxnInput.setSymbol(symbols);
    mfTxnInput.setTradeQty(qty);
    return mfTxnInput;
  }
  
  private void getInfo(int i, String symbol, ArrayList<TradeInfo> trades, 
      BigDecimal[] price, int[] qty) {
    int buy_qty=0;
    int sell_qty=0;
    int other_buy_qty=0; //from other firm data
    int other_sell_qty=0;
    BigDecimal trade_price = null;
    
    String trade_type;
    
    for (TradeInfo trade: trades) {
      trade_type = trade.getTradeType();
      if (trade_type.equals(TPCETest.type_limit_buy) 
          || trade_type.equals(TPCETest.type_market_buy)) {
        buy_qty += trade.getTradeQty();
      } else {
        sell_qty += trade.getTradeQty();
      }
      trade_price = getPrice(trade_type, trade.getPriceQuote(), symbol);
      //use the last one as the price     
    } 
    
    int max = 20;
    if (TPCETest.rand.nextBoolean()) other_buy_qty = TPCETest.rand.nextInt(max) * 100;
    else other_sell_qty = TPCETest.rand.nextInt(max) * 100;
    
    price[i] = trade_price;
    qty[i] = buy_qty + sell_qty+other_buy_qty + other_sell_qty;
    
    //local vm only, this method called from get mfTxnInput, which does not
    //use jdbc connection, so there is no conflict or anything to cause failure,
    //the qty could be added safely here to verify at the end of the test run 
    synchronized(trackQtyForSymbol) {
      ProcessedOrderQty poq = trackQtyForSymbol.get(symbol);
      if (poq == null) poq = new ProcessedOrderQty();
      poq.addBuyQty(buy_qty);
      poq.addSellQty(sell_qty);
      poq.addOtherBuyQty(other_buy_qty);
      poq.addOtherSellQty(other_sell_qty);
      trackQtyForSymbol.put(symbol, poq);
    }    
  }
  
  private BigDecimal getPrice(String trade_type, BigDecimal price, String symbol) {
    long buy_qty = trackQtyForSymbol.get(symbol) != null ? trackQtyForSymbol.get(symbol).getBuyQty() + 
    trackQtyForSymbol.get(symbol).getOtherBuyQty() : 0;
    long sell_qty = trackQtyForSymbol.get(symbol) != null ? trackQtyForSymbol.get(symbol).getSellQty() + 
    trackQtyForSymbol.get(symbol).getOtherSellQty() : 0;
    boolean isUp = buy_qty > sell_qty;
    
    if (trade_type.equals(TPCETest.type_limit_buy) || trade_type.equals(TPCETest.type_stop_loss)) {
      if (isUp) return price;
      else return getLowerOrEqualPrice(price);
    } else if (trade_type.equals(TPCETest.type_limit_sell)) {
      if (isUp) return getHigherOrEqualPrice(price);
      else return price;
    } else if (trade_type.equals(TPCETest.type_market_buy) || 
        trade_type.equals(TPCETest.type_market_sell)) {
      if (isUp) return getHigherOrEqualPrice(price);
      else return getLowerOrEqualPrice(price);
    } else { 
      throw new TestException ("Got unknown trade type " + trade_type);
    } 
        
  }
  
  private BigDecimal getLowerOrEqualPrice(BigDecimal price) {
    if (TPCETest.rand.nextBoolean()) return price;
    
    BigDecimal change =  getChange(price); 
    if (price.compareTo(change) == -1) return price;
    else return price.subtract(change);
  }
  
  private BigDecimal getHigherOrEqualPrice(BigDecimal price) {
    if (TPCETest.rand.nextBoolean()) return price;
    
    BigDecimal change =  getChange(price); 
    return price.add(change);
  }

  private BigDecimal getChange(BigDecimal price) {
    if (price.compareTo(new BigDecimal("1.00")) == -1) {
      return getRandomChange(1);
    }
    else if (price.compareTo(new BigDecimal("10.00")) == -1) {
      return getRandomChange(2);
    }
    else if (price.compareTo(new BigDecimal("25.00")) == -1) {
      return getRandomChange(3);
    } else if (price.compareTo(new BigDecimal("50.00")) == -1) {
      return getRandomChange(4);
    } else {
      return getRandomChange(5);
    }
  }
  
  private BigDecimal getRandomChange(int num) {
    return randomChange[TPCETest.rand.nextInt(num)];
  }
  
  public MarketFeedTxnInput getMfTxnInput() throws InterruptedException {
    MarketFeedTxnInput mfTxnInput = null;
    synchronized(mfTxnInputsQueue) {
      mfTxnInput = mfTxnInputsQueue.take();
    }
    return mfTxnInput;
  }
  
  public TradeResultTxnInput getTrTxnInput() throws InterruptedException {
    TradeResultTxnInput trTxnInput = null;
    synchronized(trTxnInputsQueue) {
      trTxnInput = trTxnInputsQueue.take();
    }
    return trTxnInput;
  }
  
  public int getMfTxnInputQueueSize() {
    return mfTxnInputsQueue.size();
  }
  
  public int getTrTxnInputQueueSize() {
    return trTxnInputsQueue.size();
  }

}
