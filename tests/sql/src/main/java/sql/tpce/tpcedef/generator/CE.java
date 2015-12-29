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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Connection;

import com.gemstone.gemfire.cache.query.Struct;

import sql.sqlutil.ResultSetHelper;
import sql.sqlutil.GFXDStructImpl;
import sql.tpce.TPCETest;
import sql.tpce.tpcedef.TPCETxnInput;
import sql.tpce.tpcedef.input.TradeOrderTxnInput;

public class CE {
  private static ArrayList<List<Struct>> caInfo = new ArrayList<List<Struct>>(); //tier1, tier2, tier3
  private static List<Struct> companyInfo;
  private static HashMap<String, BigDecimal> lastTradeInfo = new HashMap<String, BigDecimal>();
  
  private static String selectCAInfoJoin = "select ca_id, ca_c_id, ap_ca_id, ap_l_name, " +
  		"ap_f_name, ap_tax_id, c_id, c_tier from customer_account, account_permission, customer " +
  		"where ca_id = ap_ca_id and ca_c_id = c_id and c_tier = ?";
  
  
  private static String selectSymbolInfo = "select s_symb, s_issue from security";
  
  private static String selectCompanyInfoJoin = "select s_symb, s_issue, co_name from " +
  		"security, company where s_co_id = co_id";
  
  private static String selectLastTradeInfo = "select lt_s_symb, lt_price from last_trade";

  
  private static Boolean infoSet = false;
  
  private static final String status_pending="PNDG";
  private static Long lastTradeUpdatedTime = System.currentTimeMillis();
  private static final BigDecimal zero = new BigDecimal("0.00");
  
  public void setCaInfo(Connection conn) throws SQLException{
    synchronized (infoSet) {
      if (!infoSet) infoSet = true;
      else return;
    }
    
    if (conn == null) Log.getLogWriter().info("in ce, conn is " + conn);
    PreparedStatement ps = conn.prepareStatement(selectCAInfoJoin);
    ResultSet rs;
    //for 3 tiers
    for (int i=0; i<3; i++) {
      ps.setShort(1, (short) (i+1));
      rs = ps.executeQuery();
      caInfo.add(ResultSetHelper.asList(rs, false));
      rs.close();      
    }
    
    ps = conn.prepareStatement(selectCompanyInfoJoin);
    rs = ps.executeQuery();
    companyInfo = ResultSetHelper.asList(rs, false);
    rs.close();
    
    //Log.getLogWriter().info("in setCaInfo, conn is " + conn);
    setLastTradeInfo(conn);
    
  }
  
  public long getLastTradeUpdatedTime() {
    return lastTradeUpdatedTime;
  }
  
  public void setLastTradeInfo(Connection conn) throws SQLException {
    synchronized (lastTradeUpdatedTime) {
      Log.getLogWriter().info("in setLastTradeInfo, conn is " + conn);
      PreparedStatement ps = conn.prepareStatement(selectLastTradeInfo);
      ResultSet rs = ps.executeQuery();
      List<Struct> lastTrade = ResultSetHelper.asList(rs, false);
      rs.close();
      for (Struct ltStruct: lastTrade) {
        lastTradeInfo.put((String)ltStruct.get("lt_s_symb".toUpperCase()),
            (BigDecimal) ltStruct.get("lt_price".toUpperCase()));
      }
      lastTradeUpdatedTime = System.currentTimeMillis();
      Log.getLogWriter().info("lastTradeUpdatedTime is " + lastTradeUpdatedTime);
    }
  }
  
  private static int getTier() {
    int max = 10;
    int num = TPCETest.rand.nextInt(max);
    if (num < 1) return 0;
    else if (num < 4) return 1;
    else return 2;
  }
  
  public TPCETxnInput getTradeOrderTxn() {
    TradeOrderTxnInput toInput = new TradeOrderTxnInput(); 
    int tier = getTier();
    
    int index = TPCETest.rand.nextInt(caInfo.get(tier).size());
    GFXDStructImpl caInfoStruct = (GFXDStructImpl) caInfo.get(tier).get(index);
    
    toInput.setAcctId((Long)caInfoStruct.get("ca_id".toUpperCase()));  

    toInput.setExecFirstName((String) caInfoStruct.get("ap_f_name".toUpperCase()));
    toInput.setExecLastName((String) caInfoStruct.get("ap_l_name".toUpperCase()));
    toInput.setExecTaxId((String) caInfoStruct.get("ap_tax_id".toUpperCase()));
   
    int symbolIndex = TPCETest.rand.nextInt(companyInfo.size());
    GFXDStructImpl companyStruct = (GFXDStructImpl) companyInfo.get(symbolIndex);
    
    String symbol = (String) companyStruct.get("s_symb".toUpperCase());
    String issue =  (String) companyStruct.get("s_issue".toUpperCase());
    String co_name = (String) companyStruct.get("co_name".toUpperCase());
    
    if (TPCETest.rand.nextBoolean()){
      toInput.setSymbol(symbol);
    } else {
      toInput.setIssue(issue);
      toInput.setCoName(co_name);
      toInput.setSymbol(" ");
    }
    
    int total = 30;
    if (TPCETest.rand.nextInt(total) == 1) {
      toInput.setRollItBack(1);
    } else {
      toInput.setRollItBack(0);
    }
    
    //about 20 percent using Lifo
    total =5;
    if (TPCETest.rand.nextInt(total) == 1)
      toInput.setIsLifo(1);
    else 
      toInput.setIsLifo(0);
     
    total = 20;
    int hand = 100;
    int qty = (TPCETest.rand.nextInt(total)+1) * hand;
    if (TPCETest.rand.nextInt(total) == 1) qty += TPCETest.rand.nextInt(hand); //random qty
    toInput.setTradeQty(qty);
        
    int hundred = 100;
    int whichOne = TPCETest.rand.nextInt(hundred);
    
    BigDecimal curPrice = lastTradeInfo.get(symbol);
    if (whichOne %2 == 0) {
      //market order
      if (whichOne < hundred/2) {
        toInput.setTradeTypeId(TPCETest.type_market_sell);
      } else {
        toInput.setTradeTypeId(TPCETest.type_market_buy);
      }
      toInput.setStSubmittedId(TPCETest.status_submitted);
    } else {
      //limited order
      if (whichOne < 6){
        toInput.setTradeTypeId(TPCETest.type_stop_loss);
        toInput.setRequestedPrice(getLowerOrEqualPrice(curPrice));
      } else if (whichOne < hundred/2) {
        toInput.setTradeTypeId(TPCETest.type_limit_sell);
        toInput.setRequestedPrice(getHigherOrEqualPrice(curPrice));
      } else {
        toInput.setTradeTypeId(TPCETest.type_limit_buy);
        toInput.setRequestedPrice(getLowerOrEqualPrice(curPrice));
      }
      toInput.setStPendingId(status_pending);
    }
    
    if (TPCETest.logDML) Log.getLogWriter().info("gets toInput as: " + toInput.toString());

    return toInput;
  }
  
  private BigDecimal getLowerOrEqualPrice(BigDecimal price) {
    price.subtract(getRandomChange());
    
    if (price.compareTo(zero) == -1) return new BigDecimal("0.01");
    else return price;
  }
  
  private BigDecimal getHigherOrEqualPrice(BigDecimal price) {
    return price.add(getRandomChange());
  }
  
  private BigDecimal getRandomChange() {
    int max = 100;
    double change = TPCETest.rand.nextInt(max) * 0.01;
    return new BigDecimal(Double.toString(change));
  }
  
  
  
}
