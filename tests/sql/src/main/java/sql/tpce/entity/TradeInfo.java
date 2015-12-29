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
package sql.tpce.entity;

import java.io.Serializable;
import java.math.BigDecimal;

public class TradeInfo implements Serializable {
  private static final long serialVersionUID = 10002105;
  
  private String symbol;
  private long trade_id;
  private BigDecimal price_quote;
  private int trade_qty;
  private String trade_type;

  public BigDecimal getPriceQuote(){
    return price_quote;
  } 
  
  public String getSymbol(){
    return symbol;
  }
  
  public int getTradeQty() {
    return trade_qty;
  }
  
  public long getTradeId(){
    return trade_id;
  }
  
  public String getTradeType(){
    return trade_type;
  }
  
  public void setPriceQuotes(BigDecimal price_quote){
    this.price_quote = price_quote;
  } 
  
  public void setSymbol(String symbol){
    this.symbol = symbol;
  } 
  
  public void setTradeQty(int trade_qty) {
    this.trade_qty = trade_qty;
  }
  
  public void setTradeId(long trade_id){
    this.trade_id = trade_id;
  }
  
  public void setTradeType(String trade_type){
    this.trade_type = trade_type;
  }
  
  public String toString() {
    return "price_quote: " + price_quote + " symbol: " + symbol +
       " trade_qty: " + trade_qty + " trade_id: " + trade_id +
       " trade_type: " + trade_type;
  }
  
}
