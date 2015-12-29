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
package sql.tpce.tpcedef.input;

import java.math.BigDecimal;

import sql.tpce.tpcedef.TPCETxnInput;

public class MarketFeedTxnInput implements TPCETxnInput {
  private static final long serialVersionUID = 100020001L;
  
  private BigDecimal[] price_quotes;
  private String status_submitted;
  private String[] symbols;
  private int[] trade_qty;
  private String type_limit_buy;
  private String type_limit_sell; 
  private String type_stop_loss; 

  public BigDecimal[] getPriceQuotes(){
    return price_quotes;
  } 
  
  public String getStatusSubmitted(){
    return status_submitted;
  }
  
  public String[] getSymbol(){
    return symbols;
  } 
  
  public int[] getTradeQty() {
    return trade_qty;
  }
  
  public String getLimitBuy(){
    return type_limit_buy;
  }
  
  public String getLimitSell(){
    return type_limit_sell;
  }
  
  public String getStopLoss(){
    return type_stop_loss;
  }
  
  public void setPriceQuotes(BigDecimal[] price_quotes){
    this.price_quotes = price_quotes;
  } 
  
  public void setStatusSubmitted(String status_submitted){
    this.status_submitted = status_submitted;
  }
  
  public void setSymbol(String[] symbols){
    this.symbols = symbols;
  } 
  
  public void setTradeQty(int[] trade_qty) {
    this.trade_qty = trade_qty;
  }
  
  public void setLimitBuy(String type_limit_buy){
    this.type_limit_buy = type_limit_buy;
  }
  
  public void setLimitSell(String type_limit_sell){
    this.type_limit_sell = type_limit_sell;
  }
  
  public void setStopLoss(String type_stop_loss){
    this.type_stop_loss = type_stop_loss;
  }

}
