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

import java.io.Serializable;

public class ProcessedOrderQty implements Serializable {

  /**
   * 
   */
  private static final long serialVersionUID = 20001101;
  
  //this tracks a market feed generated based on buy qty, sell qty of trades processed and
  //additional qty from other firms.
  private long buy_qty;
  private long sell_qty;
  private long other_buy_qty;
  private long other_sell_qty;
  
  public long getBuyQty() {
    return this.buy_qty;
  }
  
  public void setBuyQty(int buy_qty) {
    this.buy_qty = buy_qty;
  }
  
  public long getSellQty() {
    return this.sell_qty;
  }
  
  public void setSellQty(int sell_qty) {
    this.sell_qty = sell_qty;
  }
  
  public long getOtherBuyQty() {
    return this.other_buy_qty;
  }
  
  public void setOtherBuyQty(int other_buy_qty) {
    this.other_buy_qty = other_buy_qty;
  }

  public long getOtherSellQty() {
    return this.other_sell_qty;
  }
  
  public void setOtherSellQty(int other_sell_qty) {
    this.other_sell_qty = other_sell_qty;
  }
  
  public void addBuyQty(int buy_qty) {
    this.buy_qty += buy_qty;
  }
  
  public void addSellQty(int sell_qty) {
    this.sell_qty += sell_qty;
  }
  
  public void addOtherBuyQty(int other_buy_qty) {
    this.other_buy_qty += other_buy_qty;
  }

  public void addOtherSellQty(int other_sell_qty) {
    this.other_sell_qty += other_sell_qty;
  }
}
