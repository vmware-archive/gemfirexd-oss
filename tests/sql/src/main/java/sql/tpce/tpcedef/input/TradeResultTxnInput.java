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

/**
 * The transaction Trade-Result input
 * TPC-E Section 3.3.8
 */
public class TradeResultTxnInput implements TPCETxnInput {
  private static final long serialVersionUID = 100021002L;
  private long trade_id;
  private BigDecimal trade_price;


  public long getTradeID(){
      return trade_id;
  }
  public BigDecimal getTradePrice(){
      return trade_price;
  }

  public void setTradeID(long trade_id){
      this.trade_id = trade_id;
  }
  public void setTradePrice(BigDecimal trade_price){
      this.trade_price = trade_price;
  }

}
