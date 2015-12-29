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
package sql.tpce.tpcedef.output;

import java.math.BigDecimal;

import sql.tpce.tpcedef.TPCETxnOutput;

/**
 * The transaction Trade-Result output
 * TPC-E Section 3.3.8
 */

public class TradeResultTxnOutput implements TPCETxnOutput {
  private static final long serialVersionUID = 1L;
  private BigDecimal acct_bal;
  private int status;
  private int load_unit;
  private long acct_id;

  public BigDecimal getAcctBal() {
      return this.acct_bal;
  }

  public void setAcctBal(BigDecimal acct_bal) {
      this.acct_bal = acct_bal;
  }
  
  public long getAcctId(){
    return acct_id;
  }
  
  public void setAcctId(long acct_id){
    this.acct_id = acct_id;
  }
    
  public int getLoadUnit(){
    return load_unit;
  }
  
  public void setLoadUnit(int load_unit){
    this.load_unit = load_unit;
  }

  public int getStatus() {
    return this.status;
  }
  
  public void setStatus(int status) {
    this.status = status;
  }

  public String toString() {
      return "AcctBal: " + acct_bal
              + "\nAcctId: " + acct_id
              + "\nLoadUnit: " + load_unit
              + "\nStatus: " + status;
  }
}
