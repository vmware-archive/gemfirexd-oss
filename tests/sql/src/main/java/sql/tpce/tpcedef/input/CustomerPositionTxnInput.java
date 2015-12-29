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

import sql.tpce.tpcedef.TPCETxnInput;

/**
 * The transaction Customer-Position input
 * TPC-E Section 3.3.2
 */

public class CustomerPositionTxnInput implements TPCETxnInput {
	private static final long serialVersionUID = 1L;
	
    private int acctIdIdx;
    private long custId;
    private long getHistory;
    private String taxId;
    
	public CustomerPositionTxnInput(int acctIdIdx, long custId, long getHistory, String taxId){
		setAcctIdIndex(acctIdIdx);
		setCustId(custId);
		setHistory(getHistory);
		setTaxId(taxId);
    }
    
    public int getAcctIdIndex(){
        return acctIdIdx;
    }
    public long getCustId(){
        return custId;
    }
    public long getHistory(){
        return getHistory;
    }    
    public String getTaxId(){
        return taxId;
    }
    public void setAcctIdIndex(int acctIdIdx){
        this.acctIdIdx =  acctIdIdx;
    }
    public void setCustId(long custId){
        this.custId = custId;
    }
    public void setHistory(long getHistory){
        this.getHistory = getHistory;
    }
    public void setTaxId(String taxId){
        this.taxId = taxId;
    }
}
