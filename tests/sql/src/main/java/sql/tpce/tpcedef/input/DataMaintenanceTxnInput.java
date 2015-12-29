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
 * The transaction Data-Maintenance input
 * TPC-E Section 3.3.11
 */
public class DataMaintenanceTxnInput implements TPCETxnInput {
	private static final long serialVersionUID = 1L;
    private long  acct_id;
    private long  c_id;
    private long  co_id;
    private int   day_of_month;
    private int   vol_incr;
    private String    symbol;
    private String    table_name;
    private String    tx_id;	

    public long getAcctId(){
        return acct_id;
    }
    public long getCId(){
        return c_id;
    }
    public long getCoId(){
        return co_id;
    }
    public int getDayOfMonth(){
        return day_of_month;
    }
    public int getVolIncr(){
        return vol_incr;
    }
    public String getSymbol(){
        return symbol;
    }
    public String getTableName(){
        return table_name;
    }
    public String getTxId(){
        return tx_id;
    }
    
    public void setAcctId(long acct_id){
        this.acct_id = acct_id;
    }
    public void setCId(long c_id){
        this.c_id = c_id;
    }
    public void setCoId(long co_id){
        this.co_id = co_id;
    }
    public void setDayOfMonth(int day_of_month){
        this.day_of_month = day_of_month;
    }
    public void setVolIncr(int vol_incr){
        this.vol_incr = vol_incr;
    }
    public void setSymbol(String symbol){
        this.symbol = symbol;
    }
    public void setTableName(String table_name){
        this.table_name = table_name;
    }
    public void setTxId(String tx_id){
        this.tx_id = tx_id;
    }
}
