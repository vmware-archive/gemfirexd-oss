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
package com.pivotal.gemfirexd.app.tpce.input;

import com.pivotal.gemfirexd.app.tpce.TPCETxnInput;
/**
 * The transaction Trade-Order input
 * TPC-E Section 3.3.7
 */
public class TradeOrderTxnInput implements TPCETxnInput {
	private static final long serialVersionUID = 1L;
    private double          requested_price;
    private long            acct_id;
    private long            is_lifo;
    private long            roll_it_back;
    private long            trade_qty;
    private long            type_is_margin;
    private long            trade_id;
    private String          co_name;
    private String          exec_f_name;
    private String          exec_l_name;
    private String          exec_tax_id;
    private String          issue;
    private String          st_pending_id;
    private String          st_submitted_id;
    private String          symbol;
    private String          trade_type_id;
        
    public double getRequestedPrice(){
        return requested_price;
    }
    public long getAcctId(){
        return acct_id;
    }
    public long getIsLifo(){
        return is_lifo;
    }
    public long getRollItBack(){
        return roll_it_back;
    }
    public long getTradeQty(){
        return trade_qty;
    }
    public long getTypeIsMargin(){
        return type_is_margin;
    }
    public long getTradeID(){
        return trade_id;
    }
    public String getCoName(){
        return co_name;
    }
    public String getExecFirstName(){
        return exec_f_name;
    }
    public String getExecLastName(){
        return exec_l_name;
    }
    public String getExecTaxId(){
        return exec_tax_id;
    }
    public String getIssue(){
        return issue;
    }
    public String getStPendingId(){
        return st_pending_id;
    }
    public String getStSubmittedId(){
        return st_submitted_id;
    }
    public String getSymbol(){
        return symbol;
    }
    public String getTradeTypeId(){
        return trade_type_id;
    }
    
    public void setRequestedPrice(double requested_price){
        this.requested_price = requested_price;
    }
    public void setAcctId(long acct_id){
        this.acct_id = acct_id;
    }
    public void setIsLifo(long is_lifo){
        this.is_lifo = is_lifo;
    }
    public void setRollItBack(long roll_it_back){
        this.roll_it_back = roll_it_back;
    }
    public void setTradeQty(long trade_qty){
        this.trade_qty = trade_qty;
    }
    public void setTypeIsMargin(long type_is_margin){
        this.type_is_margin = type_is_margin;
    }
    public void setTradeID(long trade_id){
        this.trade_id = trade_id;
    }
    public void setCoNmae(String co_name){
        this.co_name = co_name;
    }
    public void setExecFirstName(String exec_f_name){
        this.exec_f_name = exec_f_name;
    }
    public void setExecLastName(String exec_l_name){
        this.exec_l_name = exec_l_name;
    }
    public void setExecTaxId(String exec_tax_id){
        this.exec_tax_id = exec_tax_id;
    }
    public void setIssue(String issue){
        this.issue = issue;
    }
    public void setStPendingId(String st_pending_id){
        this.st_pending_id = st_pending_id;
    }
    public void setStSubmittedId(String st_submitted_id){
        this.st_submitted_id = st_submitted_id;
    }
    public void setSymbol(String symbol){
        this.symbol = symbol;
    }
    public void setTradeTypeId(String trade_type_id){
        this.trade_type_id = trade_type_id;
    }
}
