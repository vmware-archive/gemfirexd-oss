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
package com.pivotal.gemfirexd.app.tpce.output;

import com.pivotal.gemfirexd.app.tpce.TPCETxnOutput;
/**
 * The transaction Data-Maintenance output
 * TPC-E Section 3.3.11
 */

public class DataMaintenanceTxnOutput implements TPCETxnOutput {
	private static final long serialVersionUID = 1L;
	private double buy_value;
	private double sell_value;
	private int status;
	private double tax_amount;
	private long trade_id;

	public double getBuyValue() {
		return this.buy_value;
	}
	
	public void setBuyValue(double buyValue) {
		this.buy_value = buyValue;
	}
	
	public double getSellValue(double sellValue) {
		return this.sell_value;
	}
	
	public void setSellValue(double sellValue) {
		this.sell_value = sellValue;
	}
	
	public int getStatus() {
		return this.status;
	}
	
	public void setStatus(int status) {
		this.status = status;
	}
	
	public double getTaxAmount() {
		return this.tax_amount;
	}
	
	public void setTaxAmount(double taxAmount) {
		this.tax_amount = taxAmount;
	}
	
	public long getTradeId() {
		return this.trade_id;
	}
	
	public void setTradeId(long tradeId) {
		this.trade_id = tradeId;
	}
	
	public String toString() {
		return "TradeId: " + trade_id 
				+ "\nBuy Value: " + buy_value
				+ "\nSell Value: " + sell_value
				+ "\nTax Amount: " + tax_amount
				+ "\nStatus: " + status;
	}
}
