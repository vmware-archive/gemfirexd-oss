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
//import javax.persistence.*;


/**
 * The persistent class for the TRADE_REQUEST database table.
 * 
 */
//@Entity
//@Table(name="TRADE_REQUEST")
public class TradeRequest implements Serializable {
	private static final long serialVersionUID = 1L;

	//@Id
	//@Column(name="TR_T_ID", unique=true, nullable=false)
	private long trTId;

	//@Column(name="TR_BID_PRICE", nullable=false)
	private double trBidPrice;

	//@Column(name="TR_QTY", nullable=false)
	private int trQty;

	//bi-directional many-to-one association to Broker
	//@ManyToOne(fetch=FetchType.LAZY)
	//@JoinColumn(name="TR_B_ID", nullable=false)
	private Broker broker;

	//bi-directional many-to-one association to CustomerAccount
	//@ManyToOne(fetch=FetchType.LAZY)
	//@JoinColumn(name="TR_CA_ID", nullable=false)
	private CustomerAccount customerAccount;

	//bi-directional many-to-one association to Security
	//@ManyToOne(fetch=FetchType.LAZY)
	//@JoinColumn(name="TR_S_SYMB", nullable=false)
	private Security security;

	//bi-directional one-to-one association to Trade
	//@OneToOne(fetch=FetchType.LAZY)
	//@JoinColumn(name="TR_T_ID", nullable=false, insertable=false, updatable=false)
	private Trade trade;

	//bi-directional many-to-one association to TradeType
	//@ManyToOne(fetch=FetchType.LAZY)
	//@JoinColumn(name="TR_TT_ID", nullable=false)
	private TradeType tradeType;

	public TradeRequest() {
	}

	public long getTrTId() {
		return this.trTId;
	}

	public void setTrTId(long trTId) {
		this.trTId = trTId;
	}

	public double getTrBidPrice() {
		return this.trBidPrice;
	}

	public void setTrBidPrice(double trBidPrice) {
		this.trBidPrice = trBidPrice;
	}

	public int getTrQty() {
		return this.trQty;
	}

	public void setTrQty(int trQty) {
		this.trQty = trQty;
	}

	public Broker getBroker() {
		return this.broker;
	}

	public void setBroker(Broker broker) {
		this.broker = broker;
	}

	public CustomerAccount getCustomerAccount() {
		return this.customerAccount;
	}

	public void setCustomerAccount(CustomerAccount customerAccount) {
		this.customerAccount = customerAccount;
	}

	public Security getSecurity() {
		return this.security;
	}

	public void setSecurity(Security security) {
		this.security = security;
	}

	public Trade getTrade() {
		return this.trade;
	}

	public void setTrade(Trade trade) {
		this.trade = trade;
	}

	public TradeType getTradeType() {
		return this.tradeType;
	}

	public void setTradeType(TradeType tradeType) {
		this.tradeType = tradeType;
	}

}