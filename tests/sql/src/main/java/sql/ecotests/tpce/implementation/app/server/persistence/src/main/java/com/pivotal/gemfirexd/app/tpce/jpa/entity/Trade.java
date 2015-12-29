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
package com.pivotal.gemfirexd.app.tpce.jpa.entity;

import java.io.Serializable;
import javax.persistence.*;
import java.sql.Timestamp;
import java.util.Set;


/**
 * The persistent class for the TRADE database table.
 * 
 */
@Entity
@Table(name="TRADE")
public class Trade implements Serializable {
	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	@Column(name="T_ID", unique=true, nullable=false)
	private long tId;

	@Column(name="T_BID_PRICE", nullable=false)
	private double tBidPrice;

	@Column(name="T_CHRG", nullable=false)
	private double tChrg;

	@Column(name="T_COMM", nullable=false)
	private double tComm;

	@Column(name="T_DTS", nullable=false)
	private Timestamp tDts;

	@Column(name="T_EXEC_NAME", nullable=false, length=49)
	private String tExecName;

	@Column(name="T_IS_CASH", nullable=false)
	private short tIsCash;

	@Column(name="T_LIFO", nullable=false)
	private short tLifo;

	@Column(name="T_QTY", nullable=false)
	private int tQty;

	@Column(name="T_TAX", nullable=false)
	private double tTax;

	@Column(name="T_TRADE_PRICE")
	private double tTradePrice;

	//bi-directional one-to-one association to CashTransaction
	@OneToOne(mappedBy="trade", fetch=FetchType.LAZY)
	private CashTransaction cashTransaction;

	//bi-directional one-to-one association to Holding
	@OneToOne(mappedBy="trade", fetch=FetchType.LAZY)
	private Holding holding;

	//bi-directional many-to-one association to HoldingHistory
	@OneToMany(mappedBy="trade1")
	private Set<HoldingHistory> holdingHistories1;

	//bi-directional many-to-one association to HoldingHistory
	@OneToMany(mappedBy="trade2")
	private Set<HoldingHistory> holdingHistories2;

	//bi-directional one-to-one association to Settlement
	@OneToOne(mappedBy="trade", fetch=FetchType.LAZY)
	private Settlement settlement;

	//bi-directional many-to-one association to CustomerAccount
	@ManyToOne(fetch=FetchType.LAZY)
	@JoinColumn(name="T_CA_ID", nullable=false)
	private CustomerAccount customerAccount;

	//bi-directional many-to-one association to Security
	@ManyToOne(fetch=FetchType.LAZY)
	@JoinColumn(name="T_S_SYMB", nullable=false)
	private Security security;

	//bi-directional many-to-one association to StatusType
	@ManyToOne(fetch=FetchType.LAZY)
	@JoinColumn(name="T_ST_ID", nullable=false)
	private StatusType statusType;

	//bi-directional many-to-one association to TradeType
	@ManyToOne(fetch=FetchType.LAZY)
	@JoinColumn(name="T_TT_ID", nullable=false)
	private TradeType tradeType;

	//bi-directional many-to-one association to TradeHistory
	@OneToMany(mappedBy="trade")
	private Set<TradeHistory> tradeHistories;

	//bi-directional one-to-one association to TradeRequest
	@OneToOne(mappedBy="trade", fetch=FetchType.LAZY)
	private TradeRequest tradeRequest;

	public Trade() {
	}

	public long getTId() {
		return this.tId;
	}

	public void setTId(long tId) {
		this.tId = tId;
	}

	public double getTBidPrice() {
		return this.tBidPrice;
	}

	public void setTBidPrice(double tBidPrice) {
		this.tBidPrice = tBidPrice;
	}

	public double getTChrg() {
		return this.tChrg;
	}

	public void setTChrg(double tChrg) {
		this.tChrg = tChrg;
	}

	public double getTComm() {
		return this.tComm;
	}

	public void setTComm(double tComm) {
		this.tComm = tComm;
	}

	public Timestamp getTDts() {
		return this.tDts;
	}

	public void setTDts(Timestamp tDts) {
		this.tDts = tDts;
	}

	public String getTExecName() {
		return this.tExecName;
	}

	public void setTExecName(String tExecName) {
		this.tExecName = tExecName;
	}

	public short getTIsCash() {
		return this.tIsCash;
	}

	public void setTIsCash(short tIsCash) {
		this.tIsCash = tIsCash;
	}

	public short getTLifo() {
		return this.tLifo;
	}

	public void setTLifo(short tLifo) {
		this.tLifo = tLifo;
	}

	public int getTQty() {
		return this.tQty;
	}

	public void setTQty(int tQty) {
		this.tQty = tQty;
	}

	public double getTTax() {
		return this.tTax;
	}

	public void setTTax(double tTax) {
		this.tTax = tTax;
	}

	public double getTTradePrice() {
		return this.tTradePrice;
	}

	public void setTTradePrice(double tTradePrice) {
		this.tTradePrice = tTradePrice;
	}

	public CashTransaction getCashTransaction() {
		return this.cashTransaction;
	}

	public void setCashTransaction(CashTransaction cashTransaction) {
		this.cashTransaction = cashTransaction;
	}

	public Holding getHolding() {
		return this.holding;
	}

	public void setHolding(Holding holding) {
		this.holding = holding;
	}

	public Set<HoldingHistory> getHoldingHistories1() {
		return this.holdingHistories1;
	}

	public void setHoldingHistories1(Set<HoldingHistory> holdingHistories1) {
		this.holdingHistories1 = holdingHistories1;
	}

	public Set<HoldingHistory> getHoldingHistories2() {
		return this.holdingHistories2;
	}

	public void setHoldingHistories2(Set<HoldingHistory> holdingHistories2) {
		this.holdingHistories2 = holdingHistories2;
	}

	public Settlement getSettlement() {
		return this.settlement;
	}

	public void setSettlement(Settlement settlement) {
		this.settlement = settlement;
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

	public StatusType getStatusType() {
		return this.statusType;
	}

	public void setStatusType(StatusType statusType) {
		this.statusType = statusType;
	}

	public TradeType getTradeType() {
		return this.tradeType;
	}

	public void setTradeType(TradeType tradeType) {
		this.tradeType = tradeType;
	}

	public Set<TradeHistory> getTradeHistories() {
		return this.tradeHistories;
	}

	public void setTradeHistories(Set<TradeHistory> tradeHistories) {
		this.tradeHistories = tradeHistories;
	}

	public TradeRequest getTradeRequest() {
		return this.tradeRequest;
	}

	public void setTradeRequest(TradeRequest tradeRequest) {
		this.tradeRequest = tradeRequest;
	}

}