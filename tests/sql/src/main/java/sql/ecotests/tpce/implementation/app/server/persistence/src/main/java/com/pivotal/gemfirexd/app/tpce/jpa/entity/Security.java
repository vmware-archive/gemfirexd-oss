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

import java.util.Date;
import java.util.Set;


/**
 * The persistent class for the SECURITY database table.
 * 
 */
@Entity
@Table(name="SECURITY")

//"select s_ex_id, s_name, s_symb from security where s_co_id = ?1 and s_issue = ?2";
//"select s_co_id, s_ex_id, s_name from security where s_symb = ?1";
@NamedNativeQueries(
		{ @NamedNativeQuery(name = "FindSecurityByCompany", 
		query = "select s_ex_id, s_name, s_symb from security where s_co_id = ?1 and s_issue = ?2",
		resultSetMapping="FindSecurityByCompanyResults"),
		@NamedNativeQuery(name = "FindSecurityBySymbol",
		query = "select s_co_id, s_ex_id, s_name from security where s_symb = ?1",
		resultSetMapping="FindSecurityBySymbolResults")})

@SqlResultSetMappings(
		{@SqlResultSetMapping(name="FindSecurityByCompanyResults",
				columns={@ColumnResult(name="s_ex_id"),
				@ColumnResult(name="s_name"),
				@ColumnResult(name="s_symb")}),
				@SqlResultSetMapping(name="FindSecurityBySymbolResults",
				columns={@ColumnResult(name="s_co_id"),
						@ColumnResult(name="s_ex_id"),
						@ColumnResult(name="s_name")})})

public class Security implements Serializable {
	private static final long serialVersionUID = 1L;

	@Id
	@Column(name="S_SYMB", unique=true, nullable=false, length=15)
	private String sSymb;

	@Column(name="S_52WK_HIGH", nullable=false)
	private double s52wkHigh;

	@Temporal(TemporalType.DATE)
	@Column(name="S_52WK_HIGH_DATE", nullable=false)
	private Date s52wkHighDate;

	@Column(name="S_52WK_LOW", nullable=false)
	private double s52wkLow;

	@Temporal(TemporalType.DATE)
	@Column(name="S_52WK_LOW_DATE", nullable=false)
	private Date s52wkLowDate;

	@Column(name="S_DIVIDEND", nullable=false)
	private double sDividend;

	@Temporal(TemporalType.DATE)
	@Column(name="S_EXCH_DATE", nullable=false)
	private Date sExchDate;

	@Column(name="S_ISSUE", nullable=false, length=6)
	private String sIssue;

	@Column(name="S_NAME", nullable=false, length=70)
	private String sName;

	@Column(name="S_NUM_OUT", nullable=false)
	private long sNumOut;

	@Column(name="S_PE", nullable=false)
	private double sPe;

	@Temporal(TemporalType.DATE)
	@Column(name="S_START_DATE", nullable=false)
	private Date sStartDate;

	@Column(name="S_YIELD", nullable=false)
	private double sYield;
	
	//bi-directional many-to-one association to DailyMarket
	@OneToMany(mappedBy="security")
	private Set<DailyMarket> dailyMarkets;

	//bi-directional many-to-one association to Holding
	@OneToMany(mappedBy="security")
	private Set<Holding> holdings;

	//bi-directional many-to-one association to HoldingSummary
	@OneToMany(mappedBy="security")
	private Set<HoldingSummary> holdingSummary;

	//bi-directional one-to-one association to LastTrade
	@OneToOne(mappedBy="security", fetch=FetchType.LAZY)
	private LastTrade lastTrade;

	//bi-directional many-to-one association to Company
	@ManyToOne(fetch=FetchType.LAZY)
	@JoinColumn(name="S_CO_ID", nullable=false)
	private Company company;

	//bi-directional many-to-one association to Exchange
	@ManyToOne(fetch=FetchType.LAZY)
	@JoinColumn(name="S_EX_ID", nullable=false)
	private Exchange exchange;

	//bi-directional many-to-one association to StatusType
	@ManyToOne(fetch=FetchType.LAZY)
	@JoinColumn(name="S_ST_ID", nullable=false)
	private StatusType statusType;

	//bi-directional many-to-one association to Trade
	@OneToMany(mappedBy="security")
	private Set<Trade> trades;

	//bi-directional many-to-one association to TradeRequest
	@OneToMany(mappedBy="security")
	private Set<TradeRequest> tradeRequests;

	//bi-directional many-to-many association to WatchList
	@ManyToMany(mappedBy="securities")
	private Set<WatchList> watchLists;

	public Security() {
	}

	public String getSSymb() {
		return this.sSymb;
	}

	public void setSSymb(String sSymb) {
		this.sSymb = sSymb;
	}

	public double getS52wkHigh() {
		return this.s52wkHigh;
	}

	public void setS52wkHigh(double s52wkHigh) {
		this.s52wkHigh = s52wkHigh;
	}

	public Date getS52wkHighDate() {
		return this.s52wkHighDate;
	}

	public void setS52wkHighDate(Date s52wkHighDate) {
		this.s52wkHighDate = s52wkHighDate;
	}

	public double getS52wkLow() {
		return this.s52wkLow;
	}

	public void setS52wkLow(double s52wkLow) {
		this.s52wkLow = s52wkLow;
	}

	public Date getS52wkLowDate() {
		return this.s52wkLowDate;
	}

	public void setS52wkLowDate(Date s52wkLowDate) {
		this.s52wkLowDate = s52wkLowDate;
	}

	public double getSDividend() {
		return this.sDividend;
	}

	public void setSDividend(double sDividend) {
		this.sDividend = sDividend;
	}

	public Date getSExchDate() {
		return this.sExchDate;
	}

	public void setSExchDate(Date sExchDate) {
		this.sExchDate = sExchDate;
	}

	public String getSIssue() {
		return this.sIssue;
	}

	public void setSIssue(String sIssue) {
		this.sIssue = sIssue;
	}

	public String getSName() {
		return this.sName;
	}

	public void setSName(String sName) {
		this.sName = sName;
	}

	public long getSNumOut() {
		return this.sNumOut;
	}

	public void setSNumOut(long sNumOut) {
		this.sNumOut = sNumOut;
	}

	public double getSPe() {
		return this.sPe;
	}

	public void setSPe(double sPe) {
		this.sPe = sPe;
	}

	public Date getSStartDate() {
		return this.sStartDate;
	}

	public void setSStartDate(Date sStartDate) {
		this.sStartDate = sStartDate;
	}

	public double getSYield() {
		return this.sYield;
	}

	public void setSYield(double sYield) {
		this.sYield = sYield;
	}

	public Set<DailyMarket> getDailyMarkets() {
		return this.dailyMarkets;
	}

	public void setDailyMarkets(Set<DailyMarket> dailyMarkets) {
		this.dailyMarkets = dailyMarkets;
	}

	public Set<Holding> getHoldings() {
		return this.holdings;
	}

	public void setHoldings(Set<Holding> holdings) {
		this.holdings = holdings;
	}

	public Set<HoldingSummary> getHoldingSummary() {
		return this.holdingSummary;
	}

	public void setHoldingSummaryTbls(Set<HoldingSummary> holdingSummary) {
		this.holdingSummary = holdingSummary;
	}

	public LastTrade getLastTrade() {
		return this.lastTrade;
	}

	public void setLastTrade(LastTrade lastTrade) {
		this.lastTrade = lastTrade;
	}

	public Company getCompany() {
		return this.company;
	}

	public void setCompany(Company company) {
		this.company = company;
	}

	public Exchange getExchange() {
		return this.exchange;
	}

	public void setExchange(Exchange exchange) {
		this.exchange = exchange;
	}

	public StatusType getStatusType() {
		return this.statusType;
	}

	public void setStatusType(StatusType statusType) {
		this.statusType = statusType;
	}

	public Set<Trade> getTrades() {
		return this.trades;
	}

	public void setTrades(Set<Trade> trades) {
		this.trades = trades;
	}

	public Set<TradeRequest> getTradeRequests() {
		return this.tradeRequests;
	}

	public void setTradeRequests(Set<TradeRequest> tradeRequests) {
		this.tradeRequests = tradeRequests;
	}

	public Set<WatchList> getWatchLists() {
		return this.watchLists;
	}

	public void setWatchLists(Set<WatchList> watchLists) {
		this.watchLists = watchLists;
	}	
}