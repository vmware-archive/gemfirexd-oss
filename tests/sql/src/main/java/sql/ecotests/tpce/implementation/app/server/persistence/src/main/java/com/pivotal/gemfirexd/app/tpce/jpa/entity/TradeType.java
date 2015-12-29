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

import java.util.Set;


/**
 * The persistent class for the TRADE_TYPE database table.
 * 
 */
@Entity
@Table(name="TRADE_TYPE")

//Try the NamedNativeQuery & SqlResultSetMapping for TradeOrder
//Put GemFireXD specifics (e.g. hints) in this native SQL.
@NamedNativeQuery(name="FindTradeOrderTTQuery",
query="select tt_is_mrkt, tt_is_sell from trade_type where tt_id = ?1",
resultSetMapping="TradeOrderTTResults")

@SqlResultSetMapping(name="TradeOrderTTResults",
columns={@ColumnResult(name="tt_is_mrkt"),
		@ColumnResult(name="tt_is_sell")})

public class TradeType implements Serializable {
	private static final long serialVersionUID = 1L;

	@Id
	@Column(name="TT_ID", unique=true, nullable=false, length=3)
	private String ttId;

	@Column(name="TT_IS_MRKT", nullable=false)
	private short ttIsMrkt;

	@Column(name="TT_IS_SELL", nullable=false)
	private short ttIsSell;

	@Column(name="TT_NAME", nullable=false, length=12)
	private String ttName;

	//bi-directional many-to-one association to Charge
	@OneToMany(mappedBy="tradeType")
	private Set<Charge> charges;

	//bi-directional many-to-one association to CommissionRate
	@OneToMany(mappedBy="tradeType")
	private Set<CommissionRate> commissionRates;

	//bi-directional many-to-one association to Trade
	@OneToMany(mappedBy="tradeType")
	private Set<Trade> trades;

	//bi-directional many-to-one association to TradeRequest
	@OneToMany(mappedBy="tradeType")
	private Set<TradeRequest> tradeRequests;

	public TradeType() {
	}

	public String getTtId() {
		return this.ttId;
	}

	public void setTtId(String ttId) {
		this.ttId = ttId;
	}

	public short getTtIsMrkt() {
		return this.ttIsMrkt;
	}

	public void setTtIsMrkt(short ttIsMrkt) {
		this.ttIsMrkt = ttIsMrkt;
	}

	public short getTtIsSell() {
		return this.ttIsSell;
	}

	public void setTtIsSell(short ttIsSell) {
		this.ttIsSell = ttIsSell;
	}

	public String getTtName() {
		return this.ttName;
	}

	public void setTtName(String ttName) {
		this.ttName = ttName;
	}

	public Set<Charge> getCharges() {
		return this.charges;
	}

	public void setCharges(Set<Charge> charges) {
		this.charges = charges;
	}

	public Set<CommissionRate> getCommissionRates() {
		return this.commissionRates;
	}

	public void setCommissionRates(Set<CommissionRate> commissionRates) {
		this.commissionRates = commissionRates;
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

}