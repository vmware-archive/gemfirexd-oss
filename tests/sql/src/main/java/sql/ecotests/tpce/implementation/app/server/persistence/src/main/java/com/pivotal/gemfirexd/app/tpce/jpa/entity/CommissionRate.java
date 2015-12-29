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
import java.math.BigDecimal;


/**
 * The persistent class for the COMMISSION_RATE database table.
 * 
 */
@Entity
@Table(name="COMMISSION_RATE")
@NamedQuery(name = "FindCommissionRate", 
	query = "select cr.crRate from CommissionRate cr where cr.id.crCTier = ?1 and cr.id.crTtId = ?2 and cr.id.crExId = ?3 and cr.id.crFromQty <= ?4 and cr.crToQty >= ?5")
public class CommissionRate implements Serializable {
	private static final long serialVersionUID = 1L;

	@EmbeddedId
	private CommissionRatePK id;

	@Column(name="CR_RATE", nullable=false, precision=5, scale=2)
	private BigDecimal crRate;

	@Column(name="CR_TO_QTY", nullable=false)
	private int crToQty;

	//bi-directional many-to-one association to Exchange
	@ManyToOne(fetch=FetchType.LAZY)
	@JoinColumn(name="CR_EX_ID", nullable=false, insertable=false, updatable=false)
	private Exchange exchange;

	//bi-directional many-to-one association to TradeType
	@ManyToOne(fetch=FetchType.LAZY)
	@JoinColumn(name="CR_TT_ID", nullable=false, insertable=false, updatable=false)
	private TradeType tradeType;

	public CommissionRate() {
	}

	public CommissionRatePK getId() {
		return this.id;
	}

	public void setId(CommissionRatePK id) {
		this.id = id;
	}

	public BigDecimal getCrRate() {
		return this.crRate;
	}

	public void setCrRate(BigDecimal crRate) {
		this.crRate = crRate;
	}

	public int getCrToQty() {
		return this.crToQty;
	}

	public void setCrToQty(int crToQty) {
		this.crToQty = crToQty;
	}

	public Exchange getExchange() {
		return this.exchange;
	}

	public void setExchange(Exchange exchange) {
		this.exchange = exchange;
	}

	public TradeType getTradeType() {
		return this.tradeType;
	}

	public void setTradeType(TradeType tradeType) {
		this.tradeType = tradeType;
	}

}