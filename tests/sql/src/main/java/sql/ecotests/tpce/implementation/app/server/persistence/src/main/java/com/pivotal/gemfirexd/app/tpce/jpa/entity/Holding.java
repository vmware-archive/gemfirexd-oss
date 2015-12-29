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


/**
 * The persistent class for the HOLDING database table.
 * 
 */
@Entity
@Table(name="HOLDING")

@NamedQueries(
		{ @NamedQuery(name = "FindHolidingByCaIdSymbDesc", 
		query = "select h.hQty, h.hPrice from Holding h where h.customerAccount.caId = ? and h.security.sSymb = ? order by h.hDts desc"),
		@NamedQuery(name = "FindHolidingByCaIdSymbAsc",
		query = "select h.hQty, h.hPrice from Holding h where h.customerAccount.caId = ? and h.security.sSymb = ? order by h.hDts asc")})

public class Holding implements Serializable {
	private static final long serialVersionUID = 1L;

	@Id
	@Column(name="H_T_ID", unique=true, nullable=false)
	private long hTId;

	@Column(name="H_DTS", nullable=false)
	private Timestamp hDts;

	@Column(name="H_PRICE", nullable=false)
	private double hPrice;

	@Column(name="H_QTY", nullable=false)
	private int hQty;

	//bi-directional many-to-one association to CustomerAccount
	@ManyToOne(fetch=FetchType.LAZY)
	@JoinColumn(name="H_CA_ID", nullable=false)
	private CustomerAccount customerAccount;

	//bi-directional many-to-one association to Security
	@ManyToOne(fetch=FetchType.LAZY)
	@JoinColumn(name="H_S_SYMB", nullable=false)
	private Security security;

	//bi-directional one-to-one association to Trade
	@OneToOne(fetch=FetchType.LAZY)
	@JoinColumn(name="H_T_ID", nullable=false, insertable=false, updatable=false)
	private Trade trade;

	public Holding() {
	}

	public long getHTId() {
		return this.hTId;
	}

	public void setHTId(long hTId) {
		this.hTId = hTId;
	}

	public Timestamp getHDts() {
		return this.hDts;
	}

	public void setHDts(Timestamp hDts) {
		this.hDts = hDts;
	}

	public double getHPrice() {
		return this.hPrice;
	}

	public void setHPrice(double hPrice) {
		this.hPrice = hPrice;
	}

	public int getHQty() {
		return this.hQty;
	}

	public void setHQty(int hQty) {
		this.hQty = hQty;
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

}