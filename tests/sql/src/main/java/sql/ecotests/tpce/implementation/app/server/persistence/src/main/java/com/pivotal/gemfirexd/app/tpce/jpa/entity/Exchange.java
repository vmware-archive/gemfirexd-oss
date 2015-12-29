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
import java.util.Set;


/**
 * The persistent class for the EXCHANGE database table.
 * 
 */
@Entity
@Table(name="EXCHANGE")
public class Exchange implements Serializable {
	private static final long serialVersionUID = 1L;

	@Id
	@Column(name="EX_ID", unique=true, nullable=false, length=6)
	private String exId;

	@Column(name="EX_CLOSE", nullable=false, precision=4)
	private BigDecimal exClose;

	@Column(name="EX_DESC", length=150)
	private String exDesc;

	@Column(name="EX_NAME", nullable=false, length=100)
	private String exName;

	@Column(name="EX_NUM_SYMB", nullable=false, precision=6)
	private BigDecimal exNumSymb;

	@Column(name="EX_OPEN", nullable=false, precision=4)
	private BigDecimal exOpen;

	//bi-directional many-to-one association to CommissionRate
	@OneToMany(mappedBy="exchange")
	private Set<CommissionRate> commissionRates;

	//bi-directional many-to-one association to Address
	@ManyToOne(fetch=FetchType.LAZY)
	@JoinColumn(name="EX_AD_ID", nullable=false)
	private Address address;

	//bi-directional many-to-one association to Security
	@OneToMany(mappedBy="exchange")
	private Set<Security> securities;

	public Exchange() {
	}

	public String getExId() {
		return this.exId;
	}

	public void setExId(String exId) {
		this.exId = exId;
	}

	public BigDecimal getExClose() {
		return this.exClose;
	}

	public void setExClose(BigDecimal exClose) {
		this.exClose = exClose;
	}

	public String getExDesc() {
		return this.exDesc;
	}

	public void setExDesc(String exDesc) {
		this.exDesc = exDesc;
	}

	public String getExName() {
		return this.exName;
	}

	public void setExName(String exName) {
		this.exName = exName;
	}

	public BigDecimal getExNumSymb() {
		return this.exNumSymb;
	}

	public void setExNumSymb(BigDecimal exNumSymb) {
		this.exNumSymb = exNumSymb;
	}

	public BigDecimal getExOpen() {
		return this.exOpen;
	}

	public void setExOpen(BigDecimal exOpen) {
		this.exOpen = exOpen;
	}

	public Set<CommissionRate> getCommissionRates() {
		return this.commissionRates;
	}

	public void setCommissionRates(Set<CommissionRate> commissionRates) {
		this.commissionRates = commissionRates;
	}

	public Address getAddress() {
		return this.address;
	}

	public void setAddress(Address address) {
		this.address = address;
	}

	public Set<Security> getSecurities() {
		return this.securities;
	}

	public void setSecurities(Set<Security> securities) {
		this.securities = securities;
	}

}