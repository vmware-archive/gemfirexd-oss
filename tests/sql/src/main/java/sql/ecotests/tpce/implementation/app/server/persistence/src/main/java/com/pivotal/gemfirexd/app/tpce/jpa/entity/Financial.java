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


/**
 * The persistent class for the FINANCIAL database table.
 * 
 */
@Entity
@Table(name="FINANCIAL")
public class Financial implements Serializable {
	private static final long serialVersionUID = 1L;

	@EmbeddedId
	private FinancialPK id;

	@Column(name="FI_ASSETS", nullable=false)
	private double fiAssets;

	@Column(name="FI_BASIC_EPS", nullable=false)
	private double fiBasicEps;

	@Column(name="FI_DILUT_EPS", nullable=false)
	private double fiDilutEps;

	@Column(name="FI_INVENTORY", nullable=false)
	private double fiInventory;

	@Column(name="FI_LIABILITY", nullable=false)
	private double fiLiability;

	@Column(name="FI_MARGIN", nullable=false)
	private double fiMargin;

	@Column(name="FI_NET_EARN", nullable=false)
	private double fiNetEarn;

	@Column(name="FI_OUT_BASIC", nullable=false)
	private long fiOutBasic;

	@Column(name="FI_OUT_DILUT", nullable=false)
	private long fiOutDilut;

	@Temporal(TemporalType.DATE)
	@Column(name="FI_QTR_START_DATE", nullable=false)
	private Date fiQtrStartDate;

	@Column(name="FI_REVENUE", nullable=false)
	private double fiRevenue;

	//bi-directional many-to-one association to Company
	@ManyToOne(fetch=FetchType.LAZY)
	@JoinColumn(name="FI_CO_ID", nullable=false, insertable=false, updatable=false)
	private Company company;

	public Financial() {
	}

	public FinancialPK getId() {
		return this.id;
	}

	public void setId(FinancialPK id) {
		this.id = id;
	}

	public double getFiAssets() {
		return this.fiAssets;
	}

	public void setFiAssets(double fiAssets) {
		this.fiAssets = fiAssets;
	}

	public double getFiBasicEps() {
		return this.fiBasicEps;
	}

	public void setFiBasicEps(double fiBasicEps) {
		this.fiBasicEps = fiBasicEps;
	}

	public double getFiDilutEps() {
		return this.fiDilutEps;
	}

	public void setFiDilutEps(double fiDilutEps) {
		this.fiDilutEps = fiDilutEps;
	}

	public double getFiInventory() {
		return this.fiInventory;
	}

	public void setFiInventory(double fiInventory) {
		this.fiInventory = fiInventory;
	}

	public double getFiLiability() {
		return this.fiLiability;
	}

	public void setFiLiability(double fiLiability) {
		this.fiLiability = fiLiability;
	}

	public double getFiMargin() {
		return this.fiMargin;
	}

	public void setFiMargin(double fiMargin) {
		this.fiMargin = fiMargin;
	}

	public double getFiNetEarn() {
		return this.fiNetEarn;
	}

	public void setFiNetEarn(double fiNetEarn) {
		this.fiNetEarn = fiNetEarn;
	}

	public long getFiOutBasic() {
		return this.fiOutBasic;
	}

	public void setFiOutBasic(long fiOutBasic) {
		this.fiOutBasic = fiOutBasic;
	}

	public long getFiOutDilut() {
		return this.fiOutDilut;
	}

	public void setFiOutDilut(long fiOutDilut) {
		this.fiOutDilut = fiOutDilut;
	}

	public Date getFiQtrStartDate() {
		return this.fiQtrStartDate;
	}

	public void setFiQtrStartDate(Date fiQtrStartDate) {
		this.fiQtrStartDate = fiQtrStartDate;
	}

	public double getFiRevenue() {
		return this.fiRevenue;
	}

	public void setFiRevenue(double fiRevenue) {
		this.fiRevenue = fiRevenue;
	}

	public Company getCompany() {
		return this.company;
	}

	public void setCompany(Company company) {
		this.company = company;
	}

}