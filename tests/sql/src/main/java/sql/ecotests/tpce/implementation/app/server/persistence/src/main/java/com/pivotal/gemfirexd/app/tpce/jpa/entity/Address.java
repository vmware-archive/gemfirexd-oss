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
 * The persistent class for the ADDRESS database table.
 * 
 */
@Entity
@Table(name="ADDRESS")
public class Address implements Serializable {
	private static final long serialVersionUID = 1L;

	@Id
	@Column(name="AD_ID", unique=true, nullable=false)
	private long adId;

	@Column(name="AD_CTRY", length=80)
	private String adCtry;

	@Column(name="AD_LINE1", length=80)
	private String adLine1;

	@Column(name="AD_LINE2", length=80)
	private String adLine2;

	//bi-directional many-to-one association to ZipCode
	@ManyToOne(fetch=FetchType.LAZY)
	@JoinColumn(name="AD_ZC_CODE", nullable=false)
	private ZipCode zipCode;

	//bi-directional many-to-one association to Company
	@OneToMany(mappedBy="address")
	private Set<Company> companies;

	//bi-directional many-to-one association to Customer
	@OneToMany(mappedBy="address")
	private Set<Customer> customers;

	//bi-directional many-to-one association to Exchange
	@OneToMany(mappedBy="address")
	private Set<Exchange> exchanges;

	public Address() {
	}

	public long getAdId() {
		return this.adId;
	}

	public void setAdId(long adId) {
		this.adId = adId;
	}

	public String getAdCtry() {
		return this.adCtry;
	}

	public void setAdCtry(String adCtry) {
		this.adCtry = adCtry;
	}

	public String getAdLine1() {
		return this.adLine1;
	}

	public void setAdLine1(String adLine1) {
		this.adLine1 = adLine1;
	}

	public String getAdLine2() {
		return this.adLine2;
	}

	public void setAdLine2(String adLine2) {
		this.adLine2 = adLine2;
	}

	public ZipCode getZipCode() {
		return this.zipCode;
	}

	public void setZipCode(ZipCode zipCode) {
		this.zipCode = zipCode;
	}

	public Set<Company> getCompanies() {
		return this.companies;
	}

	public void setCompanies(Set<Company> companies) {
		this.companies = companies;
	}

	public Set<Customer> getCustomers() {
		return this.customers;
	}

	public void setCustomers(Set<Customer> customers) {
		this.customers = customers;
	}

	public Set<Exchange> getExchanges() {
		return this.exchanges;
	}

	public void setExchanges(Set<Exchange> exchanges) {
		this.exchanges = exchanges;
	}

}