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
 * The persistent class for the COMPANY database table.
 * 
 */
@Entity
@Table(name="COMPANY")
@NamedQueries(
		{ @NamedQuery(name = "FindCompanyById", 
			query = "select co.coName from Company co where co.coId = ?1"),
		  @NamedQuery(name = "FindCompanyByName", 
		  	query = "select co.coId from Company co where co.coName = ?1")})

public class Company implements Serializable {
	private static final long serialVersionUID = 1L;

	@Id
	@Column(name="CO_ID", unique=true, nullable=false)
	private long coId;

	@Column(name="CO_CEO", nullable=false, length=46)
	private String coCeo;

	@Column(name="CO_DESC", nullable=false, length=150)
	private String coDesc;

	@Column(name="CO_NAME", nullable=false, length=60)
	private String coName;

	@Temporal(TemporalType.DATE)
	@Column(name="CO_OPEN_DATE", nullable=false)
	private Date coOpenDate;

	@Column(name="CO_SP_RATE", nullable=false, length=4)
	private String coSpRate;

	//bi-directional many-to-one association to Address
	@ManyToOne(fetch=FetchType.LAZY)
	@JoinColumn(name="CO_AD_ID", nullable=false)
	private Address address;

	//bi-directional many-to-one association to Industry
	@ManyToOne(fetch=FetchType.LAZY)
	@JoinColumn(name="CO_IN_ID", nullable=false)
	private Industry industry;

	//bi-directional many-to-one association to StatusType
	@ManyToOne(fetch=FetchType.LAZY)
	@JoinColumn(name="CO_ST_ID", nullable=false)
	private StatusType statusType;

	//bi-directional many-to-one association to CompanyCompetitor
	@OneToMany(mappedBy="company1")
	private Set<CompanyCompetitor> companyCompetitors1;

	//bi-directional many-to-one association to CompanyCompetitor
	@OneToMany(mappedBy="company2")
	private Set<CompanyCompetitor> companyCompetitors2;

	//bi-directional many-to-one association to Financial
	@OneToMany(mappedBy="company")
	private Set<Financial> financials;

	//bi-directional many-to-many association to NewsItem
	@ManyToMany(mappedBy="companies")
	private Set<NewsItem> newsItems;

	//bi-directional many-to-one association to Security
	@OneToMany(mappedBy="company")
	private Set<Security> securities;

	public Company() {
	}

	public long getCoId() {
		return this.coId;
	}

	public void setCoId(long coId) {
		this.coId = coId;
	}

	public String getCoCeo() {
		return this.coCeo;
	}

	public void setCoCeo(String coCeo) {
		this.coCeo = coCeo;
	}

	public String getCoDesc() {
		return this.coDesc;
	}

	public void setCoDesc(String coDesc) {
		this.coDesc = coDesc;
	}

	public String getCoName() {
		return this.coName;
	}

	public void setCoName(String coName) {
		this.coName = coName;
	}

	public Date getCoOpenDate() {
		return this.coOpenDate;
	}

	public void setCoOpenDate(Date coOpenDate) {
		this.coOpenDate = coOpenDate;
	}

	public String getCoSpRate() {
		return this.coSpRate;
	}

	public void setCoSpRate(String coSpRate) {
		this.coSpRate = coSpRate;
	}

	public Address getAddress() {
		return this.address;
	}

	public void setAddress(Address address) {
		this.address = address;
	}

	public Industry getIndustry() {
		return this.industry;
	}

	public void setIndustry(Industry industry) {
		this.industry = industry;
	}

	public StatusType getStatusType() {
		return this.statusType;
	}

	public void setStatusType(StatusType statusType) {
		this.statusType = statusType;
	}

	public Set<CompanyCompetitor> getCompanyCompetitors1() {
		return this.companyCompetitors1;
	}

	public void setCompanyCompetitors1(Set<CompanyCompetitor> companyCompetitors1) {
		this.companyCompetitors1 = companyCompetitors1;
	}

	public Set<CompanyCompetitor> getCompanyCompetitors2() {
		return this.companyCompetitors2;
	}

	public void setCompanyCompetitors2(Set<CompanyCompetitor> companyCompetitors2) {
		this.companyCompetitors2 = companyCompetitors2;
	}

	public Set<Financial> getFinancials() {
		return this.financials;
	}

	public void setFinancials(Set<Financial> financials) {
		this.financials = financials;
	}

	public Set<NewsItem> getNewsItems() {
		return this.newsItems;
	}

	public void setNewsItems(Set<NewsItem> newsItems) {
		this.newsItems = newsItems;
	}

	public Set<Security> getSecurities() {
		return this.securities;
	}

	public void setSecurities(Set<Security> securities) {
		this.securities = securities;
	}

}