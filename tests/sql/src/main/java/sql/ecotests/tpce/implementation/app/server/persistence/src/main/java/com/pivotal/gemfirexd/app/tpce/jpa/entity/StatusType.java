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
 * The persistent class for the STATUS_TYPE database table.
 * 
 */
@Entity
@Table(name="STATUS_TYPE")
public class StatusType implements Serializable {
	private static final long serialVersionUID = 1L;

	@Id
	@Column(name="ST_ID", unique=true, nullable=false, length=4)
	private String stId;

	@Column(name="ST_NAME", nullable=false, length=10)
	private String stName;

	//bi-directional many-to-one association to Broker
	@OneToMany(mappedBy="statusType")
	private Set<Broker> brokers;

	//bi-directional many-to-one association to Company
	@OneToMany(mappedBy="statusType")
	private Set<Company> companies;

	//bi-directional many-to-one association to Customer
	@OneToMany(mappedBy="statusType")
	private Set<Customer> customers;

	//bi-directional many-to-one association to Security
	@OneToMany(mappedBy="statusType")
	private Set<Security> securities;

	//bi-directional many-to-one association to Trade
	@OneToMany(mappedBy="statusType")
	private Set<Trade> trades;

	//bi-directional many-to-one association to TradeHistory
	@OneToMany(mappedBy="statusType")
	private Set<TradeHistory> tradeHistories;

	public StatusType() {
	}

	public String getStId() {
		return this.stId;
	}

	public void setStId(String stId) {
		this.stId = stId;
	}

	public String getStName() {
		return this.stName;
	}

	public void setStName(String stName) {
		this.stName = stName;
	}

	public Set<Broker> getBrokers() {
		return this.brokers;
	}

	public void setBrokers(Set<Broker> brokers) {
		this.brokers = brokers;
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

	public Set<Security> getSecurities() {
		return this.securities;
	}

	public void setSecurities(Set<Security> securities) {
		this.securities = securities;
	}

	public Set<Trade> getTrades() {
		return this.trades;
	}

	public void setTrades(Set<Trade> trades) {
		this.trades = trades;
	}

	public Set<TradeHistory> getTradeHistories() {
		return this.tradeHistories;
	}

	public void setTradeHistories(Set<TradeHistory> tradeHistories) {
		this.tradeHistories = tradeHistories;
	}

}