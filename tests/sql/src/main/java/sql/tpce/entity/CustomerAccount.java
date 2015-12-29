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
package sql.tpce.entity;

import java.io.Serializable;
//import javax.persistence.*;
import java.util.Set;


/**
 * The persistent class for the CUSTOMER_ACCOUNT database table.
 * 
 */
//@Entity
//@Table(name="CUSTOMER_ACCOUNT")
public class CustomerAccount implements Serializable {
	private static final long serialVersionUID = 1L;

	//@Id
	//@Column(name="CA_ID", unique=true, nullable=false)
	private long caId;

	//@Column(name="CA_BAL", nullable=false)
	private double caBal;

	//@Column(name="CA_NAME", length=50)
	private String caName;

	//@Column(name="CA_TAX_ST", nullable=false)
	private short caTaxSt;

	//bi-directional many-to-one association to AccountPermission
	//@OneToMany(mappedBy="customerAccount")
	private Set<AccountPermission> accountPermissions;

	//bi-directional many-to-one association to Broker
	//@ManyToOne(fetch=FetchType.LAZY)
	//@JoinColumn(name="CA_B_ID", nullable=false)
	private Broker broker;

	//bi-directional many-to-one association to Customer
	//@ManyToOne(fetch=FetchType.LAZY)
	//@JoinColumn(name="CA_C_ID", nullable=false)
	private Customer customer;

	//bi-directional many-to-one association to Holding
	//@OneToMany(mappedBy="customerAccount")
	private Set<Holding> holdings;

	//bi-directional many-to-one association to HoldingSummary
	//@OneToMany(mappedBy="customerAccount")
	private Set<HoldingSummary> holdingSummary;

	//bi-directional many-to-one association to Trade
	//@OneToMany(mappedBy="customerAccount")
	private Set<Trade> trades;

	//bi-directional many-to-one association to TradeRequest
	//@OneToMany(mappedBy="customerAccount")
	private Set<TradeRequest> tradeRequests;

	public CustomerAccount() {
	}

	public long getCaId() {
		return this.caId;
	}

	public void setCaId(long caId) {
		this.caId = caId;
	}

	public double getCaBal() {
		return this.caBal;
	}

	public void setCaBal(double caBal) {
		this.caBal = caBal;
	}

	public String getCaName() {
		return this.caName;
	}

	public void setCaName(String caName) {
		this.caName = caName;
	}

	public short getCaTaxSt() {
		return this.caTaxSt;
	}

	public void setCaTaxSt(short caTaxSt) {
		this.caTaxSt = caTaxSt;
	}

	public Set<AccountPermission> getAccountPermissions() {
		return this.accountPermissions;
	}

	public void setAccountPermissions(Set<AccountPermission> accountPermissions) {
		this.accountPermissions = accountPermissions;
	}

	public Broker getBroker() {
		return this.broker;
	}

	public void setBroker(Broker broker) {
		this.broker = broker;
	}

	public Customer getCustomer() {
		return this.customer;
	}

	public void setCustomer(Customer customer) {
		this.customer = customer;
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

	public void setHoldingSummary(Set<HoldingSummary> holdingSummary) {
		this.holdingSummary = holdingSummary;
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