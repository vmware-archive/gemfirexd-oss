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
 * The persistent class for the BROKER database table.
 * 
 */
//@Entity
//@Table(name="BROKER")
public class Broker implements Serializable {
	private static final long serialVersionUID = 1L;

	//@Id
	//@Column(name="B_ID", unique=true, nullable=false)
	private long bId;

	//@Column(name="B_COMM_TOTAL", nullable=false)
	private double bCommTotal;

	//(name="B_NAME", nullable=false, length=49)
	private String bName;

	//@Column(name="B_NUM_TRADES", nullable=false)
	private int bNumTrades;

	//bi-directional many-to-one association to StatusType
	//@ManyToOne(fetch=FetchType.LAZY)
	//@JoinColumn(name="B_ST_ID", nullable=false)
	private StatusType statusType;

	//bi-directional many-to-one association to CustomerAccount
	//@OneToMany(mappedBy="broker")
	private Set<CustomerAccount> customerAccounts;

	//bi-directional many-to-one association to TradeRequest
	//@OneToMany(mappedBy="broker")
	private Set<TradeRequest> tradeRequests;

	public Broker() {
	}

	public long getBId() {
		return this.bId;
	}

	public void setBId(long bId) {
		this.bId = bId;
	}

	public double getBCommTotal() {
		return this.bCommTotal;
	}

	public void setBCommTotal(double bCommTotal) {
		this.bCommTotal = bCommTotal;
	}

	public String getBName() {
		return this.bName;
	}

	public void setBName(String bName) {
		this.bName = bName;
	}

	public int getBNumTrades() {
		return this.bNumTrades;
	}

	public void setBNumTrades(int bNumTrades) {
		this.bNumTrades = bNumTrades;
	}

	public StatusType getStatusType() {
		return this.statusType;
	}

	public void setStatusType(StatusType statusType) {
		this.statusType = statusType;
	}

	public Set<CustomerAccount> getCustomerAccounts() {
		return this.customerAccounts;
	}

	public void setCustomerAccounts(Set<CustomerAccount> customerAccounts) {
		this.customerAccounts = customerAccounts;
	}

	public Set<TradeRequest> getTradeRequests() {
		return this.tradeRequests;
	}

	public void setTradeRequests(Set<TradeRequest> tradeRequests) {
		this.tradeRequests = tradeRequests;
	}

}