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
import java.math.BigDecimal;
import java.util.Set;


/**
 * The persistent class for the TAXRATE database table.
 * 
 */
//@Entity
//@Table(name="TAXRATE")
public class Taxrate implements Serializable {
	private static final long serialVersionUID = 1L;

	//@Id
	//@Column(name="TX_ID", unique=true, nullable=false, length=4)
	private String txId;

	//@Column(name="TX_NAME", nullable=false, length=50)
	private String txName;

	//@Column(name="TX_RATE", nullable=false, precision=6, scale=5)
	private BigDecimal txRate;

	//bi-directional many-to-many association to Customer
	//@ManyToMany(mappedBy="taxrates")
	private Set<Customer> customers;

	public Taxrate() {
	}

	public String getTxId() {
		return this.txId;
	}

	public void setTxId(String txId) {
		this.txId = txId;
	}

	public String getTxName() {
		return this.txName;
	}

	public void setTxName(String txName) {
		this.txName = txName;
	}

	public BigDecimal getTxRate() {
		return this.txRate;
	}

	public void setTxRate(BigDecimal txRate) {
		this.txRate = txRate;
	}

	public Set<Customer> getCustomers() {
		return this.customers;
	}

	public void setCustomers(Set<Customer> customers) {
		this.customers = customers;
	}

}