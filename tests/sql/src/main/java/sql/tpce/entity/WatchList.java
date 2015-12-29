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
 * The persistent class for the WATCH_LIST database table.
 * 
 */
//@Entity
//@Table(name="WATCH_LIST")
public class WatchList implements Serializable {
	private static final long serialVersionUID = 1L;

	//@Id
	//@Column(name="WL_ID", unique=true, nullable=false)
	private long wlId;

	//bi-directional many-to-one association to Customer
	//@ManyToOne(fetch=FetchType.LAZY)
	//@JoinColumn(name="WL_C_ID", nullable=false)
	private Customer customer;

	//bi-directional many-to-many association to Security
	//@ManyToMany
	/*@JoinTable(
		name="WATCH_ITEM"
		, joinColumns={
			//@JoinColumn(name="WI_WL_ID", nullable=false)
			}
		, inverseJoinColumns={
			//@JoinColumn(name="WI_S_SYMB", nullable=false)
			}
		)
	*/
	private Set<Security> securities;

	public WatchList() {
	}

	public long getWlId() {
		return this.wlId;
	}

	public void setWlId(long wlId) {
		this.wlId = wlId;
	}

	public Customer getCustomer() {
		return this.customer;
	}

	public void setCustomer(Customer customer) {
		this.customer = customer;
	}

	public Set<Security> getSecurities() {
		return this.securities;
	}

	public void setSecurities(Set<Security> securities) {
		this.securities = securities;
	}

}