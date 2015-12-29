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


/**
 * The persistent class for the HOLDING_SUMMARY database table.
 * 
 */
//@Entity
//@Table(name="HOLDING_SUMMARY_TBL")
//NPE for the Holding_Summary view, see ticket:
//46865 (Error occurs in the left outer join of a table with a view)
//Switch to use the physical table HOLDING_SUMMARY_TBL
public class HoldingSummary implements Serializable {
	private static final long serialVersionUID = 1L;

	//@EmbeddedId
	private HoldingSummaryPK id;
	
	//@Column(name="HS_QTY")
	private int hsQty;

	//bi-directional many-to-one association to CustomerAccount
	//@ManyToOne(fetch=FetchType.LAZY)
	//@JoinColumn(name="HS_CA_ID", nullable=false, insertable=false, updatable=false)
	//@MapsId("hsCaId")
	private CustomerAccount customerAccount;

	//bi-directional many-to-one association to Security
	//@ManyToOne(fetch=FetchType.LAZY)
	//@JoinColumn(name="HS_S_SYMB", nullable=false, insertable=false, updatable=false)
	//@MapsId("hsSSymb")
	private Security security;
	
	public HoldingSummary() {
	}

	public HoldingSummaryPK getId() {
		return this.id;
	}

	public void setId(HoldingSummaryPK id) {
		this.id = id;
	}

	public int getHsQty() {
		return this.hsQty;
	}

	public void setHsQty(int hsQty) {
		this.hsQty = hsQty;
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
}