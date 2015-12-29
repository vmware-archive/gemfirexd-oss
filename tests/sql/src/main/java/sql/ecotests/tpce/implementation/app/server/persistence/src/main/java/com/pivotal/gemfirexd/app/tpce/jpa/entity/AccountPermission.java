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


/**
 * The persistent class for the ACCOUNT_PERMISSION database table.
 * 
 */
@Entity
@Table(name="ACCOUNT_PERMISSION")
public class AccountPermission implements Serializable {
	private static final long serialVersionUID = 1L;

	@EmbeddedId
	private AccountPermissionPK id;

	@Column(name="AP_ACL", nullable=false, length=4)
	private String apAcl;

	@Column(name="AP_F_NAME", nullable=false, length=20)
	private String apFName;

	@Column(name="AP_L_NAME", nullable=false, length=25)
	private String apLName;

	//bi-directional many-to-one association to CustomerAccount
	@ManyToOne(fetch=FetchType.LAZY)
	@JoinColumn(name="AP_CA_ID", nullable=false, insertable=false, updatable=false)
	private CustomerAccount customerAccount;

	public AccountPermission() {
	}

	public AccountPermissionPK getId() {
		return this.id;
	}

	public void setId(AccountPermissionPK id) {
		this.id = id;
	}

	public String getApAcl() {
		return this.apAcl;
	}

	public void setApAcl(String apAcl) {
		this.apAcl = apAcl;
	}

	public String getApFName() {
		return this.apFName;
	}

	public void setApFName(String apFName) {
		this.apFName = apFName;
	}

	public String getApLName() {
		return this.apLName;
	}

	public void setApLName(String apLName) {
		this.apLName = apLName;
	}

	public CustomerAccount getCustomerAccount() {
		return this.customerAccount;
	}

	public void setCustomerAccount(CustomerAccount customerAccount) {
		this.customerAccount = customerAccount;
	}

}