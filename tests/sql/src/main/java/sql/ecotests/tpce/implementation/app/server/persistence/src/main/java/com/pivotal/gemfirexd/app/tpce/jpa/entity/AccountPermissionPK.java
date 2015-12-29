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
 * The primary key class for the ACCOUNT_PERMISSION database table.
 * 
 */
@Embeddable
public class AccountPermissionPK implements Serializable {
	//default serial version id, required for serializable classes.
	private static final long serialVersionUID = 1L;

	@Column(name="AP_CA_ID", unique=true, nullable=false)
	private long apCaId;

	@Column(name="AP_TAX_ID", unique=true, nullable=false, length=20)
	private String apTaxId;

	public AccountPermissionPK() {
	}
	public long getApCaId() {
		return this.apCaId;
	}
	public void setApCaId(long apCaId) {
		this.apCaId = apCaId;
	}
	public String getApTaxId() {
		return this.apTaxId;
	}
	public void setApTaxId(String apTaxId) {
		this.apTaxId = apTaxId;
	}

	public boolean equals(Object other) {
		if (this == other) {
			return true;
		}
		if (!(other instanceof AccountPermissionPK)) {
			return false;
		}
		AccountPermissionPK castOther = (AccountPermissionPK)other;
		return 
			(this.apCaId == castOther.apCaId)
			&& this.apTaxId.equals(castOther.apTaxId);
	}

	public int hashCode() {
		final int prime = 31;
		int hash = 17;
		hash = hash * prime + ((int) (this.apCaId ^ (this.apCaId >>> 32)));
		hash = hash * prime + this.apTaxId.hashCode();
		
		return hash;
	}
}