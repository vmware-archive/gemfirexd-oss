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
 * The primary key class for the FINANCIAL database table.
 * 
 */
@Embeddable
public class FinancialPK implements Serializable {
	//default serial version id, required for serializable classes.
	private static final long serialVersionUID = 1L;

	@Column(name="FI_CO_ID", unique=true, nullable=false)
	private long fiCoId;

	@Column(name="FI_YEAR", unique=true, nullable=false, precision=4)
	private long fiYear;

	@Column(name="FI_QTR", unique=true, nullable=false)
	private short fiQtr;

	public FinancialPK() {
	}
	public long getFiCoId() {
		return this.fiCoId;
	}
	public void setFiCoId(long fiCoId) {
		this.fiCoId = fiCoId;
	}
	public long getFiYear() {
		return this.fiYear;
	}
	public void setFiYear(long fiYear) {
		this.fiYear = fiYear;
	}
	public short getFiQtr() {
		return this.fiQtr;
	}
	public void setFiQtr(short fiQtr) {
		this.fiQtr = fiQtr;
	}

	public boolean equals(Object other) {
		if (this == other) {
			return true;
		}
		if (!(other instanceof FinancialPK)) {
			return false;
		}
		FinancialPK castOther = (FinancialPK)other;
		return 
			(this.fiCoId == castOther.fiCoId)
			&& (this.fiYear == castOther.fiYear)
			&& (this.fiQtr == castOther.fiQtr);
	}

	public int hashCode() {
		final int prime = 31;
		int hash = 17;
		hash = hash * prime + ((int) (this.fiCoId ^ (this.fiCoId >>> 32)));
		hash = hash * prime + ((int) (this.fiYear ^ (this.fiYear >>> 32)));
		hash = hash * prime + ((int) this.fiQtr);
		
		return hash;
	}
}