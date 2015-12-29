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
 * The primary key class for the COMPANY_COMPETITOR database table.
 * 
 */
//@Embeddable
public class CompanyCompetitorPK implements Serializable {
	//default serial version id, required for serializable classes.
	private static final long serialVersionUID = 1L;

	//@Column(name="CP_CO_ID", unique=true, nullable=false)
	private long cpCoId;

	//@Column(name="CP_COMP_CO_ID", unique=true, nullable=false)
	private long cpCompCoId;

	//@Column(name="CP_IN_ID", unique=true, nullable=false, length=2)
	private String cpInId;

	public CompanyCompetitorPK() {
	}
	public long getCpCoId() {
		return this.cpCoId;
	}
	public void setCpCoId(long cpCoId) {
		this.cpCoId = cpCoId;
	}
	public long getCpCompCoId() {
		return this.cpCompCoId;
	}
	public void setCpCompCoId(long cpCompCoId) {
		this.cpCompCoId = cpCompCoId;
	}
	public String getCpInId() {
		return this.cpInId;
	}
	public void setCpInId(String cpInId) {
		this.cpInId = cpInId;
	}

	public boolean equals(Object other) {
		if (this == other) {
			return true;
		}
		if (!(other instanceof CompanyCompetitorPK)) {
			return false;
		}
		CompanyCompetitorPK castOther = (CompanyCompetitorPK)other;
		return 
			(this.cpCoId == castOther.cpCoId)
			&& (this.cpCompCoId == castOther.cpCompCoId)
			&& this.cpInId.equals(castOther.cpInId);
	}

	public int hashCode() {
		final int prime = 31;
		int hash = 17;
		hash = hash * prime + ((int) (this.cpCoId ^ (this.cpCoId >>> 32)));
		hash = hash * prime + ((int) (this.cpCompCoId ^ (this.cpCompCoId >>> 32)));
		hash = hash * prime + this.cpInId.hashCode();
		
		return hash;
	}
}