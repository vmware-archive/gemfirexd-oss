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
 * The primary key class for the HOLDING_SUMMARY database table (view).
 * 
 */
//@Embeddable
public class HoldingSummaryPK implements Serializable {
	//default serial version id, required for serializable classes.
	private static final long serialVersionUID = 1L;

	//@Column(name="HS_CA_ID", nullable=false, insertable=false, updatable=false)
	private long hsCaId;

	//@Column(name="HS_S_SYMB", nullable=false, length=15, insertable=false, updatable=false)
	private String hsSSymb;
	
	public HoldingSummaryPK() {
	}
	public long getHsCaId() {
		return this.hsCaId;
	}

	public void setHsCaId(long hsCaId) {
		this.hsCaId = hsCaId;
	}

	public String getHsSSymb() {
		return this.hsSSymb;
	}

	public void setHsSSymb(String hsSSymb) {
		this.hsSSymb = hsSSymb;
	}

	public boolean equals(Object other) {
		if (this == other) {
			return true;
		}
		if (!(other instanceof HoldingSummaryPK)) {
			return false;
		}
		HoldingSummaryPK castOther = (HoldingSummaryPK)other;
		return 
			(this.hsCaId == castOther.hsCaId)
			&& (this.hsSSymb.equals(castOther.hsSSymb));
	}

	public int hashCode() {
		final int prime = 31;
		int hash = 17;
		hash = hash * prime + ((int) (this.hsCaId ^ (this.hsCaId >>> 32)));
		hash = hash * prime + this.hsSSymb.hashCode();
		
		return hash;
	}
}