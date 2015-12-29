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
 * The primary key class for the COMMISSION_RATE database table.
 * 
 */
//@Embeddable
public class CommissionRatePK implements Serializable {
	//default serial version id, required for serializable classes.
	private static final long serialVersionUID = 1L;

	//@Column(name="CR_C_TIER", unique=true, nullable=false)
	private short crCTier;

	//@Column(name="CR_TT_ID", unique=true, nullable=false, length=3)
	private String crTtId;

	//@Column(name="CR_EX_ID", unique=true, nullable=false, length=6)
	private String crExId;

	//@Column(name="CR_FROM_QTY", unique=true, nullable=false)
	private int crFromQty;

	public CommissionRatePK() {
	}
	public short getCrCTier() {
		return this.crCTier;
	}
	public void setCrCTier(short crCTier) {
		this.crCTier = crCTier;
	}
	public String getCrTtId() {
		return this.crTtId;
	}
	public void setCrTtId(String crTtId) {
		this.crTtId = crTtId;
	}
	public String getCrExId() {
		return this.crExId;
	}
	public void setCrExId(String crExId) {
		this.crExId = crExId;
	}
	public int getCrFromQty() {
		return this.crFromQty;
	}
	public void setCrFromQty(int crFromQty) {
		this.crFromQty = crFromQty;
	}

	public boolean equals(Object other) {
		if (this == other) {
			return true;
		}
		if (!(other instanceof CommissionRatePK)) {
			return false;
		}
		CommissionRatePK castOther = (CommissionRatePK)other;
		return 
			(this.crCTier == castOther.crCTier)
			&& this.crTtId.equals(castOther.crTtId)
			&& this.crExId.equals(castOther.crExId)
			&& (this.crFromQty == castOther.crFromQty);
	}

	public int hashCode() {
		final int prime = 31;
		int hash = 17;
		hash = hash * prime + ((int) this.crCTier);
		hash = hash * prime + this.crTtId.hashCode();
		hash = hash * prime + this.crExId.hashCode();
		hash = hash * prime + this.crFromQty;
		
		return hash;
	}
}