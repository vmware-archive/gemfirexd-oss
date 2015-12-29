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
 * The primary key class for the CHARGE database table.
 * 
 */
@Embeddable
public class ChargePK implements Serializable {
	//default serial version id, required for serializable classes.
	private static final long serialVersionUID = 1L;

	@Column(name="CH_TT_ID", unique=true, nullable=false, length=3)
	private String chTtId;

	@Column(name="CH_C_TIER", unique=true, nullable=false)
	private short chCTier;

	public ChargePK() {
	}
	public String getChTtId() {
		return this.chTtId;
	}
	public void setChTtId(String chTtId) {
		this.chTtId = chTtId;
	}
	public short getChCTier() {
		return this.chCTier;
	}
	public void setChCTier(short chCTier) {
		this.chCTier = chCTier;
	}

	public boolean equals(Object other) {
		if (this == other) {
			return true;
		}
		if (!(other instanceof ChargePK)) {
			return false;
		}
		ChargePK castOther = (ChargePK)other;
		return 
			this.chTtId.equals(castOther.chTtId)
			&& (this.chCTier == castOther.chCTier);
	}

	public int hashCode() {
		final int prime = 31;
		int hash = 17;
		hash = hash * prime + this.chTtId.hashCode();
		hash = hash * prime + ((int) this.chCTier);
		
		return hash;
	}
}