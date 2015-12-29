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
 * The primary key class for the DAILY_MARKET database table.
 * 
 */
@Embeddable
public class DailyMarketPK implements Serializable {
	//default serial version id, required for serializable classes.
	private static final long serialVersionUID = 1L;

	@Temporal(TemporalType.DATE)
	@Column(name="DM_DATE", unique=true, nullable=false)
	private java.util.Date dmDate;

	@Column(name="DM_S_SYMB", unique=true, nullable=false, length=15)
	private String dmSSymb;

	public DailyMarketPK() {
	}
	public java.util.Date getDmDate() {
		return this.dmDate;
	}
	public void setDmDate(java.util.Date dmDate) {
		this.dmDate = dmDate;
	}
	public String getDmSSymb() {
		return this.dmSSymb;
	}
	public void setDmSSymb(String dmSSymb) {
		this.dmSSymb = dmSSymb;
	}

	public boolean equals(Object other) {
		if (this == other) {
			return true;
		}
		if (!(other instanceof DailyMarketPK)) {
			return false;
		}
		DailyMarketPK castOther = (DailyMarketPK)other;
		return 
			this.dmDate.equals(castOther.dmDate)
			&& this.dmSSymb.equals(castOther.dmSSymb);
	}

	public int hashCode() {
		final int prime = 31;
		int hash = 17;
		hash = hash * prime + this.dmDate.hashCode();
		hash = hash * prime + this.dmSSymb.hashCode();
		
		return hash;
	}
}