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
 * The primary key class for the TRADE_HISTORY database table.
 * 
 */
//@Embeddable
public class TradeHistoryPK implements Serializable {
	//default serial version id, required for serializable classes.
	private static final long serialVersionUID = 1L;

	//@Column(name="TH_T_ID", unique=true, nullable=false)
	private long thTId;

	//@Column(name="TH_ST_ID", unique=true, nullable=false, length=4)
	private String thStId;

	public TradeHistoryPK() {
	}
	public long getThTId() {
		return this.thTId;
	}
	public void setThTId(long thTId) {
		this.thTId = thTId;
	}
	public String getThStId() {
		return this.thStId;
	}
	public void setThStId(String thStId) {
		this.thStId = thStId;
	}

	public boolean equals(Object other) {
		if (this == other) {
			return true;
		}
		if (!(other instanceof TradeHistoryPK)) {
			return false;
		}
		TradeHistoryPK castOther = (TradeHistoryPK)other;
		return 
			(this.thTId == castOther.thTId)
			&& this.thStId.equals(castOther.thStId);
	}

	public int hashCode() {
		final int prime = 31;
		int hash = 17;
		hash = hash * prime + ((int) (this.thTId ^ (this.thTId >>> 32)));
		hash = hash * prime + this.thStId.hashCode();
		
		return hash;
	}
}