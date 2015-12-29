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
 * The primary key class for the HOLDING_HISTORY database table.
 * 
 */
@Embeddable
public class HoldingHistoryPK implements Serializable {
	//default serial version id, required for serializable classes.
	private static final long serialVersionUID = 1L;

	@Column(name="HH_H_T_ID", unique=true, nullable=false)
	private long hhHTId;

	@Column(name="HH_T_ID", unique=true, nullable=false)
	private long hhTId;

	public HoldingHistoryPK() {
	}
	public long getHhHTId() {
		return this.hhHTId;
	}
	public void setHhHTId(long hhHTId) {
		this.hhHTId = hhHTId;
	}
	public long getHhTId() {
		return this.hhTId;
	}
	public void setHhTId(long hhTId) {
		this.hhTId = hhTId;
	}

	public boolean equals(Object other) {
		if (this == other) {
			return true;
		}
		if (!(other instanceof HoldingHistoryPK)) {
			return false;
		}
		HoldingHistoryPK castOther = (HoldingHistoryPK)other;
		return 
			(this.hhHTId == castOther.hhHTId)
			&& (this.hhTId == castOther.hhTId);
	}

	public int hashCode() {
		final int prime = 31;
		int hash = 17;
		hash = hash * prime + ((int) (this.hhHTId ^ (this.hhHTId >>> 32)));
		hash = hash * prime + ((int) (this.hhTId ^ (this.hhTId >>> 32)));
		
		return hash;
	}
}