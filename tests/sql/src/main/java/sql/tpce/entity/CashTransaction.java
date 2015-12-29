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
import java.sql.Timestamp;


/**
 * The persistent class for the CASH_TRANSACTION database table.
 * 
 */
//@Entity
//@Table(name="CASH_TRANSACTION")
public class CashTransaction implements Serializable {
	private static final long serialVersionUID = 1L;

	//@Id
	//@Column(name="CT_T_ID", unique=true, nullable=false)
	private long ctTId;

	//@Column(name="CT_AMT", nullable=false)
	private double ctAmt;

	//@Column(name="CT_DTS", nullable=false)
	private Timestamp ctDts;

	//@Column(name="CT_NAME", length=100)
	private String ctName;

	//bi-directional one-to-one association to Trade
	//@OneToOne(fetch=FetchType.LAZY)
	//@JoinColumn(name="CT_T_ID", nullable=false, insertable=false, updatable=false)
	private Trade trade;

	public CashTransaction() {
	}

	public long getCtTId() {
		return this.ctTId;
	}

	public void setCtTId(long ctTId) {
		this.ctTId = ctTId;
	}

	public double getCtAmt() {
		return this.ctAmt;
	}

	public void setCtAmt(double ctAmt) {
		this.ctAmt = ctAmt;
	}

	public Timestamp getCtDts() {
		return this.ctDts;
	}

	public void setCtDts(Timestamp ctDts) {
		this.ctDts = ctDts;
	}

	public String getCtName() {
		return this.ctName;
	}

	public void setCtName(String ctName) {
		this.ctName = ctName;
	}

	public Trade getTrade() {
		return this.trade;
	}

	public void setTrade(Trade trade) {
		this.trade = trade;
	}

}