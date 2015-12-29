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
import java.util.Date;


/**
 * The persistent class for the SETTLEMENT database table.
 * 
 */
//@Entity
//@Table(name="SETTLEMENT")
public class Settlement implements Serializable {
	private static final long serialVersionUID = 1L;

	//@Id
	//@Column(name="SE_T_ID", unique=true, nullable=false)
	private long seTId;

	//@Column(name="SE_AMT", nullable=false)
	private double seAmt;

	//@Temporal(TemporalType.DATE)
	//@Column(name="SE_CASH_DUE_DATE", nullable=false)
	private Date seCashDueDate;

	//@Column(name="SE_CASH_TYPE", nullable=false, length=40)
	private String seCashType;

	//bi-directional one-to-one association to Trade
	//@OneToOne(fetch=FetchType.LAZY)
	//@JoinColumn(name="SE_T_ID", nullable=false, insertable=false, updatable=false)
	private Trade trade;

	public Settlement() {
	}

	public long getSeTId() {
		return this.seTId;
	}

	public void setSeTId(long seTId) {
		this.seTId = seTId;
	}

	public double getSeAmt() {
		return this.seAmt;
	}

	public void setSeAmt(double seAmt) {
		this.seAmt = seAmt;
	}

	public Date getSeCashDueDate() {
		return this.seCashDueDate;
	}

	public void setSeCashDueDate(Date seCashDueDate) {
		this.seCashDueDate = seCashDueDate;
	}

	public String getSeCashType() {
		return this.seCashType;
	}

	public void setSeCashType(String seCashType) {
		this.seCashType = seCashType;
	}

	public Trade getTrade() {
		return this.trade;
	}

	public void setTrade(Trade trade) {
		this.trade = trade;
	}

}