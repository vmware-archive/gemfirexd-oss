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
 * The persistent class for the TRADE_HISTORY database table.
 * 
 */
//@Entity
//@Table(name="TRADE_HISTORY")
public class TradeHistory implements Serializable {
	private static final long serialVersionUID = 1L;

	//@EmbeddedId
	private TradeHistoryPK id;

	//@Column(name="TH_DTS", nullable=false)
	private Timestamp thDts;

	//bi-directional many-to-one association to StatusType
	//@ManyToOne(fetch=FetchType.LAZY)
	//@JoinColumn(name="TH_ST_ID", nullable=false, insertable=false, updatable=false)
	private StatusType statusType;

	//bi-directional many-to-one association to Trade
	//@ManyToOne(fetch=FetchType.LAZY)
	//@JoinColumn(name="TH_T_ID", nullable=false, insertable=false, updatable=false)
	private Trade trade;

	public TradeHistory() {
	}

	public TradeHistoryPK getId() {
		return this.id;
	}

	public void setId(TradeHistoryPK id) {
		this.id = id;
	}

	public Timestamp getThDts() {
		return this.thDts;
	}

	public void setThDts(Timestamp thDts) {
		this.thDts = thDts;
	}

	public StatusType getStatusType() {
		return this.statusType;
	}

	public void setStatusType(StatusType statusType) {
		this.statusType = statusType;
	}

	public Trade getTrade() {
		return this.trade;
	}

	public void setTrade(Trade trade) {
		this.trade = trade;
	}

}