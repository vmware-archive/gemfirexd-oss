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
 * The persistent class for the HOLDING_HISTORY database table.
 * 
 */
@Entity
@Table(name="HOLDING_HISTORY")
public class HoldingHistory implements Serializable {
	private static final long serialVersionUID = 1L;

	@EmbeddedId
	private HoldingHistoryPK id;

	@Column(name="HH_AFTER_QTY", nullable=false)
	private int hhAfterQty;

	@Column(name="HH_BEFORE_QTY", nullable=false)
	private int hhBeforeQty;

	//bi-directional many-to-one association to Trade
	@ManyToOne(fetch=FetchType.LAZY)
	@JoinColumn(name="HH_T_ID", nullable=false, insertable=false, updatable=false)
	private Trade trade1;

	//bi-directional many-to-one association to Trade
	@ManyToOne(fetch=FetchType.LAZY)
	@JoinColumn(name="HH_H_T_ID", nullable=false, insertable=false, updatable=false)
	private Trade trade2;

	public HoldingHistory() {
	}

	public HoldingHistoryPK getId() {
		return this.id;
	}

	public void setId(HoldingHistoryPK id) {
		this.id = id;
	}

	public int getHhAfterQty() {
		return this.hhAfterQty;
	}

	public void setHhAfterQty(int hhAfterQty) {
		this.hhAfterQty = hhAfterQty;
	}

	public int getHhBeforeQty() {
		return this.hhBeforeQty;
	}

	public void setHhBeforeQty(int hhBeforeQty) {
		this.hhBeforeQty = hhBeforeQty;
	}

	public Trade getTrade1() {
		return this.trade1;
	}

	public void setTrade1(Trade trade1) {
		this.trade1 = trade1;
	}

	public Trade getTrade2() {
		return this.trade2;
	}

	public void setTrade2(Trade trade2) {
		this.trade2 = trade2;
	}

}