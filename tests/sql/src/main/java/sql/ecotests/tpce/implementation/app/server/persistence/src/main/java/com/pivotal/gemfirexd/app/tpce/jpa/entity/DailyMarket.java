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
 * The persistent class for the DAILY_MARKET database table.
 * 
 */
@Entity
@Table(name="DAILY_MARKET")
public class DailyMarket implements Serializable {
	private static final long serialVersionUID = 1L;

	@EmbeddedId
	private DailyMarketPK id;

	@Column(name="DM_CLOSE", nullable=false)
	private double dmClose;

	@Column(name="DM_HIGH", nullable=false)
	private double dmHigh;

	@Column(name="DM_LOW", nullable=false)
	private double dmLow;

	@Column(name="DM_VOL", nullable=false)
	private long dmVol;

	//bi-directional many-to-one association to Security
	@ManyToOne(fetch=FetchType.LAZY)
	@JoinColumn(name="DM_S_SYMB", nullable=false, insertable=false, updatable=false)
	private Security security;

	public DailyMarket() {
	}

	public DailyMarketPK getId() {
		return this.id;
	}

	public void setId(DailyMarketPK id) {
		this.id = id;
	}

	public double getDmClose() {
		return this.dmClose;
	}

	public void setDmClose(double dmClose) {
		this.dmClose = dmClose;
	}

	public double getDmHigh() {
		return this.dmHigh;
	}

	public void setDmHigh(double dmHigh) {
		this.dmHigh = dmHigh;
	}

	public double getDmLow() {
		return this.dmLow;
	}

	public void setDmLow(double dmLow) {
		this.dmLow = dmLow;
	}

	public long getDmVol() {
		return this.dmVol;
	}

	public void setDmVol(long dmVol) {
		this.dmVol = dmVol;
	}

	public Security getSecurity() {
		return this.security;
	}

	public void setSecurity(Security security) {
		this.security = security;
	}

}