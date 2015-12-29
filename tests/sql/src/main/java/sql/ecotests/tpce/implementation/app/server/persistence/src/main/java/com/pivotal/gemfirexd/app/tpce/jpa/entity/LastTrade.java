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
import java.sql.Timestamp;


/**
 * The persistent class for the LAST_TRADE database table.
 * 
 */
@Entity
@Table(name="LAST_TRADE")
public class LastTrade implements Serializable {
	private static final long serialVersionUID = 1L;

	@Id
	@Column(name="LT_S_SYMB", unique=true, nullable=false, length=15)
	private String ltSSymb;

	@Column(name="LT_DTS", nullable=false)
	private Timestamp ltDts;

	@Column(name="LT_OPEN_PRICE", nullable=false)
	private double ltOpenPrice;

	@Column(name="LT_PRICE", nullable=false)
	private double ltPrice;

	@Column(name="LT_VOL")
	private long ltVol;

	//bi-directional one-to-one association to Security
	@OneToOne(fetch=FetchType.LAZY)
	@JoinColumn(name="LT_S_SYMB", nullable=false, insertable=false, updatable=false)
	private Security security;

	public LastTrade() {
	}

	public String getLtSSymb() {
		return this.ltSSymb;
	}

	public void setLtSSymb(String ltSSymb) {
		this.ltSSymb = ltSSymb;
	}

	public Timestamp getLtDts() {
		return this.ltDts;
	}

	public void setLtDts(Timestamp ltDts) {
		this.ltDts = ltDts;
	}

	public double getLtOpenPrice() {
		return this.ltOpenPrice;
	}

	public void setLtOpenPrice(double ltOpenPrice) {
		this.ltOpenPrice = ltOpenPrice;
	}

	public double getLtPrice() {
		return this.ltPrice;
	}

	public void setLtPrice(double ltPrice) {
		this.ltPrice = ltPrice;
	}

	public long getLtVol() {
		return this.ltVol;
	}

	public void setLtVol(long ltVol) {
		this.ltVol = ltVol;
	}

	public Security getSecurity() {
		return this.security;
	}

	public void setSecurity(Security security) {
		this.security = security;
	}

}