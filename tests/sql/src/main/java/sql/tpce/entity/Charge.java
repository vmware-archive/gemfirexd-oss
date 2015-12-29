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
 * The persistent class for the CHARGE database table.
 * 
 */
//@Entity
//@Table(name="CHARGE")
public class Charge implements Serializable {
	private static final long serialVersionUID = 1L;

	//@EmbeddedId
	private ChargePK id;

	//@Column(name="CH_CHRG")
	private double chChrg;

	//bi-directional many-to-one association to TradeType
	//@ManyToOne(fetch=FetchType.LAZY)
	//@JoinColumn(name="CH_TT_ID", nullable=false, insertable=false, updatable=false)
	private TradeType tradeType;

	public Charge() {
	}

	public ChargePK getId() {
		return this.id;
	}

	public void setId(ChargePK id) {
		this.id = id;
	}

	public double getChChrg() {
		return this.chChrg;
	}

	public void setChChrg(double chChrg) {
		this.chChrg = chChrg;
	}

	public TradeType getTradeType() {
		return this.tradeType;
	}

	public void setTradeType(TradeType tradeType) {
		this.tradeType = tradeType;
	}

}