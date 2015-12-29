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
import java.util.Set;


/**
 * The persistent class for the SECTOR database table.
 * 
 */
@Entity
@Table(name="SECTOR")
public class Sector implements Serializable {
	private static final long serialVersionUID = 1L;

	@Id
	@Column(name="SC_ID", unique=true, nullable=false, length=2)
	private String scId;

	@Column(name="SC_NAME", nullable=false, length=30)
	private String scName;

	//bi-directional many-to-one association to Industry
	@OneToMany(mappedBy="sector")
	private Set<Industry> industries;

	public Sector() {
	}

	public String getScId() {
		return this.scId;
	}

	public void setScId(String scId) {
		this.scId = scId;
	}

	public String getScName() {
		return this.scName;
	}

	public void setScName(String scName) {
		this.scName = scName;
	}

	public Set<Industry> getIndustries() {
		return this.industries;
	}

	public void setIndustries(Set<Industry> industries) {
		this.industries = industries;
	}

}