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
 * The persistent class for the COMPANY_COMPETITOR database table.
 * 
 */
@Entity
@Table(name="COMPANY_COMPETITOR")
public class CompanyCompetitor implements Serializable {
	private static final long serialVersionUID = 1L;

	@EmbeddedId
	private CompanyCompetitorPK id;

	//bi-directional many-to-one association to Company
	@ManyToOne(fetch=FetchType.LAZY)
	@JoinColumn(name="CP_COMP_CO_ID", nullable=false, insertable=false, updatable=false)
	private Company company1;

	//bi-directional many-to-one association to Company
	@ManyToOne(fetch=FetchType.LAZY)
	@JoinColumn(name="CP_CO_ID", nullable=false, insertable=false, updatable=false)
	private Company company2;

	//bi-directional many-to-one association to Industry
	@ManyToOne(fetch=FetchType.LAZY)
	@JoinColumn(name="CP_IN_ID", nullable=false, insertable=false, updatable=false)
	private Industry industry;

	public CompanyCompetitor() {
	}

	public CompanyCompetitorPK getId() {
		return this.id;
	}

	public void setId(CompanyCompetitorPK id) {
		this.id = id;
	}

	public Company getCompany1() {
		return this.company1;
	}

	public void setCompany1(Company company1) {
		this.company1 = company1;
	}

	public Company getCompany2() {
		return this.company2;
	}

	public void setCompany2(Company company2) {
		this.company2 = company2;
	}

	public Industry getIndustry() {
		return this.industry;
	}

	public void setIndustry(Industry industry) {
		this.industry = industry;
	}

}