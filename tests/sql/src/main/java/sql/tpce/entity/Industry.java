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
import java.util.Set;


/**
 * The persistent class for the INDUSTRY database table.
 * 
 */
//@Entity
//@Table(name="INDUSTRY")
public class Industry implements Serializable {
	private static final long serialVersionUID = 1L;

	//@Id
	//@Column(name="IN_ID", unique=true, nullable=false, length=2)
	private String inId;

	//@Column(name="IN_NAME", nullable=false, length=50)
	private String inName;

	//bi-directional many-to-one association to Company
	//@OneToMany(mappedBy="industry")
	private Set<Company> companies;

	//bi-directional many-to-one association to CompanyCompetitor
	//@OneToMany(mappedBy="industry")
	private Set<CompanyCompetitor> companyCompetitors;

	//bi-directional many-to-one association to Sector
	//@ManyToOne(fetch=FetchType.LAZY)
	//@JoinColumn(name="IN_SC_ID", nullable=false)
	private Sector sector;

	public Industry() {
	}

	public String getInId() {
		return this.inId;
	}

	public void setInId(String inId) {
		this.inId = inId;
	}

	public String getInName() {
		return this.inName;
	}

	public void setInName(String inName) {
		this.inName = inName;
	}

	public Set<Company> getCompanies() {
		return this.companies;
	}

	public void setCompanies(Set<Company> companies) {
		this.companies = companies;
	}

	public Set<CompanyCompetitor> getCompanyCompetitors() {
		return this.companyCompetitors;
	}

	public void setCompanyCompetitors(Set<CompanyCompetitor> companyCompetitors) {
		this.companyCompetitors = companyCompetitors;
	}

	public Sector getSector() {
		return this.sector;
	}

	public void setSector(Sector sector) {
		this.sector = sector;
	}

}