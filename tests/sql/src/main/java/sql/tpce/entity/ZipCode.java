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
 * The persistent class for the ZIP_CODE database table.
 * 
 */
//@Entity
//@Table(name="ZIP_CODE")
public class ZipCode implements Serializable {
	private static final long serialVersionUID = 1L;

	//@Id
	//@Column(name="ZC_CODE", unique=true, nullable=false, length=12)
	private String zcCode;

	//@Column(name="ZC_DIV", nullable=false, length=80)
	private String zcDiv;

	//@Column(name="ZC_TOWN", nullable=false, length=80)
	private String zcTown;

	//bi-directional many-to-one association to Address
	//@OneToMany(mappedBy="zipCode")
	private Set<Address> addresses;

	public ZipCode() {
	}

	public String getZcCode() {
		return this.zcCode;
	}

	public void setZcCode(String zcCode) {
		this.zcCode = zcCode;
	}

	public String getZcDiv() {
		return this.zcDiv;
	}

	public void setZcDiv(String zcDiv) {
		this.zcDiv = zcDiv;
	}

	public String getZcTown() {
		return this.zcTown;
	}

	public void setZcTown(String zcTown) {
		this.zcTown = zcTown;
	}

	public Set<Address> getAddresses() {
		return this.addresses;
	}

	public void setAddresses(Set<Address> addresses) {
		this.addresses = addresses;
	}

}