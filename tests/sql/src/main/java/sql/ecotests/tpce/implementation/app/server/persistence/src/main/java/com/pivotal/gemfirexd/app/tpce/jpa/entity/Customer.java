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
import java.util.Date;
import java.util.Set;


/**
 * The persistent class for the CUSTOMER database table.
 * 
 */
@Entity
@Table(name="CUSTOMER")
public class Customer implements Serializable {
	private static final long serialVersionUID = 1L;

	@Id
	@Column(name="C_ID", unique=true, nullable=false)
	private long cId;

	@Column(name="C_AREA_1", length=3)
	private String cArea1;

	@Column(name="C_AREA_2", length=3)
	private String cArea2;

	@Column(name="C_AREA_3", length=3)
	private String cArea3;

	@Column(name="C_CTRY_1", length=3)
	private String cCtry1;

	@Column(name="C_CTRY_2", length=3)
	private String cCtry2;

	@Column(name="C_CTRY_3", length=3)
	private String cCtry3;

	@Temporal(TemporalType.DATE)
	@Column(name="C_DOB", nullable=false)
	private Date cDob;

	@Column(name="C_EMAIL_1", length=50)
	private String cEmail1;

	@Column(name="C_EMAIL_2", length=50)
	private String cEmail2;

	@Column(name="C_EXT_1", length=5)
	private String cExt1;

	@Column(name="C_EXT_2", length=5)
	private String cExt2;

	@Column(name="C_EXT_3", length=5)
	private String cExt3;

	@Column(name="C_F_NAME", nullable=false, length=20)
	private String cFName;

	@Column(name="C_GNDR", length=1)
	private String cGndr;

	@Column(name="C_L_NAME", nullable=false, length=25)
	private String cLName;

	@Column(name="C_LOCAL_1", length=10)
	private String cLocal1;

	@Column(name="C_LOCAL_2", length=10)
	private String cLocal2;

	@Column(name="C_LOCAL_3", length=10)
	private String cLocal3;

	@Column(name="C_M_NAME", length=1)
	private String cMName;

	@Column(name="C_TAX_ID", nullable=false, length=20)
	private String cTaxId;

	@Column(name="C_TIER", nullable=false)
	private short cTier;

	//If only address c_ad_id is needed, defining this field will avoid invoking proxy to get its value
	@Column(name="C_AD_ID", nullable=false, insertable=false, updatable=false)
	private long cAdId;
	
	//bi-directional many-to-one association to Address
	@ManyToOne(fetch=FetchType.LAZY)
	@JoinColumn(name="C_AD_ID", nullable=false)
	private Address address;

	@Column(name="C_ST_ID", nullable=false, insertable=false, updatable=false)
	private String cStId;
	
	//bi-directional many-to-one association to StatusType
	@ManyToOne(fetch=FetchType.LAZY)
	@JoinColumn(name="C_ST_ID", nullable=false)
	private StatusType statusType;

	//bi-directional many-to-many association to Taxrate
	@ManyToMany
	@JoinTable(
		name="CUSTOMER_TAXRATE"
		, joinColumns={
			@JoinColumn(name="CX_C_ID", nullable=false)
			}
		, inverseJoinColumns={
			@JoinColumn(name="CX_TX_ID", nullable=false)
			}
		)
	private Set<Taxrate> taxrates;

	//bi-directional many-to-one association to CustomerAccount
	@OneToMany(mappedBy="customer")
	private Set<CustomerAccount> customerAccounts;

	//bi-directional many-to-one association to WatchList
	@OneToMany(mappedBy="customer")
	private Set<WatchList> watchLists;

	public Customer() {
	}

	public long getCId() {
		return this.cId;
	}

	public void setCId(long cId) {
		this.cId = cId;
	}

	public String getCArea1() {
		return this.cArea1;
	}

	public void setCArea1(String cArea1) {
		this.cArea1 = cArea1;
	}

	public String getCArea2() {
		return this.cArea2;
	}

	public void setCArea2(String cArea2) {
		this.cArea2 = cArea2;
	}

	public String getCArea3() {
		return this.cArea3;
	}

	public void setCArea3(String cArea3) {
		this.cArea3 = cArea3;
	}

	public String getCCtry1() {
		return this.cCtry1;
	}

	public void setCCtry1(String cCtry1) {
		this.cCtry1 = cCtry1;
	}

	public String getCCtry2() {
		return this.cCtry2;
	}

	public void setCCtry2(String cCtry2) {
		this.cCtry2 = cCtry2;
	}

	public String getCCtry3() {
		return this.cCtry3;
	}

	public void setCCtry3(String cCtry3) {
		this.cCtry3 = cCtry3;
	}

	public Date getCDob() {
		return this.cDob;
	}

	public void setCDob(Date cDob) {
		this.cDob = cDob;
	}

	public String getCEmail1() {
		return this.cEmail1;
	}

	public void setCEmail1(String cEmail1) {
		this.cEmail1 = cEmail1;
	}

	public String getCEmail2() {
		return this.cEmail2;
	}

	public void setCEmail2(String cEmail2) {
		this.cEmail2 = cEmail2;
	}

	public String getCExt1() {
		return this.cExt1;
	}

	public void setCExt1(String cExt1) {
		this.cExt1 = cExt1;
	}

	public String getCExt2() {
		return this.cExt2;
	}

	public void setCExt2(String cExt2) {
		this.cExt2 = cExt2;
	}

	public String getCExt3() {
		return this.cExt3;
	}

	public void setCExt3(String cExt3) {
		this.cExt3 = cExt3;
	}

	public String getCFName() {
		return this.cFName;
	}

	public void setCFName(String cFName) {
		this.cFName = cFName;
	}

	public String getCGndr() {
		return this.cGndr;
	}

	public void setCGndr(String cGndr) {
		this.cGndr = cGndr;
	}

	public String getCLName() {
		return this.cLName;
	}

	public void setCLName(String cLName) {
		this.cLName = cLName;
	}

	public String getCLocal1() {
		return this.cLocal1;
	}

	public void setCLocal1(String cLocal1) {
		this.cLocal1 = cLocal1;
	}

	public String getCLocal2() {
		return this.cLocal2;
	}

	public void setCLocal2(String cLocal2) {
		this.cLocal2 = cLocal2;
	}

	public String getCLocal3() {
		return this.cLocal3;
	}

	public void setCLocal3(String cLocal3) {
		this.cLocal3 = cLocal3;
	}

	public String getCMName() {
		return this.cMName;
	}

	public void setCMName(String cMName) {
		this.cMName = cMName;
	}

	public String getCTaxId() {
		return this.cTaxId;
	}

	public void setCTaxId(String cTaxId) {
		this.cTaxId = cTaxId;
	}

	public short getCTier() {
		return this.cTier;
	}

	public void setCTier(short cTier) {
		this.cTier = cTier;
	}

	public Address getAddress() {
		return this.address;
	}

	public void setAddress(Address address) {
		this.address = address;
	}

	public long getCAdId() {
		return this.cAdId;
	}

	public void setCAdId(long cAdId) {
		this.cAdId = cAdId;
	}
	
	public StatusType getStatusType() {
		return this.statusType;
	}

	public void setStatusType(StatusType statusType) {
		this.statusType = statusType;
	}

	public String getCStId() {
		return this.cStId;
	}

	public void setCStId(String cStId) {
		this.cStId = cStId;
	}
	
	public Set<Taxrate> getTaxrates() {
		return this.taxrates;
	}

	public void setTaxrates(Set<Taxrate> taxrates) {
		this.taxrates = taxrates;
	}

	public Set<CustomerAccount> getCustomerAccounts() {
		return this.customerAccounts;
	}

	public void setCustomerAccounts(Set<CustomerAccount> customerAccounts) {
		this.customerAccounts = customerAccounts;
	}

	public Set<WatchList> getWatchLists() {
		return this.watchLists;
	}

	public void setWatchLists(Set<WatchList> watchLists) {
		this.watchLists = watchLists;
	}

}