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
import java.util.Set;


/**
 * The persistent class for the NEWS_ITEM database table.
 * 
 */
@Entity
@Table(name="NEWS_ITEM")
public class NewsItem implements Serializable {
	private static final long serialVersionUID = 1L;

	@Id
	@Column(name="NI_ID", unique=true, nullable=false)
	private long niId;

	@Column(name="NI_AUTHOR", length=30)
	private String niAuthor;

	@Column(name="NI_DTS", nullable=false)
	private Timestamp niDts;

	@Column(name="NI_HEADLINE", nullable=false, length=80)
	private String niHeadline;

	@Lob
	@Column(name="NI_ITEM", nullable=false)
	private byte[] niItem;

	@Column(name="NI_SOURCE", nullable=false, length=30)
	private String niSource;

	@Column(name="NI_SUMMARY", nullable=false, length=255)
	private String niSummary;

	//bi-directional many-to-many association to Company
	@ManyToMany
	@JoinTable(
		name="NEWS_XREF"
		, joinColumns={
			@JoinColumn(name="NX_NI_ID", nullable=false)
			}
		, inverseJoinColumns={
			@JoinColumn(name="NX_CO_ID", nullable=false)
			}
		)
	private Set<Company> companies;

	public NewsItem() {
	}

	public long getNiId() {
		return this.niId;
	}

	public void setNiId(long niId) {
		this.niId = niId;
	}

	public String getNiAuthor() {
		return this.niAuthor;
	}

	public void setNiAuthor(String niAuthor) {
		this.niAuthor = niAuthor;
	}

	public Timestamp getNiDts() {
		return this.niDts;
	}

	public void setNiDts(Timestamp niDts) {
		this.niDts = niDts;
	}

	public String getNiHeadline() {
		return this.niHeadline;
	}

	public void setNiHeadline(String niHeadline) {
		this.niHeadline = niHeadline;
	}

	public byte[] getNiItem() {
		return this.niItem;
	}

	public void setNiItem(byte[] niItem) {
		this.niItem = niItem;
	}

	public String getNiSource() {
		return this.niSource;
	}

	public void setNiSource(String niSource) {
		this.niSource = niSource;
	}

	public String getNiSummary() {
		return this.niSummary;
	}

	public void setNiSummary(String niSummary) {
		this.niSummary = niSummary;
	}

	public Set<Company> getCompanies() {
		return this.companies;
	}

	public void setCompanies(Set<Company> companies) {
		this.companies = companies;
	}

}