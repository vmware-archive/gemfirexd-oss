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
package cacheperf.comparisons.gemfirexd.useCase1.src.matcher.model;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * Bean class to store BO metadata for matching keys.
 */
public class MatchingInfo implements Serializable {

  private String backOfficeCode;
  private String keyName;
  private String dataType;
  private int matchingPriority;
  private String chnDataTable;
  private String boDataTable;
  private Timestamp boOnBoardTimestamp;
  private Timestamp keyOnBoardTimestamp;
  private Timestamp boEndTimestamp;

  public MatchingInfo() {
  }

  public void setBackOfficeCode(String s) {
    this.backOfficeCode = s;
  }
  public String getBackOfficeCode() {
    return this.backOfficeCode;
  }

  public void setKeyName(String s) {
    this.keyName = s;
  }
  public String getKeyName() {
    return this.keyName;
  }

  public void setDataType(String s) {
    this.dataType = s;
  }
  public String getDataType() {
    return this.dataType;
  }

  public void setMatchingPriority(int i) {
    this.matchingPriority = i;
  }
  public int getMatchingPriority() {
    return this.matchingPriority;
  }

  public void setChnDataTable(String s) {
    this.chnDataTable = s;
  }
  public String getChnDataTable() {
    return this.chnDataTable;
  }

  public void setBoDataTable(String s) {
    this.boDataTable = s;
  }
  public String getBoDataTable() {
    return this.boDataTable;
  }

  public void setBoOnBoardTimestamp(Timestamp t) {
    this.boOnBoardTimestamp = t;
  }
  public Timestamp getBoOnBoardTimestamp() {
    return this.boOnBoardTimestamp;
  }

  public void setKeyOnBoardTimestamp(Timestamp t) {
    this.keyOnBoardTimestamp = t;
  }
  public Timestamp getKeyOnBoardTimestamp() {
    return this.keyOnBoardTimestamp;
  }

  public void setBoEndTimestamp(Timestamp t) {
    this.boEndTimestamp = t;
  }
  public Timestamp getBoEndTimestamp() {
    return boEndTimestamp;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("MatchingInfo:")
      .append(" backOfficeCode=").append(backOfficeCode)
      .append(" keyName=").append(keyName)
      .append(" dataType=").append(dataType)
      .append(" matchingPriority=").append(matchingPriority)
      .append(" chnDataTable=").append(chnDataTable)
      .append(" boDataTable=").append(boDataTable)
      .append(" boOnBoardTimestamp=").append(boOnBoardTimestamp)
      .append(" keyOnBoardTimestamp=").append(keyOnBoardTimestamp)
      .append(" boEndTimestamp=").append(boEndTimestamp);
    return sb.toString();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result
        + ((backOfficeCode == null) ? 0 : backOfficeCode.hashCode());
    result = prime * result + ((keyName == null) ? 0 : keyName.hashCode());
    result = prime * result + matchingPriority;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    MatchingInfo other = (MatchingInfo)obj;
    if (backOfficeCode == null) {
      if (other.backOfficeCode != null) {
        return false;
      }
    } else if (!backOfficeCode.equals(other.backOfficeCode)) {
      return false;
    }

    if (keyName == null) {
      if (other.keyName != null) {
        return false;
      }
    } else if (!keyName.equals(other.keyName)) {
      return false;
    }

    if (matchingPriority != other.matchingPriority) {
      return false;
    }

    return true;
  }
}
