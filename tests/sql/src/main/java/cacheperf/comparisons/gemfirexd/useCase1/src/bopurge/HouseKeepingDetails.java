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
package cacheperf.comparisons.gemfirexd.useCase1.src.bopurge;

public class HouseKeepingDetails {

  public HouseKeepingDetails() {
  }

  public HouseKeepingDetails(String backOffice, String transactionType,
                             int houseKeepingValue) {
    this.backOffice = backOffice;
    this.transactionType = transactionType;
    this.houseKeepingValue = houseKeepingValue;
  }

  private String backOffice;

  private String transactionType;

  private int houseKeepingValue;

  public String getBackOffice() {
    return backOffice;
  }

  public void setBackOffice(String backOffice) {
    this.backOffice = backOffice;
  }

  public String getTransactionType() {
    return transactionType;
  }

  public void setTransactionType(String transactionType) {
    this.transactionType = transactionType;
  }

  public int getHouseKeepingValue() {
    return houseKeepingValue;
  }

  public void setHouseKeepingValue(int houseKeepingValue) {
    this.houseKeepingValue = houseKeepingValue;
  }
}
