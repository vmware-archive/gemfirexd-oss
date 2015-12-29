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
package cacheperf.poc.useCase6;

public class CashflowEntityId{

  private int _hashCode = 0;
  
  
  public int get_hashCode() {
    return _hashCode;
  }

  public void set_hashCode(int code) {
    _hashCode = code;
  }
  
  private double transactNo;
  private double legNo;
  private double cashflowNo;
  private String field005;
  private String field006;
  private String field007;
  private String field010;
  private String field011;
  private double field014;
  private double field045;
  private double field046;
  private String field047;
  private String field048;
  private double field051;
  private String field053;
  private String field054;


  
  public double getTransactNo() {
    return transactNo;
  }

  public void setTransactNo(double transactNo) {
    this.transactNo = transactNo;
  }

  public double getLegNo() {
    return legNo;
  }

  public void setLegNo(double legNo) {
    this.legNo = legNo;
  }

  public double getCashflowNo() {
    return cashflowNo;
  }

  public void setCashflowNo(double cashflowNo) {
    this.cashflowNo = cashflowNo;
  }

  public String getField005() {
    return field005;
  }

  public void setField005(String field005) {
    this.field005 = field005;
  }

  public String getField006() {
    return field006;
  }

  public void setField006(String field006) {
    this.field006 = field006;
  }

  public String getField007() {
    return field007;
  }

  public void setField007(String field007) {
    this.field007 = field007;
  }

  public String getField010() {
    return field010;
  }

  public void setField010(String field010) {
    this.field010 = field010;
  }

  public String getField011() {
    return field011;
  }

  public void setField011(String field011) {
    this.field011 = field011;
  }

  public double getField014() {
    return field014;
  }

  public void setField014(double field014) {
    this.field014 = field014;
  }

  public double getField045() {
    return field045;
  }

  public void setField045(double field045) {
    this.field045 = field045;
  }

  public double getField046() {
    return field046;
  }

  public void setField046(double field046) {
    this.field046 = field046;
  }

  public String getField047() {
    return field047;
  }

  public void setField047(String field047) {
    this.field047 = field047;
  }

  public String getField048() {
    return field048;
  }

  public void setField048(String field048) {
    this.field048 = field048;
  }

  public double getField051() {
    return field051;
  }

  public void setField051(double field051) {
    this.field051 = field051;
  }

  public String getField053() {
    return field053;
  }

  public void setField053(String field053) {
    this.field053 = field053;
  }

  public String getField054() {
    return field054;
  }

  public void setField054(String field054) {
    this.field054 = field054;
  }

  @Override
  public boolean equals(Object arg0) {
    CashflowEntityId tmpId = (CashflowEntityId)arg0;
    
    if(transactNo == tmpId.getTransactNo() && 
        legNo == tmpId.getLegNo() && 
        cashflowNo == tmpId.getCashflowNo()){
      return true;
    }else{
      return false;
    }    
    
    
  }

  @Override
  public int hashCode() {
    if(this._hashCode == 0){
      this._hashCode = super.hashCode();
    }
    
    return this._hashCode;
  }
}
