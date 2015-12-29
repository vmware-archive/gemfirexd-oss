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

public class TransactEntityId {

  private int _hashCode = 0;
  

  
  private double field001;
  private double transactNo;
  private String field007;
  private String field008;
  private double field016;
  private double field018;
  private String field026;
  private String field027;
  private double field034;
  private double field041;
  private double field051;
  
  
  
  public int get_hashCode() {
    return _hashCode;
  }

  public void set_hashCode(int code) {
    _hashCode = code;
  }
  
  public double getField001() {
    return field001;
  }

  public void setField001(double field001) {
    this.field001 = field001;
  }

  public double getTransactNo() {
    return transactNo;
  }

  public void setTransactNo(double transactNo) {
    this.transactNo = transactNo;
  }

  public String getField007() {
    return field007;
  }

  public void setField007(String field007) {
    this.field007 = field007;
  }

  public String getField008() {
    return field008;
  }

  public void setField008(String field008) {
    this.field008 = field008;
  }

  public double getField016() {
    return field016;
  }

  public void setField016(double field016) {
    this.field016 = field016;
  }

  public double getField018() {
    return field018;
  }

  public void setField018(double field018) {
    this.field018 = field018;
  }

  public String getField026() {
    return field026;
  }

  public void setField026(String field026) {
    this.field026 = field026;
  }

  public String getField027() {
    return field027;
  }

  public void setField027(String field027) {
    this.field027 = field027;
  }

  public double getField034() {
    return field034;
  }

  public void setField034(double field034) {
    this.field034 = field034;
  }

  public double getField041() {
    return field041;
  }

  public void setField041(double field041) {
    this.field041 = field041;
  }

  public double getField051() {
    return field051;
  }

  public void setField051(double field051) {
    this.field051 = field051;
  }


  @Override
  public boolean equals(Object arg0) {
  
    TransactEntityId tmpId = (TransactEntityId)arg0;
    
    if(transactNo == tmpId.getTransactNo()){
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
