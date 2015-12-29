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
package management.operations;

import java.io.Serializable;

import management.util.HydraUtil;

public class RegionObject implements Serializable {

  /**
	 * 
	 */
  private static final long serialVersionUID = 1L;

  private int prop1, prop2, prop3, prop4, prop5, prop6;
  private int prop7, prop8, prop9, prop10, prop11, prop12;
  private int prop13, prop14, prop15, prop16, prop17, prop18;

  public RegionObject(int basevalue) {
    this.prop1 = basevalue;
    this.prop2 = 2 * basevalue;
    this.prop3 = 3 * basevalue;
    this.prop4 = 4 * basevalue;
    this.prop5 = 5 * basevalue;
    this.prop6 = 6 * basevalue;
    this.prop7 = 7 * basevalue;
    this.prop8 = 8 * basevalue;
    this.prop9 = 9 * basevalue;
    this.prop10 = 10 * basevalue;
    this.prop11 = 11 * basevalue;
    this.prop12 = 12 * basevalue;
    this.prop13 = 13 * basevalue;
    this.prop14 = 14 * basevalue;
    this.prop15 = 15 * basevalue;
    this.prop16 = 16 * basevalue;
    this.prop17 = 17 * basevalue;
    this.prop18 = 18 * basevalue;
  }
  
  public boolean equals(Object other){
    HydraUtil.logFine("Equals called for Region object= "  + prop1);
    if(other instanceof RegionObject){
      RegionObject ro = (RegionObject)other;
      return ro.prop1 == prop1;
    }else return false;
  }
  
  public int hashCode(){
    return new Integer(prop1).hashCode();
  }

  public int getProp1() {
    return prop1;
  }

  public void setProp1(int prop1) {
    this.prop1 = prop1;
  }

  public int getProp2() {
    return prop2;
  }

  public void setProp2(int prop2) {
    this.prop2 = prop2;
  }

  public int getProp3() {
    return prop3;
  }

  public void setProp3(int prop3) {
    this.prop3 = prop3;
  }

  public int getProp4() {
    return prop4;
  }

  public void setProp4(int prop4) {
    this.prop4 = prop4;
  }

  public int getProp5() {
    return prop5;
  }

  public void setProp5(int prop5) {
    this.prop5 = prop5;
  }

  public int getProp6() {
    return prop6;
  }

  public void setProp6(int prop6) {
    this.prop6 = prop6;
  }

  public int getProp7() {
    return prop7;
  }

  public void setProp7(int prop7) {
    this.prop7 = prop7;
  }

  public int getProp8() {
    return prop8;
  }

  public void setProp8(int prop8) {
    this.prop8 = prop8;
  }

  public int getProp9() {
    return prop9;
  }

  public void setProp9(int prop9) {
    this.prop9 = prop9;
  }

  public int getProp10() {
    return prop10;
  }

  public void setProp10(int prop10) {
    this.prop10 = prop10;
  }

  public int getProp11() {
    return prop11;
  }

  public void setProp11(int prop11) {
    this.prop11 = prop11;
  }

  public int getProp12() {
    return prop12;
  }

  public void setProp12(int prop12) {
    this.prop12 = prop12;
  }

  public int getProp13() {
    return prop13;
  }

  public void setProp13(int prop13) {
    this.prop13 = prop13;
  }

  public int getProp14() {
    return prop14;
  }

  public void setProp14(int prop14) {
    this.prop14 = prop14;
  }

  public int getProp15() {
    return prop15;
  }

  public void setProp15(int prop15) {
    this.prop15 = prop15;
  }

  public int getProp16() {
    return prop16;
  }

  public void setProp16(int prop16) {
    this.prop16 = prop16;
  }

  public int getProp17() {
    return prop17;
  }

  public void setProp17(int prop17) {
    this.prop17 = prop17;
  }

  public int getProp18() {
    return prop18;
  }

  public void setProp18(int prop18) {
    this.prop18 = prop18;
  }

}
