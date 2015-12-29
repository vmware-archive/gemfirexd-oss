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
package udtexamples;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Random;

public class UDTPrice implements Serializable, Comparable<UDTPrice> {
  public BigDecimal highPrice;
  public BigDecimal lowPrice;
  
  public static UDTPrice setPrice(BigDecimal low, BigDecimal high) {
    return new UDTPrice(low, high);
  }
  public static UDTPrice setPrice(UDTPrice price, BigDecimal currAmount) {
    if (currAmount.compareTo(price.highPrice) >0 ) 
      return new UDTPrice(UDTPrice.getLowPrice(price), currAmount);
    else if (currAmount.compareTo(price.lowPrice) <0 ) 
      return new UDTPrice(currAmount, UDTPrice.getHighPrice(price));
    else return price;
  }
  public static BigDecimal getLowPrice(UDTPrice price) {
    return price.lowPrice;
  }
  public static BigDecimal getHighPrice(UDTPrice price) {
    return price.highPrice;
  }
  
  
  public UDTPrice() {};
  
  public UDTPrice(BigDecimal low, BigDecimal high) {
    this.highPrice = high;
    this.lowPrice = low;
  }
  
  public int hashCode() {
    int result = 17;
    result = 37 * result + highPrice.hashCode();
    result = 37 * result + lowPrice.hashCode();   
    return result;
  }
  
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    
    if (!obj.getClass().getName().equals(this.getClass().getName())) {
      return false;
    }
    
    return ((UDTPrice)obj).highPrice.compareTo(this.highPrice) == 0 && 
      ((UDTPrice)obj).lowPrice.compareTo(this.lowPrice) == 0 ;

  }

  public int compareTo(UDTPrice o) {
    if (o != null) {
      int cmp = this.highPrice.compareTo(o.highPrice);
      if (cmp != 0) {
        return cmp;
      }
      return this.lowPrice.compareTo(o.lowPrice);
    }
    else {
      return 1;
    }
  }

  public String toString() {
    return "highPrice is " + highPrice + " low price is " + lowPrice;
  }
  
  /**
   * This is a reverse of toString method.
   */
  public static UDTPrice toUDTPrice(String s) {
    // "highPrice is " + highPrice + " low price is " + lowPrice;
    String[] ele = s.split(" ");
    
    UDTPrice udt = new UDTPrice(BigDecimal.valueOf(Double.parseDouble(ele[2])),
        BigDecimal.valueOf(Double.parseDouble(ele[6])));    
    return udt;
  }
  
  public static UDTPrice getRandomUDTPrice() {
    BigDecimal low = new BigDecimal (Double.toString(((new Random()).nextInt(3000)+1) * .01)).add(new BigDecimal("20"));
    BigDecimal high = new BigDecimal("10").add(low);
    return new UDTPrice(low, high);
  }  
}
