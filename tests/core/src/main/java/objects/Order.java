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

package objects;

import java.io.Serializable;
import java.util.Date;

import com.gemstone.gemfire.internal.cache.lru.Sizeable;

/**
 *  An order, which implements Sizeable for better use with an memory evictor
 */
public class Order implements ConfigurableObject, Serializable, Sizeable {

  private int orderID;
  private String broker;
  private int quantity;
  private Date expiration;
  private double price;

  public int getSizeInBytes() {
    return (this.broker.length() * 2) /* broker String data */
      + 4 /* broker ref */
      + 4 /* orderId */ 
      + 4 /* quantity */
      + 8 /* price */
      + 4 /* expiration ref */
      + 48 + 8 + 8 /* expiration object and data */;
  }
  public Order() {
  }
  public void init( int index ) {
    this.orderID = index;
    this.broker = "John Doe";
    this.quantity = 1;
    this.expiration = new Date();
    this.price = 27.32d;
  }
  public int getIndex() {
    return this.orderID;
  }
  public void validate( int index ) {
    int encodedIndex = this.getIndex();
    if ( encodedIndex != index ) {
      throw new ObjectValidationException( "Expected index " + index + ", got " + encodedIndex );
    }
  }

  //----------------------------------------------------------------------------
  // Accessors
  //----------------------------------------------------------------------------

  public String getBroker() {
    return this.broker;
  }
  public void setBroker( String broker ) {
    this.broker = broker;
  }
  public int getQuantity() {
    return this.quantity;
  }
  public void setQuantity( int quantity ) {
    this.quantity = quantity;
  }
  public Date getExpiration() {
    return this.expiration;
  }
  public void setExpiration( Date expiration ) {
    this.expiration = expiration;
  }
  public double getPrice() {
    return this.price;
  }
  public void setPrice( double price ) {
    this.price = price;
  }
  public String toString() {
    return "Order(" + this.orderID + ")=" + this.expiration;
  }
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((broker == null) ? 0 : broker.hashCode());
    result = prime * result + ((expiration == null) ? 0 : expiration.hashCode());
    result = prime * result + orderID;
    long temp;
    temp = Double.doubleToLongBits(price);
    result = prime * result + (int) (temp ^ (temp >>> 32));
    result = prime * result + quantity;
    return result;
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Order other = (Order) obj;
    if (broker == null) {
      if (other.broker != null)
        return false;
    } else if (!broker.equals(other.broker))
      return false;
    if (expiration == null) {
      if (other.expiration != null)
        return false;
    } else if (!expiration.equals(other.expiration))
      return false;
    if (orderID != other.orderID)
      return false;
    if (Double.doubleToLongBits(price) != Double.doubleToLongBits(other.price))
      return false;
    if (quantity != other.quantity)
      return false;
    return true;
  }
}
