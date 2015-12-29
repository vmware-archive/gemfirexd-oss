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
/**
 * 
 */
package transaction;

import java.io.Serializable;

/**
 * Key for the Order region
 */
public class OrderId implements Serializable {
  private final int orderId;
  private final CustomerId custId;
  public OrderId(int orderId, CustomerId custId) {
    this.orderId = orderId;
    this.custId = custId;
  }
  public int getOrderId() {
    return orderId;
  }
  public CustomerId getCustId() {
    return custId;
  }
  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof OrderId)) {
      return false;
    }
    OrderId other = (OrderId)obj;
    return this.orderId == other.orderId && this.custId.equals(other.custId);
  }
  @Override
  public int hashCode() {
    return orderId * 17 + custId.hashCode();
  }
  @Override
  public String toString() {
    return "OrderId: "+orderId;
  }
}
