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
package com.gemstone.gemfire.internal.cache.execute.data;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.cache.execute.PRColocationDUnitTest;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Order implements DataSerializable {
  String orderName;

  public Order() {

  }

  public Order(String orderName) {
    this.orderName = orderName + PRColocationDUnitTest.getDefaultAddOnString();
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.orderName = DataSerializer.readString(in);
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(this.orderName, out);
  }

  public String toString() {
    return this.orderName;
  }

  public boolean equals(Object obj) {
    if(this == obj)
      return true;
    
    if(obj instanceof Order){
      Order other = (Order)obj;
      if(other.orderName != null && other.orderName.equals(this.orderName)){
        return true;
      }
    }
    return false;
  }
}
