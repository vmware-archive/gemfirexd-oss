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
package parReg.fixedPartitioning;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Set;

import parReg.colocation.Month;
import parReg.execute.RoutingHolder;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.EntryOperation;
import com.gemstone.gemfire.cache.FixedPartitionResolver;

public class FixedKeyResolver implements FixedPartitionResolver, Serializable,
    RoutingHolder, DataSerializable {

  private Object name;

  private Month routingObject;
  
  public FixedKeyResolver() {
  }

  public FixedKeyResolver(Object name, Month routingObject) {
    this.name = name;
    this.routingObject = routingObject;
  }

  public String getName() {
    return name.toString();
  }

  public Serializable getRoutingObject(EntryOperation opDetails) {
    return routingObject;
  }

  public Object getRoutingHint() {
    return routingObject;
  }

  public void close() {
  }

  public String getPartitionName(EntryOperation opDetails, Set targetPartitions) {
    return routingObject.getQuarter();
  }
  
  public boolean equals(Object obj) {
    if (!((obj.getClass().getName()).equals(this.getClass().getName()))) {
      return false;
    }
    else {
      if (((FixedKeyResolver)obj).name.equals(this.name)
          && ((FixedKeyResolver)obj).routingObject.equals(this.routingObject)) {
        return true;
      }
      else {
        return false;
      }
    }
  }
  
  public int hashCode() {
    return routingObject.hashCode();
  }
  
  public String toString(){
    return name.toString();
  }
  
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.name = DataSerializer.readObject(in);
    this.routingObject = DataSerializer.readObject(in);
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeObject(this.name, out);
    DataSerializer.writeObject(this.routingObject, out);
  }

}
