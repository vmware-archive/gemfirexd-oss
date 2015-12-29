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
package parReg.execute;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;

public class PartitionObjectHolder implements DataSerializable, RoutingHolder {

 
  private Object name;

  private Object routingObject;

  public PartitionObjectHolder() {
  }

  public PartitionObjectHolder(Object name, Object routingObject) {
    this.name = name;
    this.routingObject = routingObject;
  }

  public Object getName() {
    return name;
  }

  public Object getRoutingHint() {
    return routingObject;
  }

  public String toString() {
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

  /* equals (fix for bug 41303)
   */
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof PartitionObjectHolder) {
      return this.name.equals(((PartitionObjectHolder)obj).name);
    } else {
      return false;
    }
  }

  /* hashCode (fix for bug 41303)
   */
  @Override
  public int hashCode() {
    return this.name.hashCode();
  }

}
