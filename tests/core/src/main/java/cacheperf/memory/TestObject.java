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
package cacheperf.memory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.Instantiator;


class TestObject implements DataSerializable {
  private static final long serialVersionUID = 6232127232791545838L;
  static int averageCollectionSize = 5;
  static int uniqueValues = 5;
  
  private int id;

  static {
    Instantiator.register(new Instantiator(TestObject.class, (byte) 22) {

      public DataSerializable newInstance() {
        return new TestObject();
      }
    });
  }
  
  
  /** For deserialization only */
  private TestObject() {
    
  }
  
  public TestObject(int id) {
    this.id = id;
  }
  
  public int indexValue() {
    return id %uniqueValues;
  }
  
  public Collection indexCollection() {
    ArrayList result = new ArrayList();
    for(int i = 0; i < averageCollectionSize; i++) {
      result.add(new Float(indexValue() + 1.0/(i+1)));
    }
    
    return result;
  }
  
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    id = in.readInt();
  }

  public void toData(DataOutput out) throws IOException {
    out.writeInt(id);
  }

  public int hashCode() {
    return id;
  }

  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    final TestObject other = (TestObject) obj;
    if (id != other.id)
      return false;
    return true;
  }

  public String toString() {
    return "TestObject[" + id + "]";
  }
  
  
  
}