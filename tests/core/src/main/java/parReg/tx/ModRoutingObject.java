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
package parReg.tx;

import java.util.*;
import java.io.Serializable;

import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.cache.*;

import util.*;
import hydra.*;

public class ModRoutingObject implements PartitionResolver, Serializable, Comparable {

  private Object key;
  private long counterValue;
  private int modValue;

  // Takes a String key constructed by NameFactory
  ModRoutingObject(Object key) {
    this.key = key;
    this.counterValue = NameFactory.getCounterForName( key );
    this.modValue = (int)this.counterValue % (TestConfig.getInstance().getTotalVMs()-1);
  }

  public Object getKey() {
    return this.key;
  }

  public long getCounterValue() {
    return this.counterValue;
  }

  public long getModValue() {
    return this.modValue;
  }

  public String toString() {
     return counterValue + "_" + modValue;
  }

  // Override equals
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof ModRoutingObject)) {
      return false;
    }
    ModRoutingObject o = (ModRoutingObject)obj;
    if (!this.key.equals(o.getKey())) {
      return false;
    }
    if (this.counterValue != o.getCounterValue()) {
      return false;
    }
    if (this.modValue !=  o.getModValue()) {
      return false;
    }
    return true;
  }

  public int hashCode() {
     return this.modValue;
  }

  public String getName() {
    return this.getClass().getName();
  }

  public Serializable getRoutingObject(EntryOperation op) {
    return (ModRoutingObject)op.getKey();
  }

  public void close() {
  }

  public int compareTo(Object o) {
    ModRoutingObject mro = (ModRoutingObject)o;
    return (int)(this.counterValue - mro.counterValue);
  }
}

