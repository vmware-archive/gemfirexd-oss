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

/** RoutingObject for the parRegSerialView tests.
 *  Uses the integer portion of the key (Object_xxx) to route the 
 *  object to one of the VMs (totalNumBuckets = numVMs).
 *  Since PartitionedRegion transactions must use co-located entries,
 *  this allows us to select entries that will all be in the same VM.
 */
public class ViewRoutingObject implements Serializable {

  private Object key;
  private long counterValue;
  private int modValue;

  // Takes a String key constructed by NameFactory
  ViewRoutingObject(Object key) {
    this.key = key;
    this.counterValue = NameFactory.getCounterForName( key );
    this.modValue = (int)this.counterValue % (TestConfig.getInstance().getTotalVMs());
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
    if (!(obj instanceof ViewRoutingObject)) {
      return false;
    }
    ViewRoutingObject o = (ViewRoutingObject)obj;
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

}

