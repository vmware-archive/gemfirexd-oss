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

public class PidRoutingObject implements PartitionResolver, Serializable {

  private String prefix;
  private int pid;

  PidRoutingObject(String prefix, int pid) {
    this.prefix = prefix;
    this.pid = pid;
  }

  public int getPid() {
    return this.pid;
  }

  public String toString() {
     return "PidRoutingObject(" +  prefix + ": " + pid + ")";
  }

  // Override equals
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof PidRoutingObject)) {
      return false;
    }
    PidRoutingObject o = (PidRoutingObject)obj;
    if (this.pid != o.getPid()) {
      return false;
    }
    return true;
  }

  public int hashCode() {
     return pid;
  }

  public String getName() {
    return this.getClass().getName();
  } 
  
  public Serializable getRoutingObject(EntryOperation op) {
    Object callback = op.getCallbackArgument();
    PidRoutingObject ro = (PidRoutingObject)callback;
    Log.getLogWriter().info("getRoutingObject for " + op.getKey() + "returning " + ro.toString());
    return (PidRoutingObject)callback;
  }
  
  public void close() {
  }
}

