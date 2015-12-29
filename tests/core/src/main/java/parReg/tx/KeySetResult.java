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

import util.*;
import hydra.*;

public class KeySetResult implements Serializable {

  private SerializableDistributedMember dm;
  private ArrayList keySet = new ArrayList();

  KeySetResult(DistributedMember dm, Set keys) {
    this.dm = new SerializableDistributedMember(dm);
    for (Iterator it = keys.iterator(); it.hasNext(); ) {
       String key = (String)it.next();
       keySet.add(key);
    }
  }

  // getters
  public DistributedMember getDistributedMember() {
    // locally, we have a SerializableDistributedMember
    return this.dm.getDistributedMember();
  }

  public List getKeySet() {
    return this.keySet;
  }

  public String toString() {
     return this.dm.toString() + this.keySet;
  }
}

