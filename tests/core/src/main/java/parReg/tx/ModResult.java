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

public class ModResult implements Serializable {

  private SerializableDistributedMember sdm;
  private int numKeys;
  private List hashList;

  // Takes a String key constructed by NameFactory
  ModResult(DistributedMember dm, int numKeys, List list) {
    this.sdm = new SerializableDistributedMember(dm);
    this.numKeys = numKeys;
    this.hashList = list;
  }

  // getters
  public SerializableDistributedMember getDM() {
    return this.sdm;
  }

  public List getHashList() {
    return this.hashList;
  }

  public int getNumKeys() {
    return this.numKeys;
  }

  public String toString() {
     return this.sdm.toString() + this.hashList;
  }
}

