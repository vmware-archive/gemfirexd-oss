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
package com.gemstone.gemfire.internal.sequencelog;

import java.util.EnumSet;

/**
 * @author dsmith
 *
 */
public enum GraphType {
  REGION,
  KEY,
  MESSAGE,
  MEMBER;
  
  public byte getId() {
    return (byte) this.ordinal();
  }

  public static GraphType getType(byte id) {
    return values()[id];
  }

  public static EnumSet<GraphType> parse(String enabledTypesString) {
    
    EnumSet<GraphType> set = EnumSet.noneOf(GraphType.class);
    if(enabledTypesString.contains("region")) {
      set.add(REGION);
    }
    if(enabledTypesString.contains("key")) {
      set.add(KEY);
    }
    if(enabledTypesString.contains("message")) {
      set.add(MESSAGE);
    }
    if(enabledTypesString.contains("member")) {
      set.add(MEMBER);
    }
    if(enabledTypesString.contains("all")) {
      set = EnumSet.allOf(GraphType.class);
    }
    
    return set;
  }
}
