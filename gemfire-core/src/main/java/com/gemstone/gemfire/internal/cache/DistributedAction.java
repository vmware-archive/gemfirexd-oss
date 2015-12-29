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

package com.gemstone.gemfire.internal.cache;

import java.util.*;

/**
 *  Enum class that specifies the type of action being requested.
 *  @author Sudhir Menon
 * 
 */
class DistributedAction  {
  private final String name;
  private DistributedAction(String name) {this.name = name;}
  @Override
  public String toString() { return name;}
  public boolean isDistributedAction() {
    return Arrays.asList(new DistributedAction[]
                         {NETSEARCH, NETLOAD, NETWRITE }
                        ).contains(this);
  }
  public static final DistributedAction NETSEARCH = new DistributedAction("NETSEARCH");
  public static final DistributedAction NETLOAD = new DistributedAction("NETLOAD");
  public static final DistributedAction NETWRITE = new DistributedAction("NETWRITE");
}
