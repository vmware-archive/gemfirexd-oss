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
package parReg.colocation;

import java.io.Serializable;
import java.util.Properties;

import parReg.execute.RoutingHolder;

import com.gemstone.gemfire.cache.EntryOperation;
import com.gemstone.gemfire.cache.PartitionResolver;
import hydra.*;

public class KeyResolver implements PartitionResolver, Serializable,
    RoutingHolder {

  private Object name;

  private Month routingObject;

  public KeyResolver(Object name, Month routingObject) {
    this.name = name;
    this.routingObject = routingObject;
  }

  public String getName() {
    return name.toString();
  }

//  public Properties getProperties() {
//    return null;
//  }

  public Serializable getRoutingObject(EntryOperation opDetails) {
    Log.getLogWriter().info("Invoked keyResolver");
    return routingObject;
  }

  public Object getRoutingHint() {
    return routingObject;
  }

  public void close() {
  }

//  public void init(Properties props) {
//  }

}
