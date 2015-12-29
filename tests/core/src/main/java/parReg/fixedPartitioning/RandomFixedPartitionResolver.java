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
package parReg.fixedPartitioning;

import hydra.TestConfig;

import java.io.Serializable;
import java.util.Properties;
import java.util.Set;

import parReg.ParRegBB;
import parReg.colocation.Month;

import com.gemstone.gemfire.cache.EntryOperation;
import com.gemstone.gemfire.cache.FixedPartitionResolver;
import com.gemstone.gemfire.internal.cache.xmlcache.Declarable2;

public class RandomFixedPartitionResolver implements FixedPartitionResolver,
    Serializable, Declarable2 {

  public String getPartitionName(EntryOperation opDetails, Set targetPartitions) {
    Object key = opDetails.getKey();
    Month routingObject;
    if (ParRegBB.getBB().getSharedMap().get(key) != null) {
      routingObject = (Month)ParRegBB.getBB().getSharedMap().get(key);
    }
    else {
      routingObject = Month.months[TestConfig.tab().getRandGen().nextInt(11)];
      ParRegBB.getBB().getSharedMap().put(key, routingObject);
    }
    // hydra.Log.getLogWriter().info(
    // "Returning partition name " + routingObject.getQuarter()
    // + " for the routing object " + routingObject + " for key " + key);
    return routingObject.getQuarter();
  }

  public String getName() {
    return this.getClass().getName();
  }

  public Serializable getRoutingObject(EntryOperation opDetails) {
    Month routingObject;
    Object key = opDetails.getKey();
    if (ParRegBB.getBB().getSharedMap().get(key) != null) {
      routingObject = (Month)ParRegBB.getBB().getSharedMap().get(key);
    }
    else {
      routingObject = Month.months[TestConfig.tab().getRandGen().nextInt(11)];
      ParRegBB.getBB().getSharedMap().put(key, routingObject);
    }
    return routingObject;
  }

  public void close() {
  }

  public Properties getConfig() {
    return null;
  }

  public void init(Properties props) {
  }

}
