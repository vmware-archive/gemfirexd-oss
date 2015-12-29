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

import getInitialImage.InitImageBB;
import hydra.Log;
import hydra.TestConfig;

import java.io.Serializable;
import java.util.Properties;
import java.util.Set;

import parReg.colocation.Month;
import parReg.execute.PartitionObjectHolder;
import util.TestException;

import com.gemstone.gemfire.cache.EntryOperation;
import com.gemstone.gemfire.cache.FixedPartitionResolver;
import com.gemstone.gemfire.internal.cache.xmlcache.Declarable2;

public class NodePartitionResolver implements FixedPartitionResolver,
    Serializable, Declarable2 {

  public String getPartitionName(EntryOperation opDetails, Set targetPartitions) {
    Month routingObject = null;

    if (TestConfig.tab().stringAt(parReg.ParRegPrms.partitionResolverData,
        "none").equalsIgnoreCase("callbackarg")) {
      routingObject = (Month)opDetails.getCallbackArgument();
    }
    else if (TestConfig.tab().stringAt(parReg.ParRegPrms.partitionResolverData,
        "none").equalsIgnoreCase("key")) {
      routingObject = (Month)((PartitionObjectHolder)opDetails.getKey())
          .getRoutingHint();
    }
    else if (TestConfig.tab().stringAt(parReg.ParRegPrms.partitionResolverData,
        "none").equalsIgnoreCase("BB")) {
      Object key = opDetails.getKey();
      routingObject = (Month)InitImageBB.getBB().getSharedMap().get(key);
    }
    else if (opDetails.getKey() instanceof Integer) {
      routingObject = Month.months[((int)(((Integer)opDetails.getKey()) % 12))];
    }
    else if (opDetails.getKey() instanceof FixedKeyResolver) {
      Object key = opDetails.getKey();
      routingObject = (Month)((FixedKeyResolver)key).getRoutingHint();
    }
    String quarter = routingObject.getQuarter();

    if (!targetPartitions.contains(quarter)) {
      throw new TestException("For the key " + opDetails.getKey()
          + " the partition name returned " + quarter
          + " is not available in the targetNodes set " + targetPartitions);
    }
    
    return quarter;
  }

  public String getName() {
    return getClass().getName();
  }

  public Serializable getRoutingObject(EntryOperation opDetails) {

    if (TestConfig.tab().stringAt(parReg.ParRegPrms.partitionResolverData,
        "none").equalsIgnoreCase("callbackarg")) {
      Month routingObject = (Month)opDetails.getCallbackArgument();
      return routingObject;

    }
    else if (TestConfig.tab().stringAt(parReg.ParRegPrms.partitionResolverData,
        "none").equalsIgnoreCase("key")) {
      Month routingObject = (Month)((PartitionObjectHolder)opDetails.getKey())
          .getRoutingHint();
      return routingObject;
    }
    else if (TestConfig.tab().stringAt(parReg.ParRegPrms.partitionResolverData,
        "none").equalsIgnoreCase("BB")) {
      Object key = opDetails.getKey();
      return (Month)InitImageBB.getBB().getSharedMap().get(key);
    }
    else if (opDetails.getKey() instanceof Integer) {
      return Month.months[((int)(((Integer)opDetails.getKey()) % 12))];
    }
    else if (opDetails.getKey() instanceof FixedKeyResolver) {
      Object key = opDetails.getKey();
      Month routingObject = (Month)((FixedKeyResolver)key).getRoutingHint();
      return routingObject;
    }
    else {
      return null;
    }
  }

  public void close() {
  }

  // @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!obj.getClass().equals(this.getClass())) {
      return false;
    }
    return true;
  }

  public Properties getConfig() {
    return null;
  }

  public void init(Properties props) {
  }

}
