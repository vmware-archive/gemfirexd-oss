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

import parReg.execute.PartitionObjectHolder;

import getInitialImage.InitImageBB;

import com.gemstone.gemfire.cache.EntryOperation;
import com.gemstone.gemfire.cache.PartitionResolver;
import com.gemstone.gemfire.internal.cache.xmlcache.Declarable2;

import hydra.*;

public class MonthPartitionResolver implements PartitionResolver, Serializable, Declarable2 {
  private final Properties resolveProps;
  public MonthPartitionResolver() {
    this.resolveProps = new Properties();
    this.resolveProps.setProperty("routingType", "key");
  }

  public String getName() {
    return getClass().getName();
  }

  public Serializable getRoutingObject(EntryOperation opDetails) {

    if (TestConfig.tab().stringAt(parReg.ParRegPrms.partitionResolverData)
        .equalsIgnoreCase("callbackarg")) {
      Month routingObject = (Month)opDetails.getCallbackArgument();
      return routingObject;

    }
    else if (TestConfig.tab().stringAt(parReg.ParRegPrms.partitionResolverData)
        .equalsIgnoreCase("key")) {
      Month routingObject = (Month)((PartitionObjectHolder)opDetails.getKey())
          .getRoutingHint();
      return routingObject;
    }
    else if (TestConfig.tab().stringAt(parReg.ParRegPrms.partitionResolverData)
        .equalsIgnoreCase("BB")) {
      Object key = opDetails.getKey();
      return (Month)InitImageBB.getBB().getSharedMap().get(key);
    }
    else {
      return null;
    }
  }

  public void close() {}

  // @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (! obj.getClass().equals(this.getClass())) {
      return false;
    }
    MonthPartitionResolver other = (MonthPartitionResolver)obj; 
    if (!this.resolveProps.equals(other.getConfig())) {
      return false;
    }

    return true;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.xmlcache.Declarable2#getConfig()
   */
  public Properties getConfig() {
    return this.resolveProps;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.Declarable#init(java.util.Properties)
   */
  public void init(Properties props) {
    this.resolveProps.putAll(props);
  }
}
