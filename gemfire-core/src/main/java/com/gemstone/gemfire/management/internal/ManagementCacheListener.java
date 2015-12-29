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
package com.gemstone.gemfire.management.internal;

import javax.management.ObjectName;

import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.i18n.LogWriterI18n;

/**
 * This listener is attached to the Monitoring Region to receive any addition or
 * deletion of MBEans
 * 
 * It updates the last refreshed time of proxy once it gets the update request
 * from the Managed Node
 * 
 * @author rishim
 * 
 */
public class ManagementCacheListener extends
    CacheListenerAdapter<String, Object> {

  private MBeanProxyFactory proxyHelper;
  private LogWriterI18n logger;
  
  private volatile boolean  readyForEvents;
  
  private static final String THIS_COMPONENT = ManagementCacheListener.class.getName();

  public ManagementCacheListener(MBeanProxyFactory proxyHelper) {
    this.proxyHelper = proxyHelper;
    logger = InternalDistributedSystem.getLoggerI18n();
    this.readyForEvents = false;
  }

  @Override
  public void afterCreate(EntryEvent<String, Object> event) {
    if (!readyForEvents) {
      return;
    }
    ObjectName objectName = null;

    try {
      objectName = ObjectName.getInstance(event.getKey());
      Object newObject = event.getNewValue();
      proxyHelper.createProxy(event.getDistributedMember(), objectName, event
          .getRegion(), newObject);
    } catch (Exception e) {
      if (logger.fineEnabled()) {
        logger.fine(THIS_COMPONENT + ": Proxy Create failed for " + objectName
            + "With Exception " + e);
      }
    }

  }

  @Override
  public void afterDestroy(EntryEvent<String, Object> event) {
    ObjectName objectName = null;

    try {
      objectName = ObjectName.getInstance(event.getKey());
      Object oldObject = event.getOldValue();
      proxyHelper.removeProxy(event.getDistributedMember(), objectName,
          oldObject);
    } catch (Exception e) {
      if (logger.fineEnabled()) {
        logger.fine(THIS_COMPONENT + ": Proxy Destroy failed for " + objectName
            + "With Exception " + e);
      }
    }

  }

  @Override
  public void afterUpdate(EntryEvent<String, Object> event) {
    ObjectName objectName = null;
    try {
      if (!readyForEvents) {
        return;
      }
      objectName = ObjectName.getInstance(event.getKey());

      ProxyInfo proxyInfo = proxyHelper.findProxyInfo(objectName);
      if (proxyInfo != null) {
        ProxyInterface proxyObj = (ProxyInterface) proxyInfo.getProxyInstance();
        // Will return null if proxy is filtered out
        if (proxyObj != null) {
          proxyObj.setLastRefreshedTime(System.currentTimeMillis());
        }
        Object oldObject = event.getOldValue();
        Object newObject = event.getNewValue();
        proxyHelper.updateProxy(objectName, proxyInfo, newObject, oldObject);
      }

    } catch (Exception e) {
      if (logger.fineEnabled()) {
        logger.fine(THIS_COMPONENT + ": Proxy Update failed for " + objectName
            + "With Exception " + e);
      }

    }

  }
  
  void markReady(){
    readyForEvents = true;
  }

}