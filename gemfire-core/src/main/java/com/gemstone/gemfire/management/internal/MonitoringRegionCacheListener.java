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

import java.util.HashMap;
import java.util.Map;

import javax.management.ObjectName;

import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.ClassLoadUtil;

/**
 * Cache listener on local monitoring region to get Manager's local MBean
 * updates. This acts as an aggregator interface for Manager. Its used only on
 * Manager's side.
 * 
 * @author rishim
 * 
 */
public class MonitoringRegionCacheListener extends
    CacheListenerAdapter<String, Object> {

  private LogWriterI18n logger;

  private SystemManagementService service;

  private Map<String, Class> classRef = new HashMap<String, Class>();

  private static final String THIS_COMPONENT = MonitoringRegionCacheListener.class
      .getName();

  public MonitoringRegionCacheListener(SystemManagementService service) {
    this.service = service;
    this.logger = InternalDistributedSystem.getLoggerI18n();
  }
  
  @Override
  public void afterCreate(EntryEvent<String, Object> event) {
    ObjectName objectName = null;
    try {

      if (!service.isStartedAndOpen() || !service.isManager()) {
        // NO OP return; No work for Non Manager Nodes
        return;
      }
      objectName = ObjectName.getInstance(event.getKey());

      FederationComponent newObject = (FederationComponent) event.getNewValue();
      String className = newObject.getMBeanInterfaceClass();
      Class interfaceClass;
      if (classRef.get(className) != null) {
        interfaceClass = classRef.get(className);
      } else {
        interfaceClass = ClassLoadUtil.classFromName(className);
        classRef.put(className, interfaceClass);
      }

     service.afterPseudoCreateProxy(objectName, interfaceClass, null, newObject);

    } catch (Exception e) {
      if (logger.fineEnabled()) {
        logger.fine(THIS_COMPONENT + ": Aggregation Failed failed for "
            + objectName + "With Exception " + e);
      }
    }
  }

  @Override
  public void afterUpdate(EntryEvent<String, Object> event) {
    ObjectName objectName = null;
    try {

      if (!service.isStartedAndOpen() || !service.isManager()) {
        // NO OP return; No work for Non Manager Nodes
        return;
      }
      objectName = ObjectName.getInstance(event.getKey());

      FederationComponent oldObject = (FederationComponent) event.getOldValue();
      FederationComponent newObject = (FederationComponent) event.getNewValue();
      String className = newObject.getMBeanInterfaceClass();
      Class interfaceClass;
      if (classRef.get(className) != null) {
        interfaceClass = classRef.get(className);
      } else {
        interfaceClass = ClassLoadUtil.classFromName(className);
        classRef.put(className, interfaceClass);
      }

      service.afterUpdateProxy(objectName, interfaceClass, null, newObject, oldObject);

    } catch (Exception e) {
      if (logger.fineEnabled()) {
        logger.fine(THIS_COMPONENT + ": Aggregation Failed failed for "
            + objectName + "With Exception " + e);
      }
    }

  }

}
