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
package com.pivotal.gemfirexd.internal.engine.management.impl;

import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import javax.management.Notification;
import javax.management.ObjectName;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.management.internal.FederationComponent;
import com.gemstone.gemfire.management.internal.ProxyListener;
import com.gemstone.gemfire.management.internal.beans.AggregateHandler;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.management.AggregateStatementMXBean;
import com.pivotal.gemfirexd.internal.engine.management.GfxdMemberMXBean;
import com.pivotal.gemfirexd.internal.engine.management.StatementMXBean;
import com.pivotal.gemfirexd.internal.engine.management.TableMXBean;



/**
*
* @author Ajay Pande
* @since gfxd 1.0
*/

public class GfxdMBeanAggregator implements ProxyListener {

  private static final List<Class> aggregateMBeanList = new ArrayList<Class>();


  /**
   * Log writer
   */

  protected LogWriterI18n logger;


  /**
   * Static list of MBeans which are currently supported for aggregation
   */
  static {
    aggregateMBeanList.add(GfxdMemberMXBean.class);
    aggregateMBeanList.add(StatementMXBean.class);
    aggregateMBeanList.add(TableMXBean.class);

  }


  private GfxdDistributedSystemBridge gfxdDistributedSystemBridge;
  private GfxdMemberHandler gfxdMemberHandler;
  private StatementAggregateHanlder statementHandler;
  private GfxdTableMBeanHandler gfxdTableMBeanHandler;

  private InternalManagementService service;



  /**
   * Public constructor.
   * @param gfxdDsBridge
   */
  public GfxdMBeanAggregator(GfxdDistributedSystemBridge dsBridge) {
    this.gfxdMemberHandler = new GfxdMemberHandler();
    this.statementHandler = new StatementAggregateHanlder();
    this.logger = Misc.getI18NLogWriter();
    this.gfxdDistributedSystemBridge = dsBridge;
    this.service = InternalManagementService.getInstance(Misc.getMemStore());
    this.gfxdTableMBeanHandler = new GfxdTableMBeanHandler();

  }

  /**
   *
   * @param interfaceClass
   *          class of the proxy interface
   * @return appropriate handler instance to handle the proxy addition or
   *         removal
   */
  private AggregateHandler getHandler(Class interfaceClass) {
    if (interfaceClass.equals(GfxdMemberMXBean.class)) {
      return this.gfxdMemberHandler;
    }
    if (interfaceClass.equals(StatementMXBean.class)) {
      return statementHandler;
    }
    if(interfaceClass.equals(TableMXBean.class)){
        return this.gfxdTableMBeanHandler;
    }
    return null;
  }

  @Override
  public void afterCreateProxy(ObjectName objectName, Class interfaceClass, Object proxyObject,
      FederationComponent newVal) {
    if (!aggregateMBeanList.contains(interfaceClass)) {
      return;
    }
    AggregateHandler handler = getHandler(interfaceClass);
    handler.handleProxyAddition(objectName, interfaceClass, proxyObject, newVal);
  }

  @Override
  public void afterRemoveProxy(ObjectName objectName, Class interfaceClass, Object proxyObject,
      FederationComponent oldVal) {
    /*logger.info(LocalizedStrings.DEBUG,
        "GFXDMbeanAggregator     afterRemoveProxy 1" );*/
    if (!aggregateMBeanList.contains(interfaceClass)) {
      return;
    }

   /* logger.info(LocalizedStrings.DEBUG,
        "GFXDMbeanAggregator     afterRemoveProxy 2" );*/
    AggregateHandler handler = getHandler(interfaceClass);
    handler.handleProxyRemoval(objectName, interfaceClass, proxyObject, oldVal);


   /* logger.info(LocalizedStrings.DEBUG,
        "GFXDMbeanAggregator     afterRemoveProxy 3" );*/
  }

  @Override
  public void afterUpdateProxy(ObjectName objectName, Class interfaceClass, Object proxyObject,
      FederationComponent newVal, FederationComponent oldVal) {

    /*logger.info(LocalizedStrings.DEBUG,
        "GFXDMbeanAggregator     afterUpdateProxy 1" );*/
    if (!aggregateMBeanList.contains(interfaceClass)) {
      return;
    }

    /*logger.info(LocalizedStrings.DEBUG,
        "GFXDMbeanAggregator     afterUpdateProxy 2" );*/
    AggregateHandler handler = getHandler(interfaceClass);
    handler.handleProxyUpdate(objectName, interfaceClass, proxyObject, newVal, oldVal);

    /*logger.info(LocalizedStrings.DEBUG,
        "GFXDMbeanAggregator     afterUpdateProxy 3" );*/
  }

  @Override
  public void afterPseudoCreateProxy(ObjectName objectName, Class interfaceClass, Object proxyObject, FederationComponent newVal){
    
  }

  /**
   * Handler class for Distributed System
   *
   * @author Ajayp
   *
   */
  private class GfxdMemberHandler implements AggregateHandler {

    public void handleProxyAddition(ObjectName objectName, Class interfaceClass, Object proxyObject,
        FederationComponent newVal) {
      /*logger.info(LocalizedStrings.DEBUG,
          "GFXDMbeanAggregator     GfxdMemberHandler  handleProxyAddition 1" + interfaceClass.getName());*/

      GfxdMemberMXBean memberProxy = (GfxdMemberMXBean) interfaceClass.cast(proxyObject);
     /* logger.info(LocalizedStrings.DEBUG,
          "GFXDMbeanAggregator     GfxdMemberHandler  handleProxyAddition 2" +memberProxy.getId());*/

      gfxdDistributedSystemBridge.addMemberToSystem(objectName, memberProxy, newVal);
     /* logger.info(LocalizedStrings.DEBUG,
          "GFXDMbeanAggregator     GfxdMemberHandler  handleProxyAddition 3" );*/

    }

    public void handleProxyRemoval(ObjectName objectName, Class interfaceClass, Object proxyObject,
        FederationComponent oldVal) {
      GfxdMemberMXBean memberProxy = (GfxdMemberMXBean) interfaceClass.cast(proxyObject);
      gfxdDistributedSystemBridge.removeMemberFromSystem(objectName, memberProxy, oldVal);
    }

    @Override
    public void handleProxyUpdate(ObjectName objectName, Class interfaceClass, Object proxyObject,
        FederationComponent newVal, FederationComponent oldVal) {
      
      // RightNow nothing is getting updated in AggregateMemberMBean. No need to call this method.
    }

    @Override
    public void handlePseudoCreateProxy(ObjectName objectName, Class interfaceClass, Object proxyObject,
        FederationComponent newVal) {
      
    }
  }

  /**
   * Handler class for Distributed Lock Service
   *
   * @author rishim
   *
   */
  private class StatementAggregateHanlder implements AggregateHandler {

    public void handleProxyAddition(ObjectName objectName, Class interfaceClass, Object proxyObject,
        FederationComponent newVal) {
      AggregateStatementMBean aggregateStmtBean = getAggregateStatementMBean(objectName,true);

      String memberName = objectName.getKeyProperty("member");
      aggregateStmtBean.getBridge().aggregate(memberName, newVal, null);

    }

    public void handleProxyRemoval(ObjectName objectName, Class interfaceClass, Object proxyObject,
        FederationComponent oldVal) {

      AggregateStatementMBean aggregateStmtBean = getAggregateStatementMBean(objectName,false);
      if(aggregateStmtBean == null){
        return;
      }

      String memberName = objectName.getKeyProperty("member");
      aggregateStmtBean.getBridge().aggregate(memberName, null, oldVal);
      if (aggregateStmtBean.getBridge().getMemberCount() == 0) {
        String name = objectName.getKeyProperty("name");
        ObjectName aggregateStatementName = ManagementUtils.getAggregateStatementMBeanName(name);
        service.unregisterMBean(aggregateStatementName);
      }
    }

    @Override
    public void handleProxyUpdate(ObjectName objectName, Class interfaceClass, Object proxyObject,
        FederationComponent newVal, FederationComponent oldVal) {
      AggregateStatementMBean aggregateStmtBean = getAggregateStatementMBean(objectName, false);
      if(aggregateStmtBean == null){
        return;
      }

      String memberName = objectName.getKeyProperty("member");
      aggregateStmtBean.getBridge().aggregate(memberName, newVal, oldVal);
    }

    private synchronized AggregateStatementMBean getAggregateStatementMBean(ObjectName statementObjectName, boolean createAggregateMBean) {

      String name = statementObjectName.getKeyProperty("name");

      ObjectName aggregateStatementName = ManagementUtils.getAggregateStatementMBeanName(name);

      Object bean = service.getMBeanInstance(aggregateStatementName, AggregateStatementMXBean.class);

      AggregateStatementMBean statementMBean = null;
      if (bean != null) {
        statementMBean = (AggregateStatementMBean) bean;
      } else if (createAggregateMBean) {
        AggregateStatementMBeanBridge bridge = new AggregateStatementMBeanBridge();
        statementMBean = new AggregateStatementMBean(bridge);
        service.registerMBean(statementMBean, aggregateStatementName);
      }

      return statementMBean;

    }

    @Override
    public void handlePseudoCreateProxy(ObjectName objectName, Class interfaceClass, Object proxyObject,
        FederationComponent newVal) {
      
    }

  }

  /**
   * Handler class for Distributed table
   *
   * @author Ajayp
   *
   */
  private class GfxdTableMBeanHandler implements AggregateHandler {
    public void handleProxyAddition(ObjectName objectName, Class interfaceClass, Object proxyObject,
        FederationComponent newVal) {
      TableMXBean memberProxy = (TableMXBean) interfaceClass.cast(proxyObject);
      gfxdDistributedSystemBridge.addTableToSystem(objectName, memberProxy, newVal);

    }

    public void handleProxyRemoval(ObjectName objectName, Class interfaceClass, Object proxyObject,
        FederationComponent oldVal) {
      TableMXBean memberProxy = (TableMXBean) interfaceClass.cast(proxyObject);
      gfxdDistributedSystemBridge.removeTableFromSystem(objectName, memberProxy, oldVal);
    }

    @Override
    public void handleProxyUpdate(ObjectName objectName, Class interfaceClass, Object proxyObject,
        FederationComponent newVal, FederationComponent oldVal) {
      gfxdDistributedSystemBridge.updateTableFromSystem(objectName, newVal, oldVal);
    }

    @Override
    public void handlePseudoCreateProxy(ObjectName objectName, Class interfaceClass, Object proxyObject,
        FederationComponent newVal) {
      
    }
  }

  @Override
  public void memberDeparted(InternalDistributedMember id, boolean crashed) {
    gfxdDistributedSystemBridge.memberDeparted(id, crashed);
  }

  @Override
  public void memberJoined(InternalDistributedMember id) {
    gfxdDistributedSystemBridge.memberJoined(id);

  }

  public void quorumLost(Set<InternalDistributedMember> failures, List<InternalDistributedMember> remaining) {

  }

  @Override
  public void memberSuspect(InternalDistributedMember id,
      InternalDistributedMember whoSuspected) {
    gfxdDistributedSystemBridge.memberSuspect(id, whoSuspected);
  }

  @Override
  public void handleNotification(Notification notification) {
    gfxdDistributedSystemBridge.sendSystemLevelNotification(notification);
  }

}
