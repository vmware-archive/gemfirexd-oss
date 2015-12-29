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

import java.sql.CallableStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.management.Notification;
import javax.management.ObjectName;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.management.DiskBackupStatus;
import com.gemstone.gemfire.management.internal.FederationComponent;
import com.gemstone.gemfire.management.internal.MBeanJMXAdapter;
import com.gemstone.gemfire.management.internal.ManagementConstants;
import com.gemstone.gemfire.management.internal.SystemManagementService;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.management.AggregateMemberMXBean;
import com.pivotal.gemfirexd.internal.engine.management.AggregateTableMXBean;
import com.pivotal.gemfirexd.internal.engine.management.NetworkServerConnectionStats;
import com.pivotal.gemfirexd.internal.engine.management.NetworkServerNestedConnectionStats;
import com.pivotal.gemfirexd.internal.engine.management.GfxdMemberMXBean;
import com.pivotal.gemfirexd.internal.engine.management.TableMXBean;
import com.pivotal.gemfirexd.internal.engine.management.impl.InternalManagementService.ConnectionWrapperHolder;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnection;

/**
 *
 * @author Ajay Pande
 * @since gfxd 1.0
 */

public class GfxdDistributedSystemBridge {

  public SystemManagementService service = null;
  protected LogWriterI18n logger = InternalDistributedSystem.getLoggerI18n();

  private ObjectName thisMemberName;
  
  
  private List<String> tableList = Collections.synchronizedList(new ArrayList<String>());
  
  private GfxdMemberClusterStatsMonitor gfxdMemberClusterStatsMonitor;

  private final Map<ObjectName, AggregateTableMBeanBridge> aggregateTableMBeanBridgeMap;

  private ConnectionWrapperHolder connectionWrapperHolder = null;

  private Map<ObjectName, Long > mbeanCounterMap = null;
  
  private Map<ObjectName, String> members = new ConcurrentHashMap<ObjectName, String>();
  
  private InternalDistributedMember thisMember;
  
  Lock lock = new ReentrantLock();

  public GfxdDistributedSystemBridge(SystemManagementService systemManagementService,ConnectionWrapperHolder connectionWrapHolder, InternalDistributedMember thisMember) {

    this.connectionWrapperHolder = connectionWrapHolder;
    this.service = systemManagementService;
    this.thisMember = thisMember;
    String memberNameOrId = MBeanJMXAdapter.getMemberNameOrId(Misc.getGemFireCache().getDistributedSystem().getDistributedMember());

    this.thisMemberName = ManagementUtils.getMemberMBeanName(memberNameOrId,ManagementUtils.DEFAULT_SERVER_GROUP);
    this.gfxdMemberClusterStatsMonitor = new GfxdMemberClusterStatsMonitor();
    this.aggregateTableMBeanBridgeMap = new HashMap<ObjectName, AggregateTableMBeanBridge>();
    this.mbeanCounterMap = Collections.synchronizedMap(new HashMap<ObjectName, Long >());
  }

  public static void addRegion(ObjectName objectName, Object proxyObject,
      FederationComponent newVal) {

  }

  // handle member addition
  public void addMemberToSystem(ObjectName objectName, Object proxyObject,
      FederationComponent newVal) {
    if (objectName.equals(thisMemberName)) {
      ObjectName distrObjectName = ManagementUtils.getClusterMBeanName();
      
      AggregateMemberMXBean cluserMXBean = new AggregateMemberMBean(this);
      service.registerInternalMBean(cluserMXBean, distrObjectName);
      members.put(objectName, thisMember.getId());
    }else{
      updateMember(objectName, newVal, null);
    }
   
  }

  // handle member removal
  public boolean removeMemberFromSystem(ObjectName objectName,
      GfxdMemberMXBean memberProxy, FederationComponent oldState) {
    if (thisMemberName.equals(objectName) ) {
      ObjectName distrObjectName = ManagementUtils.getClusterMBeanName();
      service.unregisterMBean(distrObjectName);
      members.remove(objectName);
    }else{
      updateMember(objectName, null, oldState);
    }

    return false;
  }

  public void updateMember(ObjectName objectName, FederationComponent newVal, FederationComponent oldVal) {    
    // Removal
    if (oldVal != null && newVal == null) {         
          members.remove(objectName);
    }
    // Addition
    if (oldVal == null && newVal != null) {
      if (newVal.getValue("Id") != null) {
        String name = (String) newVal.getValue("Id");
        if (name != null) {
          members.put(objectName, name);
        }
      }
    }
  }
  
  // get all member ids in cluster
  public String[] getMembers() {
    Iterator<String> memberIterator = members.values().iterator();
    Set<String> membersList = new HashSet<String>();
    while (memberIterator.hasNext()) {
      membersList.add(memberIterator.next());
    }
    String[] members = new String[membersList.size()];

    if (membersList.size() > 0) {
      return membersList.toArray(members);
    }

    return ManagementConstants.NO_DATA_STRING;
  }
 
  public void updateMemberDetails(FederationComponent newState,
      FederationComponent oldState) {
   
    

  } 

  public void memberDeparted(InternalDistributedMember id, boolean crashed) {
    // TODO Auto-generated method stub

  }

  public void memberJoined(InternalDistributedMember id) {
    // TODO Auto-generated method stub

  }

  public void memberSuspect(InternalDistributedMember id,
      InternalDistributedMember whoSuspected) {
    // TODO Auto-generated method stub

  }

  public void sendSystemLevelNotification(Notification notification) {
    // TODO Auto-generated method stub

  }

  public DiskBackupStatus backupAllMembers(String targetDirPath) {
    // TODO Auto-generated method stub
    return null;
  }


  public void enableStatementStats(boolean enableOrDisableStats,
      boolean enableOrDisableTimeStats) {
    CallableStatement globalStatsEnableStatement;
    try {
      if (connectionWrapperHolder.hasConnection()) {
        EmbedConnection connection = connectionWrapperHolder.getConnection();
        globalStatsEnableStatement = connection
            .prepareCall("CALL SYS.SET_GLOBAL_STATEMENT_STATISTICS(?, ?)");
        globalStatsEnableStatement.setBoolean(1, enableOrDisableStats);
        globalStatsEnableStatement.setBoolean(2, enableOrDisableTimeStats);
        globalStatsEnableStatement.execute();
      } else {
        this.logger.info(LocalizedStrings.DEBUG,
            "For cluster does not have connection: ");
      }
    } catch (SQLException e) {
      this.logger.info(LocalizedStrings.DEBUG,
          "Error occurred while enabling statement stats for cluster. Reason: "
              + e.getMessage());
    }
  }

  public void addTableToSystem(ObjectName objectName, TableMXBean memberProxy,
      FederationComponent newVal) {
    String fullPath = objectName.getKeyProperty("table");
    ObjectName aggregateTableObjectName = ManagementUtils.getAggrgateTableMBeanName(fullPath);

    try {
      
      lock.lock();
      
      if (!this.tableList.contains(fullPath)) {
        AggregateTableMBeanBridge bridge = new AggregateTableMBeanBridge(
            memberProxy.getName(), memberProxy.getParentSchema(),
            memberProxy.getDefinition());
        
          AggregateTableMXBean mbean = new AggregateTableMBean(bridge);
          aggregateTableMBeanBridgeMap.put(aggregateTableObjectName, bridge);
          service.registerInternalMBean(mbean, aggregateTableObjectName);
          tableList.add(fullPath);
          bridge.update(newVal, null, objectName);
      }

      if (mbeanCounterMap.containsKey(aggregateTableObjectName)) {
        Long count = mbeanCounterMap.get(aggregateTableObjectName);
        count++;
        mbeanCounterMap.put(aggregateTableObjectName, count);
      } else {
        mbeanCounterMap.put(aggregateTableObjectName, (long) 1);
      }
      
    } catch(Exception e){
      logger.warning(LocalizedStrings.DEBUG, "Exception in aggregate table mbean. Exception " + e);
    }finally {
      lock.unlock();
    }
  }

  public void removeTableFromSystem(ObjectName objectName,
      TableMXBean memberProxy, FederationComponent oldVal) {
    String fullPath = objectName.getKeyProperty("table");
    ObjectName aggregateTableObjectName = ManagementUtils.getAggrgateTableMBeanName(fullPath);
    
    if(mbeanCounterMap.containsKey(aggregateTableObjectName) ){
      try {        
        lock.lock();
        Long count = mbeanCounterMap.get(aggregateTableObjectName);
        count--;
        mbeanCounterMap.put(aggregateTableObjectName, count);
        if(count == 0 ){
          service.unregisterMBean(aggregateTableObjectName);
          this.aggregateTableMBeanBridgeMap.remove(aggregateTableObjectName);
          mbeanCounterMap.remove(aggregateTableObjectName);
          this.tableList.remove(fullPath);
          logger.info(LocalizedStrings.DEBUG, "Removed  aggregateTableMbean "
              + aggregateTableObjectName);
        }        
      } catch (Exception e) {
    	  logger.warning(LocalizedStrings.DEBUG, "Exception in removing aggregate table mbean. Excpetion " + e);

      } finally {         
          lock.unlock();
      }
    }
  }

  public void updateTableFromSystem(ObjectName objectName,
      FederationComponent newValue, FederationComponent oldValue) {
    String fullPath = objectName.getKeyProperty("table");
    ObjectName aggregateTableObjectName = ManagementUtils
        .getAggrgateTableMBeanName(fullPath);

    AggregateTableMBeanBridge bridge = this.aggregateTableMBeanBridgeMap
        .get(aggregateTableObjectName);
    if (bridge != null) {
      FederationComponent newProxy = (newValue);
      FederationComponent oldProxy = null;
      if (oldValue != null) {
        oldProxy = oldValue;
      }

      bridge.update(newProxy, oldProxy, objectName);
    }

  }

  public int getProcedureCallsCompleted(){
    return gfxdMemberClusterStatsMonitor.getProcedureCallsCompleted();
  }

  public int getProcedureCallsInProgress() {
    return gfxdMemberClusterStatsMonitor.getProcedureCallsInProgress();
  }

  public NetworkServerConnectionStats getNetworkServerClientConnectionStats(){
    return gfxdMemberClusterStatsMonitor.getNetworkServerClientConnectionStats();
  }

  public NetworkServerConnectionStats getNetworkServerPeerConnectionStats() {
    return gfxdMemberClusterStatsMonitor.getNetworkServerPeerConnectionStats();
  }


  public NetworkServerNestedConnectionStats getNetworkServerNestedConnectionStats() {
    return gfxdMemberClusterStatsMonitor.getNetworkServerNestedConnectionStats();
  }
  public NetworkServerNestedConnectionStats getNetworkServerInternalConnectionStats() {
    return gfxdMemberClusterStatsMonitor.getNetworkServerInternalConnectionStats();
  }


}
