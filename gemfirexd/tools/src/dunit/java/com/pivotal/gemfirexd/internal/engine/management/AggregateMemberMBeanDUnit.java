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

package com.pivotal.gemfirexd.internal.engine.management;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.management.internal.MBeanJMXAdapter;
import com.gemstone.gemfire.management.internal.ManagementConstants;
import com.gemstone.gemfire.management.internal.NotificationHub.NotificationHubListener;
import com.gemstone.gemfire.management.internal.SystemManagementService;
import com.gemstone.gemfire.management.internal.beans.MemberMBean;
import com.gemstone.gemfire.management.internal.beans.SequenceNumber;
import com.gemstone.gemfire.management.internal.cli.CliUtil;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.management.impl.AggregateMemberMBean;
import com.pivotal.gemfirexd.internal.engine.management.impl.GfxdManagementTestBase;
import com.pivotal.gemfirexd.internal.engine.management.impl.InternalManagementService;
import com.pivotal.gemfirexd.internal.engine.management.impl.ManagementUtils;
import io.snappydata.test.dunit.AvailablePortHelper;
import io.snappydata.test.dunit.SerializableCallable;
import io.snappydata.test.dunit.SerializableRunnable;
import io.snappydata.test.dunit.VM;

/**
 *
 * @author Ajay Pande
 * @since gfxd 1.0
 */

public class AggregateMemberMBeanDUnit extends GfxdManagementTestBase {

  private static final long serialVersionUID = 1L;
  
  static final List<Notification> notifList = new ArrayList<Notification>();

  /** The <code>MBeanServer</code> for this application */
  public static MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
  
  private static final String NOTIF_STR = "NOTIF_STR";

  private static final int MAX_WAIT = 20 * 1000;
  
  public AggregateMemberMBeanDUnit(String name) {
    super(name);
  }

  static Properties getManagerConfig(boolean startManager, boolean startAgent) {
    Properties p = new Properties();

    p.setProperty(DistributionConfig.JMX_MANAGER_NAME, "true");
    p.setProperty(DistributionConfig.JMX_MANAGER_START_NAME, String.valueOf(startManager));
    if(startAgent){
      p.setProperty(DistributionConfig.JMX_MANAGER_PORT_NAME,
          String.valueOf(AvailablePortHelper.getRandomAvailableTCPPort()));
    }else{
      p.setProperty(DistributionConfig.JMX_MANAGER_PORT_NAME, String.valueOf(0));
    }
    
    

    return p;
  }
  

  public void testAggregateMemberstats() throws Exception {
    try {

      Properties serverInfo = new Properties();
      serverInfo.setProperty("gemfire.enable-time-statistics", "true");
      serverInfo.setProperty("statistic-sample-rate", "100");
      serverInfo.setProperty("statistic-sampling-enabled", "true");

      startServerVMs(1, 0, "grp1", serverInfo);

      serverInfo.setProperty("jmx-manager", "true");
      serverInfo.setProperty("jmx-manager-start", "true");
      serverInfo.setProperty("jmx-manager-port", "0");// No need to start an
                                                      // Agent for this test

      startServerVMs(1, 0, "grp2", serverInfo);
      
      Properties info = new Properties();
      info.setProperty("host-data", "false");
      info.setProperty("gemfire.enable-time-statistics", "true");

      // start a client, register the driver.
      startClientVMs(1, 0, "grp1", info);
      
      checkAggregateMemberMBeanStats(1);      
      stopVMNums(1, -1);
    } finally {
      GemFireXDQueryObserverHolder.clearInstance();
      
    }
  }
  
  public void testAggregateMemberstatsManagerFirst() throws Exception {
    try {

      Properties serverInfo = new Properties();
      serverInfo.setProperty("gemfire.enable-time-statistics", "true");
      serverInfo.setProperty("statistic-sample-rate", "100");
      serverInfo.setProperty("statistic-sampling-enabled", "true");
      
      Properties managerInfo = new Properties();
      
      
      managerInfo.putAll(serverInfo);
      
      
      managerInfo.setProperty("jmx-manager", "true");
      managerInfo.setProperty("jmx-manager-start", "true");
      managerInfo.setProperty("jmx-manager-port", "0");// No need to start an
                                                      // Agent for this test


      startServerVMs(1, 0, "CG,Peer", managerInfo);

    

      startServerVMs(1, 0, "CG,Peer", serverInfo);
      
      Properties info = new Properties();
      info.setProperty("host-data", "false");
      info.setProperty("gemfire.enable-time-statistics", "true");

      // start a client, register the driver.
      startClientVMs(1, 0, "grp1", info);
      
      checkAggregateMemberMBeanStats(0);      
      stopVMNums(1, -1);
    } finally {
      GemFireXDQueryObserverHolder.clearInstance();
      
    }
  }

  public void testNotification() throws Exception {

    try {
      startServerVMs(1, 0, ManagementUtils.DEFAULT_SERVER_GROUP, getManagerConfig(false, false));

      startServerVMs(1, 0, ManagementUtils.DEFAULT_SERVER_GROUP, getManagerConfig(true, false));

      VM serverVM = this.serverVMs.get(0);
      VM managerVM = this.serverVMs.get(1);

      final DistributedMember serverMember = getMember(serverVM); 
      attchListenerToDSMBean(managerVM);
      waitForManagerToRegisterListener(serverVM);      
      sendNotifications(serverVM, 15);
      countNotifications(managerVM, ManagementConstants.NOTIF_REGION_MAX_ENTRIES, 15, serverMember);
      
      //Send some more notifications
      sendNotifications(serverVM, 35);
      
      //Count should still be equal to ManagementConstants.NOTIF_REGION_MAX_ENTRIES
      countNotifications(managerVM, ManagementConstants.NOTIF_REGION_MAX_ENTRIES, 50, serverMember);
    } finally {
      GemFireXDQueryObserverHolder.clearInstance();
    }
  }
  
  public DistributedMember getMember(final VM vm) {
    SerializableCallable getMember = new SerializableCallable("Get Member") {
      public Object call() {
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        return cache.getDistributedSystem().getDistributedMember();

      }
    };
    return (DistributedMember) vm.invoke(getMember);
  }
  
  
  protected void attchListenerToDSMBean(final VM vm) {
    SerializableRunnable attchListenerToDSMBean = new SerializableRunnable("Attach Listener to DS MBean") {
      public void run() {
        SystemManagementService service = (SystemManagementService) SystemManagementService.getExistingManagementService(Misc.getMemStore()
            .getGemFireCache());
        assertTrue(service.isManager());
        
        notifList.clear();

        NotificationListener nt = new NotificationListener() {
          @Override
          public void handleNotification(Notification notification, Object handback) {
            if (notification.getMessage().contains(NOTIF_STR)) {
              notifList.add(notification);
            }
          }
        };

        try {
          mbeanServer.addNotificationListener(MBeanJMXAdapter.getDistributedSystemName(), nt, null, null);
        } catch (InstanceNotFoundException e) {
          fail("Failed With Exception " + e);
        }

      }
    };
    vm.invoke(attchListenerToDSMBean);
  }
  
  public void waitForManagerToRegisterListener(final VM vm) {
    SerializableRunnable waitForManagerToRegisterListener = new SerializableRunnable("Wait for Manager to register Listener") {
      private static final long serialVersionUID = 1L;
      public void run() {
        SystemManagementService service = (SystemManagementService) SystemManagementService.getExistingManagementService(Misc.getMemStore()
            .getGemFireCache());
        GemFireCacheImpl cache = Misc.getMemStore().getGemFireCache();
        DistributedMember member = cache.getDistributedSystem().getDistributedMember();
        
        final ObjectName memberMBeanName = service.getMemberMBeanName(member);
        
        final Map<ObjectName, NotificationHubListener> hubMap = service.getNotificationHub().getListenerObjectMap();

        waitForCriterion(new WaitCriterion() {
          public String description() {
            return "Waiting for manager to register the listener";
          }

          public boolean done() {
            boolean done = (hubMap.get(memberMBeanName) != null);
            return done;
          }

        }, MAX_WAIT, 500, true);
      }
    };
    vm.invoke(waitForManagerToRegisterListener);
  }
  
  public void sendNotifications(final VM vm, final int count) {
    SerializableRunnable sendNotifications = new SerializableRunnable("Send Notifications") {
      private static final long serialVersionUID = 1L;

      public void run() {
        
        SystemManagementService service = (SystemManagementService) SystemManagementService.getExistingManagementService(Misc.getMemStore()
            .getGemFireCache());

        MemberMBean memberMBean = (MemberMBean) service.getMemberMXBean();

        for (int i = 1; i <= count; i++) {
          Notification notification = new Notification(NOTIF_STR + i, memberMBean.getName(), SequenceNumber.next(), System
              .currentTimeMillis(), NOTIF_STR + i);
          memberMBean.sendNotification(notification);

        }
      }
    };
    vm.invoke(sendNotifications);
  }
  
  public void countNotifications(final VM vm, final int expectedCount, final int expectedNotifListSize, final DistributedMember notifSendingmember) {
    SerializableRunnable countNotifications = new SerializableRunnable("Count Notifications") {

      private static final long serialVersionUID = 1L;

      public void run() {
        GemFireCacheImpl cache = Misc.getMemStore().getGemFireCache();
        final String appender = MBeanJMXAdapter.getUniqueIDForMember(notifSendingmember);

        waitForCriterion(new WaitCriterion() {
          public String description() {
            return "Waiting for " + expectedNotifListSize + " notifications to reach the manager while size of notifList = " + notifList.size();
          }

          public boolean done() {
            boolean done = (expectedNotifListSize == notifList.size());
            return done;
          }

        }, MAX_WAIT, 500, true);

        assertEquals(expectedNotifListSize, notifList.size());

        Region member1NotifRegion = cache.getRegion(ManagementConstants.NOTIFICATION_REGION + "_" + appender);
        assertEquals(expectedCount, member1NotifRegion.size());
      }
    };
    vm.invoke(countNotifications);
  }
  
  
  private void checkAggregateMemberMBeanStats(int index) throws Exception {
    final VM serverVM = this.serverVMs.get(index); // Server Started as a manager
    serverVM.invoke(this.getClass(), "getAggregateMemberMBean");
  }

  public static void getAggregateMemberMBean() {
    final InternalManagementService service = InternalManagementService.getInstance(Misc.getMemStore());    

    final ObjectName distrObjectName = MBeanJMXAdapter
        .getObjectName(ManagementConstants.OBJECTNAME__AGGREGATEMEMBER_MXBEAN);

    waitForCriterion(new WaitCriterion() {
      public String description() {
        return "Waiting for the testAggregateMemberstats to get reflected at managing node";
      }

      public boolean done() {
        AggregateMemberMXBean bean = service.getMBeanInstance(distrObjectName,
            AggregateMemberMXBean.class);
        
        //check that there are default 4 members
        boolean done = (bean != null && bean.getMembers().length == 4 );
        
        return done;
      }

    }, ManagementConstants.REFRESH_TIME * 4, 500, true);

    AggregateMemberMBean aggregateMemberMBean = (AggregateMemberMBean) service
        .getMBeanInstance(distrObjectName, AggregateMemberMXBean.class);

      assertTrue(aggregateMemberMBean != null);
    

      Set<DistributedMember> set = InternalDistributedSystem.getConnectedInstance().getAllOtherMembers();
      Set<DistributedMember> memberSet = new HashSet<DistributedMember>();
      memberSet.add(InternalDistributedSystem.getConnectedInstance().getDistributedMember());
      memberSet.addAll(set);
      
      String[] members = aggregateMemberMBean.getMembers();
      List<String> memberList = new ArrayList<String>();
      for(String member : members){
        memberList.add(member);
      }
      
      
      logInfo("JMX API members = "+ memberList.toString());
      
      logInfo("Gem API members = "+ memberSet.toString());
      
      assertEquals(members.length , memberSet.size());
      
      assertEquals(true, aggregateMemberMBean.getProcedureCallsInProgress() >= 0 ? true
          : false);
      assertEquals(true, aggregateMemberMBean.getNetworkServerClientConnectionStats() != null ? true
          : false);
      assertEquals(true, aggregateMemberMBean.getNetworkServerInternalConnectionStats() != null ? true
          : false);
      assertEquals(true, aggregateMemberMBean.getNetworkServerNestedConnectionStats() != null ? true
          : false);
      assertEquals(true, aggregateMemberMBean.getNetworkServerPeerConnectionStats() != null ? true
          : false);
      assertEquals(true, aggregateMemberMBean.getProcedureCallsCompleted() >= 0 ? true
          : false);
 
      long NETWORK_SERVER_CLIENT_CONNECTIONS_OPENED = 0;
      long NETWORK_SERVER_CLIENT_CONNECTIONS_CLOSED = 0;
      long NETWORK_SERVER_CLIENT_CONNECTIONS_ATTEMPTED = 0;
      long NETWORK_SERVER_CLIENT_CONNECTIONS_FAILED = 0;
      long NETWORK_SERVER_CLIENT_CONNECTIONS_LIFETIME = 0;
      long NETWORK_SERVER_CLIENT_CONNECTIONS_OPEN = 0 ;
      long NETWORK_SERVER_CLIENT_CONNECTIONS_IDLE = 0;

      long NETWORK_SERVER_PEER_CONNECTIONS_OPENED = 0;
      long NETWORK_SERVER_PEER_CONNECTIONS_CLOSED = 0;
      long NETWORK_SERVER_PEER_CONNECTIONS_ATTEMPTED = 0;
      long NETWORK_SERVER_PEER_CONNECTIONS_FAILED = 0;
      long NETWORK_SERVER_PEER_CONNECTIONS_LIFETIME = 0;
      long NETWORK_SERVER_PEER_CONNECTIONS_OPEN = 0 ;
      long NETWORK_SERVER_PEER_CONNECTIONS_IDLE = 0;
      
      long NETWORK_SERVER_NESTED_CONNECTIONS_OPEN = 0;
      long NETWORK_SERVER_NESTED_CONNECTIONS_CLOSED = 0;
      long NETWORK_SERVER_NESTED_CONNECTIONS_ACTIVE = 0;
      
      long NETWORK_SERVER_INTERNAL_CONNECTIONS_OPEN = 0;
      long NETWORK_SERVER_INTERNAL_CONNECTIONS_CLOSED = 0;
      long NETWORK_SERVER_INTERNAL_CONNECTIONS_ACTIVE= 0;      

      Set<DistributedMember> dsMembers = CliUtil.getAllMembers(Misc.getGemFireCacheNoThrow());      
      Iterator<DistributedMember> it = dsMembers.iterator();     
      
      while (it.hasNext()) {
        DistributedMember dsMember = it.next();        
        ObjectName memberMBeanName = ManagementUtils.getMemberMBeanName( MBeanJMXAdapter.getMemberNameOrId(dsMember), "DEFAULT");        
        GfxdMemberMXBean mbean = (GfxdMemberMXBean) (InternalManagementService.getAnyInstance().getMBeanInstance(memberMBeanName,GfxdMemberMXBean.class));
        if (mbean != null) {        
          NetworkServerConnectionStats clientStats              = mbean.getNetworkServerClientConnectionStats();
          NetworkServerNestedConnectionStats internalStats      = mbean.getNetworkServerInternalConnectionStats();
          NetworkServerNestedConnectionStats nestedStats        = mbean.getNetworkServerNestedConnectionStats();
          NetworkServerConnectionStats peerStats                = mbean.getNetworkServerPeerConnectionStats();

          // aggregate client stats
          if (clientStats != null) {        
            NETWORK_SERVER_CLIENT_CONNECTIONS_OPENED += clientStats.getConnectionsOpened();
            NETWORK_SERVER_CLIENT_CONNECTIONS_CLOSED += clientStats.getConnectionsClosed();
            NETWORK_SERVER_CLIENT_CONNECTIONS_ATTEMPTED += clientStats.getConnectionsAttempted();
            NETWORK_SERVER_CLIENT_CONNECTIONS_FAILED += clientStats.getConnectionsFailed();
            NETWORK_SERVER_CLIENT_CONNECTIONS_LIFETIME += clientStats.getConnectionLifeTime();
            NETWORK_SERVER_CLIENT_CONNECTIONS_OPEN += clientStats.getConnectionsOpen();
            NETWORK_SERVER_CLIENT_CONNECTIONS_IDLE += clientStats.getConnectionsIdle();            
          }

          // aggregate peer stats
          if (peerStats != null) {            
            NETWORK_SERVER_PEER_CONNECTIONS_OPENED += peerStats.getConnectionsOpened();
            NETWORK_SERVER_PEER_CONNECTIONS_CLOSED += peerStats.getConnectionsClosed();
            NETWORK_SERVER_PEER_CONNECTIONS_ATTEMPTED += peerStats.getConnectionsAttempted();
            NETWORK_SERVER_PEER_CONNECTIONS_FAILED += peerStats.getConnectionsFailed();
            NETWORK_SERVER_PEER_CONNECTIONS_LIFETIME += peerStats.getConnectionLifeTime();
            NETWORK_SERVER_PEER_CONNECTIONS_OPEN += peerStats.getConnectionsOpen();
            NETWORK_SERVER_PEER_CONNECTIONS_IDLE += peerStats.getConnectionsIdle();                
            
          }

          // aggregate nested stats
          if (nestedStats != null) {            
            NETWORK_SERVER_NESTED_CONNECTIONS_OPEN += nestedStats.getConnectionsOpened();
            NETWORK_SERVER_NESTED_CONNECTIONS_CLOSED += nestedStats.getConnectionsClosed();
            NETWORK_SERVER_NESTED_CONNECTIONS_ACTIVE += nestedStats.getConnectionsActive();
          }

          // aggregate internal stats
          if (internalStats != null) {
            NETWORK_SERVER_INTERNAL_CONNECTIONS_OPEN += internalStats.getConnectionsOpened();
            NETWORK_SERVER_INTERNAL_CONNECTIONS_CLOSED += internalStats.getConnectionsClosed();
            NETWORK_SERVER_INTERNAL_CONNECTIONS_ACTIVE += internalStats .getConnectionsActive();
          }
        }
      }    
      
      //verify client stats      
      NetworkServerConnectionStats clientStats = aggregateMemberMBean.getNetworkServerClientConnectionStats();      
      logInfo("AggregateMemberMBean NETWORK_SERVER_CLIENT_CONNECTIONS_OPEN=="+NETWORK_SERVER_CLIENT_CONNECTIONS_OPENED);
      logInfo("AggregateMemberMBean NETWORK_SERVER_CLIENT_CONNECTIONS_CLOSED=="+NETWORK_SERVER_CLIENT_CONNECTIONS_CLOSED);      
      logInfo("AggregateMemberMBean NETWORK_SERVER_CLIENT_CONNECTIONS_ATTEMPTED=="+NETWORK_SERVER_CLIENT_CONNECTIONS_ATTEMPTED);
      logInfo("AggregateMemberMBean NETWORK_SERVER_CLIENT_CONNECTIONS_FAILED=="+NETWORK_SERVER_CLIENT_CONNECTIONS_FAILED);
      logInfo("AggregateMemberMBean NETWORK_SERVER_CLIENT_CONNECTIONS_LIFETIME=="+NETWORK_SERVER_CLIENT_CONNECTIONS_LIFETIME);
      logInfo("AggregateMemberMBean NETWORK_SERVER_CLIENT_CONNECTIONS_OPEN=="+NETWORK_SERVER_CLIENT_CONNECTIONS_OPEN);   
      logInfo("AggregateMemberMBean NETWORK_SERVER_CLIENT_CONNECTIONS_ACTIVE=="+NETWORK_SERVER_CLIENT_CONNECTIONS_IDLE);
      
      
      logInfo("AggregateMemberMBean clientStats.getConnectionsOpened()=="+clientStats.getConnectionsOpened());
      logInfo("AggregateMemberMBean clientStats.getConnectionsClosed()=="+clientStats.getConnectionsClosed());
      logInfo("AggregateMemberMBean clientStats.getConnectionsAttempted()=="+clientStats.getConnectionsAttempted());
      logInfo("AggregateMemberMBean clientStats.getConnectionsFailed()=="+clientStats.getConnectionsFailed());
      logInfo("AggregateMemberMBean clientStats.getConnectionLifeTime()=="+clientStats.getConnectionLifeTime());
      logInfo("AggregateMemberMBean clientStats.getConnectionsOpen()=="+clientStats.getConnectionsOpen());
      logInfo("AggregateMemberMBean clientStats.getConnectionsIdle()=="+clientStats.getConnectionsIdle());     
      
      
      assertEquals(true,clientStats.getConnectionsOpened() == NETWORK_SERVER_CLIENT_CONNECTIONS_OPENED ? true : false);
      assertEquals(true,clientStats.getConnectionsClosed() == NETWORK_SERVER_CLIENT_CONNECTIONS_CLOSED ? true : false);
      assertEquals(true,clientStats.getConnectionsAttempted() == NETWORK_SERVER_CLIENT_CONNECTIONS_ATTEMPTED ? true : false);
      assertEquals(true,clientStats.getConnectionsFailed() == NETWORK_SERVER_CLIENT_CONNECTIONS_FAILED ? true : false);
      assertEquals(true,clientStats.getConnectionLifeTime() == NETWORK_SERVER_CLIENT_CONNECTIONS_LIFETIME ? true : false);
      assertEquals(true,clientStats.getConnectionsOpen() == NETWORK_SERVER_CLIENT_CONNECTIONS_OPEN ? true : false);
      assertEquals(true,clientStats.getConnectionsIdle() == NETWORK_SERVER_CLIENT_CONNECTIONS_IDLE ? true : false);  
      assertEquals(true,clientStats.getConnectionStatsType().equals("Client")  ? true : false);  
      
      //verify peer stats
      NetworkServerConnectionStats peerStats = aggregateMemberMBean.getNetworkServerPeerConnectionStats();
      
      logInfo("AggregateMemberMBean NETWORK_SERVER_PEER_CONNECTIONS_OPEN=="+NETWORK_SERVER_PEER_CONNECTIONS_OPENED);
      logInfo("AggregateMemberMBean NETWORK_SERVER_PEER_CONNECTIONS_CLOSED=="+NETWORK_SERVER_PEER_CONNECTIONS_CLOSED);
      logInfo("AggregateMemberMBean NETWORK_SERVER_PEER_CONNECTIONS_ATTEMPTED=="+NETWORK_SERVER_PEER_CONNECTIONS_ATTEMPTED);
      logInfo("AggregateMemberMBean NETWORK_SERVER_PEER_CONNECTIONS_FAILED=="+NETWORK_SERVER_PEER_CONNECTIONS_FAILED);
      logInfo("AggregateMemberMBean NETWORK_SERVER_PEER_CONNECTIONS_LIFETIME=="+NETWORK_SERVER_PEER_CONNECTIONS_LIFETIME);
      logInfo("AggregateMemberMBean NETWORK_SERVER_PEER_CONNECTIONS_OPEN=="+NETWORK_SERVER_PEER_CONNECTIONS_OPEN);     
      logInfo("AggregateMemberMBean NETWORK_SERVER_PEER_CONNECTIONS_IDLE=="+NETWORK_SERVER_PEER_CONNECTIONS_IDLE);
            
      logInfo("AggregateMemberMBean peerStats.getConnectionsOpened()=="+peerStats.getConnectionsOpened());
      logInfo("AggregateMemberMBean peerStats.getConnectionsClosed()=="+peerStats.getConnectionsClosed());
      logInfo("AggregateMemberMBean peerStats.getConnectionsAttempted()=="+peerStats.getConnectionsAttempted());
      logInfo("AggregateMemberMBean peerStats.getConnectionsFailed()=="+peerStats.getConnectionsFailed());
      logInfo("AggregateMemberMBean peerStats.getConnectionLifeTime()=="+peerStats.getConnectionLifeTime());
      logInfo("AggregateMemberMBean peerStats.getConnectionsActive()=="+peerStats.getConnectionsIdle());
      
      
      
      assertEquals(true,peerStats.getConnectionsOpened() == NETWORK_SERVER_PEER_CONNECTIONS_OPENED ? true : false);
      assertEquals(true,peerStats.getConnectionsClosed() == NETWORK_SERVER_PEER_CONNECTIONS_CLOSED ? true : false);
      assertEquals(true,peerStats.getConnectionsAttempted() == NETWORK_SERVER_PEER_CONNECTIONS_ATTEMPTED ? true : false);
      assertEquals(true,peerStats.getConnectionsFailed() == NETWORK_SERVER_PEER_CONNECTIONS_FAILED ? true : false);
      assertEquals(true,peerStats.getConnectionLifeTime() == NETWORK_SERVER_PEER_CONNECTIONS_LIFETIME ? true : false);
      assertEquals(true,peerStats.getConnectionsOpen() == NETWORK_SERVER_PEER_CONNECTIONS_OPEN ? true : false);
      assertEquals(true,peerStats.getConnectionsIdle() == NETWORK_SERVER_PEER_CONNECTIONS_IDLE ? true : false);
      assertEquals(true,peerStats.getConnectionStatsType().equals("Peer")  ? true : false);  
      
      //verify nested stats
      NetworkServerNestedConnectionStats nestedStats = aggregateMemberMBean.getNetworkServerNestedConnectionStats();
      
      logInfo("AggregateMemberMBean NETWORK_SERVER_NESTED_CONNECTIONS_OPEN=="+NETWORK_SERVER_NESTED_CONNECTIONS_OPEN);
      logInfo("AggregateMemberMBean NETWORK_SERVER_NESTED_CONNECTIONS_CLOSED=="+NETWORK_SERVER_NESTED_CONNECTIONS_CLOSED);
      logInfo("AggregateMemberMBean NETWORK_SERVER_NESTED_CONNECTIONS_ACTIVE=="+NETWORK_SERVER_NESTED_CONNECTIONS_ACTIVE);
      
      logInfo("AggregateMemberMBean nestedStats.getConnectionsOpened()=="+nestedStats.getConnectionsOpened());
      logInfo("AggregateMemberMBean nestedStats.getConnectionsClosed()=="+nestedStats.getConnectionsClosed());
      logInfo("AggregateMemberMBean nestedStats.getConnectionsActive()=="+nestedStats.getConnectionsActive());
      
      
      assertEquals(true,nestedStats.getConnectionsOpened() == NETWORK_SERVER_NESTED_CONNECTIONS_OPEN ? true : false);
      assertEquals(true,nestedStats.getConnectionsClosed() == NETWORK_SERVER_NESTED_CONNECTIONS_CLOSED ? true : false);
      assertEquals(true,nestedStats.getConnectionsActive() == NETWORK_SERVER_NESTED_CONNECTIONS_ACTIVE ? true : false);
      assertEquals(true,nestedStats.getConnectionStatsType().equals("Nested")  ? true : false);  
      
      //verify internal stats
      NetworkServerNestedConnectionStats internalStats = aggregateMemberMBean.getNetworkServerInternalConnectionStats();
      
      logInfo("AggregateMemberMBean NETWORK_SERVER_INTERNAL_CONNECTIONS_OPEN=="+NETWORK_SERVER_INTERNAL_CONNECTIONS_OPEN);
      logInfo("AggregateMemberMBean NETWORK_SERVER_INTERNAL_CONNECTIONS_CLOSED=="+NETWORK_SERVER_INTERNAL_CONNECTIONS_CLOSED);
      logInfo("AggregateMemberMBean NETWORK_SERVER_INTERNAL_CONNECTIONS_ACTIVE=="+NETWORK_SERVER_INTERNAL_CONNECTIONS_ACTIVE);
      
      logInfo("AggregateMemberMBean internalStats.getConnectionsOpened()=="+internalStats.getConnectionsOpened());
      logInfo("AggregateMemberMBean internalStats.getConnectionsClosed()=="+internalStats.getConnectionsClosed());
      logInfo("AggregateMemberMBean internalStats.getConnectionsActive()=="+internalStats.getConnectionsActive());
      
      assertEquals(true,internalStats.getConnectionsOpened() == NETWORK_SERVER_INTERNAL_CONNECTIONS_OPEN ? true : false);
      assertEquals(true,internalStats.getConnectionsClosed() == NETWORK_SERVER_INTERNAL_CONNECTIONS_CLOSED ? true : false);
      assertEquals(true,internalStats.getConnectionsActive() == NETWORK_SERVER_INTERNAL_CONNECTIONS_ACTIVE ? true : false);
      assertEquals(true,internalStats.getConnectionStatsType().equals("Internal")  ? true : false);        


  }

  @Override
  public void tearDown2() throws Exception {
    super.tearDown2();
  }
  
  


}
