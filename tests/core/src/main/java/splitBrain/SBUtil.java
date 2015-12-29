
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
package splitBrain;

import util.*;
import vsphere.vijava.VMotionBB;
import hydra.*;
import hydra.blackboard.*;

import com.gemstone.gemfire.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.jgroup.MembershipManagerHelper;
import com.gemstone.gemfire.distributed.internal.membership.MembershipTestHook;

import java.util.*;

/**
 * A class to contain methods useful for all splitBrain tests.
 * todo@lhughes - add locators to BB expectForcedDisconnects list
 * todo@lhughes - once forcedDisconnecct received add to forcedDisconnectList
 */
public class SBUtil {

  public static final int ONEWAY = 0;
  public static final int TWOWAY = 1;

  //============================================================================
  // INITTASKS/CLOSETASKS
  //============================================================================

  /**
   * Creates a (disconnected) locator.
   */
  public static void createLocatorTask() {
    DistributedSystemHelper.createLocator();
  }

  /**
   * Connects a locator to its distributed system.
   */
  public static void startAndConnectLocatorTask() {
    DistributedSystemHelper.startLocatorAndAdminDS();

    // if this VM is not the coordinator, add to a list of 
    // eligibleCoordinators on the BB
    if (isCoordinator()) {
       Log.getLogWriter().info("This VM is currently the Coordinator");
    } else {
       SplitBrainBB.addEligibleCoordinator(RemoteTestModule.getMyClientName());
    }
  }

  /**
   * Stops a locator.
   */
  public static void stopLocatorTask() {
    DistributedSystemHelper.stopLocator();
  }

  /**
   * Restore Connectivity between test hosts
   * The dropType (oneWay vs. twoWay) is determined by the SplitBrainPrm
   * dropType (which defaults to twoWay).
   */
  public static void restoreConnection() {
    SharedLock criticalCodeSection = SplitBrainBB.getBB().getSharedLock();
    criticalCodeSection.lock();
    try {
      String hdName1 = TestConfig.tab().stringAt(SplitBrainPrms.hostDescription1, "host1");
      String hdName2 = TestConfig.tab().stringAt(SplitBrainPrms.hostDescription2, "host2");
      String src = TestConfig.getInstance().getHostDescription(hdName1).getHostName(); 
      String target = TestConfig.getInstance().getHostDescription(hdName2).getHostName();
      restoreConnection(src, target);
    } finally {
      criticalCodeSection.unlock();
    }
    VMotionBB.getBB().getSharedMap().put("connectionDropped", Boolean.FALSE);
  }
  
  /**
   * Restore Connectivity between src and target hosts
   * The dropType (oneWay vs. twoWay) is determined by the SplitBrainPrm
   * dropType (which defaults to twoWay).
   */
  public static void restoreConnection(String src, String target) {
    boolean enableNetworkHelper = TestConfig.tab().booleanAt(SplitBrainPrms.enableNetworkHelper, true);
    if (!enableNetworkHelper) {
      Log.getLogWriter().info("Not restoring connection between " + src + " and " + target + " as enableNetworkHelper is false");
      return;
    }

    NetworkHelper.printConnectionState();

    int dropType = SplitBrainPrms.getDropType();
    try {
      switch (dropType) {
      case ONEWAY:
        NetworkHelper.restoreConnectionOneWay(src, target);
        break;
      case TWOWAY:
        NetworkHelper.restoreConnectionTwoWay(src, target);
        break;
      default:
        throw new TestException("Invalid dropType = " + dropType);
      }
    } catch (IllegalStateException e) {
      Log.getLogWriter().info("Caught " + e + " indicating that connections are already restored");
    }

    NetworkHelper.printConnectionState();
  }
  
  /**
   * Restore connectivity between multiple host pairs.
   * (Which is dropped initially using dropMultipleConnections() method
   * Host pairs are specified using SplitBrainPrms.hostPairsForConnectionDropRestore
   * The parameter needs pairs of hostDescriptions with a comma separated list.
   * E.g. hostPairsForConnectionDropRestore = hd1 hd2, hd2 hd4;
   * 
   */
  public static void restoreMultipleConnections() {
    Vector hostDescriptions = TestConfig.tab().vecAt(SplitBrainPrms.hostPairsForConnectionDropRestore);
    SharedLock criticalCodeSection = SplitBrainBB.getBB().getSharedLock();
    criticalCodeSection.lock();
    try {
      for (int i = 0; i < hostDescriptions.size(); i++) {
        Vector vector = (Vector)hostDescriptions.elementAt(i);
        String hostDescName1 = (String) vector.elementAt(0);
        String hostDescName2 = (String) vector.elementAt(1);
        String src = TestConfig.getInstance().getHostDescription(hostDescName1).getHostName(); 
        String target = TestConfig.getInstance().getHostDescription(hostDescName2).getHostName();
        restoreConnection(src, target);
      }
    } finally {
      criticalCodeSection.unlock();
    }
    
  }

  //============================================================================
  // TASKS
  //============================================================================

  /**
   * Kill an EligibleCoordinator 
   *
   * Restriction: This must be called from a client in the same DS as the 
   * LeadMember.  For example, edge clients are not connected to a DS 
   * (they are loners), so this call will have to be made from bridgeServers 
   * in hct tests.
   */
  public static void killEligibleCoordinator() {
     Set aSet= SplitBrainBB.getEligibleCoordinators();
     if (aSet.isEmpty()) {
        throw new TestException("Cannot kill eligibleCoordinator, none available");
     }
     Object[] vms = aSet.toArray();
     ClientVmInfo targetVm = new ClientVmInfo(null, (String)vms[0], null);
     try {
        ClientVmMgr.stopAsync( "Killing EligibleCoordinator " + targetVm,
                               ClientVmMgr.MEAN_KILL,
                               ClientVmMgr.NEVER,
                               targetVm);
      } catch (ClientVmNotFoundException e) {
        throw new TestException("Failed to kill eligibleCoordinator, caught Exception " + e);
      }
  }

  /**
   * Kill the current Coordinator
   *
   * Restriction: This must be called from a client in the same DS as the 
   * LeadMember.  For example, edge clients are not connected to a DS 
   * (they are loners), so this call will have to be made from bridgeServers 
   * in hct tests.
   */
  public static void killCoordinator() {
     DistributedMember dm = getCoordinator();
     String host = dm.getHost();
     int pid = dm.getProcessId();

     ClientVmInfo targetVm = getClientVmInfo( host, pid );
     try {
        ClientVmMgr.stopAsync( "Killing Coordinator " + dm.toString(),
                               ClientVmMgr.MEAN_KILL,
                               ClientVmMgr.NEVER,
                               targetVm);
      } catch (ClientVmNotFoundException e) {
        throw new TestException("Failed to kill coordinator, caught Exception " + e);
      }
  }

  /**
   * Kill the current Lead Member
   *
   * Restriction: This must be called from a client in the same DS as the 
   * LeadMember.  For example, edge clients are not connected to a DS 
   * (they are loners), so this call will have to be made from bridgeServers 
   * in hct tests.
   */
  public static void killLeadMember() {
     DistributedMember dm = getLeadMember();
     String host = dm.getHost();
     int pid = dm.getProcessId();

     ClientVmInfo targetVm = getClientVmInfo( host, pid );
     try {
        ClientVmMgr.stopAsync( "Killing LeadMember " + dm.toString(),
                               ClientVmMgr.MEAN_KILL,
                               ClientVmMgr.NEVER,
                               targetVm);
      } catch (ClientVmNotFoundException e) {
        throw new TestException("Failed to kill leadMember, caught Exception " + e);
      }
  }

  /** Drop the connection between the two test hosts (host1, host2)
   *  (machines which host the test client and locator VMs).
   *  The dropType is determined by SplitBrainPrms.dropType (default = TWOWAY)
   *
   *  Tests which require coordination between client VMs and the networkDrop
   *  do so through a hydra.blackboard.SharedLock (see criticalCodeSection) 
   *  Currently used by splitBrain/PRNetDown.java.
   *
   *  Tests which require time for the system to 'settle down' after
   *  a network partition (for example, to re-establish redundancy in PRs)
   *  can set SplitBrainPrms.dropWaitTimeSec to cause this task to wait for
   *  dropWaitTimeSec before returning.  (defaults to 0)
   */
  public static void dropConnection() {
     String hdName1 = TestConfig.tab().stringAt(SplitBrainPrms.hostDescription1, "host1");
     String hdName2 = TestConfig.tab().stringAt(SplitBrainPrms.hostDescription2, "host2");
     String src = TestConfig.getInstance().getHostDescription(hdName1).getHostName(); 
     String target = TestConfig.getInstance().getHostDescription(hdName2).getHostName();
     dropConnection(src, target);
     SplitBrainBB.getBB().getSharedCounters().increment(SplitBrainBB.dropConnectionComplete);
  }
  
  public static void dropConnection(String src, String target) {
    boolean enableNetworkHelper = TestConfig.tab().booleanAt(SplitBrainPrms.enableNetworkHelper, true);

    // coordinate this critical section of code with client VMs via
    // a hydra.blackboard.SharedLock
    // Currently used in PRNetDownTest.concVerify()
    SharedLock criticalCodeSection = SplitBrainBB.getBB().getSharedLock();
    criticalCodeSection.lock();
    Log.getLogWriter().fine("SBUtil.dropConnections() obtained the SharedLock!");
    try {
       if (enableNetworkHelper) {
          NetworkHelper.printConnectionState();
          int dropType = SplitBrainPrms.getDropType();
          switch (dropType) {
             case ONEWAY:
                NetworkHelper.dropConnectionOneWay(src, target);
                break;
             case TWOWAY:
                NetworkHelper.dropConnectionTwoWay(src, target);
                break;
             default:
                throw new TestException("Invalid dropType = " + dropType);
          }
          NetworkHelper.printConnectionState();
          VMotionBB.getBB().getSharedMap().put("connectionDropped", Boolean.TRUE);
       } else {
          Log.getLogWriter().info("Not dropping connection between " + src + " and " + target + " as enableNetworkHelper is false"); 
       }
  
       int waitTimeSec = SplitBrainPrms.getDropWaitTimeSec();
       if (waitTimeSec > 0) {
          Log.getLogWriter().info("SBUtil.dropConnections sleeping for " +  waitTimeSec + " seconds");
          MasterController.sleepForMs(waitTimeSec * 1000);
       }
    } finally {
       criticalCodeSection.unlock();
       Log.getLogWriter().fine("SBUtil.dropConnections() released the SharedLock!");
    } 
  }
  
  /** Drop the multiple connection between the pairs of the hosts specified
   * using parameter: SplitBrainPrms.hostPairsForConnectionDropRestore
   * The parameter needs pairs of hostDescriptions with a comma separated list.
   * E.g. hostPairsForConnectionDropRestore = hd1 hd2, hd2 hd4;
   */
  public static void dropMultipleConnections() {
     Vector hostDescriptions = TestConfig.tab().vecAt(SplitBrainPrms.hostPairsForConnectionDropRestore);
     
     for (int i = 0; i < hostDescriptions.size(); i++) {
       Vector vector = (Vector)hostDescriptions.elementAt(i);
       String hostDescName1 = (String) vector.elementAt(0);
       String hostDescName2 = (String) vector.elementAt(1);
       String src = TestConfig.getInstance().getHostDescription(hostDescName1).getHostName(); 
       String target = TestConfig.getInstance().getHostDescription(hostDescName2).getHostName();
       dropConnection(src, target);
     }
     SplitBrainBB.getBB().getSharedCounters().increment(SplitBrainBB.dropConnectionComplete);
  }


  //============================================================================
  // Support methods
  //============================================================================

  /**
   * get eligibleCoordinator info and return true if the calling VM
   * is the coordinator
   */
  public static boolean isCoordinator() {
     DistributedMember coordinator = getCoordinator();
     DistributedSystem ds = DistributedSystemHelper.getDistributedSystem();
     if (coordinator.equals(ds.getDistributedMember())) {
        return true;
     }
     return false;
  }

  /**
   * get the DistributedMember info for the Current Coordinator
   */
  protected static DistributedMember getCoordinator() {
     DistributedSystem ds = DistributedSystemHelper.getDistributedSystem();
     if (ds == null) {
        throw new TestException("Can't get coordinator because DistributedSystem is " + ds);
     }
     DistributedMember dm = MembershipManagerHelper.getCoordinator(ds);
     return dm;
  }

  /**
   * get the DistributedMember info for the Current LeadMember
   */
  public static DistributedMember getLeadMember() {
     DistributedSystem ds = DistributedSystemHelper.getDistributedSystem();
     if (ds == null) {
        throw new TestException("Can't get lead member because DistributedSystem is " + ds);
     }
     DistributedMember dm = MembershipManagerHelper.getLeadMember(ds);
     return dm;
  }

  /**
   * get leadMember info and return true if the calling VM
   * is the leadMember
   */
  public static boolean isLeadMember() {
     DistributedMember leadMember = getLeadMember();
     DistributedSystem ds = DistributedSystemHelper.getDistributedSystem();
     if (leadMember.equals(ds.getDistributedMember())) {
        return true;
     }
     return false;
  }

  /*
   * For a given host and PID (retrieved from a DistributedMemberId), return
   * the corresponding ClientVmInfo.
   */
  protected static ClientVmInfo getClientVmInfo(String host, int pid) {

     Integer vmid = null;
     try {
        vmid = RemoteTestModule.Master.getVmid(host, pid);
     } catch (java.rmi.RemoteException e) {
        throw new TestException("Could not convert dm host and pid to vmid, caught Exception " + e);
     }
     if (vmid == null) {
       throw new TestException("killCoordinator cannot get vmId for host " + host + " and PID " + pid);
     }
     return new ClientVmInfo(vmid.intValue());
  }

  /** Given a member, wait for the lead member to become different than
   *  the given member.
   *
   *  @param member A distributed member or null. This method waits until
   *         the current lead member is different than this one.
   *  @param msToSleep The number of millis to sleep between checks for
   *         the lead member.
   *  @param return A lead member different than member.
   *
   *  @return [0] (Long) The number of millseconds it took to detect a lead change. 
   *              Note that the value of msToSleep can affect this value.
   *          [1] (DistributedMember) The new DistributedMember
   */
  protected static Object[] waitForLeadChange(DistributedMember member,
                                             int msToSleep) {
     final long LOG_INTERVAL_MILLIS = 10000;
     Log.getLogWriter().info("Waiting for lead member to change from " + member + "...");
     long lastLogTime = System.currentTimeMillis();
     long startTime = System.currentTimeMillis();
     while (true) {
        DistributedMember currentLead = getLeadMember();
        boolean leadHasChanged = ((member == null) && (currentLead != null)) ||
                                 ((member != null) && (currentLead == null)) ||
                                 ((member != null) && (currentLead != null) &&
                                  (member.getProcessId() != currentLead.getProcessId()));
        if (leadHasChanged) {
           long endTime = System.currentTimeMillis();
           long duration = endTime - startTime;
           Log.getLogWriter().info("Lead member has changed from " + member + " to " + 
               currentLead + " after waiting " + duration + " ms");
           return new Object[] {new Long(duration), currentLead};
        }
        if (System.currentTimeMillis() - lastLogTime > LOG_INTERVAL_MILLIS) {
           Log.getLogWriter().info("Waiting for lead member to change from " + currentLead + "...");
           lastLogTime = System.currentTimeMillis();
        }
        MasterController.sleepForMs(msToSleep);
     }
  }

  /** Given a member, wait for the coordinator member to become different than
   *  the given member.
   *
   *  @param member A distributed member or null. This method waits until
   *         the current coordinator member is different than this one.
   *  @param msToSleep The number of millis to sleep between checks for
   *         the coordinator member.
   *
   *  @return [0] (Long) The number of millseconds it took to detect a coordinator change.
   *              Note that the value of msToSleep can affect this value.
   *          [1] (DistributedMember) The new DistributedMember
   */
  protected static Object[] waitForCoordChange(DistributedMember member,
                                               int msToSleep) {
     final long LOG_INTERVAL_MILLIS = 10000;
     Log.getLogWriter().info("Waiting for coordinator to change from " + member + "...");
     long lastLogTime = System.currentTimeMillis();
     long startTime = System.currentTimeMillis();
     while (true) {
        DistributedMember currentCoord = getCoordinator();
        boolean coordHasChanged = ((member == null) && (currentCoord != null)) ||
                                  ((member != null) && (currentCoord == null)) ||
                                  ((member != null) && (currentCoord != null) &&
                                   (member.getProcessId() != currentCoord.getProcessId()));
        if (coordHasChanged) {
           long endTime = System.currentTimeMillis();
           long duration = endTime - startTime;
           Log.getLogWriter().info("Coordinator has changed from " + member + " to " +
               currentCoord + " after waiting " + duration + " ms");
           return new Object[] {new Long(duration), currentCoord};
        }
        if (System.currentTimeMillis() - lastLogTime > LOG_INTERVAL_MILLIS) {
           Log.getLogWriter().info("Waiting for coordinator to change from " + currentCoord + "...");
           lastLogTime = System.currentTimeMillis();
        }
        MasterController.sleepForMs(msToSleep);
     }
  }

  /**
   *  Uses RemoteTestModule information to produce a name to uniquely identify
   *  a client vm (vmid, clientName, host, pid) for the calling thread
   */
  public static String getMyUniqueName() {
    StringBuffer buf = new StringBuffer( 50 );
    buf.append("vm_" ).append(RemoteTestModule.getMyVmid());
    buf.append( "_" ).append(RemoteTestModule.getMyClientName());
    buf.append( "_" ).append(RemoteTestModule.getMyHost());
    buf.append( "_" ).append(RemoteTestModule.getMyPid());
    return buf.toString();
  }

  /** Install a test hook to notify test when a force disconnect has occurred.
   */
  public static void addMembershipHook(MembershipTestHook hook) {
     MembershipManagerHelper.addTestHook(DistributedSystemHelper.getDistributedSystem(), hook);
  }

  /** Remove a test hook installed by calling SBUtil.addMembershipHook(...).
   */
  public static void removeMembershipHook(MembershipTestHook hook) {
     MembershipManagerHelper.removeTestHook(DistributedSystemHelper.getDistributedSystem(), hook);
  }

  /** Cause this vm to be sick
   */
  public static void beSick() {
     Log.getLogWriter().info("Calling beSickMember");
     MembershipManagerHelper.beSickMember(DistributedSystemHelper.getDistributedSystem());
     Log.getLogWriter().info("Done calling beSickMember");
  }

  /** Cause this vm to be healthy
   */
  public static void beHealthy() {
     Log.getLogWriter().info("Calling beHealthyMember");
     MembershipManagerHelper.beHealthyMember(DistributedSystemHelper.getDistributedSystem());
     Log.getLogWriter().info("Done calling beHealthyMember");
  }

  /** Cause this vm to not respond to ARE_YOU_DEAD messages
   */
  public static void playDead() {
     Log.getLogWriter().info("Calling playDead()");
     MembershipManagerHelper.playDead(DistributedSystemHelper.getDistributedSystem());
     Log.getLogWriter().info("Done calling playDead");
  }

}
