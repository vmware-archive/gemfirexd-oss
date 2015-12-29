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
package util;

import hydra.*;

import java.util.*;
import com.gemstone.gemfire.admin.AdminDistributedSystem;
import com.gemstone.gemfire.admin.AdminException;
import com.gemstone.gemfire.distributed.DistributedMember;


import util.TestException;
import vsphere.vijava.VMotionUtil;

/** This class is a thread that will stop a VM, wait for it to stop, then
 *  start another VM and wait for it to start. This is useful for concurrently
 *  stopping more than 1 VM, but with the ability to wait for a restart.
 *
 */
public class StopStartVMs extends Thread {

private ClientVmInfo targetVm;  // for this thread, the vm to stop and/or start
private int          stopMode;  // for this thread, the stopMode to use
private boolean      stop;      // thread will do a stop
private boolean      start;     // thread will do a start

private static final String NiceKillInProgress = "NiceKill_VmId_";
private static final String VmInfoKey = "StopStartVMInfo_for_vmid_";

/** Constructor to specify stop and start.
 *
 *  @param aVmId The VmId of the VM to stop then restart.
 *  @param howToStop The hydra.ClientVmMgr stopMode (unused if stop is false).
 */
public StopStartVMs(ClientVmInfo aVmId, int howToStop) {
   targetVm = aVmId;
   stopMode = howToStop;
   stop = true;
   start = true;
   setName("<StopStartVMs-thread to " + getAction(stop, start) + " " + aVmId + " with " + ClientVmMgr.toStopModeString(stopMode) + ">");
}

/** Constructor to specify stop only or start only or both.
 *
 *  @param aVmId The VmId of the VM to stop then restart.
 *  @param howToStop The hydra.ClientVmMgr stopMode (unused if stop is false).
 *  @parram doStop If true then this thread stops vms.
 *  @parram doStart If true then this thread starts vms.
 */
public StopStartVMs(ClientVmInfo aVmId, int howToStop, boolean doStop, boolean doStart) {
   if (!doStop && !doStart) {
      throw new TestException("You must specify at least one of stop or start, stop is " + doStop + " and start is " + doStart);
   }
   targetVm = aVmId;
   stopMode = howToStop;
   stop = doStop;
   start = doStart;
   if (doStop) {
      setName("<StopStartVMs-thread to " + getAction(stop, start) + " " + aVmId + " with " + ClientVmMgr.toStopModeString(stopMode) + ">");
   } else {
      setName("<StopStartVMs-thread to " + getAction(stop, start) + " " + aVmId + ">");
   }
}

/** Run method for this thread: if this thread is set to stop, then
 *  stop a VM and wait for it to stop. If this thread is set to start
 *  then start a VM and wait for it to start. Any errors are 
 *  placed in the blackboard errStr.
 */
public void run() {
   try {
      Log.getLogWriter().info("Started " + getName());
      try {
         if (stop) {
            ClientVmMgr.stop(
                "Test is synchronously stopping " + targetVm + " with " + ClientVmMgr.toStopModeString(stopMode), 
                stopMode, 
                ClientVmMgr.ON_DEMAND, 
                targetVm);
         }
         VMotionUtil.doVMotion(targetVm);
         if (start) {
            ClientVmMgr.start("Test is synchronously starting " + targetVm, targetVm);
         }
      } catch (hydra.ClientVmNotFoundException e) {
         Log.getLogWriter().info("Caught in thread " + getName() + ":" + TestHelper.getStackTrace(e));
         StopStartBB.getBB().getSharedMap().put(StopStartBB.errKey, TestHelper.getStackTrace(e));
      }
   } catch (Throwable e) {
      Log.getLogWriter().info("Caught in thread " + getName() + ":" + TestHelper.getStackTrace(e));
      StopStartBB.getBB().getSharedMap().put(StopStartBB.errKey, TestHelper.getStackTrace(e));
   }
   Log.getLogWriter().info(getName() + " terminating");
}

/** Get a list of "other" vms, along with stop modes. The "other" vms are
 *  any vms other than the currently executing vm.
 *  The stop mode for each VM is chosen from util.StopStartPrms.stopMode.
 *
 *  @param numToTarget The number of VMs to target for a stop/start.
 *
 *  @throws TestException if there aren't numVMsToStop VMs available.
 *  @returns Object[0] - a List of ClientVmInfos.
 *           Object[1] - a List of stop modes.
 *           Object[2] - a List of vms not chosen (including this vm).
 *
 */
public static Object[] getOtherVMs(int numToTarget) {
   return getOtherVMsWithExclude(numToTarget, null);
}

/** Get a list of "other" vms, along with stop modes. The "other" vms are
 *  any vms other than the currently executing vm.
 *  The stop mode for each VM is chosen from util.StopStartPrms.stopMode.
 *
 *  @param numVMsToTarget The number of VMs to target for a stop/start.
 *  @param clientMatchStr A string the must be contained in the client
 *         name to be included in the vms to target, or null.
 *
 *  @throws TestException if there aren't numToTarget VMs available.
 *  @returns Object[0] - a List of ClientVmInfos.
 *           Object[1] - a List of stop modes.
 *           Object[2] - a List of vms eligible but not chosen.
 *
 */
public static Object[] getOtherVMs(int numToTarget, String clientMatchStr) {
   Log.getLogWriter().info("Choosing " + numToTarget + " vms (other than this one)");
   // get the VMs; vmList and stopModeList are parallel lists
   ArrayList vmList = new ArrayList();
   ArrayList stopModeList = new ArrayList();
   int myVmID = RemoteTestModule.getMyVmid();

   // get VMs that contain the clientMatchStr
   List vmInfoList = getAllVMs();
   vmInfoList = getMatchVMs(vmInfoList, clientMatchStr);

   // now all vms in vmInfoList match the clientMatchStr
   do {
      if (vmInfoList.size() == 0) {
         throw new TestException("Unable to find " + numToTarget +
               " vms to stop with client match string " + clientMatchStr +
               "; either a test problem or add StopStartVMs.StopStart_initTask to the test");
      }
      // add a VmId to the list of vms to stop
      int randInt = TestConfig.tab().getRandGen().nextInt(0, vmInfoList.size()-1);
      ClientVmInfo info = (ClientVmInfo)(vmInfoList.get(randInt));
      if (info.getVmid().intValue() != myVmID) { // info is not the current VM
         vmList.add(info);

         // choose a stopMode
         String choice = TestConfig.tab().stringAt(StopStartPrms.stopModes);
         stopModeList.add(choice);
      }
      vmInfoList.remove(randInt);
   } while (vmList.size() < numToTarget);
   return new Object[] {vmList, stopModeList, vmInfoList};
}   

/** Get a list of "other" vms, along with stop modes. The "other" vms are
 *  any vms other than the currently executing vm.
 *  The stop mode for each VM is chosen from util.StopStartPrms.stopMode.
 *
 *  @param numVMsToTarget The number of VMs to target for a stop/start.
 *  @param clientExcludeStr Any vm name that contains this string as a
 *         substring is excluded from consideration.
 *
 *  @throws TestException if there aren't numToTarget VMs available.
 *  @returns Object[0] - a List of ClientVmInfos.
 *           Object[1] - a List of stop modes.
 *           Object[2] - a List of vms eligible but not chosen.
 *
 */
public static Object[] getOtherVMsWithExclude(int numToTarget, String clientExcludeStr) {
   Log.getLogWriter().info("Choosing " + numToTarget + " vms (other than this one)");
   // get the VMs; vmList and stopModeList are parallel lists
   ArrayList vmList = new ArrayList();
   ArrayList stopModeList = new ArrayList();
   int myVmID = RemoteTestModule.getMyVmid();

   // put numToTarget in vmList
   List vmInfoList = getAllVMs();
   do {
      if (vmInfoList.size() == 0) {
         throw new TestException("Unable to find " + numToTarget + 
               " vms to stop with client exclude string " + clientExcludeStr + 
               "; either a test problem or add StopStartVMs.StopStart_initTask to the test");
      }
      // add a VmId to the list of vms to stop
      int randInt = TestConfig.tab().getRandGen().nextInt(0, vmInfoList.size()-1);
      Object anObj = vmInfoList.get(randInt);
      if (anObj instanceof ClientVmInfo) {
         ClientVmInfo info = (ClientVmInfo)(anObj);
         if (info.getVmid().intValue() != myVmID) { // info is not the current VM
            if ((clientExcludeStr != null) && (info.getClientName().indexOf(clientExcludeStr) >= 0)) {
               // exclude this vm
            } else {
               vmList.add(info);

               // choose a stopMode
               String choice = TestConfig.tab().stringAt(StopStartPrms.stopModes);
               stopModeList.add(choice);
            }
         }
      }
      vmInfoList.remove(randInt);
   } while (vmList.size() < numToTarget);
   return new Object[] {vmList, stopModeList, vmInfoList};
}   

/** Get a list of "other" vms, along with stop modes. The "other" vms are
 *  any vms other than the currently executing vm and matching vmIds.
 *  The stop mode for each VM is chosen from util.StopStartPrms.stopMode.
 *
 *  @param numVMsToTarget The number of VMs to target for a stop/start.
 *  @param clientMatchStr A string the must be contained in the client
 *         name to be included in the vms to target, or null.
 *  @param vmIds An ArrayList of Integer contains the vmIds not targeted to be stopped 
 *
 *  @throws TestException if there aren't numToTarget VMs available.
 *  @returns Object[0] - a List of ClientVmInfos.
 *           Object[1] - a List of stop modes.
 *           Object[2] - a List of vms eligible but not chosen.
 *
 */
public static Object[] getOtherVMsWithExcludeVmid(int numToTarget, String clientMatchStr, 
    ArrayList<Integer> vmIds) {
  Log.getLogWriter().info("Choosing " + numToTarget + " vms (other than this one)");
  // get the VMs; vmList and stopModeList are parallel lists
  ArrayList vmList = new ArrayList();
  ArrayList stopModeList = new ArrayList();
  int myVmID = RemoteTestModule.getMyVmid();

  // get VMs that contain the clientMatchStr
  List vmInfoList = getAllVMs();
  vmInfoList = getMatchVMs(vmInfoList, clientMatchStr);

  // now all vms in vmInfoList match the clientMatchStr
  do {
     if (vmInfoList.size() == 0) {
        throw new TestException("Unable to find " + numToTarget +
              " vms to stop with client match string " + clientMatchStr +
              "; either a test problem or add StopStartVMs.StopStart_initTask to the test");
     }
     // add a VmId to the list of vms to stop
     int randInt = TestConfig.tab().getRandGen().nextInt(0, vmInfoList.size()-1);
     ClientVmInfo info = (ClientVmInfo)(vmInfoList.get(randInt));
     boolean exclude = false;
     for (int vmId: vmIds) {       
       if (info.getVmid().intValue() == vmId) exclude = true;
     }
     if (info.getVmid().intValue() != myVmID && !exclude) { // info is not the current VM
       vmList.add(info);

       // choose a stopMode
       String choice = TestConfig.tab().stringAt(StopStartPrms.stopModes);
       stopModeList.add(choice);
     }
     vmInfoList.remove(randInt);
  } while (vmList.size() < numToTarget);
  return new Object[] {vmList, stopModeList, vmInfoList};
} 

/** Concurrently stop and restart the specified number of VMs, other than this VM.
 *  The stop mode for each VM is chosen from util.StopStartPrms.stopMode.
 *
 *  Side effects: see stopStartVMs(List, List).
 *
 *  @param  numToTarget The number of VMs, other than this VM, to stop and restart.
 *
 *  @throws TestException if there aren't  numToTarget VMs available for stopping.
 *
 */
public static void stopStartOtherVMs(int  numToTarget) {
   Log.getLogWriter().info("In stopStartVMs, attempting to stop/restart " + numToTarget + " vms (other than this one)");
   Object[] tmpArr = getOtherVMs(numToTarget);
   // get the VMs to stop; vmList and stopModeList are parallel lists
   List vmList = (List)(tmpArr[0]);
   List stopModeList = (List)(tmpArr[1]);
   StopStartVMs.stopStartVMs(vmList, stopModeList);
   Log.getLogWriter().info("In stopStartVMs, done with stop/restart " + numToTarget + " vms (other than this one)");
}   

/**
 * Concurrently stops a List of VMs, then restarts them.  Waits for the
 *  restart to complete before returning.
 *
 * Executes {@link #stopStartAsync(List,List)} followed by {@link
 * #joinStopStart(List,List)}.
 *
 *  @param targetVmList A list of hydra.ClientVmInfos to stop then restart.
 *  @param stopModeList A parallel list of stop modes (Strings).
 */
public static void stopStartVMs(List targetVmList, List stopModeList) {
   List threadList = stopStartAsync(targetVmList, stopModeList);
   joinStopStart(targetVmList, threadList);
}   

/**
 * Concurrently stops a List of Vms, then restart them. Does not wait for the
 * restart to complete before returning.  This allows the caller to carry out
 * operations concurrent with the stop and start activity, then wait for the
 * restart to complete using {@link #joinStopStartThreads(List,List)}.
 *
 *  Side effects: For any VM which is performing a NICE_KILL because of this call,
 *     it is marked as such in the StopStartBB (blackboard). The blackboard will 
 *     contain a key prefixed with util.StopStartVMs.NiceKillInProgress and appended 
 *     to with its VmId. The value of this key is the true. 
 * 
 *     This blackboard entry is for the convenience of the caller, and it is up to the 
 *     caller to look for it or not. When a VM undergoes a NICE_KILL, the VM closes the 
 *     cache and disconnects from the distributed system. Any threads in that VM doing 
 *     work can get exceptions during the close and/or disconnect step. This blackboard 
 *     entry is useful for those threads to allow exceptions when they know an exception 
 *     is possible. 
 *
 *  @param targetVmList A list of hydra.ClientVmInfos to stop then restart.
 *  @param stopModeList A parallel list of stop modes (Strings).
 *  @return threadList The threads created to stop the VMs.
 */
public static List stopStartAsync(List targetVmList, List stopModeList) {
   if (targetVmList.size() != stopModeList.size()) {
      throw new TestException("Expected targetVmList " + targetVmList + " and stopModeList " + 
                stopModeList + " to be parallel lists of the same size, but they have different sizes");
   }
   Log.getLogWriter().info("In stopStartVMs, vms to stop: " + targetVmList + 
       ", corresponding stop modes: " + stopModeList);

   // mark in the blackboard any vms that will undergo a NICE_KILL
   for (int i = 0; i < stopModeList.size(); i++) {
      String stopMode = (String)(stopModeList.get(i));
      if (ClientVmMgr.toStopMode(stopMode) == ClientVmMgr.NICE_KILL) { 
         // write nice kills to the blackboard so the killed vm may, if it chooses, handle any
         // exceptions that are underway.
         StopStartBB.getBB().getSharedMap().put(
            NiceKillInProgress + ((ClientVmInfo)targetVmList.get(i)).getVmid(), new Boolean(true));
      }
   }

   // stop all vms in targetVms list
   List threadList = new ArrayList();
   for (int i = 0; i < targetVmList.size(); i++) {
      ClientVmInfo targetVm = (ClientVmInfo)(targetVmList.get(i));
      int stopMode = ClientVmMgr.toStopMode((String)(stopModeList.get(i)));
      Thread aThread = new StopStartVMs(targetVm, stopMode);
      aThread = new HydraSubthread(aThread);
      threadList.add(aThread);
      aThread.start();
   }
   return threadList;
}

/**
 * Asynchronously start the given vms and return a List of threads that spawned
 * the start.  This allows the caller to carry out operations concurrent with the 
 * start activity, then wait for the start to complete using {@link #joinStopStartThreads(List,List)}.
 *
 *  @param targetVmList A list of hydra.ClientVmInfos to start.
 *  @return threadList The threads created to start the VMs.
 */
public static List startAsync(List targetVmList) {
   Log.getLogWriter().info("In stopStartVMs, vms to start: " + targetVmList);

   // start all vms in targetVms list
   List threadList = new ArrayList();
   for (int i = 0; i < targetVmList.size(); i++) {
      ClientVmInfo targetVm = (ClientVmInfo)(targetVmList.get(i));
      Thread aThread = new StopStartVMs(targetVm, 0, false, true);
      aThread = new HydraSubthread(aThread);
      threadList.add(aThread);
      aThread.start();
   }
   return threadList;
}

/**
 * Waits for a List of threads concurrently stopping and starting Vms to finish,
 * as returned by {@link #stopStartAsync(List,List)}.  Resets the niceKill
 * blackboard entry for each vm is set to false.
 *
 * @param targetVmList A list of hydra.ClientVmInfos to stop then restart.
 * @param threadList A parallel list of threads on whom to wait.
 */
public static void joinStopStart(List targetVmList, List threadList) {
   Log.getLogWriter().info("Joining stop/start threads...");
   // wait for all threads to complete the stop and start
   for (int i = 0; i < threadList.size(); i++) {
      HydraSubthread aThread = (HydraSubthread)(threadList.get(i));
      try {
         aThread.join();
      } catch (InterruptedException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      }       
      Object err = StopStartBB.getBB().getSharedMap().get(StopStartBB.errKey);
      if (err != null) {
         throw new TestException(err.toString());
      }
   }   

   // reset the blackboard entries 
   for (int i = 0; i < targetVmList.size(); i++) {
      ClientVmInfo targetVm = (ClientVmInfo)(targetVmList.get(i));
      StopStartBB.getBB().getSharedMap().put(StopStartVMs.NiceKillInProgress + targetVm.getVmid(), new Boolean(false));
   }
   Log.getLogWriter().info("Done joining stop/start threads...");
}   

/** Returns true if a NICE_KILL is in progress in this VM, false otherwise
 */
public static boolean niceKillInProgress() {
   Object niceKill = StopStartBB.getBB().getSharedMap().get(
                     NiceKillInProgress + RemoteTestModule.getMyVmid());
   boolean niceKillInProgress = (niceKill != null) && (((Boolean)niceKill).booleanValue());
   return niceKillInProgress;
}

/** An init task to write ClientVmInfo instances to the blackboard, for
 *  use later in choosing vms to stop/start. This is only necessary if
 *  getOtherVMs(numToTarget, clientMatchStr) is used.
 */
public static void StopStart_initTask() {
   int myVMid = RemoteTestModule.getMyVmid();
   ClientVmInfo myInfo = new ClientVmInfo(new Integer(myVMid), RemoteTestModule.getMyClientName(), null);
   StopStartBB.getBB().getSharedMap().put(VmInfoKey + myVMid, myInfo);
}

/** Return a List of all ClientVmInfo instances in the StopStartVMs blackboard.
 */
public static List getAllVMs() {
   List aList = new ArrayList();
   Map aMap = StopStartBB.getBB().getSharedMap().getMap();
   Iterator it = aMap.keySet().iterator();
   while (it.hasNext()) {
      Object key = it.next();
      Object value = aMap.get(key);
      if (value instanceof ClientVmInfo) {
         aList.add(value);
      }
   }
   return aList;  
}

/** Return a string describing the stop/start action of this thread
 */
private static String getAction(boolean doStop, boolean doStart) {
   String actionStr = null;
   if (doStop && doStart) {
      actionStr = "stop and start";
   } else if (doStop && !doStart) {
      actionStr = "stop";
   } else if (!doStop && doStart) {
      actionStr = "start";
   } else { // neither is set
      actionStr = "unknown";
   }
   return actionStr;
}

/** Concurrently stop a List of Vms. Wait for the operations to 
 *  complete before returning.
 *
 *  Side effects: For any VM which is performing a NICE_KILL because of this call,
 *     it is marked as such in the StopStartBB (blackboard). The blackboard will 
 *     contain a key prefixed with util.StopStartVMs.NiceKillInProgress and appended 
 *     to with its VmId. The value of this key is the true. 
 * 
 *     This blackboard entry is for the convenience of the caller, and it is up to the 
 *     caller to look for it or not. When a VM undergoes a NICE_KILL, the VM closes the 
 *     cache and disconnects from the distributed system. Any threads in that VM doing 
 *     work can get exceptions during the close and/or disconnect step. This blackboard 
 *     entry is useful for those threads to allow exceptions when they know an exception 
 *     is possible. 
 *
 *  @param targetVmList A list of hydra.ClientVmInfos to stop and/or start.
 *  @param stopModeList A parallel list of stop modes (Strings), if doStop is true.
 */
public static void stopVMs(List targetVmList, List stopModeList) {
   stopStartVMs(targetVmList, stopModeList, true, false);
}

/** Concurrently stop the given number of vms other than this vm. Wait for the 
 *  operations to complete before returning.
 *
 *  Side effects: For any VM which is performing a NICE_KILL because of this call,
 *     it is marked as such in the StopStartBB (blackboard). The blackboard will 
 *     contain a key prefixed with util.StopStartVMs.NiceKillInProgress and appended 
 *     to with its VmId. The value of this key is the true. 
 * 
 *     This blackboard entry is for the convenience of the caller, and it is up to the 
 *     caller to look for it or not. When a VM undergoes a NICE_KILL, the VM closes the 
 *     cache and disconnects from the distributed system. Any threads in that VM doing 
 *     work can get exceptions during the close and/or disconnect step. This blackboard 
 *     entry is useful for those threads to allow exceptions when they know an exception 
 *     is possible. 
 *
 *  @param numVMsToStop The number of vms other than this vm to stop.
 *
 *  @returns A List of VmInfo which are stopped vms.
 */
public static List stopVMs(int numVMsToStop) {
   Object[] anArr = StopStartVMs.getOtherVMs(numVMsToStop);
   List targetVmList = (List)(anArr[0]);
   List stopModeList = (List)(anArr[1]);
   stopStartVMs(targetVmList, stopModeList, true, false);
   return targetVmList;
}

/** Concurrently start a List of Vms. Wait for the operations to 
 *  complete before returning.
 *
 *  @param targetVmList A list of hydra.ClientVmInfos to stop and/or start.
 */
public static void startVMs(List targetVmList) {
   stopStartVMs(targetVmList, null, false, true);
}

/** Start a single vm. Wait for the operation to 
 *  complete before returning.
 *
 *  @param targetVM The VM to start. 
 */
public static void startVMs(ClientVmInfo aVM) {
   List aList = new ArrayList();
   aList.add(aVM);
   stopStartVMs(aList, null, false, true);
}

/** Concurrently stop and/or start a List of Vms. Wait for the operations to 
 *  complete before returning.
 *
 *  Side effects: For any VM which is performing a NICE_KILL because of this call,
 *     it is marked as such in the StopStartBB (blackboard). The blackboard will 
 *     contain a key prefixed with util.StopStartVMs.NiceKillInProgress and appended 
 *     to with its VmId. The value of this key is the true. 
 * 
 *     This blackboard entry is for the convenience of the caller, and it is up to the 
 *     caller to look for it or not. When a VM undergoes a NICE_KILL, the VM closes the 
 *     cache and disconnects from the distributed system. Any threads in that VM doing 
 *     work can get exceptions during the close and/or disconnect step. This blackboard 
 *     entry is useful for those threads to allow exceptions when they know an exception 
 *     is possible. 
 *
 *  @param targetVmList A list of hydra.ClientVmInfos to stop and/or start.
 *  @param stopModeList A parallel list of stop modes (Strings), if doStop is true.
 *  @param doStop True if we want to stop the targetVmList, false if not.
 *  @param doStart True if we want to start the targetVmList, false if not.
 *
 */
private static void stopStartVMs(List targetVmList, List stopModeList, boolean doStop, boolean doStart) {
   Log.getLogWriter().info("In stopStartVMs, vms to " + getAction(doStop, doStart) + ": " + targetVmList +
      ", corresponding stop modes: " + stopModeList);
   if (doStop) {

      // mark in the blackboard any vms that will undergo a NICE_KILL
      for (int i = 0; i < stopModeList.size(); i++) {
         String stopMode = (String)(stopModeList.get(i));
         if (ClientVmMgr.toStopMode(stopMode) == ClientVmMgr.NICE_KILL) { 
            // write nice kills to the blackboard so the killed vm may, if it chooses, handle any
            // exceptions that are underway.
            StopStartBB.getBB().getSharedMap().put(
               NiceKillInProgress + ((ClientVmInfo)targetVmList.get(i)).getVmid(), new Boolean(true));
         }
      }
   }

   // process the vms in targetVms list
   List threadList = new ArrayList();
   for (int i = 0; i < targetVmList.size(); i++) {
      ClientVmInfo targetVm = (ClientVmInfo)(targetVmList.get(i));
      int stopMode = 0;
      if (doStop) {
         stopMode = ClientVmMgr.toStopMode((String)(stopModeList.get(i)));
      }
      StringBuffer aStr = new StringBuffer();
      Thread aThread = new StopStartVMs(targetVm, stopMode, doStop, doStart);
      aThread = new HydraSubthread(aThread);
      threadList.add(aThread);
      aThread.start();
   }

   // wait for all threads to complete their actions
   for (int i = 0; i < threadList.size(); i++) {
      Thread aThread = (Thread)(threadList.get(i));
      try {
         aThread.join();
      } catch (InterruptedException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      }       
   }   
   String err = (String)(StopStartBB.getBB().getSharedMap().get(StopStartBB.errKey));
   if (err != null) {
      throw new TestException(err);
   }

   // reset the blackboard entries 
   if (doStop) {
      for (int i = 0; i < targetVmList.size(); i++) {
         ClientVmInfo targetVm = (ClientVmInfo)(targetVmList.get(i));
      StopStartBB.getBB().getSharedMap().put(StopStartVMs.NiceKillInProgress + targetVm.getVmid(), new Boolean(false));
      }
   }
   Log.getLogWriter().info("In stopStartVMs, done with " + getAction(doStop, doStart) + ": " + targetVmList);
}   

/** Get the client name for the given vmID, by looking in the
 *  StopStartBB blackboard shared map. The map must have been
 *  created by calling StopStart_initTask. If this method was
 *  never invoked, then null is returned as the clientName cannot
 *  be deteremined.
 *
 *  @param vmID The vmID to get the client name for.
 *  @returns The client name for vmID, or null if it cannot be
 *           determined.
 */
public static String getClientName(Integer vmID) {
   Iterator it = StopStartBB.getBB().getSharedMap().getMap().values().iterator();
   while (it.hasNext()) {
      Object anObj = it.next();
      if (anObj instanceof ClientVmInfo) {
         ClientVmInfo info = (ClientVmInfo)(anObj);
         if (info.getVmid().equals(vmID)) { // its a match
            return info.getClientName();
         }
      }
   } 
   return null;
}

/** Get the vms whose client names contain matchStr
 *
 *  @param vmInfoList A List of ClientVmInfo instances.
 *  @param matchStr The string to check for in the client names.
 *  @returns A List of ClientVmInfos whose clientName contains matchStr.
 */
public static List getMatchVMs(List vmInfoList, String matchStr) {
   List returnList = new ArrayList();
   for (int i = 0; i < vmInfoList.size(); i++) {
      Object anObj = vmInfoList.get(i);
      if (anObj instanceof ClientVmInfo) {
         ClientVmInfo info = (ClientVmInfo)(anObj);
         if (info.getClientName().indexOf(matchStr) >= 0) { // its a match
            returnList.add(info);
         }
      }
   } 
   return returnList;
}

/** Start a single vm. This returns when the start is complete. 
 * @param vm The vm to start
 */
public static void startVM(ClientVmInfo vm) {
  List aList = new ArrayList();
  aList.add(vm);
  startVMs(aList);
}

/** Stop a single vm. This returns when the stop is complete.
 * @param vm The vm to stop
 * @param stopMode The stop mode to use when stopping the vm. 
 */
public static void stopVM(ClientVmInfo vm, String stopMode) {
  List vmList = new ArrayList();
  vmList.add(vm);
  List stopModeList = new ArrayList();
  stopModeList.add(stopMode);
  stopVMs(vmList, stopModeList);
}

/** Return a List of all other vms except for one along with a parallel stop mode
 *   list, and also return the one vm not in the list.
 *   
 * @return [0] (List<ClientVmInfo>)A List of ClientVmInfo instances; this includes all 
 *                       vms except the current vm and except for one other vm.
 *                 [1] (List<String>) A parallel List to [0] containing stop modes.
 *                 [2] (ClientVMInfo) The ClientVmInfo instance of the one vm excluded from [0] (other
 *                       than this vm)
 *                 [3] (String) The stop mode for [2]
 *                 [4] (List<ClientVmInfo>) A List of ClientVmInfo instances for all vms
 *                       except this one. 
 *                 [5] (List<String>) A parallel List to [4] of stop modes.
 */
public static Object[] getOtherVMsDivided() {
  Vector<Integer> otherVmIDs = ClientVmMgr.getOtherClientVmids();
  List<ClientVmInfo> allOtherVMs = new ArrayList();
  List<String> stopModeList = new ArrayList();
  for (int i = 0; i < otherVmIDs.size(); i++) {
    ClientVmInfo info = new ClientVmInfo(otherVmIDs.get(i));
    allOtherVMs.add(info);
    stopModeList.add(TestConfig.tab().stringAt(StopStartPrms.stopModes));
  }
  List<ClientVmInfo> allOtherVMsExceptOne = allOtherVMs.subList(0, allOtherVMs.size()-1);
  List<String> stopModesExceptOne = stopModeList.subList(0, stopModeList.size()-1);
  ClientVmInfo remainingVM = allOtherVMs.get(allOtherVMs.size()-1);
  String remainingStopMode = stopModeList.get(stopModeList.size()-1);
  return new Object[] {allOtherVMsExceptOne, stopModesExceptOne, remainingVM, remainingStopMode, allOtherVMs, stopModeList};
}

/** Return a List of all other vms except for one along with a parallel stop mode
 *   list, and also return the one vm not in the list.
 *   
 * @return [0] (List<ClientVmInfo>)A List of ClientVmInfo instances; this includes all 
 *                       vms except the current vm and except for one other vm.
 *                 [1] (List<String>) A parallel List to [0] containing stop modes.
 *                 [2] (ClientVMInfo) The ClientVmInfo instance of the one vm excluded from [0] (other
 *                       than this vm)
 *                 [3] (String) The stop mode for [2]
 *                 [4] (List<ClientVmInfo>) A List of ClientVmInfo instances for all vms
 *                       except this one. 
 *                 [5] (List<String>) A parallel List to [4] of stop modes.
 */
public static Object[] getOtherVMsDivided(String[] excludedClientNames) {
  Vector<Integer> otherVmIDs = ClientVmMgr.getOtherClientVmids();
  List<ClientVmInfo> allOtherVMs = new ArrayList();
  List<String> stopModeList = new ArrayList();
  for (int i = 0; i < otherVmIDs.size(); i++) {
    ClientVmInfo info = new ClientVmInfo(otherVmIDs.get(i));
    ClientVmInfo infoFromBB = (ClientVmInfo) StopStartBB.getBB().getSharedMap().get(VmInfoKey + otherVmIDs.get(i));
    if (infoFromBB != null) {
      info = infoFromBB;
    }
    String clientName = info.getClientName();
    if (clientName == null) {
      allOtherVMs.add(info);
      stopModeList.add(TestConfig.tab().stringAt(StopStartPrms.stopModes));
    } else {
      boolean inExcludedNames = false;
      for (String excludeName: excludedClientNames) {
        if (clientName.indexOf(excludeName) >= 0)
          inExcludedNames = true;
          break;
      }
      if (!inExcludedNames) {
        allOtherVMs.add(info);
        stopModeList.add(TestConfig.tab().stringAt(StopStartPrms.stopModes));
      }
    }
  }
  List<ClientVmInfo> allOtherVMsExceptOne = allOtherVMs.subList(0, allOtherVMs.size()-1);
  List<String> stopModesExceptOne = stopModeList.subList(0, stopModeList.size()-1);
  ClientVmInfo remainingVM = allOtherVMs.get(allOtherVMs.size()-1);
  String remainingStopMode = stopModeList.get(stopModeList.size()-1);
  return new Object[] {allOtherVMsExceptOne, stopModesExceptOne, remainingVM, remainingStopMode, allOtherVMs, stopModeList};
}

/**
 *  Invokes AdminDistributedSystem.shutDownAllMembers() which disconnects
 *  all members but leaves the vms up (because hydra threads remain) including
 *  the possibility of this vm being disconnected, then this actually stops those 
 *  vms (except this vm if it was targeted in the shutDownAllMembers...this vm
 *  will remain up but disconnect). Stopped vms are stopped with ON_DEMAND
 *  restart. This returns when the vms disconnected by shutDownAllMembers()
 *  (other than this one) are all stopped .
 *
 *  @param adminDS The admin distributed system instance to use to call
 *                  shutdownAllMembers.
 *  @return An Array
 *          [0] List of {@link ClientVmInfo} instances describing the VMs that 
 *                  were stopped.
 *          [1] Set, the return from shutdownAllMembers()
 *  @throws AdminException if the shutDownAllMembers call throws this exception.
 */
public static Object[] shutDownAllMembers(AdminDistributedSystem adminDS)  {
  List stopModes = new ArrayList();
  stopModes.add(ClientVmMgr.NiceExit);
  return shutDownAllMembers(adminDS, stopModes);
}

  /**
   *  Invokes AdminDistributedSystem.shutDownAllMembers() which disconnects
   *  all members but leaves the vms up (because hydra threads remain) including
   *  the possibility of this vm being disconnected, then this actually stops those 
   *  vms (except this vm if it was targeted in the shutDownAllMembers...this vm
   *  will remain up but disconnect). Stopped vms are stopped with ON_DEMAND
   *  restart. This returns when the vms disconnected by shutDownAllMembers()
   *  (other than this one) are all stopped .
   *
   *  @param adminDS The admin distributed system instance to use to call
   *                  shutdownAllMembers.
   *  @param stopModes The stop modes to choose from.
   *  @return An Array
   *          [0] List of {@link ClientVmInfo} instances describing the VMs that 
   *                  were stopped.
   *          [1] Set, the return from shutdownAllMembers()
   *  @throws AdminException if the shutDownAllMembers call throws this exception.
   */
  public static Object[] shutDownAllMembers(
      AdminDistributedSystem adminDS, List<String> stopModes)  {
    if (adminDS == null) {
      throw new HydraRuntimeException("AdminDistributedSystem cannot be null");
    }
    
    // Invoke shutDownAllMembers
    Log.getLogWriter().info("AdminDS " + adminDS + " is shutting down all members...");
    Set<DistributedMember> memberSet;
    try {
      long startTime = System.currentTimeMillis();
      memberSet = adminDS.shutDownAllMembers();
      long duration = System.currentTimeMillis() - startTime;
      Log.getLogWriter().info("AdminDS " + adminDS + " shut down (disconnected) the following members " +
          "(vms remain up): " + memberSet + "; shutDownAll duration " + duration + "ms");
    } catch (AdminException e1) {
      throw new TestException(TestHelper.getStackTrace(e1));
    }

    // Now actually stop the vms.
    // First get the ClientVmInfos for the members that shutDownAllMembers 
    // disconnected.
    List<ClientVmInfo> allClientInfos = new ArrayList(); // all members that were shutdown
    List<ClientVmInfo> allOtherClientInfos = new ArrayList(); // all members that were shutdown except this member
    ClientVmInfo thisClientInfo = null; // this member, or will remain null if this member was not shutdown
    List<String> stopModesToUse = new ArrayList();
    for (DistributedMember aMember: memberSet) {
      Integer vmId = null;
      try {
        vmId = new Integer(RemoteTestModule.Master.getVmid(aMember.getHost(), 
            aMember.getProcessId()));
      } catch (java.rmi.RemoteException e) {
        throw new HydraRuntimeException("Unable to get vmID for " + aMember + 
            ": " + e);
      }
      ClientVmInfo infoFromBB = (ClientVmInfo)StopStartBB.getBB().getSharedMap().get("StopStartVMInfo_for_vmid_" + vmId);
      String clientName = null;
      if (infoFromBB != null) {
         clientName = infoFromBB.getClientName();
      }
      ClientVmInfo info = new ClientVmInfo(vmId, clientName, null);
      allClientInfos.add(info);
      if (vmId == RemoteTestModule.getMyVmid()) { 
        // shutdownAll disconnected this vm
        thisClientInfo = info;
      } else { // aMember is not the current vm
        allOtherClientInfos.add(info);
      }
      stopModesToUse.add(stopModes.get(TestConfig.tab().getRandGen().nextInt(0, stopModes.size()-1)));
    }

    // now actually stop the vms; if this vm is included, do it last
    Object[] returnArr = new Object[2];
    if (thisClientInfo == null) { // shutDownAllMembers did not disconnect this vm
      //we can just stop all of them now and this vm lives on
      StopStartVMs.stopVMs(allClientInfos, stopModesToUse); // restart is ON_DEMAND
      returnArr[0] = allClientInfos;
    } else { // this vm was disconnected by shutDownAllMembers
      // first shutdown all other members except this one
      StopStartVMs.stopVMs(allOtherClientInfos, stopModesToUse.subList(
          0, stopModesToUse.size()));
      returnArr[0] = allOtherClientInfos;
    }
    returnArr[1] = memberSet;
    return returnArr;
  }

}
