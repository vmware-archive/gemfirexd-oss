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
package vsphere.vijava;

import hydra.ClientVmInfo;
import hydra.ClientVmMgr;
import hydra.HostHelper;
import hydra.HydraSubthread;
import hydra.HydraVector;
import hydra.Log;
import hydra.RemoteTestModule;
import hydra.TestConfig;
import hydra.blackboard.SharedMap;

import java.net.URL;
import java.rmi.RemoteException;
import java.util.Map;
import java.util.Set;

import com.vmware.vim25.HostVMotionCompatibility;
import com.vmware.vim25.TaskInfo;
import com.vmware.vim25.VirtualMachineMovePriority;
import com.vmware.vim25.VirtualMachinePowerState;
import com.vmware.vim25.mo.ComputeResource;
import com.vmware.vim25.mo.Folder;
import com.vmware.vim25.mo.HostSystem;
import com.vmware.vim25.mo.InventoryNavigator;
import com.vmware.vim25.mo.ServiceInstance;
import com.vmware.vim25.mo.Task;
import com.vmware.vim25.mo.VirtualMachine;

import splitBrain.SBUtil;
import util.TestException;

public class VIJavaUtil {

  private static String url = "";

  private static String username = "";

  private static String password = "";

  private static String[] vmNames = new String[]{};

  private static String[] hostNames = new String[]{};

  private static String lastTargetHost = "";

  /**
   * In seconds.
   */
  private static int vMotionInterval = 60*7;

  private static int delayVMotionStartTime;

  /**
   * A true value indicates that a randomly selected VM will be restarted at the
   * same time as that of commencement of vMotion of (another) VM.
   */
  public static boolean STOP_START_SERVER = false;

  public static final String VMOTION_FAILOVER = "vMotionfailover";

  /**
   * for this thread, the vm to stop and/or start
   */
  private static ClientVmInfo targetVm;

  public static final String VMOTION_TRIGGERRED = "vMotionTriggerred";

  public static final String LAST_MIGRATION_TIME = "LAST_MIGRATION_TIME";

  public static final String LAST_MIGRATED_VM = "LAST_MIGRATED_VM";

  public static final String LAST_TARGET_HOST = "LAST_TARGET_HOST";

  public static final String VMOTION_COUNTER = "VMOTION_COUNTER";

  public static final String RESTORE_CONNECTION = "RESTORE_CONNECTION";

  public static final String VMOTION_DURING_STOP_START = "VMOTION_DURING_STOP_START";

  public static final String VMOTION_DURING_LOCATOR_STOP_START = "VMOTION_DURING_LOCATOR_STOP_START";

  public static final String VM_NAME = "VM_NAME_";

  public static final String PRE_VMOTION_COUNTER = "PRE_VMOTION_COUNTER";


  public static void HydraTask_migrateVM() throws Exception {
    delayVMotionStartTime = TestConfig.tab().intAt(
        VIJavaPrms.delayVMotionStartTime, 10);
    Log.getLogWriter().info(
        "delayVMotionStartTime is " + delayVMotionStartTime + " seconds.");
    SharedMap sm = VMotionBB.getBB().getSharedMap();
    Boolean value = (Boolean)sm.get(VMOTION_TRIGGERRED);
    if (value == null || !value) {
      sm.put(VMOTION_TRIGGERRED, Boolean.TRUE);
      Thread.sleep(delayVMotionStartTime * 1000);
    }
    VIJavaUtil.doMigrateVM();
  }

  public static void HydraTask_migrateVMToTargetHost() throws Exception {
    initializeParams();
    doMigrateVM(vmNames[0], lastTargetHost);
  }

  public static void HydraCloseTask_verifyVMotion() throws Exception {
    Integer count = (Integer)VMotionBB.getBB().getSharedMap().get(VMOTION_COUNTER);
    if (count == null || count < 0) {
      throw new TestException("No successful vMotion of any vm was observed during the test.");
    } else {
      Log.getLogWriter().info(count + " successful vMotion(s) observed during the test.");
    }
  }

  public static void migrateVMToTargetHost(String vmName) throws Exception {
    initializeParams();
    doMigrateVM(vmName, lastTargetHost);
  }

  public static synchronized boolean doMigrateVM() throws Exception {

    initializeParams();

    if (!validateParams()) {
      return false;
    }

    // lock
    // Check migration interval ... ? (parallel migrations can happen)
    // Get available hosts and VMs.
    // Pick up a VM on a round robin basis.
    // Pick up a host on a round robin basis.
    // Check and discard last migrated vm.
    // Check and discard last target host.
    // Check if vm is powered on. (?)
    // Do vMotion
    // Update black board
    // unlock

//    long lastTime = 0;
//    Object time = VMotionBB.getBB().getSharedMap().get(LAST_MIGRATION_TIME);
//    lastTime = (time == null) ? 0 : (Long)time;
    SharedMap sm = VMotionBB.getBB().getSharedMap();
    String lastMigratedVM = (String)sm.get(LAST_MIGRATED_VM);
    String lastTargetHost = (String)sm.get(LAST_TARGET_HOST);

    if (vmNames.length == 1 && hostNames.length == 1) {
      return migrateVM(vmNames[0], hostNames[0], false, false);
    } else if (vmNames.length == 1) {
      /*long elapsedTime = System.currentTimeMillis() - lastTime;
      if ((elapsedTime / 1000) < vMotionInterval) {
        Log.getLogWriter().info(
            "Elapsed time " + elapsedTime + " is less than vMotion interval "
                + vMotionInterval + ". Sleeping for remaining time before triggering vMotion.");
        try {
          long i = (vMotionInterval * 1000) - elapsedTime;
          for (; i >= 0; i -= 100) {
            Thread.sleep(100);
          }
        } catch (Exception e) {
          Log.getLogWriter().info("Exception while sleeping", e);
          return false;
        }
      }*/
      return migrateVM(vmNames[0], selectHostName(hostNames, lastTargetHost),
          false, true);
    } else if (hostNames.length == 1) {
      return migrateVM(selectVMName(vmNames, lastMigratedVM), hostNames[0],
          true, false);
    } else {
      return migrateVM(selectVMName(vmNames, lastMigratedVM),
          selectHostName(hostNames, lastTargetHost), true, true);
    }
  }

  private static boolean validateParams() {
    if (url.equals("") || username.equals("") || password.equals("")
        || vmNames.length == 0 || hostNames.length == 0) {
      StringBuffer sb = new StringBuffer("");
      sb.append((url.equals("") ? "url, " : ""))
          .append(username.equals("") ? "username, " : "")
          .append(password.equals("") ? "password, " : "")
          .append(vmNames.length == 0 ? "vm name, " : "")
          .append(hostNames.length == 0 ? "target host name, " : "");
      Log.getLogWriter().info(sb + " not provided.");
      return false;
    }
    return true;
  }

  private static String selectHostName(String[] hostNames2, String previousHost) {
    if (previousHost == null || "".equals(previousHost)) {
      return hostNames2[0];
    }
    for (int i = 0; i < hostNames2.length; i++) {
      if (previousHost.equals(hostNames2[i])) {
        return (i == hostNames2.length - 1) ? hostNames2[0] : hostNames2[i + 1];
      }
    }
    return hostNames2[0];
  }

  private static String selectVMName(String[] vmNames2, String previousVM) {
    if (previousVM == null || "".equals(previousVM)) {
      return vmNames2[0];
    }
    for (int i = 0; i < vmNames2.length; i++) {
      if (previousVM.equals(vmNames2[i])) {
        return (i == vmNames2.length - 1) ? vmNames2[0] : vmNames2[i + 1];
      }
    }
    return vmNames2[0];
  }

  private static boolean migrateVM(String targetVMName, String newHostName,
      boolean tryAnotherVM, boolean tryAnotherHost) throws Exception {
    ServiceInstance si = new ServiceInstance(new URL(url), username, password,
        true);

    try {
      Folder rootFolder = si.getRootFolder();
      HostSystem newHost = (HostSystem)new InventoryNavigator(rootFolder)
          .searchManagedEntity("HostSystem", newHostName);
      
      return migrateVM(si, rootFolder, newHost, targetVMName, newHostName,
          tryAnotherVM, tryAnotherHost);
    } finally {
      si.getServerConnection().logout();
    }
  }

  private static boolean migrateVM(ServiceInstance si, Folder rootFolder,
      HostSystem newHost, String targetVMName, String newHostName,
      boolean tryAnotherVM, boolean tryAnotherHost) throws Exception {

    if (!validateVMNotOnHost(si, rootFolder, newHost, targetVMName, newHostName)) {
      if (!(tryAnotherVM || tryAnotherHost)) {
        Log.getLogWriter().info(
            "vMotion not possible with the available host and [vm]: "
                + newHostName + " [" + targetVMName + "]");
        return false;
      } else {
        if (tryAnotherVM) {
          targetVMName = findVMNotOnHost(newHost);
          if (targetVMName != null) {
            return migrateVM(si, rootFolder, newHost, targetVMName, newHostName);
          }
        }
        if (tryAnotherHost) {
          newHostName = selectHostName(hostNames, newHostName);
          newHost = (HostSystem)new InventoryNavigator(rootFolder)
              .searchManagedEntity("HostSystem", newHostName);
          return migrateVM(si, rootFolder, newHost, targetVMName, newHostName);
        }
        Log.getLogWriter().info(
            "Could not find valid host[vm] pair for vMotion.");
        return false;
      }
    }

    return migrateVM(si, rootFolder, newHost, targetVMName, newHostName);
  }

  private static boolean validateVMNotOnHost(ServiceInstance si,
      Folder rootFolder, HostSystem newHost, String vmName, String hostName)
      throws Exception {
    VirtualMachine[] vms = newHost.getVms();
    for (VirtualMachine vmac : vms) {
      if (vmac.getName().equals(vmName)) {
        Log.getLogWriter().info(
            vmName + " is already running on target host " + hostName
                + ". Selecting another pair...");
        return false;
      }
    }
    return true;
  }
  
  private static String findVMNotOnHost(HostSystem host) throws RemoteException {
    VirtualMachine[] hostVMs = host.getVms();
    boolean foundMatch = false;
    String targetVM = null;
    for (String vmName : vmNames) {
      for (VirtualMachine vm : hostVMs) {
        if (vmName.equals(vm.getName())) {
          foundMatch = true;
          break;
        }
      }
      if (!foundMatch) {
        targetVM = vmName;
        break;
      }
      foundMatch = false;
    }
    return targetVM;
  }

  private static boolean migrateVM(ServiceInstance si, Folder rootFolder,
      HostSystem newHost, String targetVMName, String newHostName)
      throws Exception {

    Log.getLogWriter().info("Selected host [vm] for vMotion: " + newHostName + " [" + targetVMName + "]");
    VirtualMachine vm = (VirtualMachine)new InventoryNavigator(rootFolder)
        .searchManagedEntity("VirtualMachine", targetVMName);
    if (vm == null) {
      throw new TestException("Could not find vm " + vm + " for vMotion.");
    }
    ComputeResource cr = (ComputeResource)newHost.getParent();

    String[] checks = new String[] { "cpu", "software" };
    HostVMotionCompatibility[] vmcs = si.queryVMotionCompatibility(vm,
        new HostSystem[] { newHost }, checks);

    String[] comps = vmcs[0].getCompatibility();
    if (checks.length != comps.length) {
      Log.getLogWriter().info("CPU/software NOT compatible, vMotion failed.");
      return false;
    }

    long start = System.currentTimeMillis();
    stopStartVM();
    restoreConnection();
    Task task = vm.migrateVM_Task(cr.getResourcePool(), newHost,
        VirtualMachineMovePriority.highPriority,
        VirtualMachinePowerState.poweredOn);
    VMotionBB.getBB().getSharedMap().remove(VMOTION_FAILOVER);
    if (task.waitForMe() == Task.SUCCESS) {
      long end = System.currentTimeMillis();
      SharedMap sm = VMotionBB.getBB().getSharedMap();
      VMotionBB.getBB().getSharedLock().lock();
      sm.put(LAST_MIGRATION_TIME, end);
      sm.put(LAST_MIGRATED_VM, targetVMName);
      sm.put(LAST_TARGET_HOST, newHostName);
      Integer count = (Integer)sm.get(VMOTION_COUNTER);
      count = (count == null) ? 1 : count+1;
      sm.put(VMOTION_COUNTER, count);
      VMotionBB.getBB().getSharedLock().unlock();

      Log.getLogWriter().info(
          "vMotion of " + targetVMName + " to " + newHostName + " completed in "
              + (end - start) + "ms. Task result: "
              + task.getTaskInfo().getResult());
      return true;
    } else {
      TaskInfo info = task.getTaskInfo();
      Log.getLogWriter().warning(
          "vMotion of " + targetVMName + " to " + newHostName
              + " failed. Error details: " + info.getError().getFault());
      return false;
    }
  }

  private static void stopStartVM() throws Exception {
    if (STOP_START_SERVER) {
      SharedMap sm = VMotionBB.getBB().getSharedMap();
      Boolean serialTest = TestConfig.tab().booleanAt(
          hydra.Prms.serialExecution, true);
      if (serialTest) {
        Log.getLogWriter().info("Serial execution mode.");
        stopStartVMAsync();
      } else {
        // This is work in progress.
        Log.getLogWriter().info("Concurrent execution mode.");
        String value = (String)sm.get(VMOTION_FAILOVER);
        int myVMid = RemoteTestModule.getMyVmid();
        String currentVMName = new ClientVmInfo(new Integer(myVMid),
            RemoteTestModule.getMyClientName(), null).getClientName();
        if (value == null) {
          sm.put(VMOTION_FAILOVER, currentVMName);
        }
        value = (String)sm.get(VMOTION_FAILOVER);
        long startTime = System.currentTimeMillis();
        while (!value.equalsIgnoreCase("RESUME")
            && ((System.currentTimeMillis() - startTime) < 400000)) {
          Thread.sleep(100);
          value = (String)sm.get(VMOTION_FAILOVER);
        }
      }
    }
  }

  private static void restoreConnection() {
    Boolean restoreCon = (Boolean)VMotionBB.getBB().getSharedMap().get("RESTORE_CONNECTION");
    if (restoreCon != null && restoreCon) {
      Thread task = new Thread(new Runnable() {
        public void run() {
          try {
            Thread.sleep(90 * 1000);
            // vMotion usually takes around 120 sec to complete for VMs (we
            // normally use) with around 32GB of RAM. Selected 90 sec here so
            // that connection is restored towards the end of vmotion.
            // TODO (ashetkar) Make this period configurable.
            SBUtil.restoreConnection();
          } catch (Exception e) {
            Log.getLogWriter().info("Failure while restoring connection in a thread", e);
          }
        }
      });

      task.start();
    }
  }

  private static void stopStartVMAsync() {
    // new thread
    // sleep
    // stop vm
    // start vm
    // thread ends
    // start the thread, return true;
    // return false, otherwise

    Thread task = new HydraSubthread(new Runnable() {
      public void run() {
        // 1. Get the list of hydra vm names hosting cache servers
        // 2. Get the current vm name
        // 3. Iterate through the list of vm names obtained in step 1.
        // 4. If a vm name in the list is not the current vm, select it as
        //    target vm to be stopped and restarted.
        // 5. Sleep for 30 seconds before killing the target vm
        // 6. Restart the target vm.
        // 7. Reset all the flags.
        try {
          String vmName = null;
          String stopMode = TestConfig.tab().stringAt(VIJavaPrms.stopMode);
          Map aMap = VMotionBB.getBB().getSharedMap().getMap();
          Set<Map.Entry> aList = aMap.entrySet();
          int myVMid = RemoteTestModule.getMyVmid();
          String currentVMName = new ClientVmInfo(new Integer(myVMid),
              RemoteTestModule.getCurrentThread().getMyClientName(), null)
              .getClientName();
          for (Map.Entry ele : aList) {
            if (ele.getValue() instanceof ClientVmInfo) {
              ClientVmInfo clientVmInfo = (ClientVmInfo)ele.getValue();
              vmName = clientVmInfo.getClientName();
              if (!vmName.equalsIgnoreCase(currentVMName)) {
                targetVm = clientVmInfo;
                Log.getLogWriter().info("Target VM to kill is :: " + targetVm);
                break;
              }
            }
          }

          Thread.sleep(30000);
          Log.getLogWriter().info(
              "About to stop the target VM: " + targetVm.getClientName());
          ClientVmMgr.stop("Test is synchronously stopping " + vmName
              + " with " + ClientVmMgr.toStopMode(stopMode),
              ClientVmMgr.toStopMode(stopMode), ClientVmMgr.ON_DEMAND, targetVm);
          Log.getLogWriter().info("Sever has been stopped successfully.");
          ClientVmMgr.start("Test is synchronously starting " + targetVm,
              targetVm);
        }
        catch (hydra.ClientVmNotFoundException e) {
          Log.getLogWriter().info("Exception in StopStart of a server during vMotion", e);
        }
        catch (InterruptedException ie) {
          Log.getLogWriter().info("Exception in StopStart of a server during vMotion", ie);
        }
        catch (Throwable e) {
          Log.getLogWriter().info("Exception in StopStart of a server during vMotion", e);
        }
        finally {
          STOP_START_SERVER = false;
        }
      }
    });
    task.start();
  }

  private static synchronized boolean doMigrateVM(String targetVMName,
      String newHostName) throws Exception {
    ServiceInstance si = new ServiceInstance(new URL(url), username, password,
        true);
    try {
      Folder rootFolder = si.getRootFolder();
      InventoryNavigator in = new InventoryNavigator(rootFolder);
      HostSystem newHost = (HostSystem)in.searchManagedEntity("HostSystem",
          newHostName);
      if (newHost == null) {
        throw new TestException("Could not find host " + newHostName + " as a target host for vMotion.");
      }

      return migrateVM(si, rootFolder, newHost, targetVMName, newHostName);

    } finally {
      si.getServerConnection().logout();
    }
  }

  public static void HydraTask_migrateNetDownVM() throws Exception {
    SharedMap sMap = VMotionBB.getBB().getSharedMap();

    Boolean bool = (Boolean)sMap.get("connectionDropped");
    if (bool == null || !bool) {
      return;
    }

    initializeParams();
    if (!validateParams()) {
      return;
    }

    ServiceInstance si = new ServiceInstance(new URL(url), username, password,
        true);

    try {
      Folder rootFolder = si.getRootFolder();
      HostSystem newHost = (HostSystem)new InventoryNavigator(rootFolder)
          .searchManagedEntity("HostSystem", hostNames[0]);
      migrateVM(si, rootFolder, newHost, vmNames[0], hostNames[0], false, false);
    } finally {
      si.getServerConnection().logout();
    }
  }


  private static void initializeParams() {
    url = TestConfig.tab().stringAt(VIJavaPrms.wsUrl, "");
    username = TestConfig.tab().stringAt(VIJavaPrms.username, "");
    password = TestConfig.tab().stringAt(VIJavaPrms.password, "");
    vMotionInterval = TestConfig.tab().intAt(VIJavaPrms.vMotionIntervalSec, 60*7);
    lastTargetHost = TestConfig.tab().stringAt(VIJavaPrms.targetHost, "");

    HydraVector hvVms = TestConfig.tab().vecAt(VIJavaPrms.vmNames, new HydraVector());
    vmNames = new String[hvVms.size()];
    StringBuffer sb1 = new StringBuffer("");
    for (int i = 0; i < hvVms.size(); i++) {
      vmNames[i] = (String)hvVms.get(i);
      sb1.append(vmNames[i] + " ");
    }
    Log.getLogWriter().info("VM list: " + sb1);

    HydraVector hvHosts = TestConfig.tab().vecAt(VIJavaPrms.hostNames, new HydraVector());
    hostNames = new String[hvHosts.size()];
    StringBuffer sb2 = new StringBuffer("");
    for (int i = 0; i < hvHosts.size(); i++) {
      hostNames[i] = (String)hvHosts.get(i);
      sb2.append(hostNames[i] + " ");
    }
    Log.getLogWriter().info("Host list: " + sb2);
  }

  public static void updateVMName() {
    VMotionBB.getBB().getSharedMap().put(VM_NAME + RemoteTestModule.getMyVmid(), HostHelper.getLocalHost());
  }

  public static void setRestoreConnection() {
    VMotionBB.getBB().getSharedMap().put(RESTORE_CONNECTION, Boolean.TRUE);
  }

  public static void resetRestoreConnection() {
    VMotionBB.getBB().getSharedMap().put(RESTORE_CONNECTION, Boolean.FALSE);
  }

  public static void enableVMotionDuringStopStart() {
    VMotionBB.getBB().getSharedMap().put(VMOTION_DURING_STOP_START, Boolean.TRUE);
  }

  public static void disableVMotionDuringStopStart() {
    VMotionBB.getBB().getSharedMap().put(VMOTION_DURING_STOP_START, Boolean.FALSE);
  }

  public static void enableVMotionDuringLocatorStopStart() {
    VMotionBB.getBB().getSharedMap().put(VMOTION_DURING_LOCATOR_STOP_START, Boolean.TRUE);
  }

  public static void disableVMotionDuringLocatorStopStart() {
    VMotionBB.getBB().getSharedMap().put(VMOTION_DURING_LOCATOR_STOP_START, Boolean.FALSE);
  }

  /**
   * This is being called from StopStartVMs.run() between stop and start of a
   * VM.
   * 
   * @param vmId
   *          The id of hydra-vm being restarted.
   */
  public static void doVMotion(ClientVmInfo targetVmToMove) {
    try {
      try {
        VMotionBB.getBB().getSharedLock().lock();
        Integer count = (Integer)VMotionBB.getBB().getSharedMap()
            .get(PRE_VMOTION_COUNTER);
        if (count != null && count == 1) {
          // Allow just one vmotion for this test.
          return;
        }
        count = (count == null) ? 1 : ++count;
        VMotionBB.getBB().getSharedMap().put(PRE_VMOTION_COUNTER, count);
      } finally {
        VMotionBB.getBB().getSharedLock().unlock();
      }

      initializeParams();
      String targetVMName = (String)VMotionBB.getBB().getSharedMap()
          .get(VM_NAME + targetVmToMove.getVmid());
      migrateVM(targetVMName, hostNames[0], false, true);
    } catch (Exception e) {
      Log.getLogWriter().warning("Could not trigger vMotion.", e);
    }
  }
}
