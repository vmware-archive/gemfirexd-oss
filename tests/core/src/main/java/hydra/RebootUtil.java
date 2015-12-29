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
package hydra;

import hydra.HostHelper.OSType;
import java.rmi.RemoteException;

/**
 * This class is used by the hydra master to reboot hosts. For the public API,
 * see {@link RebootMgr}.
 */
public class RebootUtil {

  /**
   * Reboots the specified virtual machine.
   */
  public static void reboot(String vm) {
    String host = RebootPrms.getBaseHost(vm);
    String vmconfig = RebootPrms.getConfigurationFileName(vm);
    String type = RebootPrms.useHardStop() ? "hard" : "soft";
    String header = Platform.SSH + " " + host + " "
                  + ((UnixPlatform)Platform.getInstance()).sudo() + " ";

    Log.getLogWriter().info("Rebooting virtual machine " + vm + " (" + type
       + ") on " + host + " using " + vmconfig);

    //-------------------- CHECK --------------------
    Log.getLogWriter().info("Checking for virtual machine " + vm);
    if (!(isListed(vm) && responds(vm))) {
      String s = "The virtual machine " + vm + " is not running on " + host;
      throw new DynamicActionException(s);
    }

    //-------------------- STOP --------------------
    Log.getLogWriter().info("Stopping virtual machine " + vm);
    String stopcmd = header + "vmrun -T ws stop " + vmconfig + " " + type;
    String stopresult = ProcessMgr.fgexec(stopcmd, 120);
    Log.getLogWriter().info("Stopped virtual machine " + vm + " on host: "
                           + host + "\n" + stopresult);

    //-------------------- WAIT --------------------
    Log.getLogWriter().info("Waiting for virtual machine " + vm + " to stop");
    long stoplimit = System.currentTimeMillis() + 120*1000; // two minutes
    while (isListed(vm)) {
      if (System.currentTimeMillis() > stoplimit) {
        String s = "Timed out waiting for virtual machine to stop: " + vm;
        throw new HydraTimeoutException(s);
      }
      MasterController.sleepForMs(500);
    }

    //-------------------- START --------------------
    Log.getLogWriter().info("Starting virtual machine " + vm);
    String startcmd = header + "vmrun -T ws start " + vmconfig + " nogui";
    String startresult = ProcessMgr.fgexec(startcmd, 120);
    Log.getLogWriter().info("Started virtual machine " + vm + " on host  "
                           + host + "\n" + startresult);

    //-------------------- WAIT --------------------
    Log.getLogWriter().info("Waiting for virtual machine " + vm + " to start");
    long startlimit = System.currentTimeMillis() + 120*1000; // two minutes
    boolean exists = isListed(vm);
    while (!isListed(vm)) {
      if (System.currentTimeMillis() > startlimit) {
        String s = "Timed out waiting for virtual machine to start: " + vm;
        throw new HydraTimeoutException(s);
      }
      MasterController.sleepForMs(500);
    }
    while (!responds(vm)) {
      if (System.currentTimeMillis() > startlimit) {
        String s = "Timed out waiting for virtual machine to respond to ssh: "
                 + vm;
        throw new HydraTimeoutException(s);
      }
      MasterController.sleepForMs(500);
    }

    Log.getLogWriter().info("Rebooted virtual machine " + vm);
  }

  /**
   * Answers whether the virtual machine exists and is responding to ssh.
   */
  protected static boolean isAlive(String vm) {
    return isListed(vm) && responds(vm);
  }

  /**
   * Answers whether the virtual machine exists. Status output looks like:
   *   Total running VMs: 0
   * or
   *   Total running VMs: 1
   *   /w2-gst-dev41a/vms/w2-gst-cert-22/w2-gst-cert-22.vmx
   */
  private static boolean isListed(String vm) {
    String host = RebootPrms.getBaseHost(vm);
    String vmconfig = RebootPrms.getConfigurationFileName(vm);
    String header = Platform.SSH + " " + host + " "
                  + ((UnixPlatform)Platform.getInstance()).sudo() + " ";
    String cmd = header + "vmrun -T ws list";
    String result = ProcessMgr.fgexec(cmd, 60);
    Log.getLogWriter().info("Executed " + cmd + "\n" + result);
    return result.contains(vmconfig);
  }

  /**
   * Answers whether the host responds to ssh.
   */
  private static boolean responds(String host) {
    HostDescription hd = TestConfig.getInstance()
                                   .getAnyPhysicalHostDescription(host);
    if (hd.getOSType() == OSType.windows) {
      try {
        String cmd = "echo alive";
        String url = "rmi://" + hd.getHostName()
                   + ":" + hd.getBootstrapPort()
                   + "/" + Bootstrapper.RMI_NAME;
        Log.getLogWriter().info("Looking up bootstrapper at " + url);
        BootstrapperProxyIF bootstrapper =
            (BootstrapperProxyIF)RmiRegistryHelper.lookup(url);
        if (bootstrapper == null) {
          return false; // let it go, we'll time out eventually
        }
        Log.getLogWriter().info("Using bootstrapper to execute " + cmd);
        String result = bootstrapper.fgexec(cmd, 60);
        Log.getLogWriter().info("Executed " + cmd + "\n" + result);
        return result.contains("alive");
      } catch (RemoteException e) {
        return false; // let it go, we'll time out eventually
      }
    } else {
      String cmd = Platform.SSH + " " + host + " echo alive";
      try {
        String result = ProcessMgr.fgexec(cmd, 60);
        Log.getLogWriter().info("Executed " + cmd + "\n" + result);
        return result.contains("alive");
      } catch (HydraRuntimeException e) {
        return false; // let it go, we'll time out eventually
      }
    }
  }
}
