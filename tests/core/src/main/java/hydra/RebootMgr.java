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

import com.gemstone.gemfire.LogWriter;
import java.rmi.RemoteException;
import java.util.*;

/**
 * <p>
 * This class provides a public API that hydra clients can use to dynamically
 * reboot hosts during a test run. The API can only be used for hosts that are
 * virtual machines, and only virtual machines running on Unix platforms.
 * Using reboot on a Window hosts requires the {@link Bootstrapper} to be
 * installed as a Windows service with the proper user name, using the build
 * under test.
 * <p>
 * The API is also restricted to hydra tasks of type "TASK". Only hosts used by
 * hydra client JVMs and/or hydra-managed Hadoop processes are available for
 * reboot. The source of a reboot, and other test processes, can be running on
 * the same host as the virtual machine being rebooted and are unaffected by
 * the reboot.
 * <p>
 * A host is not available to be rebooted if:
 * <ul>
 *   <li>the host is not a virtual machine running on a Unix platform
 *   <li>the hydra master is running on the host
 *   <li>the source of the reboot is running on the host (a client cannot
 *       reboot its own host)</li>
 *   <li>the host is already the target of a reboot</li>
 *   <li>any client on the host is the source or target of a dynamic action
 *       via the {@link ClientVmMgr} API</li>
 *   <li>the RebootInfo for the host from a previous reboot has not been
 *       cleared using {@link #clearInfo}</li>
 * </ul>
 * A {@link RebootHostNotFoundException} is thrown if a host is not available
 * to be used as a target.
 * <p>
 * Methods are available in synchronous versions only. They do not return until
 * the host has rebooted and the hydra hostagent has been restarted on the host.
 * <p>
 * The reboot API returns the information needed to restart hydra client and
 * Hadoop processes on a host after it is rebooted. This information is
 * returned in a {@link RebootInfo} object, and is also available to any hydra
 * client after a reboot through the {@link #getInfo} method.
 * <p>
 * The test must ensure that no Hadoop processes are being stopped or started
 * during a reboot. Hydra does not enforce this, but assumes that all processes
 * in a Hadoop cluster have been started at least once prior to any reboot.
 * The test must also take care not to invoke any other methods that require
 * the hostagent on the host being rebooted.
 * <p>
 * The reboot method can optionally return only processes that were live at the
 * time of the reboot or include those that were previously stopped using the
 * {@link ClientVmMgr} and/or {@link HadoopHelper} APIs.
 * <p>
 * It is the responsibility of the test to use the {@link ClientVmMgr} and
 * {@link HadoopHelper} APIs to restart the desired processes after reboot
 * using the information contained in the RebootInfo object. The test must then
 * use {@link #clearInfo} to make the host available for another reboot.
 * <p>
 * The RebootInfo does not include processes started by hydra clients through
 * {@link ProcessMgr#bgexec} or {@link ProcessMgr#fgexec} methods, {@link Java
 * #java}, {@link HadoopHelper#runHadoopCommand}, or {@link CacheServerHelper}.
 * If processes started through these methods are running at the time of a
 * reboot, it is up to the test to manage the outcome, such as updating
 * nukerun scripts, and to optionally restart the processes using information
 * the test has saved.
 * <p>
 * Reboot is equivalent to pulling the plug on a host. As a result, the log
 * files for processes running on the host will not show messages that were
 * buffered at the time of the reboot. The product logger does a flush on each
 * write, but messages are still logged by the operating system, disk subsystem,
 * and physical disk. NFS also buffers, so message loss can be reduced by
 * running processes with user directories local to the host being rebooted.
 * <p>
 * Dynamic actions are carried out in the master, using threads named for the
 * source client and the action, e.g., <code>Dynamic Rebooter</code>. To see
 * the steps in a reboot, look for messages from this thread in the hydra
 * taskmaster log.
 */
public class RebootMgr {

  //////////////////////   Reboot Methods   //////////////////////

  /**
   * Reboots the given host. This method is synchronous and returns when the
   * target host has finished rebooting.
   *
   * @param reason The reason for rebooting the host.
   * @param target The name of the host.
   * @param liveOnly Whether to return only the processes on the host
   *                 that were live at the time of the reboot. When false,
   *                 processes that were stopped prior to the reboot using
   *                 other APIs are also returned.
   * @return A {@link RebootInfo} describing processes on the target host.
   * @throws RebootHostNotFoundException if the host is unavailable as a target.
   * @throws IllegalArgumentException if the host is not used in the test.
   * @throws DynamicActionException if the test is terminating abnormally and
   *         refuses to allow any further dynamic actions.
   */
  public static RebootInfo reboot(String reason, String target,
                boolean liveOnly) throws RebootHostNotFoundException {
    validate();
    return reserveAndReboot(reason, target, liveOnly);
  }

  /**
   * Returns the {@link RebootInfo} for a rebooted host. This method can be
   * used after {@link #reboot} to get the information needed to restart
   * processes. When the info is no longer needed, use {@link #clearRebootInfo}
   * to make the host available for reboot.
   */
  public static RebootInfo getInfo(String target) {
    return (RebootInfo)RebootBlackboard.getInstance()
                                       .getSharedMap().get(target);
  }

  /**
   * Clears the {@RebootInfo} for a rebooted host when it is no longer needed.
   */
  public static void clearInfo(String target) {
    RebootBlackboard.getInstance().getSharedMap().remove(target);
    log().info("Removed " + target + " from reboot blackboard");
  }

  ////////////////////   Generic Methods   /////////////////////

  /**
   * Reserves the target host and reboots it.
   */
  private static RebootInfo reserveAndReboot(final String reason,
                                             final String target,
                                             final boolean liveOnly)
  throws RebootHostNotFoundException {
    log().info("Reserving " + target + " for dynamic reboot with liveOnly "
              + liveOnly);
    // make sure the target is being used
    Vector hosts = TestConfig.getInstance().getPhysicalHostsIncludingHadoop();
    List hostNames = new ArrayList(hosts);
    if (!hostNames.contains(target)) {
      String s = "Host " + target +
               " is not in the list of hosts used in this test:" + hostNames;
      throw new IllegalArgumentException(s);
    }

    final String srcName = Thread.currentThread().getName();
    final int srcVmid = RemoteTestModule.MyVmid;
    final RebootInfo info = reserveToRebootRemote(srcName, srcVmid, target,
                                                  liveOnly);
    String act = "dynamic reboot " + target + "(liveOnly " + liveOnly
               +") because " + reason;
    log().info("Reserved " + target + " for " + act);
    final int actionId = DynamicActionUtil.nextActionId();
    Runnable action = new Runnable() {
      public void run() {
        rebootRemote(srcName, srcVmid, actionId, reason, info);
      }
    };
    DynamicActionUtil.runActionThread(actionId, act, action, srcName, true);

    return info;
  }

  /////////////////////   Remote Methods   /////////////////////

  /**
   * Reserves the host for future reboot.
   */
  private static RebootInfo reserveToRebootRemote(
    String srcName, int srcVmid, String target, boolean liveOnly)
  throws RebootHostNotFoundException {
    RebootInfo info = null;
    try {
      info = RemoteTestModule.Master.reserveForReboot(srcName, srcVmid,
                                                      target, liveOnly);
    } catch (RemoteException e) {
      throw new HydraRuntimeException("Problem with remote operation", e);
    }
    return info;
  }

  /**
   * Reboots a host previously reserved.
   */
  protected static void rebootRemote(String srcName, int srcVmid, int actionId,
                        String reason, RebootInfo target) {
    try {
      RemoteTestModule.Master.reboot
            (
              srcName, srcVmid, actionId, reason, target
            );
    } catch (RemoteException e) {
      throw new HydraRuntimeException("Problem with remote operation", e);
    }
  }

  ////////////////////   Utility Methods   /////////////////////

  /**
   * Ensures API is used only from tasks of type TASK and validates arguments.
   */
  private static void validate() {
    if (RemoteTestModule.getCurrentThread() == null) {
      String s = "The RebootMgr API can only be used from hydra threads"
               + " -- "
               + "use HydraSubthread when creating threads that use the API";
      throw new UnsupportedOperationException(s);
    }
    TestTask task = RemoteTestModule.getCurrentThread().getCurrentTask();
    if (task.getTaskType() != TestTask.TASK) {
      String s = "The RebootMgr API can only be used from tasks of type TASK";
      throw new UnsupportedOperationException(s);
    }
  }

  private static LogWriter log() {
    return Log.getLogWriter();
  }
}
