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

import java.io.*;
import java.lang.management.ManagementFactory;
import java.rmi.RemoteException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import util.TestHelper;

/**
*
* This class provides support for managing processes.  It is used to
* execute and terminate processes, in the foreground and background,
* on local and remote hosts, and on UNIX and Windows platforms.  It is
* also used to get information about the local host, the current process,
* or a process specified by its host and PID.
*
*/
public class ProcessMgr {

  /**
   * Returns the process ID for this process.
   * <p>
   * Uses the RuntimeMXBean.  There is no guarantee that this returns a PID,
   * but it currently works on all platforms supported by hydra.
   *
   * @throws HydraRuntimeException if there is a problem getting the PID
   *                               from the RuntimeMXBean
   */
  public static int getProcessId() {
    // UNIX could alternatively fgexec "/bin/sh -c 'echo $PPID'";
    String name = ManagementFactory.getRuntimeMXBean().getName();
    try {
      return Integer.parseInt(name.substring(0, name.indexOf('@')));
    } catch (NumberFormatException e) {
      String s = "Something changed in the RuntimeMXBean name, "
               + "which we rely on to get the PID: " + name;
      throw new HydraRuntimeException(s, e);
    }
  }

  /**
  * Starts a foreground process on the local host, waits for it to complete,
  * and returns the output.
  * @param host the host on which to start the process.
  * @param command the command to run.
  * @param maxWaitSec the maximum amount of time to wait before timing out.
  * @return the output of the command, both stdout and stderr.
  * @throws HydraRuntimeException if the process cannot be created
  *                               or its exit code is non-zero.
  * @throws HydraTimeoutException if the command times out.
  */
  public static String fgexec(String command, int maxWaitSec) {
    log("Executing fgexec command: " + command);
    String output = Platform.getInstance().fgexec(command, maxWaitSec);
    log("Executed fgexec command: " + command);
    return output;
  }

  /**
  * Starts a foreground process on the local host, waits for it to complete,
  * and returns the output.
  * @param host the host on which to start the process.
  * @param command the command to run.
  * @param envp the environment for the command.
  * @param maxWaitSec the maximum amount of time to wait before timing out.
  * @return the output of the command, both stdout and stderr.
  * @throws HydraRuntimeException if the process cannot be created
  *                               or its exit code is non-zero.
  * @throws HydraTimeoutException if the command times out.
  */
  public static String fgexec(String command, String[] envp, int maxWaitSec) {
    List envps = Arrays.asList(envp);
    log("Executing fgexec command: " + command + " with envp " + envps);
    String output = Platform.getInstance().fgexec(command, envp, maxWaitSec);
    log("Executed fgexec command: " + command + " with envp " + envps);
    return output;
  }

  /**
  * Starts a foreground process on the local host, waits for it to complete,
  * and returns the output.
  * @param host the host on which to start the process.
  * @param command the command to run.
  * @param maxWaitSec the maximum amount of time to wait before timing out.
  * @return the output of the command, both stdout and stderr.
  * @throws HydraRuntimeException if the process cannot be created
  *                               or its exit code is non-zero.
  * @throws HydraTimeoutException if the command times out.
  */
  public static String fgexec(String[] command, int maxWaitSec) {
    log("Executing fgexec command: " + Platform.getInstance()
                                               .getString(command));
    String output = Platform.getInstance().fgexec(command, maxWaitSec);
    log("Executed fgexec command: " + Platform.getInstance()
                                              .getString(command));
    return output;
  }

  /**
  * Starts a foreground process on the specified host, waits for it to complete,
  * and returns the output.
  * @param host the host on which to start the process.
  * @param command the command to run.
  * @param maxWaitSec the maximum amount of time to wait before timing out.
  * @return the output of the command, both stdout and stderr.
  * @throws HydraRuntimeException if the process cannot be created
  *                               or its exit code is non-zero.
  * @throws HydraTimeoutException if the command times out.
  */
  public static String fgexec(String host, String command, int maxWaitSec) {
    if (HostHelper.isLocalHost(host)) {
      log("Issuing fgexec command on localhost " + host + ": " + command);
      String result = fgexec(command, maxWaitSec);
      log("Issued fgexec command on localhost " + host + ": " + command + " with result: " + result);
      return result;
    } else {
      log("Issuing fgexec command on remote host " + host + ": " + command);
      try {
        String result =  HostAgentMgr.getHostAgent(host).fgexec(command, maxWaitSec);
        log("Issued fgexec command on remote host " + host + ": " + command + " with result: " + result);
        return result;
      } catch (RemoteException e) {
        String s = "Unable to complete operation using hostagent on " + host
                 + ": " + command;
        throw new HydraRuntimeException(s, e);
      }
    }
  }

  /**
  * Starts a foreground process on the specified host, waits for it to complete,
  * and returns the output.
  * @param host the host on which to start the process.
  * @param command the command to run.
  * @param envp the environment for the command.
  * @param maxWaitSec the maximum amount of time to wait before timing out.
  * @return the output of the command, both stdout and stderr.
  * @throws HydraRuntimeException if the process cannot be created
  *                               or its exit code is non-zero.
  * @throws HydraTimeoutException if the command times out.
  */
  public static String fgexec(String host, String command, String[] envp, int maxWaitSec) {
    List envps = Arrays.asList(envp);
    if (HostHelper.isLocalHost(host)) {
      log("Issuing fgexec command on localhost " + host + ": " + command + " with envp " + envps);
      String result = fgexec(command, envp, maxWaitSec);
      log("Issued fgexec command on localhost " + host + ": " + command + " with env " + envps + " with result: " + result);
      return result;
    } else {
      log("Issuing fgexec command on remote host " + host + ": " + command + " with envp " + envps);
      try {
        String result =  HostAgentMgr.getHostAgent(host).fgexec(command, envp, maxWaitSec);
        log("Issued fgexec command on remote host " + host + ": " + command  + " with envp " + envps + " with result: " + result);
        return result;
      } catch (RemoteException e) {
        String s = "Unable to complete operation using hostagent on " + host
                 + ": " + command + " with envp " + envps;
        throw new HydraRuntimeException(s, e);
      }
    }
  }

 /**
  * Starts a background process on the local host and returns the PID.
  * The user directory for the process is the caller's current directory.
  * Process output is written to a file in this directory with a default
  * name of the form <code>bgexec<id>_<pid>.log</code>.
  * @param command the command to run.
  * @return the PID.
  * @throws HydraRuntimeException if the process cannot be created.
  */
  public static int bgexec( String command ) {
    return bgexec(command, null, null);
  }

  /**
  *
  * Starts a background process on the specified host and returns the PID.
  * The user directory for the process is the caller's current directory.
  * Process output is written to a file in this directory with a default
  * name of the form <code>bgexec<id>_<pid>.log</code>.
  * @param host the host on which to start the process.
  * @param command the command to run.
  * @return the PID.
  * @throws HydraRuntimeException if the process cannot be created.
  *
  */
  public static int bgexec( String host, String command ) {
    if (HostHelper.isLocalHost(host)) {
      return bgexec(command, null, null);
    } else {
      return bgexec(host, command, null, null);
    }
  }

  /**
  * Starts a background process on the local host and returns the PID.
  * The user directory for the process is set to the specified directory.
  * Process output is written to a file with the specified name in this
  * directory.
  * @param command the command to run.
  * @param workdir the user directory for the process.
  * @param logfile the name of the file containing process output.
  * @return the PID.
  * @throws HydraRuntimeException if the process cannot be created.
  */
  public static int bgexec(String command, String workdir, String logfile) {
    log("Executing bgexec command: " + command);
    int pid;
    try {
      pid = Platform.getInstance().bgexec(command,
                FileUtil.fileForName(workdir), FileUtil.fileForName(logfile));
    } catch (IOException e) {
      String s = "Unable to complete operation: " + command;
      throw new HydraRuntimeException(s, e);
    }
    log("Executed bgexec command: " + command);
    return pid;
  }

  /**
  *
  * Starts a background process on the specified host and returns the PID.
  * The user directory for the process is set to the specified directory.
  * Process output is written to a file with the specified name in this
  * directory.
  * @param host the host on which to start the process.
  * @param command the command to run.
  * @param workdir the user directory for the process.
  * @param logfile the name of the file containing process output.
  * @return the PID.
  * @throws HydraRuntimeException if the process cannot be created.
  *
  */
  public static int bgexec( String host, String command, String workdir,
                                                         String logfile ) {
    if (HostHelper.isLocalHost(host)) {
      return bgexec(command, workdir, logfile);
    } else {
      log("Issuing bgexec command on host " + host + ": " + command);
      int pid;
      try {
        pid = HostAgentMgr.getHostAgent( host ).bgexec( command,
                                          FileUtil.fileForName(workdir),
                                          FileUtil.fileForName(logfile));
      } catch( RemoteException e ) {
        String s = "Unable to complete operation using hostagent on " + host
                 + ": " + command;
        throw new HydraRuntimeException(s, e);
      }
      log("Issued bgexec command on host " + host + ": " + command);
      return pid;
    }
  }

  /**
  * Tests whether a process exists.
  * @param host the host the process is running on.
  * @param pid its PID.
  * @return <code>true</code> if the process exists.
  */
  public static boolean processExists(String host, int pid) {
    if (HostHelper.isLocalHost(host)) {
      return Platform.getInstance().exists(pid);
    } else {
      try {
        return HostAgentMgr.getHostAgent(host).exists(pid);
      } catch (RemoteException e) {
        String s = "Unable to complete operation using hostagent on " + host;
        throw new HydraRuntimeException(s, e);
      }
    }
  }

  /**
  * Tells a process to shut down.  This method does not wait for the process
  * to shut down.  If the process does not exist, it has no effect.
  * @param host the host the process is running on.
  * @param pid its PID.
  */
  public static void shutdownProcess(String host, int pid) {
    if (HostHelper.isLocalHost(host)) {
      log("Executing shutdown command on process:" + pid);
      Platform.getInstance().shutdown(pid);
      log("Executed shutdown command on process:" + pid);
    } else {
      try {
        log("Issuing shutdown command to process " + host + ":" + pid);
        HostAgentMgr.getHostAgent(host).shutdown(pid);
        log("Issued shutdown command to process " + host + ":" + pid);
      } catch (RemoteException e) {
        String s = "Unable to complete operation using hostagent on " + host;
        throw new HydraRuntimeException(s, e);
      }
    }
  }

  /**
  * Kills a process without warning.  This method does not wait for the process
  * to die.  If the process does not exist, it has no effect.
  * @param host the host the process is running on.
  * @param pid its PID.
  */
  public static void killProcess(String host, int pid) {
    if (HostHelper.isLocalHost(host)) {
      log("Executing kill command on process:" + pid);
      Platform.getInstance().kill(pid);
      log("Executed kill command on process:" + pid);
    } else {
      try {
        log("Issuing kill command to process " + host + ":" + pid);
        HostAgentMgr.getHostAgent(host).kill(pid);
        log("Issued kill command to process " + host + ":" + pid);
      } catch (RemoteException e) {
        String s = "Unable to complete operation using hostagent on " + host;
        throw new HydraRuntimeException(s, e);
      }
    }
  }

  /**
  * Does a thread stack dump on a process.  If the process does not exist,
  * it has no effect.
  * @param host the host the process is running on.
  * @param pid its PID.
  */
  public static void printProcessStacks(String host, int pid) {
    if (HostHelper.isLocalHost(host)) {
      log("Executing stack dump command on process:" + pid);
      Platform.getInstance().printStacks(pid);
      log("Executed stack dump command on process:" + pid);
    } else {
      try {
        log("Issuing stack dump command to process " + host + ":" + pid);
        HostAgentMgr.getHostAgent(host).printStacks(pid);
        log("Issued stack dump command to process " + host + ":" + pid);
      } catch (RemoteException e) {
        String s = "Unable to complete operation using hostagent on " + host;
        throw new HydraRuntimeException(s, e);
      }
    }
  }

  /**
  * Does a heap dump on a process.  If the process does not exist,
  * it has no effect.
  * @param host the host the process is running on.
  * @param pid its PID.
  */
  public static void dumpHeap(String host, int pid, String userDir, String options) {
    if (HostHelper.isLocalHost(host)) {
      log("Executing heap dump command on process:" + pid);
      Platform.getInstance().dumpHeap(pid, userDir, options);
      log("Executed heap dump command on process:" + pid);
    } else {
      try {
        log("Issuing heap dump command to process " + host + ":" + pid);
        HostAgentMgr.getHostAgent(host).dumpHeap(pid, userDir, options);
        log("Issued heap dump command to process " + host + ":" + pid);
      } catch (RemoteException e) {
        String s = "Unable to complete operation using hostagent on " + host;
        throw new HydraRuntimeException(s, e);
      }
    }
  }

  /**
   * Logs network statistics.  This method throws no exceptions.  If an error
   * occurs, the stack is logged at info level.
   */
  public static void logNetworkStatistics() {
    log("Logging network statistics");
    try {
      String netstats = Platform.getInstance().getNetworkStatistics(120);
      log("Network statistics:\n" + netstats);
    } catch (HydraRuntimeException e) {
      log("Unable to log network statistics: " + TestHelper.getStackTrace(e));
    } catch (HydraTimeoutException e) {
      log("Unable to log network statistics: " + TestHelper.getStackTrace(e));
    }
  }

  /**
  * Tells a process to shut down and waits up to <code>maxWaitSec</code> for this
  * to occur.  Returns <code>false</code> if the process still exists after this time
  * limit has expired, otherwise it returns <code>true</code>.
  * @param host the host the process is running on.
  * @param pid its PID.
  * @param maxWaitSec the maximum number of seconds to wait for the process to shut down.
  * @return <code>true</code> if the process no longer exists
  */
  public static boolean shutdownProcessWait(String host, int pid, int maxWaitSec) {
    shutdownProcess(host, pid);
    if (processExists(host, pid))
      return waitForDeath( host, pid, maxWaitSec );
    else
      return true;
  }

  /**
  * Kills a process without warning and waits up to <code>maxWaitSec</code> for this
  * to occur.  Returns <code>false</code> if the process still exists after this time
  * limit has expired, otherwise it returns <code>true</code>.
  * @param host the host the process is running on.
  * @param pid its PID.
  * @param maxWaitSec the maximum number of seconds to wait for the process to shut down.
  * @return <code>true</code> if the process no longer exists
  */
  public static boolean killProcessWait( String host, int pid, int maxWaitSec ) {
    killProcess(host, pid);
    if (processExists(host, pid)) {
      return waitForDeath(host, pid, maxWaitSec);
    } else {
      return true;
    }
  }

  /**
  * Waits up to <code>maxWaitSec</code> for a process to die.  Returns
  * <code>false</code> if the process still exists after this time limit has
  * expired, otherwise it returns <code>true</code>.  The process is assumed
  * to have been shut down or killed elsewhere.
  * @param host the host the process is running on.
  * @param pid its PID.
  * @param maxWaitSec the maximum number of seconds to wait for the process to shut down.
  * @return <code>true</code> if the process no longer exists
  */
  public static boolean waitForDeath( String host, int pid, int maxWaitSec ) {
    long timeout = System.currentTimeMillis() + maxWaitSec * 1000;
    while ( System.currentTimeMillis() < timeout ) {
      MasterController.sleepForMs(1000);
      if (!processExists(host, pid))
        return true;
    }
    return false;
  }

  /**
  * Waits up to <code>maxWaitSec</code> for a process to live.  Returns
  * <code>false</code> if the process does not exist after this time limit has
  * expired, otherwise it returns <code>true</code>.  The process is assumed
  * to have been created elsewhere.
  * @param host the host the process is running on.
  * @param pid its PID.
  * @param maxWaitSec the maximum number of seconds to wait for the process to wake up.
  * @return <code>true</code> if the process is alive.
  */
  public static boolean waitForLife( String host, int pid, int maxWaitSec ) {
    long timeout = System.currentTimeMillis() + maxWaitSec * 1000;
    while ( System.currentTimeMillis() < timeout ) {
      MasterController.sleepForMs(1000);
      if (processExists(host, pid)) {
        return true;
      }
    }
    return false;
  }

  /**
  *
  * Returns a formatted <code>String</code> containing various information about
  * the runtime environment of this process: host, PID, user name, user.name,
  * user.home, user.dir, os.name, os.arch, os.version, java.version,
  * java.vm.vendor, java.home, java.class.path, and java.library.path.
  * Also includes information about the GemFire build: product version, when
  * it was built, who built it, what directory it was built from, the platform
  * it was built on, and the JDK it was build with.  Most importantly, it
  * gives the date of the last SVN update to use in bug reports.
  * @return formatted <code>String</code> of information about this process.
  *
  */
  public static String processAndBuildInfoString() {
    StringBuffer buff = new StringBuffer();

    buff.append("\n#####################################################\n\n");
    buff.append("Host name: " + HostHelper.getLocalHost() + "\n");
    buff.append("Process id: " + getProcessId() + "\n");
    buff.append("OS User name: " + System.getProperty("user.name") + "\n");
    buff.append("User home: " + System.getProperty("user.home") + "\n");
    buff.append("Current directory: " + System.getProperty("user.dir") + "\n\n");

    buff.append("OS name: " + System.getProperty("os.name") + "\n");
    buff.append("Architecture: " + System.getProperty("os.arch") + "\n");
    buff.append("OS version: " + System.getProperty("os.version") + "\n\n");
    buff.append("Java version: " + System.getProperty("java.version") + "\n");
    buff.append("Java vm name: " + System.getProperty("java.vm.name") + "\n");
    buff.append("Java vendor: " + System.getProperty("java.vm.vendor") + "\n");
    buff.append("Java home: " + System.getProperty("java.home") + "\n\n");
    buff.append("Java class path: " + System.getProperty("java.class.path") + "\n\n");
    buff.append("Java library path: " + System.getProperty("java.library.path") + "\n\n");
    buff.append("#####################################################\n\n");
    Properties p = ProductVersionHelper.getInfo();
    if (p != null) {
      buff.append("Product Version: " + p.getProperty(ProductVersionHelper.PRODUCT_VERSION) + "\n");
      buff.append("Source Date: " + p.getProperty(ProductVersionHelper.SOURCE_DATE) + "\n");
      buff.append("Source Revision: " + p.getProperty(ProductVersionHelper.SOURCE_REVISION) + "\n");
      buff.append("Source Repository: " + p.getProperty(ProductVersionHelper.SOURCE_REPOSITORY) + "\n\n");
      buff.append("Build Id: " + p.getProperty(ProductVersionHelper.BUILD_ID) + "\n");
      buff.append("Build Date: " + p.getProperty(ProductVersionHelper.BUILD_DATE) + "\n");
      buff.append("Build Version: " + p.getProperty(ProductVersionHelper.PRODUCT_VERSION) + " "
                 + p.getProperty(ProductVersionHelper.BUILD_ID) + " "
                 + p.getProperty(ProductVersionHelper.BUILD_DATE)
                 + " javac " + p.getProperty(ProductVersionHelper.BUILD_JAVA_VERSION) + "\n");
      buff.append("Build JDK: Java " + p.getProperty(ProductVersionHelper.BUILD_JAVA_VERSION) + "\n");
      buff.append("Build Platform: " + p.getProperty(ProductVersionHelper.BUILD_PLATFORM) + "\n\n");
      buff.append("#####################################################");
    }
    return buff.toString();
  }

  /**
   * Returns the process status for the local host (for all users).
   * @param maxWaitSec the maximum number of seconds to wait for status.
   * @throws HydraRuntimeException if the status cannot be determined.
   * @throws HydraTimeoutException if the attempt to get status times out.
   */
  public static String getProcessStatus(int maxWaitSec) {
    return Platform.getInstance().getProcessStatus(maxWaitSec);
  }

  /**
   * Returns the process status for the given host (for all users).
   * @param maxWaitSec the maximum number of seconds to wait for status.
   * @throws HydraRuntimeException if the status cannot be determined.
   * @throws HydraTimeoutException if the attempt to get status times out.
   */
  public static String getProcessStatus(String host, int maxWaitSec) {
    if (HostHelper.isLocalHost(host)) {
      return getProcessStatus(maxWaitSec);
    } else {
      log("Getting process status on host " + host);
      try {
        return HostAgentMgr.getHostAgent(host).getProcessStatus(maxWaitSec);
      } catch (RemoteException e) {
        String s = "Unable to complete operation using hostagent on " + host;
        throw new HydraRuntimeException(s, e);
      }
    }
  }

  /**
   * Returns the memory usage for the local host.
   * @param maxWaitSec the maximum number of seconds to wait for status.
   * @throws HydraRuntimeException if the status cannot be determined.
   * @throws HydraTimeoutException if the attempt to get status times out.
   */
  public static String getMemoryStatus(int maxWaitSec) {
    return Platform.getInstance().getMemoryStatus(maxWaitSec);
  }

  /**
   * Gives read permission on all files in the given directory to all users.
   * @throws HydraRuntimeException if a suitable command cannot be found.
   */
  public static void setReadPermission(String dir) {
    Platform.getInstance().setReadPermission(dir);
  }

  /**
   * Successfully logs message before log writer creation.
   */
  private static void log(String msg) {
    try {
      Log.getLogWriter().info(msg);
    }
    catch (HydraRuntimeException e) {
      System.out.println(msg);
    }
  }
}
