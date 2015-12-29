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

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;

/**
 * Supports Windows commands.  Does not rely on a shell.  Requires native
 * support for background process creation.
 */
public class WindowsPlatform extends Platform {

    private static final String LINE_SEP = System.getProperty("line.separator");
    private static final String IMAGE_NAME_FIELD = "Image Name:";
    private static final String SPACE_REGEX = " +";

    private static final int EXISTS_TIMEOUT_SEC = 600;
    private static final int EXISTS_RETRY_ATTEMPTS = 2;
    private static final int EXISTS_RETRY_SLEEP_MS = 1000;

    private static final String COMMAND_PATH = ""; //"c:\\windows\\System32\\";

    private static final String TASKLIST = COMMAND_PATH + "tasklist.exe";
    private static final String TASKKILL = COMMAND_PATH + "taskkill.exe";
    private static final String SYSTEM_INFO = COMMAND_PATH + "systeminfo.exe";
    private static final String NETSTAT = COMMAND_PATH + "netstat.exe";
    private static final String CACLS = COMMAND_PATH + "cacls.exe";
    private static final String ICACLS = COMMAND_PATH + "icacls.exe";

//------------------------------------------------------------------------------
// native library

  static {
    if (System.getProperty("os.name").contains("Windows")) {
      loadTestLibrary();
    }
  }

  /**
   * Loads the native test library,
   */
  private static void loadTestLibrary() {
    StringBuffer libraryName = new StringBuffer("gemfiretest");

    if (is64Bit()) {
      libraryName.append("64");
    }
    if (Boolean.getBoolean("gemfire.debug")) {
      libraryName.append("_g");
    }

    String library = System.mapLibraryName(libraryName.toString());
    String libraryPath = null;
    try {
      try {
        URL libraryURL = HostAgent.class.getResource("/lib/" + library);
        if ( libraryURL == null ) {
          throw new UnsatisfiedLinkError("Didnt find library on the classpath");
        }
        libraryPath = URLDecoder.decode(libraryURL.getFile(), "UTF-8");
        if ( libraryPath.charAt(0) == '/' && libraryPath.charAt(2) == ':' ) {
          libraryPath = libraryPath.substring(1);
        }
      } catch ( UnsupportedEncodingException uee ) {
        //This should never happen because UTF-8 is required to be implemented
        throw new RuntimeException(uee);
      }
      System.load(libraryPath);
    } catch (UnsatisfiedLinkError ule) {
      try {
        //Reattempting using System.loadLibrary instead
        System.loadLibrary(library);
      } catch (UnsatisfiedLinkError ule2) {
        //We've failed to load both the library using the classpath
        // and System.loadLibrary.
        String str = "Failed to load native test library. "
                   + "Check your PATH variable. details: "
                   + ule + "more details: " + ule2;
        throw new HydraRuntimeException(str);
      }
    }
  }

  private static boolean is64Bit() {
    final int bits = Integer.getInteger("sun.arch.data.model", 0).intValue();
    if (bits == 64) {
      return true;
    } else if (bits == 32) {
      return false;
    } else {
      // did not see the expected value of 32 or 64
      System.out.println("sun.arch.data.model did not match the expected 32 or 64, it was instead " + bits + "\nDefaulting to 32 bit.");
      return false;
    }
  }

//------------------------------------------------------------------------------

    /**
     * Starts a background command writing its stdout and stderr to
     * the specified log file.
     *
     * @param cmdarray An array of strings that specify the command to run.
     * The first element must be the executable.
     * Each additional command line argument should have its own entry
     *  in the array.
     * @param workdir the current directory of the created process
     * @param logfile the file the created process will write
     * stdout and stderr to.
     * @return the process id of the created process; -1 on failure
     * @exception IOException if a child process could not be created.
     */
    protected int bgexecInternal(String[] cmdarray,
				     String workdir,
				     String logfile)
    throws IOException {
      String trace = System.getProperty("com.gemstone.gemfire.internal.OSProcess.trace");
      if (trace != null && trace.length() > 0) {
        for (int i=0; i < cmdarray.length; i++) {
          System.out.println("cmdarray[" + i + "] = " + cmdarray[i]);
        }
        System.out.println("workdir=" + workdir);
        System.out.println("logfile=" + logfile);
      }
      final int result = _bgexecInternal(cmdarray, workdir, logfile);
      if (result != -1 && trace != null && trace.length() > 0) {
        System.out.println("bgexec child pid is: " + result);
      }
      return result;
    }

    static private native int _bgexecInternal(String[] cmdarray,
				     String workdir,
				     String logfile)
    throws IOException;

    static private native boolean nativeExists(int pid);
    static private native boolean nativeKill(int pid);

  /**
   * Answers whether a Java process exists with the given PID.
   */
  protected boolean exists(int pid) {
    checkPid(pid);
    return nativeExists(pid);
  }

  protected void shutdown(int pid) {
    checkPid(pid);
    boolean killed = nativeKill(pid);
    if (!killed) {
      String s = "Failed to complete shutdown command to " + pid;
      throw new HydraRuntimeException(s);
    }
  }

  protected void kill(int pid) {
    checkPid(pid);
    boolean killed = nativeKill(pid);
    if (!killed) {
      String s = "Failed to complete kill command to " + pid;
      throw new HydraRuntimeException(s);
    }
  }

  protected void printStacks(int pid) {
    checkPid(pid);
    String cmd = getPrintStacksCommand(pid);
    try {
      fgexec(cmd, 300);
    } catch (HydraRuntimeException e) {
      Log.getLogWriter().warning("Failed to issue print stacks command to "
                                + pid + "\n" + getStackTrace(e));
    } catch (HydraTimeoutException e) {
      Log.getLogWriter().warning("Failed to issue print stacks command to "
                                + pid + "\n" + getStackTrace(e));
    }
  }

  protected void dumpHeap(int pid, String userDir, String options) {
    checkPid(pid);
    String[] cmd = getDumpHeapCommand(pid, userDir, options);
    if (cmd != null) {
      try {
        fgexec(cmd, new String[1], null);
      } catch (IOException e) {
        Log.getLogWriter().severe(getStackTrace(e));
      }
    }
  }

  protected String getNetworkStatistics(int maxWaitSec) {
    return fgexec(NETSTAT + " -an", maxWaitSec);
  }

  protected String getShutdownCommand(int pid) {
    checkPid(pid);
    return TASKKILL + " /F /PID " + pid;
  }

  protected String getKillCommand(int pid) {
    checkPid(pid);
    return TASKKILL + " /F /PID " + pid;
  }

  protected String getDumpLocksCommand(int pid) {
    // can't do this on Windows; maybe someday
    return null;
  }

  protected String getPrintStacksCommand(int pid) {
    checkPid(pid);
    String cmd = System.getProperty("JTESTS") + "\\sendsigbreak\\";
    cmd += is64Bit() ? "sendsigbreak64.exe" : "sendsigbreak.exe";
    cmd += " " + pid;
    return cmd;
  }

  protected String[] getDumpHeapCommand(int pid, String userDir, String options) {
    checkPid(pid);
    String[] cmd = null;
    if (HostPrms.getJavaVendor().equals(HostPrms.SUN)) {
      cmd = new String[3];
      String dir = userDir == null ? System.getProperty("user.dir") : userDir;
      String fn = dir + "\\java_" + HostHelper.getLocalHost()
                + "_" + pid + ".hprof";
      cmd[0] = System.getProperty("java.home") + "\\..\\bin\\jmap.exe";
      cmd[1] = options == null ? "-dump:live,format=b" : options;
      cmd[1] += ",file=" + fn;
      cmd[2] = String.valueOf(pid);
    }
    return cmd;
  }

//------------------------------------------------------------------------------
// network management

  protected String getNetcontrolCommand(String target, int op) {
    String cmd = "ipseccmd.exe";
    if (!FileUtil.exists(cmd)) {
      String s = cmd + " not found, please work with I.S. to install";
      throw new HydraRuntimeException(s);
    }
    switch (op) {
      case NetworkHelper.DROP:
        return cmd + " -f [" + target + "=0]";
      case NetworkHelper.RESTORE:
        return cmd + " -f (" + target +"=0) ";
      case NetworkHelper.CLEAR:
        return cmd + " -u";
      case NetworkHelper.SHOW:
        return cmd + " show filters";
      default:
        String s = "Should not happen";
        throw new HydraInternalException(s);
    }
  }

//------------------------------------------------------------------------------
// time management

  protected void restartNTP() {
    Log.getLogWriter().info("Skipping NTP restart on Windows");
  }

//------------------------------------------------------------------------------
// file management

  protected String getFileExtension() {
    return ".bat";
  }

  protected String getScriptHeader() {
    return "";
  }

  protected String getCommentPrefix() {
    return "@rem ";
  }

  protected void setExecutePermission(String fn) {
    if (System.getProperty("os.name").contains("XP")) {
      fgexec(CACLS + " " + fn + " /E /C /G Everyone:F", 600);
    } else {
      fgexec(ICACLS + " " + fn + " /grant Everyone:F /C", 600);
    }
  }

  protected void setReadPermission(String dir) {
    if (System.getProperty("os.name").contains("XP")) {
      fgexec(CACLS + " " + dir + " /E /C /T /G Everyone:F", 600);
    } else {
      fgexec(ICACLS + " " + dir + " /grant Everyone:F /C /T", 600);
    }
  }

//------------------------------------------------------------------------------
// host status

  protected String getProcessStatus(int maxWaitSec) {
    String cmd = "cmd.exe /c " + TASKLIST + " /V";
    return fgexec(cmd, maxWaitSec);
  }

  protected String getMemoryStatus(int maxWaitSec) {
    String cmd = "cmd.exe /c " + SYSTEM_INFO;
    return fgexec(cmd, maxWaitSec);
  }
}
