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

/**
 * Supports UNIX commands.
 */
public class UnixPlatform extends Platform {

  private static final String DEVNULL = "/dev/null"; 
  private static final String KILL = "/bin/kill"; 
  protected static final String SHELL = "/bin/bash"; 

  private static final String SOLARIS_CHMOD = "/opt/sfw/bin/gchmod";
  private static final String LINUX_CHMOD = "/bin/chmod";

  private static final String EXISTS = "0";
  private static final String SIGQUIT = "QUIT";
  private static final String SIGKILL = "KILL";
  private static final String SIGTERM = "TERM";
  private static final String SIGURG = "URG";

//------------------------------------------------------------------------------
// process management

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
  **/
  protected int bgexecInternal(String[] cmdarray, 
	 		    String workdir, 
			    String logfile)
  throws IOException
  {
    if (logfile == null || logfile.equals("")) {
      logfile = DEVNULL;
    }

    if (cmdarray.length == 0) {
      throw new java.lang.IndexOutOfBoundsException();
    }
    for (int i = 0; i < cmdarray.length; i++) {
      if (cmdarray[i] == null) {
        throw new NullPointerException();
      }
    }

    StringBuffer sb = new StringBuffer();
    for ( int i=0; i<cmdarray.length; i++ ) {
      if (i != 0) sb.append(" ");
      sb.append("\"");
      sb.append(cmdarray[i]);
      sb.append("\"");
    }
    sb.append(" <");
    sb.append(DEVNULL);
    sb.append(" >");
    sb.append(logfile);
    sb.append(" 2>&1 & echo $!");
    String[] job = { SHELL, "--norc", "-c", sb.toString()};
    File cmd = new File(job[0]);
    if (!cmd.exists()) {
      throw new IOException("the executable \""
                            + cmd.getPath() + "\" does not exist");
    }
    SecurityManager security = System.getSecurityManager();
    if (security != null) {
      security.checkExec(job[0]);
    }
    /*
    ProcessBuilder pb = new ProcessBuilder(job);  
    pb.directory(workdir);  
    Process p = pb.start();  
    try {
      //Wait for shell spwaning process to complete.
      if(p.waitFor() != 0 ) {
        return -1;
      }
    } catch ( InterruptedException ie ) {
      return -1;
    }
    
    try { 
      InputStream is = p.getInputStream();
      InputStreamReader isr = new InputStreamReader(is);
      BufferedReader br = new BufferedReader(isr);
      String line = br.readLine();
      return Integer.parseInt(line.trim());
    } catch( IOException ioe ) {
      System.out.println("Exception: Failed to get PID: " + ioe.getMessage());
    } catch (NumberFormatException nfe) {
      System.out.println("Exception: Failed to get PID: " + nfe.getMessage());
    }
    */
    //implement the above code without ProcessBuilder since it is a 1.5 feature
    String[] buf = new String[1];
    int pid = -1;
    try {
      if ( fgexec(job, buf, new File(workdir) ) == 0 ) {
        pid = Integer.parseInt(buf[0].trim());
      }
    } catch( IOException ioe ) {
      System.out.println("Exception: Failed to get PID: " + ioe.getMessage());
    } catch (NumberFormatException nfe) {
      System.out.println("Exception: Failed to get PID: " + nfe.getMessage());
    }
    /**
     * It contains "bgexec" so tack on the pid.
     */
    if (logfile.indexOf("bgexec") != -1) {
      File f = new File(logfile);
      String fn = f.getName();
      fn = fn.substring(0, fn.lastIndexOf(".log"));
      String path = f.getParent();
      String newfn = path + File.separator + fn + "_" + pid + ".log";
      File newf = new File(newfn);
      if(pid != -1 && ! f.renameTo(newf)) {
        Log.getLogWriter().warning("Failed to rename log file " + logfile );
      }
    }
    return pid;
  }

  private boolean sendSignal(String sig, int pid)
      throws IllegalArgumentException {
    String[] cmdarray = {KILL, "-" + sig, String.valueOf(pid)};
    try {
      String[] tmp = new String[1]; 
      if ( fgexec( cmdarray, tmp, null) == 0 ) {
        return true;
      } else {
        return false;
      }
    } catch ( IOException ioe ) {
      return false;
    }
  } 

  protected boolean exists(int pid) {
    checkPid(pid);
    return sendSignal(EXISTS, pid);
  }

  protected void shutdown(int pid) {
    checkPid(pid);
    sendSignal(SIGTERM, pid);
  }

  protected void kill(int pid) {
    checkPid(pid);
    sendSignal(SIGKILL, pid);
  }

  protected void printStacks(int pid) {
    checkPid(pid);
    // SIGURG currently used by GemFireXD to dump the lock table etc.
    sendSignal(SIGURG, pid);
    sendSignal(SIGQUIT, pid);
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
    String cmd = "/bin/netstat";
    if (FileUtil.exists(cmd)) {
      cmd += " -an";
      return fgexec(cmd, maxWaitSec);
    } else {
      String s = "Unable to access network statistics";
      throw new HydraRuntimeException(s);
    }
  }

  protected String getShutdownCommand(int pid) {
    return KILL + " -" + SIGTERM + " " + pid;
  }

  protected String getKillCommand(int pid) {
    return KILL + " -" + SIGKILL + " " + pid;
  }

  protected String getDumpLocksCommand(int pid) {
    return KILL + " -" + SIGURG + " " + pid;
  }

  protected String getPrintStacksCommand(int pid) {
    return KILL + " -" + SIGQUIT + " " + pid;
  }

  protected String[] getDumpHeapCommand(int pid, String userDir, String options) {
    checkPid(pid);
    String[] cmd = null;
    if (HostPrms.getJavaVendor().equals(HostPrms.SUN)) {
      cmd = new String[3];
      String dir = userDir == null ? System.getProperty("user.dir") : userDir;
      String fn = dir + "/java_" + HostHelper.getLocalHost()
                + "_" + pid + ".hprof";
      cmd[0] = System.getProperty("java.home") + "/../bin/jmap";
      cmd[1] = options == null ? "-dump:live,format=b" : options;
      cmd[1] += ",file=" + fn;
      cmd[2] = String.valueOf(pid);
    }
    return cmd;
  }

//------------------------------------------------------------------------------
// network management

  protected String getNetcontrolCommand(String target, int op) {
    String script = "/export/localnew/scripts/netcontrol";
    if (!FileUtil.exists(script)) {
      script = "/export/shared/sysadm/scripts/netcontrol";
      if (!FileUtil.exists(script)) {
        String s = "netcontrol script not found, please work with I.S. to install";
        throw new HydraRuntimeException(s);
      }
    }
    String cmd = sudo() + " " + script;
    switch (op) {
      case NetworkHelper.DROP:
        return cmd + " -a -D -t -H " + target;
      case NetworkHelper.RESTORE:
        return cmd + " -r -D -t -H " + target;
      case NetworkHelper.CLEAR:
        return cmd + " -C";
      case NetworkHelper.SHOW:
        return cmd + " -S";
      default:
        String s = "Should not happen";
        throw new HydraInternalException(s);
    }
  }

//------------------------------------------------------------------------------
// time management

  /**
   * Uses NTP to synchronize the local clock with the network time server.
   */
  protected void restartNTP() {
    // stop NTP
    //invokeNTP("stop");

    // reset the clock (this fails if NTP is already running)
    //resetNTPClock();

    // start NTP
    invokeNTP("restart");
  }

  /**
   * Issues the NTP operation on the local host.
   */
  private void invokeNTP(String op) {
    String sudo = sudo();
    if (sudo == null) {
      Log.getLogWriter().warning("Unable to find sudo to invoke NTP");
    } else {
      String ntp = ntp();
      if (ntp == null) {
        Log.getLogWriter().warning("Unable to find NTP executable");
      } else {
        String cmd = sudo + " " + ntp + " " + op;
        Log.getLogWriter().info("Executing NTP " + op + "...");
        String result = fgexec(cmd, 300);
        Log.getLogWriter().info("Executed NTP " + op + ":\n" + result);
      }
    }
  }

  /**
   * Resets the NTP clock on the local host.
   */
  private void resetNTPClock() {
    String sudo = sudo();
    if (sudo == null) {
      Log.getLogWriter().warning("Unable to find sudo to invoke NTP");
    } else {
      String ntpdate = ntpdate();
      if (ntpdate == null) {
        Log.getLogWriter().warning("Unable to find NTP date executable");
      } else {
        Log.getLogWriter().info("Resetting the clock");
        String cmd = sudo + " " + ntpdate + " gateway";
        String result = fgexec(cmd, 300);
        Log.getLogWriter().info("Reset the clock:\n" + result);
      }
    }
  }

  protected String sudo() {
    String sudo = "/usr/bin/sudo";
    if (!FileUtil.exists(sudo)) {
      sudo = "/opt/sfw/bin/sudo";
      if (!FileUtil.exists(sudo)) {
        return null;
      }
    }
    return sudo;
  }

  private String ntp() {
    String ntp = "/etc/init.d/ntpd";
    if (!FileUtil.exists(ntp)) {
      ntp = "/etc/init.d/xntpd";
      if (!FileUtil.exists(ntp)) {
        ntp = "/etc/init.d/ntp";
        if (!FileUtil.exists(ntp)) {
          ntp = "/etc/init.d/xntpd";
          if (!FileUtil.exists(ntp)) {
            return null;
          }
        }
      }
    }
    return ntp;
  }

  private String ntpdate() {
    String ntpdate = "/usr/sbin/ntpdate";
    if (!FileUtil.exists(ntpdate)) {
      return null;
    }
    return ntpdate;
  }

//------------------------------------------------------------------------------
// file management

  protected String getFileExtension() {
    return ".sh";
  }

  protected String getScriptHeader() {
    return "#!/bin/sh\n#\n";
  }

  protected String getCommentPrefix() {
    return "# ";
  }

  protected void setExecutePermission(String fn) {
    fgexec(getChmodCommand() + " u+rwx " + fn, 600);
  }

  protected void setReadPermission(String dir) {
    fgexec(getChmodCommand() + " -R uog+r " + dir, 600);
  }

  private String getChmodCommand() {
    if (FileUtil.exists(SOLARIS_CHMOD)) {
      return SOLARIS_CHMOD;
    } else if (FileUtil.exists(LINUX_CHMOD)) {
      return LINUX_CHMOD;
    } else {
      String s = "Unable to find chmod command";
      throw new HydraRuntimeException(s);
    }
  }

//------------------------------------------------------------------------------
// host status

  protected String getProcessStatus(int maxWaitSec) {
    String cmd = "ps -e -o user,pid,pmem,stime,args";
    return fgexec(cmd, maxWaitSec);
  }

  protected String getMemoryStatus(int maxWaitSec) {
    String cmd = "/usr/bin/free";
    if (FileUtil.exists(cmd)) {
      return fgexec(cmd, maxWaitSec);
    } else {
      // could also use prtconf to get total available memory
      cmd = "/usr/bin/vmstat";
      if (FileUtil.exists(cmd)) {
        return fgexec(cmd, maxWaitSec);
      } else {
        String s = "Unable to get memory status";
        throw new HydraRuntimeException(s);
      }
    }
  }
}
