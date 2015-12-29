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

import hydra.HadoopPrms.NodeType;
import hydra.HostHelper.OSType;
import java.io.*;
import java.rmi.RemoteException;
import java.util.*;

/**
 *  Supports cleaning up after hydra runs.  Generates executable scripts
 *  in the test directory such as <code>nukerun.sh</code> or
 *  <code>nukerun.bat</code> that kills all processes for that test.
 *  Additional scripts are generated dor dumping stacks, dumping heap,
 *  and moving remote test directories (where applicable).
 */
public class Nuker
{
  /** singleton instance, used only by hydra master */
  private static Nuker nuker;

  protected static final String UNIX_SYSDIRS = "unix_sysdirs.txt";
  protected static final String WINDOWS_SYSDIRS = "windows_sysdirs.txt";
  protected static final String HDFS_DIRS = "hdfs_dirs.txt";

  private Platform platform;
  private HostDescription mhd;
  private String commentPrefix;
  private String userDir; // the master user and test result directory
  private String mfn; // move dir script file name
  private String nfn; // nuke run script file name
  private String hmfn; // hadoop move dir script file name
  private String hnfn; // hadoop nuke run script file name
  private String dfn; // stack dump script file name
  private String hfn; // heap dump script file name


  /** stores all known test artifact directories, both local and remote */
  private Set directories = new HashSet();
  private Set hdfs_directories = new HashSet();

//------------------------------------------------------------------------------
// Methods for use by hydra master
//------------------------------------------------------------------------------

  protected static void initialize(HostDescription hd) {
    nuker = new Nuker(hd);
  }

  /**
   * Returns the singleton instance of the nuker.  All hydra client access
   * goes through the master proxy. For hydra master use only.
   */
  public static Nuker getInstance() {
    if (nuker == null) {
      String s = "Reserved for the hydra master controller.  "
               + "Use methods in MasterProxyIF instead";
      throw new UnsupportedOperationException(s);
    }
    return nuker;
  }

  private Nuker(HostDescription hd) {
    this.platform = Platform.getInstance();
    this.mhd = hd;
    this.userDir = this.mhd.getUserDir() + this.mhd.getFileSep();

    String fileExtension = this.platform.getFileExtension();
    this.mfn = this.userDir + "movedirs" + fileExtension;
    this.nfn = this.userDir + "nukerun" + fileExtension;
    this.hmfn = this.userDir + "movehadoop" + fileExtension;
    this.hnfn = this.userDir + "nukehadoop" + fileExtension;
    this.dfn = this.userDir + "dumprun" + fileExtension;
    this.hfn = this.userDir + "heapdumprun" + fileExtension;

    String scriptHeader = platform.getScriptHeader();
    this.commentPrefix = platform.getCommentPrefix();

    FileUtil.appendToFile(this.mfn, scriptHeader + this.commentPrefix + " execute this script to move all remote directories in this test\n\n");
    FileUtil.appendToFile(this.nfn, scriptHeader + this.commentPrefix + " execute this script to kill with malice all processes in this test\n\n");
    FileUtil.appendToFile(this.hmfn, scriptHeader + this.commentPrefix + " execute this script to clean all Hadoop directories in this test\n\n");
    FileUtil.appendToFile(this.hnfn, scriptHeader + this.commentPrefix + " execute this script to kill with malice all Hadoop processes in this test\n\n");
    FileUtil.appendToFile(this.dfn, scriptHeader + this.commentPrefix + " execute this script to do thread dumps on all processes in this test\n\n");
    FileUtil.appendToFile(this.hfn, scriptHeader + this.commentPrefix + " execute this script to do heap dumps on all processes in this test\n\n");

    platform.setExecutePermission(this.mfn);
    platform.setExecutePermission(this.nfn);
    platform.setExecutePermission(this.hmfn);
    platform.setExecutePermission(this.hnfn);
    platform.setExecutePermission(this.dfn);
    platform.setExecutePermission(this.hfn);
  }

  /**
   * Returns the known test artifact directories.
   */
  protected Set getDirectories() {
    return this.directories;
  }

//------------------------------------------------------------------------------
// dirs

  /**
   * Records a root command in moveHadoop.sh for secure HDFS.
   */
  public synchronized void recordRootCommand(HostDescription hd, String cmd, String dir) {
    String wrappedCmd = wrapRootCommand(cmd, hd);
    boolean added = false;
    List<String> lines;
    try {
      lines = FileUtil.getTextAsList(this.hmfn);
    } catch (IOException e) {
      String s = "Unable to access file: " + this.hmfn;
      throw new HydraInternalException(s);
    }
    for (int i = 0; i < lines.size(); i++) {
      if (lines.get(i).contains(dir)) {
        lines.add(i, wrappedCmd);
        added = true;
        break;
      }
    }
    if (added) { // overwrite the file contents
      for (int i = 0; i < lines.size(); i++) {
        if (i == 0) {
          FileUtil.writeToFile(this.hmfn, lines.get(i) + "\n");
        } else {
          FileUtil.appendToFile(this.hmfn, lines.get(i) + "\n");
        }
      }
    } else {
      FileUtil.appendToFile(this.hmfn, wrappedCmd + "\n");
    }
  }

  protected String wrapRootCommand(String cmd, HostDescription hd) {
    if (HostHelper.isLocalHost(hd.getHostName())) {
      return cmd;
    } else if (canUseSSH(hd)) {
      return Platform.SSH + " " + hd.getHostName() + " " + cmd;
    } else {
      String s = "Unable to record root command without ssh: " + cmd;
      throw new HydraRuntimeException(s);
    }
  }

  /**
   * Records a remote HDFS directory in an existing cluster for later removal.
   */
  public synchronized void recordHDFSDir(String hadoopDist, String url,
                                         boolean moveAfterTest) {
    String src = url + "/user/" + System.getProperty("user.name");
    if (moveAfterTest) {
      String dst = this.userDir + "movedHadoopData";
      String mkdircmd = UnixPlatform.SHELL + " mkdir -p " + dst + "\n";
      FileUtil.appendToFile(this.hmfn, mkdircmd);
      String mvcmd = UnixPlatform.SHELL + " " + hadoopDist
                   + "/bin/hadoop fs -copyToLocal " + src + " " + dst + "\n";
      FileUtil.appendToFile(this.hmfn, mvcmd);
    }
    String rmcmd = UnixPlatform.SHELL + " "
                 + hadoopDist + "/bin/hadoop fs -rm -r -f " + src + "\n";
    FileUtil.appendToFile(this.hmfn, rmcmd);
  }

  /**
   * Records a remote HDFS directory for later move. Adds both local and remote
   * directories to the HDFS directory list in {@link #HDFS_DIRS} to support
   * both runtime use by clients and script use by the master controller and
   * batterytest.
   */
  protected synchronized void recordHDFSDir(HostDescription hd,
            NodeType nodeType, String dir, boolean moveAfterTest) {
    if (!this.hdfs_directories.contains(dir)) {
      String dirname = getDirName(hd, dir);
      String dirpath = getDirPath(hd, dir);
      recordHDFSDir(hd, nodeType, dir, dirname);
      if (moveAfterTest) {
        recordMoveDirCommands(this.hmfn, hd, dir, dirpath, dirname);
      } else {
        recordRemoveDirCommands(this.hmfn, hd, dir, dirpath, dirname);
      }
    }
  }

  private void recordHDFSDir(HostDescription hd, NodeType nodeType,
                             String dir, String dirname) {
    this.hdfs_directories.add(dir);
    FileUtil.appendToFile(HDFS_DIRS, nodeType + " " + dir + "\n");
  }

  /**
   * Records a remote directory for later move.  Adds it to the directory list
   * for all platforms needed to support both runtime use by clients and script
   * use by the master controller and batterytest.
   */
  protected synchronized void recordDir(HostDescription hd, String name,
                                                            String dir) {
    if (!this.directories.contains(dir)) {
      String dirname = getDirName(hd, dir);
      String dirpath = getDirPath(hd, dir);
      recordDir(hd, name, dir, dirname);
      recordMoveDirCommands(this.mfn, hd, dir, dirpath, dirname);
    }
  }

  private void recordDir(HostDescription hd, String name,
                         String dir, String dirname) {
    this.directories.add(dir);
    switch (hd.getOSType()) {
      case unix:
        FileUtil.appendToFile(UNIX_SYSDIRS, name + " " + dir + "\n");
        switch (this.mhd.getOSType()) {
          case unix:
            break;
          case windows:
            FileUtil.appendToFile(WINDOWS_SYSDIRS,
                                  name + " " + this.userDir + dirname + "\n");
            break;
          default:
            throw new HydraInternalException("Should not happen");
        }
        break;
      case windows:
        FileUtil.appendToFile(WINDOWS_SYSDIRS, name + " " + dir + "\n");
        switch (this.mhd.getOSType()) {
          case unix:
            FileUtil.appendToFile(UNIX_SYSDIRS,
                                  name + " " + this.userDir + dirname + "\n");
            break;
          case windows:
            break;
          default:
            throw new HydraInternalException("Should not happen");
        }
        break;
      default:
        throw new HydraInternalException("Should not happen");
    }
  }

  private String getDirPath(HostDescription hd, String dir) {
    return dir.substring(0, dir.lastIndexOf(hd.getFileSep()));
  }

  private String getDirName(HostDescription hd, String dir) {
    return dir.substring(dir.lastIndexOf(hd.getFileSep()) + 1, dir.length());
  }

  private void recordMoveDirCommands(String fn, HostDescription hd, String dir,
                                    String dirpath, String dirname) {
    String target = this.userDir + dirname;
    if (FileUtil.exists(target)) {
      if (Log.getLogWriter().fineEnabled()) {
        Log.getLogWriter().fine("Already in master user directory: " + dirname);
      }
      return;
    }
    String cmd = null;
    String rmdir = null;
    switch (hd.getOSType()) {
      case unix:
        switch (this.mhd.getOSType()) {
          case unix:
            if (TestConfig.tab().booleanAt(Prms.useNFS, true)) {
              rmdir = "/bin/rmdir " + dirpath;
              replaceInFile(fn, rmdir, this.commentPrefix + rmdir);
              cmd = "/bin/mv " + dir + " " + this.userDir + "\n" + rmdir;
            } else {
              rmdir = Platform.SSH + " " + hd.getHostName()
                    + " /bin/rm -rf " + dirpath;
              replaceInFile(fn, rmdir, this.commentPrefix + rmdir);
              cmd = "scp -r " + System.getProperty("user.name")
                  + "@" + hd.getHostName() + ":" + dir + " " + this.userDir
                  + "\n" + rmdir;
            }
            break;
          case windows:
            if (TestConfig.tab().booleanAt(Prms.useNFS, true)) {
              cmd = "@rem please manually move " + dir
                  + " to " + this.userDir;
            } else {
              cmd = "@rem please manually move " + dirpath
                  + " to " + this.userDir;
            }
            break;
          default:
            throw new HydraInternalException("Should not happen");
        }
        break;
      case windows:
        switch (this.mhd.getOSType()) {
          case unix:
            if (TestConfig.tab().booleanAt(Prms.useNFS, true)) {
              rmdir = wrapCommand("cmd.exe /c rmdir /S /Q " + dirpath, hd);
              replaceInFile(fn, rmdir, this.commentPrefix + rmdir);
              cmd = wrapCommand("cmd.exe /c robocopy " + dir + " "
                  + hd.getUserDir() + "\\" + dirname + " /move", hd) + "\n" + rmdir;
            } else {
              cmd = "# please manually move " + dirpath + " to " + this.userDir;
            }
            break;
          case windows:
            if (TestConfig.tab().booleanAt(Prms.useNFS, true)) {
              rmdir = "cmd.exe /c rmdir /S /Q " + dirpath;
              replaceInFile(fn, rmdir, this.commentPrefix + rmdir);
              cmd = "cmd.exe /c robocopy " + dir + " " + this.userDir
                  + " /move" + "\n" + rmdir;
            } else {
              cmd = "@rem please manually move " + dirpath
                  + " to " + this.userDir;
            }
            break;
          default:
            throw new HydraInternalException("Should not happen");
        }
        break;
      default:
        throw new HydraInternalException("Should not happen");
    }
    if (cmd != null) {
      FileUtil.appendToFile(fn, cmd + "\n");
    }
  }

  // add a remove command for the directory regardless of its location
  private void recordRemoveDirCommands(String fn, HostDescription hd, String dir,
                                    String dirpath, String dirname) {
    String target = this.userDir + dirname;
    boolean isUserDir = FileUtil.exists(target);
    String cmd = null;
    String rmdir = null;
    switch (hd.getOSType()) {
      case unix:
        switch (this.mhd.getOSType()) {
          case unix:
            if (TestConfig.tab().booleanAt(Prms.useNFS, true)) {
              if (isUserDir) {
                cmd = "/bin/rm -rf " + dir;
              } else {
                rmdir = "/bin/rmdir " + dirpath;
                replaceInFile(fn, rmdir, this.commentPrefix + rmdir);
                cmd = "/bin/rm -rf " + dir + "\n" + rmdir;
              }
            } else {
              if (isUserDir) {
                cmd = Platform.SSH + " " + hd.getHostName() + "/bin/rm -rf "
                    + dir;
              } else {
                rmdir = Platform.SSH + " " + hd.getHostName()
                      + " /bin/rm -rf " + dirpath;
                replaceInFile(fn, rmdir, this.commentPrefix + rmdir);
                cmd = Platform.SSH + " " + hd.getHostName() + "/bin/rm -rf "
                    + dir + "\n" + rmdir;
              }
            }
            break;
          case windows:
            if (TestConfig.tab().booleanAt(Prms.useNFS, true)) {
              cmd = "@rem please manually remove " + dir;
            } else {
              cmd = "@rem please manually remove " + dir;
            }
            break;
          default:
            throw new HydraInternalException("Should not happen");
        }
        break;
      case windows:
        switch (this.mhd.getOSType()) {
          case unix:
            if (TestConfig.tab().booleanAt(Prms.useNFS, true)) {
              if (isUserDir) {
                cmd = wrapCommand("cmd.exe /c rmdir /S /Q " + dir
                    + "\\" + dirname, hd);
              } else {
                rmdir = wrapCommand("cmd.exe /c rmdir /S /Q " + dirpath, hd);
                replaceInFile(fn, rmdir, this.commentPrefix + rmdir);
                cmd = wrapCommand("cmd.exe /c rmdir /S /Q " + dir
                    + "\\" + dirname, hd) + "\n" + rmdir;
              }
            } else {
              cmd = "# please manually remove " + dir;
            }
            break;
          case windows:
            if (TestConfig.tab().booleanAt(Prms.useNFS, true)) {
              if (isUserDir) {
                cmd = "cmd.exe /c rmdir /S /Q " + dir;
              } else {
                rmdir = "cmd.exe /c rmdir /S /Q " + dirpath;
                replaceInFile(fn, rmdir, this.commentPrefix + rmdir);
                cmd = "cmd.exe /c rmdir /S /Q " + dir + "\\"
                    + dirname + "\n" + rmdir;
              }
            } else {
              cmd = "@rem please manually remove " + dir;
            }
            break;
          default:
            throw new HydraInternalException("Should not happen");
        }
        break;
      default:
        throw new HydraInternalException("Should not happen");
    }
    if (cmd != null) {
      FileUtil.appendToFile(fn, cmd + "\n");
    }
  }

//------------------------------------------------------------------------------
// pids

  /**
   * Records a PID in the nukerun script.
   */
  protected void recordPIDNoDumps(HostDescription hd, int pid) {
    recordPID(this.nfn, this.dfn, this.hfn, hd, pid, false, false);
  }

  /**
   * Records a PID in the nukerun, dumprun, and heapdumprun scripts.
   * For internal hydra use only.
   */
  protected void recordPID(HostDescription hd, int pid) {
    recordPID(this.nfn, this.dfn, this.hfn, hd, pid, true, false);
  }

  /**
   * Records a Hadoop PID in the nukehadoop script.
   */
  protected void recordHDFSPIDNoDumps(HostDescription hd, int pid,
                                      boolean secure) {
    recordPID(this.hnfn, null, null, hd, pid, false, secure);
  }

  /**
   * Records a PID in various scripts, as indicated.
   */
  private synchronized void recordPID(String nukefn, String dumpfn, String heapfn,
          HostDescription hd, int pid, boolean includeDumps, boolean secure) {
    // jsvc restarts after a mean kill, so used nice kill for secure pids
    String ncmd = secure ? getStopPIDCmd(hd, pid) : getNukePIDCmd(hd, pid);
    if (ncmd != null) {
      FileUtil.appendToFile(nukefn, ncmd + "\n");
    }
    if (includeDumps) {
      String dcmd = getDumpLocksCmd(hd, pid);
      if (dcmd != null) {
        FileUtil.appendToFile(dumpfn, dcmd + "\n");
      }
      dcmd = getDumpPIDCmd(hd, pid);
      if (dcmd != null) {
        FileUtil.appendToFile(dumpfn, dcmd + "\n");
      }
      String hcmd = getDumpHeapPIDCmd(hd, pid);
      if (hcmd != null) {
        FileUtil.appendToFile(heapfn, hcmd + "\n");
      }
    }
  }

  /**
   * Removes a PID from the nukerun scripts using the given nuke command.
   * Used by the Reboot API for removing hostagent PIDs.
   */
  protected void removePIDNoDumps(String ncmd) {
    replaceInFile(this.nfn, ncmd, this.commentPrefix + ncmd);
  }

  /**
   * Removes a PID from the nukerun scripts.
   */
  protected void removePIDNoDumps(HostDescription hd, int pid) {
    removePID(this.nfn, this.dfn, this.hfn, hd, pid, false, false);
  }

  /**
   * Removes a PID from the nukerun, dumprun, and heapdumprun scripts.
   * For internal hydra use only.
   */
  protected void removePID(HostDescription hd, int pid ) {
    removePID(this.nfn, this.dfn, this.hfn, hd, pid, true, false);
  }

  /**
   * Removes an HDFS PID from the nukehadoop scripts.
   */
  protected void removeHDFSPIDNoDumps(HostDescription hd, int pid,
                                      boolean secure) {
    removePID(this.hnfn, null, null, hd, pid, false, secure);
  }

  /**
   * Removes a PID from various scripts, as indicated.
   */
  private synchronized void removePID(String nukefn, String dumpfn, String heapfn,
          HostDescription hd, int pid, boolean includeDumps, boolean secure) {
    // jsvc restarts after a mean kill, so used nice kill for secure pids
    String ncmd = secure ? getStopPIDCmd(hd, pid) : getNukePIDCmd(hd, pid);
    if (ncmd != null) {
      replaceInFile(nukefn, ncmd, this.commentPrefix + ncmd);
    }
    if (includeDumps) {
      String dcmd = getDumpLocksCmd(hd, pid);
      if (dcmd != null) {
        replaceInFile(dumpfn, dcmd, this.commentPrefix + dcmd);
      }
      dcmd = getDumpPIDCmd(hd, pid);
      if (dcmd != null) {
        replaceInFile(dumpfn, dcmd, this.commentPrefix + dcmd);
      }
      String hcmd = getDumpHeapPIDCmd(hd, pid);
      if (hcmd != null) {
        replaceInFile(heapfn, hcmd, this.commentPrefix + hcmd);
      }
    }
  }

  private String getStopPIDCmd(HostDescription hd, int pid) {
    String cmd = getShutdownCommand(hd.getHostName(), pid);
    return wrapCommand(cmd, hd);
  }

  /**
   * The Reboot API fetches the nuke command for removing hostagent PIDs from
   * the nukerun script before rebooting, since the hostagent is needed to
   * produce the command after rebooting.
   */
  protected String getNukePIDCmd(HostDescription hd, int pid) {
    String cmd = getKillCommand(hd.getHostName(), pid);
    return wrapCommand(cmd, hd);
  }

  private String getDumpLocksCmd(HostDescription hd, int pid ) {
    String cmd = getDumpLocksCommand(hd.getHostName(), pid);
    return wrapCommand(cmd, hd);
  }

  private String getDumpPIDCmd(HostDescription hd, int pid ) {
    String cmd = getPrintStacksCommand(hd.getHostName(), pid);
    return wrapCommand(cmd, hd);
  }

  private String getDumpHeapPIDCmd(HostDescription hd, int pid) {
    String options = TestConfig.tab().stringAt(Prms.jmapHeapDumpOptions, null);
    String[] cmdarr = getDumpHeapCommand(hd.getHostName(), pid,
                                         hd.getUserDir(), options);
    String cmd = null;
    if (cmdarr != null) {
      cmd = "";
      for (int i = 0; i < cmdarr.length; i++) {
        cmd += cmdarr[i];
        if (i < cmdarr.length - 1) {
          cmd += " ";
        }
      }
    }
    return wrapCommand(cmd, hd);
  }

  private String getShutdownCommand(String host, int pid) {
    if (HostHelper.isLocalHost(host)) {
      return platform.getShutdownCommand(pid);
    } else {
      try {
        return HostAgentMgr.getHostAgent(host).getShutdownCommand(pid);
      } catch (RemoteException e) {
        String s = "Unable to complete operation using hostagent on " + host;
        throw new HydraRuntimeException(s, e);
      }
    }
  }

  private String getKillCommand(String host, int pid) {
    if (HostHelper.isLocalHost(host)) {
      return platform.getKillCommand(pid);
    } else {
      try {
        return HostAgentMgr.getHostAgent(host).getKillCommand(pid);
      } catch (RemoteException e) {
        String s = "Unable to complete operation using hostagent on " + host;
        throw new HydraRuntimeException(s, e);
      }
    }
  }

  private String getDumpLocksCommand(String host, int pid) {
    if (HostHelper.isLocalHost(host)) {
      return platform.getDumpLocksCommand(pid);
    } else {
      try {
        return HostAgentMgr.getHostAgent(host).getDumpLocksCommand(pid);
      } catch (RemoteException e) {
        String s = "Unable to complete operation using hostagent on " + host;
        throw new HydraRuntimeException(s, e);
      }
    }
  }

  private String getPrintStacksCommand(String host, int pid) {
    if (HostHelper.isLocalHost(host)) {
      return platform.getPrintStacksCommand(pid);
    } else {
      try {
        return HostAgentMgr.getHostAgent(host).getPrintStacksCommand(pid);
      } catch (RemoteException e) {
        String s = "Unable to complete operation using hostagent on " + host;
        throw new HydraRuntimeException(s, e);
      }
    }
  }

  private String[] getDumpHeapCommand(String host, int pid, String userDir,
                                    String options) {
    if (HostHelper.isLocalHost(host)) {
      return platform.getDumpHeapCommand(pid, userDir, options);
    } else {
      try {
        return HostAgentMgr.getHostAgent(host)
                           .getDumpHeapCommand(pid, userDir, options);
      } catch (RemoteException e) {
        String s = "Unable to complete operation using hostagent on " + host;
        throw new HydraRuntimeException(s, e);
      }
    }
  }

  protected String wrapCommand(String cmd, HostDescription hd) {
    if (cmd != null) {
      if (HostHelper.isLocalHost(hd.getHostName())) {
        return isWindows(hd) ? "start /WAIT " + cmd : cmd;
      } else if (canUseSSH(hd)) {
        return Platform.SSH + " " + hd.getHostName() + " " + cmd;
      } else {
        String wcmd = isWindows(hd) ? wrapCommandAsArg(cmd) : cmd;
        return getBootstrapperClientCmd(hd) + " \"" + wcmd + "\"";
      }
    }
    return null;
  }

  private boolean canUseSSH(HostDescription hd) {
    return this.mhd.getOSType() == OSType.unix && hd.getOSType() == OSType.unix;
  }

  private boolean isWindows(HostDescription hd) {
    return hd.getOSType() == OSType.windows;
  }

  private String getBootstrapperClientCmd(HostDescription hd) {
    return System.getProperty("java.home")
                 + File.separator + "bin" + File.separator + "java "
                 + "-classpath " + System.getProperty("java.class.path")
                 + " hydra.BootstrapperClient " + getURL(hd);
  }

  private String getURL(HostDescription hd) {
    return "rmi://" + hd.getHostName() + ":" + hd.getBootstrapPort()
              + "/" + Bootstrapper.RMI_NAME;
  }

  private String wrapCommandAsArg(String cmd) {
    String s = cmd;
    s = s.replace("\\", "\\\\");
    s = winQuote(s);
    return s;
  }

  // The two methods below were copied from http://bugs.sun.com/bugdatabase/view_bug.do;jsessionid=43212fa123adfaffffffffbf848cf1ca30051?bug_id=6511002

  private String winQuote(String s) {
    if (!needsQuoting(s))
      return s;
    s = s.replaceAll("([\\\\]*)\"", "$1$1\\\\\"");
    s = s.replaceAll("([\\\\]*)\\z", "$1$1");
    return s;
  }

  private boolean needsQuoting(String s) {
    int len = s.length();
    if (len == 0) // empty string have to be quoted
      return true;
    for (int i = 0; i < len; i++) {
      switch (s.charAt(i)) {
      case ' ': case '\t': case '\\': case '"':
        return true;
      }
    }
    return false;
  }

  /**
   *  Replaces the text line starting with the specified prefix with the new text.
   *  This relies on the fact that the new file will be longer than the old file.
   *  The whole file cannot be replaced without losing its file permissions.
   */
  private void replaceInFile( String fn, String prefix, String newtxt )
  {
    try {
      BufferedReader br = new BufferedReader( new FileReader( fn ) );
      StringBuffer buf = new StringBuffer();
      String line = null;
      while ( ( line = br.readLine() ) != null ) {
        if ( line.startsWith( prefix ) ) {
          line = newtxt;
        }
        buf.append( line + "\n" );
      }
      br.close();

      FileUtil.writeToFile( fn, buf.toString() );
    } catch( FileNotFoundException e ) {
      throw new HydraRuntimeException( "Unable to find file: " + fn, e );
    } catch( IOException e ) {
      throw new HydraRuntimeException( "Unable to modify file: " + fn, e );
    }
  }
}
