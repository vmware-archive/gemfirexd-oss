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
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import hydra.HostHelper.OSType;
import java.io.*;
import java.rmi.RemoteException;
import java.util.*;

/**
 * Provides support for command-line cache servers managed by hydra clients.
 * Automatically manages server PIDs in the nukerun.sh script so they are nuked
 * when the test is nuked.
 * <p>
 * Each cache server must be given a unique name.  Methods in this class assume
 * but do not enforce this and are not thread-safe.
 */
public class CacheServerHelper {

//------------------------------------------------------------------------------
// CacheServer
//------------------------------------------------------------------------------

  /**
   * Starts a command-line cache server with the given name on the local host,
   * configured using the XML file.  Sets the system directory to the same name
   * in the user directory of the creator.  The name must be unique test-wide.
   *
   * @throws HydraRuntimeException if the cache server does not start.
   */
  public static void startCacheServer(String name, String cacheXmlFile) {
    startCacheServer(name, cacheXmlFile, null);
  }

  /**
   * Starts a command-line cache server with the given name on the local host,
   * configured using the XML file and optional extra arguments.  Sets the
   * system directory to the same name in the user directory of the creator.
   * The name must be unique test-wide.
   *
   * @throws HydraRuntimeException if the cache server does not start.
   */
  public static void startCacheServer(String name, String cacheXmlFile,
                                                   String[] extraArgs) {
    // check arguments
    if (name == null) {
      throw new IllegalArgumentException("name cannot be null");
    }
    if (cacheXmlFile == null) {
      throw new IllegalArgumentException("cacheXmlFile cannot be null");
    }
    log("Starting cache server: " + name);

    String clientName = RemoteTestModule.getMyClientName();
    ClientDescription cd = TestConfig.getInstance()
                                     .getClientDescription(clientName);
    // build the cmd
    String cmd = getStartCommand(name, cd, cacheXmlFile, extraArgs);

    // start the cache server and verify the result
    String result = ProcessMgr.fgexec(cmd, 300);
    log(result);
    int pid = verifyStart(name, result);

    // record info
    Record record = putRecord(name, cd.getVmDescription().getHostDescription(),
                              pid);
    recordPID(record);
    log("Started cache server: " + record);
  }

  /**
   * Kills the cache server with the given name with a mean kill.
   *
   * @throws HydraRuntimeException if the cache server is not running.
   * @throws HydraTimeoutException if the kill takes more than 300 seconds.
   */
  public static void killCacheServer(String name) {
    Record record = getRecord(name);
    log("Killing cache server: " + record);

    // kill the cache server
    if (!ProcessMgr.killProcessWait(record.getHost(), record.getPID(), 300)) {
      String s = "Timed out killing cache server: " + record;
      throw new HydraTimeoutException(s);
    }

    // remove info
    removeRecord(name);
    removePID(record);
    log("Killed cache server: " + name);
  }

  /**
   * Stops the cache server with the given name.
   *
   * @throws HydraRuntimeException if the cache server is not running or did
   *         not stop.
   */
  public static void stopCacheServer(String name) {
    String clientName = RemoteTestModule.getMyClientName();
    HostDescription hd = TestConfig.getInstance()
                                   .getClientDescription(clientName)
                                   .getVmDescription().getHostDescription();
    Record record = getRecord(name);
    log("Stopping cache server: " + record);

    // build the cmd
    String cmd = getStopCommand(name, hd);

    // stop the cache server and verify the result
    String result = ProcessMgr.fgexec(cmd, 300);
    log(result);
    verifyStop(name, result);

    // record info
    removeRecord(name);
    removePID(record);
    log("Stopped cache server: " + name);
  }

//------------------------------------------------------------------------------
// Command-line construction
//------------------------------------------------------------------------------

  /**
   * Returns the command line for a "start" operation on the given cache server.
   */
  public static String getStartCommand(String name, ClientDescription cd,
                               String cacheXmlFile, String[] extraArgs) {
    VmDescription vmd = cd.getVmDescription();
    HostDescription hd = vmd.getHostDescription();

    String cmd = getCacheServerCommand(hd) + " start";

    // give the host and port of the master RMI registry
    cmd += " -J-D" + MasterController.RMI_HOST_PROPERTY + "="
         + System.getProperty(MasterController.RMI_HOST_PROPERTY)
         + " -J-D" + MasterController.RMI_PORT_PROPERTY + "="
         + System.getProperty(MasterController.RMI_PORT_PROPERTY);

    // add any extra args first
    if (extraArgs != null) {
      for (int i = 0; i < extraArgs.length; i++) {
        cmd += " " + extraArgs[i];
      }
    }
    // add xml file
    cmd += " " + DistributionConfig.CACHE_XML_FILE_NAME + "="
        + FileUtil.absoluteFilenameFor(EnvHelper.expandEnvVars(cacheXmlFile));

    // add locator/multicast specification
    GemFireDescription gfd = cd.getGemFireDescription();
    String ds = gfd.getDistributedSystem();
    cmd += " " + DistributionConfig.LOCATORS_NAME
        + "=" + getLocators(ds);
    cmd += " " + DistributionConfig.MCAST_PORT_NAME
        + "=" + gfd.getMcastPort().intValue();
    cmd += " " + DistributionConfig.MCAST_ADDRESS_NAME
        + "=" + gfd.getMcastAddress();

    // add system directory and archiving
    String dir = getSystemDirectory(name, hd);
    cmd += " " + DistributionConfig.LOG_FILE_NAME
        + "=" + dir + "/system.log";
    cmd += " " + DistributionConfig.STATISTIC_ARCHIVE_FILE_NAME
        + "=" + dir + "/" +  gfd.getStatisticArchiveFile();
    cmd += " " + DistributionConfig.STATISTIC_SAMPLING_ENABLED_NAME
        + "=" + gfd.getStatisticSamplingEnabled();
    cmd += " -dir=" + dir;

    // add classpath
    cmd += " -classpath=" + vmd.getClassPath();

    return cmd;
  }

  /**
   * Returns the locators for the given distributed system.
   */
  private static String getLocators(String ds) {
    if (TestConfig.tab().booleanAt(Prms.manageLocatorAgents)) {
      return TestConfig.getInstance().getMasterDescription().getLocator(ds);
    } else {
      List endpoints = DistributedSystemHelper.getEndpoints(ds);
      return DistributedSystemHelper.endpointsToString(endpoints);
    }
  }

  /**
   * Returns the command line for a "stop" operation on the given cache server.
   */
  public static String getStopCommand(String name, HostDescription hd) {
    return getCacheServerCommand(hd) + " stop -dir="
         + getSystemDirectory(name, hd);
  }

  /**
   * Returns the proper cacheserver command for the host.
   */
  private static String getCacheServerCommand(HostDescription hd) {
    String cmd = null;
    char sep = hd.getFileSep();
    switch (hd.getOSType()) {
      case windows:
        cmd = "cmd /c "
            + "set GF_JAVA=" + hd.getJavaHome() + sep + "bin" + sep + "java.exe"
            + " && cmd /c "
            + hd.getGemFireHome() + sep + "bin" + sep + "cacheserver.bat";
        break;
      case unix:
        cmd = "env GF_JAVA=" + hd.getJavaHome() + sep + "bin" + sep + "java "
            + hd.getGemFireHome() + sep + "bin" + sep + "cacheserver";
        break;
    }
    return cmd;
  }

  /**
   * Returns the cache server system directory, creating it if needed.
   */
  private static String getSystemDirectory(String name, HostDescription hd) {
    String sysdir = hd.getUserDir() + hd.getFileSep() + name;
    FileUtil.mkdir(new File(sysdir));
    try {
      RemoteTestModule.Master.recordDir(hd, name, sysdir);
    } catch (RemoteException e) {
      String s = "Unable to record system directory with master: " + sysdir;
      throw new HydraRuntimeException(s, e);
    }
    return sysdir;
  }

//------------------------------------------------------------------------------
// Result validation
//------------------------------------------------------------------------------

  /**
   * Reads the result of starting the cache server with the given name.
   * Verifies that it is running and returns the PID.
   */
  private static int verifyStart(String name, String result) {
    BufferedReader reader = new BufferedReader(new StringReader(result));
    try {
      boolean running = false;
      int pid = -1;

      // read the result
      String line = null;
      while ((line = reader.readLine()) != null) {
        StringTokenizer t = new StringTokenizer(line, " ", false);
        while (t.hasMoreTokens()) {
          String token = t.nextToken();
          if (token.equals("status:")) {
            try {
              token = t.nextToken();
              if (token.equals("running")) {
                running = true;
              } else {
                String s = "Cache server " + name + " did not start: " + token;
                throw new HydraRuntimeException(s);
              }
            } catch (NoSuchElementException e) {
              String s = "Missing value for cache server status: " + name;
              throw new HydraRuntimeException(s, e);
            }
          } else if (token.equals("pid:")) {
            try {
              token = t.nextToken();
              pid = Integer.parseInt(token);
            } catch (NumberFormatException e) {
              String s = "Cache server " + name + " has invalid PID: " + token;
              throw new HydraRuntimeException(s, e);
            } catch (NoSuchElementException e) {
              String s = "Cache server " + name + " missing PID";
              throw new HydraRuntimeException(s, e);
            }
          }
        }
      }
      // done reading, so check that all is well
      if (!running || pid == -1) {
        String s = "Cache server " + name + " did not start properly";
        throw new HydraRuntimeException(s);
      }
      return pid;

    } catch (IOException e) {
      throw new HydraRuntimeException("Problem with reader", e);
    } finally {
      try {
        reader.close();
      } catch (IOException e) {}
    }
  }

  /**
   * Reads the result of stopping the cache server with the given name.
   * Verifies that it is stopped.
   */
  private static void verifyStop(String name, String result) {
    BufferedReader reader = new BufferedReader(new StringReader(result));
    try {
      boolean stopped = false;

      // read the result
      String line = null;
      while ((line = reader.readLine()) != null) {
        StringTokenizer t = new StringTokenizer(line, " ", false);
        while (t.hasMoreTokens()) {
          String token = t.nextToken();
          if (token.startsWith("stopped")) {
            stopped = true;
          }
        }
      }
      // done reading, so check that all is well
      if (!stopped) {
        String s = "Cache server " + name + " did not stop";
        throw new HydraRuntimeException(s);
      }

    } catch (IOException e) {
      throw new HydraRuntimeException("Problem with reader", e);
    } finally {
      try {
        reader.close();
      } catch (IOException e) {}
    }
  }

//------------------------------------------------------------------------------
// Record
//------------------------------------------------------------------------------

  /**
   * Puts the cache server record into the shared {@link CacheServerBlackboard}
   * map.
   */
  private static Record putRecord(String name, HostDescription hd, int pid) {
    Record record = new Record(hd, pid);
    CacheServerBlackboard.getInstance().getSharedMap().put(name, record);
    return record;
  }

  /**
   * Gets the cache server record from the shared {@link CacheServerBlackboard}
   * map.
   * @throws HydraRuntimeException if the record is not found.
   */
  private static Record getRecord(String name) {
    Record record = (Record)CacheServerBlackboard.getInstance().getSharedMap()
                                                               .get(name);
    if (record == null) {
      String s = "Cache server record not found: " + name;
      throw new HydraRuntimeException(s);
    }
    return record;
  }

  /**
   * Removes the cache server record from the shared {@link
   * CacheServerBlackboard} map, if it exists.
   */
  private static void removeRecord(String name) {
    CacheServerBlackboard.getInstance().getSharedMap().remove(name);
  }

  /**
   * Represents the runtime record for a cache server.
   */
  public static class Record implements Serializable {
    HostDescription hd;
    int pid;

    /**
     * Creates a record for a cache server.
     *
     * @param hd the cache server host description.
     * @param pid the cache server pid.
     */
    public Record(HostDescription hd, int pid) {
      this.hd = hd;
      this.pid = pid;
    }

    /**
     * Returns the cache server host description.
     */
    public HostDescription getHostDescription() {
      return this.hd;
    }

    /**
     * Returns the cache server host.
     */
    public String getHost() {
      return this.hd.getHostName();
    }

    /**
     * Returns the cache server O/S.
     */
    public OSType getOSType() {
      return this.hd.getOSType();
    }

    /**
     * Returns the cache server PID.
     */
    public int getPID() {
      return this.pid;
    }

    /**
     * Returns the record as a string.
     */
    public String toString() {
      return this.getHost() + ":" + this.pid + "(" + this.getOSType() + ")";
    }
  }

//------------------------------------------------------------------------------
// PID management
//------------------------------------------------------------------------------

  /**
   * Adds the cache server to nukerun.sh.
   */
  private static void recordPID(Record record) {
    try {
      RemoteTestModule.Master.recordPID(record.getHostDescription(),
                                        record.getPID());
    } catch (RemoteException e) {
      String s = "Failed to remove PID from nukerun script: " + record;
      throw new HydraRuntimeException(s);
    }
  }

  /**
   * Removes the cache server from nukerun.sh.
   */
  private static void removePID(Record record) {
    try {
      RemoteTestModule.Master.removePID(record.getHostDescription(),
                                        record.getPID());
    } catch (RemoteException e) {
      String s = "Failed to remove PID from nukerun script: " + record;
      throw new HydraRuntimeException(s);
    }
  }

//------------------------------------------------------------------------------
// Log
//------------------------------------------------------------------------------

  private static LogWriter log;
  private static synchronized void log(String s) {
    if (log == null) {
      log = Log.getLogWriter();
    }
    if (log.infoEnabled()) {
      log.info(s);
    }
  }
}

