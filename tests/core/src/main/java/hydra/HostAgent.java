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
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import hydra.HostHelper.OSType;
import hydra.timeserver.TimeProtocolHandler;
import hydra.timeserver.TimeServerPrms;
import hydra.timeserver.TimeProtocolHandler.SkewData;
import java.io.File;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.rmi.NoSuchObjectException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Arrays;
import java.util.Properties;
import util.TestHelper;

/**
 *
 * Instances of this class represent remote host agents
 * for master controller in hydra multiuser tests.
 *
 */
public class HostAgent
    extends UnicastRemoteObject
    implements HostAgentIF, Runnable { 

  /** The vm host description */ 
  private static HostDescription MyHostDescription = null;

  /** The vm host */ 
  private static String MyHost = null;

  /** The vm process id */ 
  private static int MyPid = -1;

  /** The thread name */ 
  private static String MyThreadName = null;

  /**  Stub reference to a remote master controller */
  private static MasterProxyIF Master = null;

  /** The platform-dependent object to which operations are delegated. */
  private Platform platform = null;

  //////////////////////////////////////////////////////////////////////////////
  ////    CONSTRUCTORS                                                      ////
  //////////////////////////////////////////////////////////////////////////////

  private HostAgent() throws RemoteException {
    super();
    platform = Platform.getInstance();
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    STARTUP                                                           ////
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  Initializes and registers with the master controller. 
   */
  public void run() {
    Thread.currentThread().setName( MyThreadName );

    TestConfig tc = TestConfig.getHostAgentInstance();

    // make sure the host is in sync with the master
    synchronizeWithTimeServer(tc);

    // start the statistics monitor
    StatMonitor.start(MyHostDescription, Master, tc);

    HostAgentRecord har = new HostAgentRecord(MyHostDescription, MyPid, this);

    try {
      synchronized( Master ) {
        Master.registerHostAgent( har );
      }
    } catch( RemoteException e ) {
      throw new HydraRuntimeException( MyThreadName + " unable to contact master", e );
    }
    logStatus();
  }

  /**
   * Starts the statistics archiver.
   */
  private static String startStatArchiver() {
    // connect to loner distributed system with a statistics sampler and archive
    Properties p = new Properties();
    String dirName = System.getProperty("user.dir") + "/" + MyThreadName;
    FileUtil.mkdir(new File(dirName));
    try {
      Master.recordDir(MyHostDescription, "hostagent", dirName);
    } catch (RemoteException e) {
      String s = "Unable to record system directory with master: " + dirName;
      throw new HydraRuntimeException(s, e);
    }
    p.setProperty(DistributionConfig.STATISTIC_ARCHIVE_FILE_NAME,
                  dirName + "/statArchive.gfs");
    p.setProperty(DistributionConfig.STATISTIC_SAMPLING_ENABLED_NAME, "true");
    p.setProperty(DistributionConfig.LOCATORS_NAME, "");
    p.setProperty(DistributionConfig.MCAST_PORT_NAME, String.valueOf(0));

    DistributedSystem.connect(p);
    return dirName;
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    RUNTIME: PROCESS MANAGEMENT                                       ////
  //////////////////////////////////////////////////////////////////////////////

  public int bgexec( String command, File workdir, File logfile )
  throws RemoteException {
    log().info("Executing bgexec command: " + command);
    int pid;
    try {
      pid = platform.bgexec( command, workdir, logfile );
    } catch( IOException e ) {
      throw new HydraRuntimeException( "Unable to complete operation", e );
    }
    log().info("Executed bgexec command: " + command);
    return pid;
  }

  public String fgexec(String command, int maxWaitSec)
  throws RemoteException {
    log().info("Executing fgexec command: " + command);
    String output = platform.fgexec(command, maxWaitSec);
    log().info("Executed fgexec command: " + command);
    return output;
  }

  public String fgexec(String command, String[] envp, int maxWaitSec)
  throws RemoteException {
    log().info("Executing fgexec command: " + command + " with envp " + Arrays.asList(envp));
    String output = platform.fgexec(command, envp, maxWaitSec);
    log().info("Executed fgexec command: " + command + " with envp " + Arrays.asList(envp));
    return output;
  }

  public String fgexec(String[] command, int maxWaitSec)
  throws RemoteException {
    log().info("Executing fgexec command: " + platform.getString(command));
    String output = platform.fgexec(command, maxWaitSec);
    log().info("Executed fgexec command: " + platform.getString(command));
    return output;
  }

  public boolean exists( int pid ) throws RemoteException {
    return platform.exists( pid );
  }

  public void shutdown( int pid ) throws RemoteException {
    log().info( "Executing shutdown on process:" + pid );
    platform.shutdown( pid );
    log().info( "Executed shutdown on process:" + pid );
  }
  public void kill( int pid ) throws RemoteException {
    log().info( "Executing kill on process:" + pid );
    platform.kill( pid );
    log().info( "Executed kill on process:" + pid );
  }
  public void printStacks( int pid ) throws RemoteException {
    log().info( "Executing stack dump on process:" + pid );
    platform.printStacks( pid );
    log().info( "Executed stack dump on process:" + pid );
  }
  public void dumpHeap( int pid, String userDir, String options )
  throws RemoteException {
    log().info( "Executing heap dump on process:" + pid );
    platform.dumpHeap( pid, userDir, options );
    log().info( "Executed heap dump on process:" + pid );
  }

  public String getShutdownCommand(int pid) throws RemoteException {
    return platform.getShutdownCommand(pid);
  }
  public String getKillCommand(int pid) throws RemoteException {
    return platform.getKillCommand(pid);
  }
  public String getDumpLocksCommand(int pid) throws RemoteException {
    return platform.getDumpLocksCommand(pid);
  }
  public String getPrintStacksCommand(int pid) throws RemoteException {
    return platform.getPrintStacksCommand(pid);
  }
  public String[] getDumpHeapCommand(int pid, String userDir, String options)
  throws RemoteException {
    return platform.getDumpHeapCommand(pid, userDir, options);
  }
  public String getNetcontrolCommand(String target, int op)
  throws RemoteException {
    return platform.getNetcontrolCommand(target, op);
  }
  public String getProcessStatus(int maxWaitSec)
  throws RemoteException {
    return platform.getProcessStatus(maxWaitSec);
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    HOST STATUS
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Record the status of the local host (such as "ps" and "free").
   */
  private void logStatus() {
    log().info("Logging host status");
    String status = "";
    try {
      status += ProcessMgr.getProcessStatus(60);
    } catch (HydraRuntimeException e) {
      log().warning("Unable to log process status: " + TestHelper.getStackTrace());
    } catch (HydraTimeoutException e) {
      log().warning("Unable to log process status: " + TestHelper.getStackTrace());
    }
    try {
      status += ProcessMgr.getMemoryStatus(60);
    } catch (HydraRuntimeException e) {
      log().warning("Unable to log memory status: " + TestHelper.getStackTrace());
    } catch (HydraTimeoutException e) {
      log().warning("Unable to log memory status: " + TestHelper.getStackTrace());
    }
    String dir = System.getProperty("user.dir");
    String fn = dir + File.separator + "hoststats_" + MyHost + ".txt";
    FileUtil.appendToFile(fn, status);
    log().info("Logged host status");
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    TIME MANAGEMENT
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Synchronizes the clock on this host with the time server to within the
   * configured tolerance, {@link Prms.clockSkewThresholdMs}, using NTP.
   */
  private void synchronizeWithTimeServer(TestConfig tc) {
    // get the maximum allowed clock skew
    long maxSkew = TimeServerPrms.clockSkewThresholdMs(tc);
    if (maxSkew <= 0) {
      return;
    }
    String serverHost = tc.getMasterDescription().getTimeServerHost();
    int serverPort    = tc.getMasterDescription().getTimeServerPort();
    boolean throwError = TimeServerPrms.errorOnExceededClockSkewThreshold(tc);


    log().info("Checking clock skew...");

    TimeProtocolHandler handler = null;

    try {
      handler = new TimeProtocolHandler(serverHost, serverPort, (int) maxSkew, false);

      // check within tolerance, otherwise adjust by restarting NTP
      SkewData data = checkSkew(handler, maxSkew);

      if(data == null || data.getLatency() > maxSkew)
      {
        String s = "Time server is slow or unresponsive, unable to check time";
        if (throwError) {
          throw new HydraRuntimeException(s);
        } else {
          log().warning(s);
          return;
        }
      }

      long currentSkew = data.getSkew();
      log().info("Clock skew is " + currentSkew + " +/- " + data.getLatency());

      if(currentSkew > maxSkew) {
        // use NTP to synchronize local clock
        String maxSkewKey = BasePrms.nameForKey(TimeServerPrms.clockSkewThresholdMs);
        log().info("Clock skew (" + currentSkew + " ns)" + " exceeds "
            + maxSkewKey + " (" + maxSkew + " ns), correcting...");
        try {
          platform.restartNTP();
        }
        catch (VirtualMachineError e) {
          SystemFailure.initiateFailure(e);
          throw e;
        }
        catch (Throwable t) { // do not cause failure here, just move along
          log().warning("Problem checking NTP\n" + TestHelper.getStackTrace(t));
        }

        data = checkSkew(handler, maxSkew);

        if(data == null || data.getLatency() > maxSkew)
        {
          String s = "Time server is slow or unresponsive, unable to check time";
          if (throwError) {
            throw new HydraRuntimeException(s);
          } else {
            log().warning(s);
            return;
          }
        }

        currentSkew = data.getSkew();

        if (currentSkew > maxSkew) {
          String s = "Clock skew (" + currentSkew + " ns)" + " exceeds "
          +  maxSkewKey + " (" + maxSkew + " ns) "
          + "despite attempt at correction";
          if (throwError) {
            throw new HydraRuntimeException(s);
          } else {
            log().warning(s);
          }
        } else {
          log().info("Clock skew is within "
              + maxSkewKey + " (" + maxSkew + " ns)");
        }
      }
    } catch(Exception e) {
      log().warning("Error synchronizing time with master controller", e);
    } finally {
      if(handler != null) {
        handler.close();
      }
    }
  }

  private static SkewData checkSkew(TimeProtocolHandler handler, long maxSkew)
  {
    SkewData data = null;
    try {
      for(int i = 0; i < 3 && (data == null || data.getLatency() > maxSkew); i++)
      {
        try
        {
          data = handler.checkSkew();
        } catch(SocketTimeoutException e)
        {
          //try again;
        }
      }
    } catch (IOException e) {
      log().warning("Error communicating with time server" + TestHelper.getStackTrace(e));
    }

    return data;
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    SHUTDOWN                                                          ////
  //////////////////////////////////////////////////////////////////////////////

  public void shutDownHostAgent() throws RemoteException {
    log().info( MyThreadName + " shutting down at master's request." );
    try {
      Master.removePIDNoDumps(MyHostDescription, MyPid);
      logStatus();
    } catch (Throwable t) {
      log().severe("Unable to remove PID " + MyPid + "\n"
                  + TestHelper.getStackTrace(t));
    } finally {
      shutdown();
    }
  }

  private void shutdown() {
    // take special care to shut down in a way that prevents the master rmi
    // call from hanging on windows when the agent has used sendsigbreak
    // (which seems to be the only case where System.exit alone fails to work)
    try {
      UnicastRemoteObject.unexportObject(this, false);
    } catch (NoSuchObjectException e) {
      throw new HydraRuntimeException("Could not unexport object, quiting anyway", e);
    }
    new Thread() {
      @Override
      public void run() {
        try {
          Thread.sleep(2000);
        } catch (InterruptedException e) {
          // I don't care
        }
        log().info( "Now terminating process." );
        System.exit(0);
      }
    }.start();
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    MISC                                                              ////
  //////////////////////////////////////////////////////////////////////////////

  private static LogWriter log() {
    return Log.getLogWriter();
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    MAIN                                                              ////
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  The startup routine for this host agent.  Creates a log file, looks up
   *  the master controller, and starts a thread to wait for requests.
   */
  public static void main(String[] args) {

    // note the info for this vm
    MyHost = HostHelper.getLocalHost();
    MyPid = ProcessMgr.getProcessId();
    MyThreadName = "hostagent_" + MyHost + "_" + MyPid;
    Thread.currentThread().setName( MyThreadName );

    // make sure the user directory has been created
    FileUtil.mkdir(System.getProperty("user.dir"));

    // create the log
    LogWriter log = Log.createLogWriter( MyThreadName, MyThreadName, "all", System.getProperty( "user.dir" ), true );
    Log.setLogWriterLevel(TestConfig.getHostAgentInstance().getParameters()
                                    .stringAt(Prms.logLevelForHostAgent));

    // log info for this vm
    log.info( ProcessMgr.processAndBuildInfoString() );

    // do RMI lookup of Master
    Master = RmiRegistryHelper.lookupMaster();

    // create the host agent
    log.info("Starting host agent...");
    MyHostDescription = TestConfig.getHostAgentInstance()
                         .getAnyPhysicalHostDescription(MyHost);
    HostAgent agent = null;
    try {
      agent = new HostAgent();
    } catch( RemoteException e ) {
      throw new HydraRuntimeException( "Unable to create HostAgent", e );
    }
    new Thread( agent ).start();

    if (Boolean.getBoolean("archiveStats")) {
      String sysdir = startStatArchiver();
      log.info("Started host agent with statistics archive in " + sysdir);
    } else {
      log.info("Started host agent");
    }
  }
}
