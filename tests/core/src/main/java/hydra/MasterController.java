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

import hydra.log.LogPrms;
import hydra.timeserver.*;

import java.lang.reflect.Method;
import java.io.*;
import java.rmi.*;
import java.rmi.registry.Registry;
import java.rmi.server.RMISocketFactory;
import java.util.*;

import perffmwk.*;

/**
 *  This class acts as the master controller for hydra
 *  tests.  It has overall responsibility for parsing config files,
 *  starting clients, scheduling test jobs, and so on.
 *  <p>
 *  Run a hydra test by first selecting a configuration file.  To override
 *  hydra configuration parameters set in the file, create a "local.conf"
 *  file in the directory where the master will be run.
 *  <p>
 *  Usage:
 *  <blockquote><pre>
 *    java -server -DJTESTS=&lt;path_to_test_classes&gt;
 *         -Dgemfire.home=&lt;path_to_gemfire_product_tree&gt;
 *         -DconfigFileName=&lt;absolute_path_to_hydra_configuration_file&gt;
 *          [optional_test_specific_system_properties]
 *         hydra.MasterController
 *  </pre></blockquote>
 *  <p>
 *  Example:
 *  <blockquote><pre>
 *     java -server 
 *          -classpath $GEMFIRE/lib/gemfire.jar:$JTESTS:$JTESTS/junit.jar
 *          -DJTESTS=$JTESTS
 *          -Dgemfire.home=$GEMFIRE
 *          -DconfigFileName=$JTESTS/hydra/samples/connclient.bt
 *          -DscalingFactorForMyTest=20
 *          hydra.MasterController
 *  </pre></blockquote>
 *  <p>
 *  Test-specific system properties must be specified for all system
 *  properties used in the test configuration file, if any.  See
 *  <A href="hydra_grammar.txt">hydra_grammar.txt</A>,
 *  <A href="../batterytest/batterytest_grammar.txt">batterytest_grammar.txt</A>,
 *  and {@link batterytest.BatteryTest} for more details.
 *  <p>
 *  The test will generate log files in the directory where the master
 *  controller was started.  Master*.log is the primary log for the master.
 *  During the actual test execution it logs to taskmaster*.log.  At the end
 *  of the test, the Master*.log will indicate whether the test passed or
 *  failed.
 *  <p>
 *  See {@link batterytest.BatteryTest} for a convenient alternative for
 *  running suites of hydra tests.
 */
public class MasterController {

  /** Register the SocketFactory here to insure it is used for all RMI instances */
  static {
    try {
      RMISocketFactory.setSocketFactory(new RMIHydraSocketFactory());
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }
  
  public static final String RMI_NAME = "master";
  public static final String RMI_HOST_PROPERTY = "rmiHost";
  public static final String RMI_PORT_PROPERTY = "rmiPort";

  /** Log for this vm */
  public static LogWriter log;

  private static final boolean parseOnly = Boolean.getBoolean("parseOnly");

  //////////////////////////////////////////////////////////////////////////////
  //                            SLEEPING                                      //
  //////////////////////////////////////////////////////////////////////////////

  /**
  *
  * Sleep "soundly" for msToSleep milliseconds.
  *
  */
  public static void sleepForMs(int msToSleep) {
    if (msToSleep != 0) {
      try {
        Thread.sleep(msToSleep);
      }
      catch (InterruptedException ex) {
      }
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  //                             NAMING                                       //
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  Used to name log files and threads.
   */
  protected static String getNameFor( int vmid, int tid, String clientName,
                                      String host, int pid ) {
    StringBuffer buf = new StringBuffer( 50 );
    buf.append( "vm_" ).append( vmid );
    if ( tid >= 0 ) {
      buf.append( "_thr_" ).append( tid );
    }
    buf.append( "_" ).append( clientName )
       .append( "_" ).append( host );
    if ( pid >= 0 ) {
      buf.append( "_" ).append( pid );
    }
    return buf.toString();
  }

  //////////////////////////////////////////////////////////////////////////////
  //                             VALUE COMMUNICATION                          //
  //////////////////////////////////////////////////////////////////////////////

  /**
  *
  * Return the ConfigHashtable that contains hydra parameters parsed
  * from the hydra configuration file.
  *
  */
  private static ConfigHashtable tab() {
    return TestConfig.getInstance().getParameters();
  }

  /**
  *
  * Invokes the specified method on an instance of the specified class.  Intended
  * for use by client threads.  Note that the invocation is synchronized across
  * clients, so this is suitable for things like lazily creating shared blackboards.
  *
  */
  public static synchronized Object invoke( String classname, String methodname) {
    MethExecutorResult result = MethExecutor.executeInstance(classname, methodname);
    if (result.getStackTrace() != null)
      throw new HydraRuntimeException(result.toString());
    return result.getResult();
  }

  /**
  *
  * Invokes the specified method on an instance of the specified class.  Intended
  * for use by client threads.  Note that the invocation is synchronized across
  * clients, so this is suitable for things like lazily creating shared blackboards.
  *
  */
  public static synchronized Object invoke( String classname, String methodname,
                                            Class[] types, Object[] args ) {
    log.info( "Invoking " + classname + "." + methodname );
    MethExecutorResult result = MethExecutor.executeInstance( classname, methodname, types, args );
    if ( result.getStackTrace() != null )
      throw new HydraRuntimeException( result.toString() );
    return result.getResult();
  }

  //////////////////////////////////////////////////////////////////////////////
  //                            TASK MANAGEMENT                               //
  //////////////////////////////////////////////////////////////////////////////

  /**
  * Does unit tests using the client descriptions.
  * Starts and stops the clients.  Does nothing if there are no tests.
  */
  private static boolean doUnitTests() {
    TestConfig tc = TestConfig.getInstance();
    Vector inittasks = tc.getInitTasks();
    TaskScheduler ts = new SimpleConcurrentTaskScheduler
                       ( "UnitTests", inittasks, null );
    ts.createClients( tc.getClientDescriptions(), tc.getThreadGroups() );
    ts.startClients();
    boolean passed = ts.executeTasks( tab().booleanAt( Prms.haltIfBadResult ),
                                      tab().longAt( Prms.maxResultWaitSec ) );
    ts.stopClients();
    ts.printReport();
    return passed;
  }
  /**
  * Does STARTTASK tasks using the client descriptions.
  * Starts and stops the clients.  Does nothing if there are no tasks.
  */
  private static boolean doStartTasks() {
    TestConfig tc = TestConfig.getInstance();
    Vector starttasks = tc.getStartTasks();
    if ( starttasks == null ) return true;
    boolean passed = false;
    TaskScheduler ts;
    if (tab().booleanAt(Prms.doStartAndEndTasksLockStep)) {
      ts = new ClientNameLockStepTaskScheduler("StartTasks", starttasks, "start");
    } else {
      ts = new ClientNameTaskScheduler("StartTasks", starttasks, "start");
    }
    ts.createClients( tc.getClientDescriptions(), null );
    ts.startClients();
    passed = ts.executeTasks( tab().booleanAt( Prms.haltIfBadResult ),
                              tab().longAt( Prms.maxResultWaitSec ) );
    ts.stopClients();
    ts.printReport();
    return passed;
  }

  /**
  * Does INITTASK tasks on the custom configured clients.
  */
  private static boolean doInitTasks() {
    TestConfig tc = TestConfig.getInstance();
    Vector inittasks = tc.getInitTasks();
    if ( inittasks == null ) return true;
    boolean serial = tab().booleanAt( Prms.doInitTasksSequentially );
    TaskScheduler ts = null;
    if ( serial ) {
      ts = new SimpleSerialTaskScheduler( "InitTasks", inittasks, null );
    } else {
      ts = new SimpleConcurrentTaskScheduler( "InitTasks", inittasks, null );
    }
    ts.createClients( tc.getClientDescriptions(), tc.getThreadGroups() );
    ts.startClients();
    boolean passed = ts.executeTasks( tab().booleanAt( Prms.haltIfBadResult ),
                                      tab().longAt( Prms.maxResultWaitSec ) );
    // only stop the clients if there are no TASK or CLOSETASK tasks
    // @todo lises use config param to decide whether to stop on failure
    if ( ( tc.getTasks() == null && tc.getCloseTasks() == null ) || ! passed ) {
      ts.stopClients();
    }
    ts.printReport();
    return passed;
  }

  /**
  * Does TASK tasks on the custom configured clients.
  */
  private static boolean doTasks() {
    TestConfig tc = TestConfig.getInstance();
    Vector tasks = tc.getTasks();
    if ( tasks == null ) return true;
    boolean roundRobin = tab().booleanAt( Prms.roundRobin );
    boolean serialExecution = tab().booleanAt( Prms.serialExecution );
    long secs = tab().longAt( Prms.totalTaskTimeSec );
    TaskScheduler ts = null;
    if (roundRobin) {
       ts = new ComplexRoundRobinTaskScheduler( "Tasks", tasks, null, secs);
    } else if ( serialExecution ) {
       ts = new ComplexSerialTaskScheduler( "Tasks", tasks, null, secs );
    } else {
       ts = new ComplexConcurrentTaskScheduler( "Tasks", tasks, null, secs);
    }
    // only create and start the clients if there were no INITTASK tasks
    if ( tc.getInitTasks() == null ) {
      ts.createClients( tc.getClientDescriptions(), tc.getThreadGroups() );
      ts.startClients();
    }
    boolean passed = ts.executeTasks( tab().booleanAt( Prms.haltIfBadResult ),
                                      tab().longAt( Prms.maxResultWaitSec ) );
    // only stop the clients if there were no CLOSETASK tasks
    // @todo lises use config param to decide whether to stop on failure
    if ( tc.getCloseTasks() == null || ! passed ) {
      ts.stopClients();
    }
    ts.printReport();
    return passed;
  }

  /**
  * Does CLOSETASK tasks on the custom configured clients.
  */
  private static boolean doCloseTasks() {
    TestConfig tc = TestConfig.getInstance();
    Vector closetasks = tc.getCloseTasks();
    if ( closetasks == null ) return true;
    TaskScheduler ts = new SimpleConcurrentTaskScheduler
                           ( "CloseTasks", closetasks, null );
    // only create and start the clients if there were no INITTASK or TASK tasks
    if ( tc.getInitTasks() == null && tc.getTasks() == null ) {
      ts.createClients( tc.getClientDescriptions(), tc.getThreadGroups() );
      ts.startClients();
    }
    long waitSec = tab().longAt( Prms.maxCloseTaskResultWaitSec,
                                 tab().longAt( Prms.maxResultWaitSec ) );
    boolean passed = ts.executeTasks( tab().booleanAt( Prms.haltIfBadResult ),
                                      waitSec );
    // @todo lises use config param to decide whether to stop on failure
    ts.stopClients();
    ts.printReport();
    return passed;
  }

  /**
  * Does ENDTASK tasks using the client descriptions.
  */
  private static boolean doEndTasks() {
    return doEndTasks( false );
  }

  /**
  * Does ENDTASK tasks using the default client description regardless of
  * outcome of prior task types.
  */
  private static boolean doEndTasksForced() {
    if (ResultLogger.hasHangFile()) {
      log.severe("Hang detected: dumping stacks before starting ENDTASKs");
      dumpStacks(2);
    }
    return doEndTasks( true );
  }

  /**
  * Does ENDTASK tasks, forcing or not according to the value of "forced'.
  */
  private static boolean doEndTasks( boolean forced ) {
    TestConfig tc = TestConfig.getInstance();
    Vector endtasks = tc.getEndTasks();
    if ( endtasks == null ) return true;
    TaskScheduler ts;
    if (tab().booleanAt(Prms.doStartAndEndTasksLockStep)) {
      ts = new ClientNameLockStepTaskScheduler("EndTasks", endtasks, "end");
    } else {
      ts = new ClientNameTaskScheduler("EndTasks", endtasks, "end");
    }
    ts.createClients( tc.getClientDescriptions(), null );
    if ( forced ) {
      ts.startClientsForced();
    } else {
      ts.startClients();
    }
    boolean halt = tab().booleanAt( Prms.haltIfBadResult );
    long waitSec = tab().longAt( Prms.maxEndTaskResultWaitSec,
                                 tab().longAt( Prms.maxResultWaitSec ) );
    boolean passed = ts.executeTasks( halt, waitSec );
    // @todo lises use config param to decide whether to stop on failure
    ts.stopClients();
    ts.printReport();
    return passed;
  }

  //////////////////////////////////////////////////////////////////////////////
  //                        MAIN CONTROL LOOP                                 //
  //////////////////////////////////////////////////////////////////////////////

  private static boolean doUnitTestLoop() {
//    boolean result = false;
    try {
      return doUnitTests();
    } 
    catch (VirtualMachineError e) {
      // Don't try to handle this; let thread group catch it.
      throw e;
    }
    catch( Throwable t ) {
      ResultLogger.reportAsErr( "MasterController.doUnitTestLoop", t );
      return false;
    }
  }
  private static boolean doTaskLoop() {
    try {
      if ( ! doStartTasks() ) { wrapup(); return false; }
      clearEndpointBlackboards();
      if ( ! doInitTasks() )  { wrapup(); return false; }
      if ( ! doTasks() )      { wrapup(); return false; }
      if ( ! doCloseTasks() ) { wrapup(); return false; }
      clearEndpointBlackboards();
      if ( ! doEndTasks() )   return false;
    } 
    catch (VirtualMachineError e) {
      // Don't try to handle this; let thread group catch it.
      throw e;
    }
    catch( Throwable t ) {
      ResultLogger.reportAsHang( "MasterController.doTaskLoop", t );
      wrapup();
      return false;
    }
    return true;
  }
  private static void wrapup() {
    if ( tab().booleanAt( Prms.alwaysDoEndTasks ) ) {
      clearEndpointBlackboards();
      doEndTasksForced();
    }
  }
  private static void clearEndpointBlackboards() {
    log.info("Clearing endpoint blackboards");
    AgentBlackboard.getInstance().getSharedMap().clear();
    BridgeBlackboard.getInstance().getSharedMap().clear();
    if (!TestConfig.tab().booleanAt(Prms.manageLocatorAgents)) {
      DistributedSystemBlackboard.getInstance().getSharedMap().clear();
    }
    GatewayHubBlackboard.getInstance().getSharedMap().clear();
    log.info("Cleared endpoint blackboards");
  }
  /**
  *
  * The main control loop for a multiuser test.
  *
  */
  private static boolean runtest() {

    log.info( "Starting hydra master controller." );
    log.info( ProcessMgr.processAndBuildInfoString() );

    // get the configuration file
    String configFileName = System.getProperty( "configFileName" );
    if (configFileName == null) {
      throw new HydraConfigException( "Failed to specify configFileName" );
    }

    // pre-parse the config file, skipping random includes, to get random seed
    TestConfig tcTmp = TestConfig.create();
    MasterDescription.configure(tcTmp); // set master configuration
    log.info( "Pre-parsing hydra configuration file: " + configFileName + "..." );
    try {
      ConfigParser.parseFile(configFileName, tcTmp, null /* outfn */);
    } catch (FileNotFoundException e) {
      throw new HydraConfigException("Hydra configuration file not found: " + configFileName, e);
    }
    long randomSeed = tcTmp.getRandomSeed();
    if (randomSeed == -1) {
      randomSeed = tcTmp.generateRandomSeed();
    }
    ConfigParser.reset(); // clean up the parser
    tcTmp.destroy(); // clean up the singleton

    // parse the full config file, including random includes, using the random seed
    TestConfig tc = TestConfig.create();

    // set the random seed as soon as we can
    tc.setRandomSeed(randomSeed);

    // set master configuration
    MasterDescription.configure( tc );
    HostDescription mhd = tc.getMasterDescription().getVmDescription()
                            .getHostDescription();
    Nuker.initialize(mhd);
    Nuker.getInstance().recordPID(mhd, ProcessMgr.getProcessId());

    log.info( "Parsing hydra configuration file: " + configFileName + "..." );
    try {
      ConfigParser.parseFile(configFileName, tc, "latest.conf");
    } catch (FileNotFoundException e) {
      throw new HydraConfigException("Hydra configuration file not found: " + configFileName, e);
    }

    // make sure random includes don't also try to set the random seed
    if (tc.getRandomSeed() != randomSeed) {
      String s = "A randomInclude file sets " + BasePrms.nameForKey(Prms.randomSeed)
               + "=" + tc.getRandomSeed() + ". This is not allowed.";
      throw new HydraConfigException(s);
    }

    // Check to see if this test should be run on this platform
    String onlyOnPlatforms =
      tc.getSystemProperty(HostHelper.ONLY_ON_PLATFORMS_PROP);
    if (!HostHelper.shouldRun(onlyOnPlatforms)) {
      String s = "Test " + configFileName + " can only run on " +
        onlyOnPlatforms + " and this platform is " +
        System.getProperty("os.name");
      throw new HydraConfigException(s);
    }

    if (TestConfig.tab().booleanAt(Prms.useFixedRandomInMaster, false)) {
      System.setProperty("hydra.useFixedRandomInMaster", "true");
    }

    // set the protocol as soon as we can
    if (TestConfig.tab().booleanAt(Prms.useIPv6, false)) {
      System.setProperty(Prms.USE_IPV6_PROPERTY, "true");
    }

    // update the log level as soon as we can
    Log.setLogWriterLevel( tab().stringAt( LogPrms.file_logLevel ) );

    // configure gemfire classes
    tc.configure();

    // set the test name and runner
    String testDir = mhd.getTestDir();
    if (configFileName.startsWith(testDir)) {
      tc.setTestName(configFileName.substring( testDir.length() + 1 ));

    } else {
      tc.setTestName(configFileName);
    }
    tc.setTestUser(System.getProperty("user.name"));

    // Start the time server
    if (!parseOnly && TimeServerPrms.clockSkewThresholdMs() > 0) {
      try
      {
        Platform.getInstance().restartNTP();
      } 
      catch (VirtualMachineError e) {
        // Don't try to handle this; let thread group catch it.
        throw e;
      }
      catch (Throwable t) { // do not cause failure here, just move along
        log.warning("Problem restarting NTP\n", t);
      }
    }
    if (!parseOnly && (TimeServerPrms.clockSkewThresholdMs() > 0 ||
                       TimeServerPrms.clockSkewUpdateFrequencyMs() > 0)) {
      TimeServer timeServer = TimeServerMgr.startTimeServer();
      tc.getMasterDescription().setTimeServerHost(timeServer.getHost());
      tc.getMasterDescription().setTimeServerPort(timeServer.getPort());
    }

    // do the postprocessing step
    tc.postprocess();

    // start the master rmi registry
    Registry registry = null;
    if (!parseOnly) {
      int port = PortHelper.getRandomPort();
      registry = RmiRegistryHelper.startRegistry(RMI_NAME, port);

      System.setProperty(RMI_HOST_PROPERTY, mhd.getHostName());
      System.setProperty(RMI_PORT_PROPERTY, String.valueOf(port));
    }

    // put the test configs out there for others to find
    if (tc.getClassNames("hydra.gemfirexd") == null) {
      tc.share();
    } // else leave them to the gemfirexd layer

    // configure gemfirexd classes
    if (tc.getClassNames("hydra.gemfirexd") != null) {
      Class[] cargs = {TestConfig.class};
      Object[] oargs = {tc};
      invoke("hydra.gemfirexd.GfxdTestConfig", "configure", cargs, oargs);
    }

    log.info("Parsed the hydra configuration file.");

    if (parseOnly) return true;

    // create and register the master proxy
    log.info( "Creating the master proxy" );
    MasterProxy master;
    try {
      master = new MasterProxy();
    } catch( RemoteException e ) {
      throw new HydraRuntimeException( "MasterProxy could not be created", e );
    }
    log.info( "Registering the master" );
    RmiRegistryHelper.bind(registry, RMI_NAME, master);

    // Create and bind the performance report
    log.info( "Creating the remote performance report" );
    RemotePerfStatMgrImpl report;
    try {
      report = new RemotePerfStatMgrImpl();
    } catch( RemoteException e ) {
      throw new HydraRuntimeException( "Could not create remote performance report", e );
    }
    log.info( "Registering the performance report" );
    RmiRegistryHelper.bind(registry, RemotePerfStatMgrImpl.RMI_NAME, report);

    try {
      // Start the tools testing VM, if needed
      GFMonMgr.start();

      // Start all of the hostagents needed, if any.
      if ( tc.getHostAgentDescriptions().size() > 0 )
        HostAgentMgr.startHostAgents( tc.getHostAgentDescriptions() );

      // Start the distribution locator agents, if any
      if (tab().booleanAt(Prms.manageLocatorAgents)) {
        GemFireLocatorAgentMgr.startLocatorAgents();
      }

      // Start the derby server, if any
      if (tab().booleanAt(Prms.manageDerbyServer)) {
        DerbyServerMgr.startDerbyServer();
      }
    }
    catch (HydraRuntimeException e) {
      // treat this as hang as well to trigger nukerun on master-managed VMs
      ResultLogger.reportAsHang("Problem with resource startup", e);
      throw e;
    }
    catch (HydraTimeoutException e) {
      ResultLogger.reportAsHang("Problem with resource startup", e);
      throw e;
    }

    // start a statistics monitor on master
    StatMonitor.start(mhd, master, tc);

    // switch to the task log
    log.info( "Switching to taskmaster log..." );
    Log.cacheLogWriter();
    log = Log.createLogWriter(
                               "taskmasterloop",
                               "taskmaster_" + ProcessMgr.getProcessId(),
                               tab().booleanAt( LogPrms.file_logging ),
                               tab().stringAt(  LogPrms.file_logLevel ),
                               tab().intAt(     LogPrms.file_maxKBPerVM ) );

    boolean passed = false;

    // do the test loop
    if ( tc.getUnitTests() == null ) {
      // do the task loop
      log.info( "=========== START TEST ===========" );
      try {
        passed = doTaskLoop();
      } 
      catch (VirtualMachineError e) {
        // Don't try to handle this; let thread group catch it.
        throw e;
      }
      catch( Throwable t ) {
        passed = false;
        ResultLogger.reportAsErr( "MasterController.doTaskLoop", t );
      }
      log.info( "============ END TEST ============" );
    } else {
      // do the unit test loop
      log.info( "=========== START UNIT TESTS ===========" );
      try {
        passed = doUnitTestLoop();
      } 
      catch (VirtualMachineError e) {
        // Don't try to handle this; let thread group catch it.
        throw e;
      }
      catch( Throwable t ) {
        passed = false;
        ResultLogger.reportAsErr("MasterController.doUnitTestLoop", t);
      }
      log.info( "============ END UNIT TESTS ============" );
    }

    // switch back to the regular log
    try {
      Log.closeLogWriter();
      log = Log.uncacheLogWriter( tc.getMasterDescription().getName() );
      log.info( "...switching back to regular log" );
    } 
    catch (VirtualMachineError e) {
      // Don't try to handle this, let thread group catch it.
      throw e;
    }
    catch( Throwable t ) {
      passed = false;
      ResultLogger.reportAsErr( "MasterController.runtest", t );
    }

    // stop the statistics monitor
    StatMonitor.stop();

    // sleep before shutting down, if requested
    int sleepBeforeShutdownSec = tab().intAt( Prms.sleepBeforeShutdownSec );
    if ( sleepBeforeShutdownSec > 0 ) {
      log.info( "Sleeping " + sleepBeforeShutdownSec
              + " seconds before shutting down test" );
      MasterController.sleepForMs( 1000 * sleepBeforeShutdownSec );
    }

    // wait for the WindowTester/GFMon VM to stop
    try {
      GFMonMgr.waitForStop();
    } catch (HydraTimeoutException e) {
      passed = false;
      ResultLogger.reportAsHang("Problem with WindowTester/GFMon shutdown", e);
      dumpStacks(2);
    }
    if (ResultLogger.hasErrorFile()) {
      passed = false;
    }

    // Stop the derby server, if any
    if (tab().booleanAt(Prms.manageDerbyServer)) {
      DerbyServerMgr.stopDerbyServer();
    }

    String masterUserDir = System.getProperty("user.dir");
    List sysdirs = TestFileUtil.getSystemDirectoriesAsList(masterUserDir);
    if ( ResultLogger.hasHangFile() ) {
      passed = false;
      dumpStacks(2);
      if (sysdirs != null) {
        log.severe( "Leaving system resources running" );
        if (tab().booleanAt(Prms.removeDiskFilesAfterTest, false)) {
          try {
            log.severe("Removing disk files...");
            TestFileUtil.removeDiskFilesAfterTest();
          } catch (HydraRuntimeException e) {
            passed = false;
            ResultLogger.reportAsHang("Problem processing hang", e);
          }
        }
      }
    } else if (sysdirs != null) {
      try {
        if ( ( ! tab().booleanAt( Prms.stopSystemsOnError ) )
             && ResultLogger.hasErrorFile() ) {
          passed = false;
          ResultLogger.reportAsHang( "Leaving system resources running on test "
                                   + "error, as requested", null );
        } else {
          try {
            if (tab().booleanAt(Prms.manageLocatorAgents)) {
              GemFireLocatorAgentMgr.stopLocatorAgentsAfterTest();
            }
          } catch( HydraTimeoutException e ) {
            passed = false;
            ResultLogger.reportAsHang( "Problem with resource shutdown", e );
            dumpStacks(2);
          }
        }
        if (tab().booleanAt(Prms.removeDiskFilesAfterTest, false)) {
          log.severe("Removing disk files...");
          TestFileUtil.removeDiskFilesAfterTest();
        }
      } catch( HydraRuntimeException e ) {
        passed = false;
        ResultLogger.reportAsHang( "Problem processing test error, "
                                 + "leaving systems in current state", e );
        if (tab().booleanAt(Prms.removeDiskFilesAfterTest, false)) {
          log.severe("Removing disk files...");
          TestFileUtil.removeDiskFilesAfterTest();
        }
      }
    }

    if (TestFileUtil.getStatisticArchivesAsList(masterUserDir) != null) {
      // Generate the trim spec file
      log.info( "Generating trim specifications file" );
      report.generateTrimSpecificationsFile();

      // Generate the statistics specification file
      log.info( "Generating statistics specifications file" );
      report.generateStatisticsSpecificationsFile();

      // Generate the performance report
      if ( tab().booleanAt( PerfReportPrms.generatePerformanceReport ) ) {
        log.info( "Generating performance report" );
        ResultLogger.generatePerformanceReportFile();
      } else {
        log.info( "Skipping performance report" );
      }
    } else {
      log.info( "Skipping performance report since there are no archives" );
    }

    // Stop all the hostagents
    if ( tc.getHostAgentDescriptions().size() != 0 )
      HostAgentMgr.shutDownAllHostAgents();

    return passed;
  }

  private static void terminate( String msg, boolean passed ) {
    ResultLogger.logFinalOutcome( msg, passed );
    ResultLogger.mergeLogFiles( passed );
    FileUtil.deleteFile( "in_progress.txt" );

    HostDescription hd = TestConfig.getInstance().getMasterDescription()
                                   .getVmDescription().getHostDescription();
    Nuker.getInstance().removePID(hd, ProcessMgr.getProcessId());

    if ( passed ) {
      System.exit(0);
    } else {
      System.exit(1);
    }
  }

  protected static void dumpStacksOnError(int times) {
    if (tab().booleanAt(Prms.dumpStacksOnError)) {
      dumpStacks(times);
    }
  }
  protected static void dumpStacks(int times) {
    for (int i = 0; i < times; i++) {
      log.severe( "Doing stack dump on all processes" );
      dumpStacks();
      if (times > 1) {
        log.severe( "Sleeping 5 seconds between stack dumps");
        sleepForMs( 5000 );
      }
    }
  }
  protected static void dumpStacks() {
    TestConfig tc = TestConfig.getInstance();
    log.severe( "Doing stack dump on all clients (and master)" );
    ClientMgr.printProcessStacks();
    if ( tc.getGemFireDescriptions().size() != 0 ) {
      if (tab().booleanAt(Prms.manageLocatorAgents)) {
        log.severe("Doing stack dump on all locators");
        GemFireLocatorAgentMgr.printProcessStacks();
      }
    }
  }

  public static void main( String ignore[] ) {

    try {
      FileUtil.appendToFile( "in_master.txt", "Successfully spawned hydra.MasterController...\n" );
      FileUtil.appendToFile( "in_progress.txt", "hydra.MasterController in action...\n" );

      // set the thread name
      int pid = ProcessMgr.getProcessId();

      Thread.currentThread().setName( "master_" + pid );

      // immediately create log, using most sensitive level to start with
      log = Log.createLogWriter( "master", "Master_" + pid, "all", false );

      boolean passed = runtest();
      if ( passed ) {
        String msg = "This concludes your test run.  On behalf of everyone in "
                   + "the GemStone Systems QA department, I'd like to thank "
                   + "you for using the hydra test harness.  "
                   + "Have a pleasant day.";
        terminate( msg, passed );
      } else {
        String msg = "This concludes your test run.  I'm sorry to report that "
                   + "the test found errors. Look for details in the test and "
                   + "system logs.  Better luck next time.";
        terminate( msg, passed );
      }
    } 
    catch (VirtualMachineError e) {
      // Don't try to handle this; let thread group catch it.
      throw e;
    }
    catch( Throwable t ) {
      ResultLogger.reportAsErr( "MasterController.main", t );
      String msg = "This concludes your test run.  The test harness "
                 + "encountered an unexpected exception.  Look for details in "
                 + "the test and system logs.  Better luck next time.";
      terminate( msg, false );
    }
  }
}
