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

/**
 *
 * A class used to store keys for hydra framework configuration settings.
 * <p>
 * To add hydra framework parameters, simply add a field definition and javadoc
 * anywhere in the source for this class (ideally among related parameters), like
 * so: <code>public static Long someNewHydraParameter;</code>
 *
 */

public class Prms extends BasePrms {

  public static final String MASTER_HOST_PROPERTY = "masterHost";
  public static final String MASTER_PID_PROPERTY = "masterPid";
  public static final String USE_IPV6_PROPERTY = "java.net.preferIPv6Addresses";
  public static final String DISABLE_CREATE_BUCKET_RANDOMNESS_PROPERTY = "gemfire.DISABLE_CREATE_BUCKET_RANDOMNESS";

  /**
   *  (String) Description of the test in this test configuration file.
   */
  public static Long testDescription;

  /**
   *  (String) Topology used by the test.
   */
  public static Long testTopology;

  /**
   *  (String) Requirement the test is expected to meet.
   */
  public static Long testRequirement;

/**
* Hydra relies on a random number generator for purposes such as task
* scheduling and getting values for "range" and "oneof" parameters.
* Tests may also rely random number generation.
*
* Set this parameter to specify a seed for the random number generator.
* This is useful for reproducing problems.  By default, the seed is derived
* from the system clock.
*/
    public static Long randomSeed;

  /**
   * (boolean:false)
   * If true, this parameter causes the hydra master controller to use a newly
   * minted random number generator seeded with {@link #randomSeed} for each
   * access of a OneOf or Range.  This limits randomization by causing a OneOf
   * or Range accessed multiple times by master to return a random but constant
   * value.
   * <p>
   * This is useful for cases where there are many logical descriptions of a
   * particular type, all of which need to share the same constant, but
   * randomly chosen, value.
   * <p>
   * Note that if a given parameter uses multiple Ranges or OneOfs, they will
   * all return the same value unless the choices are different.
   * <p>
   * This setting has no effect on parameters read from hydra clients.
   */
  public static Long useFixedRandomInMaster;

  /**
   * Whether to use IPv6 addresses in master-spawned VMs such as locators
   * and hydra clients.
   */
  public static Long useIPv6;

  /**
   * (boolean:false)
   * Guarantees that both buckets and primary buckets will be perfectly
   * balanced when buckets are pre-created using the PartitionRegionHelper
   * assignBucketsToPartitions(region) method.  If true, the hydra master
   * adds "-Dgemfire.DISABLE_CREATE_BUCKET_RANDOMNESS=true" to the system
   * properties for each hydra client JVM.
   * <p>
   * This parameter is a temporary workaround until the product fixes the
   * load balance issue without requiring the system property.
   */
  public static Long disableCreateBucketRandomness;

/**
* Whether the hosts use NFS (defaults to true).
*/
    public static Long useNFS;

/**
* If true, the hydra master controller will stop distributing test tasks as
* soon as a client thread reports an error.  If false, the master will
* try to continue farming out jobs.
*/
    public static Long haltIfBadResult;

/**
* (boolean) If true (default), the hydra parser will verify that task
* methods are on the master classpath.  Turn this off if tasks will only
* be on client classpaths.
*/
    public static Long checkTaskMethodsExist;

    /**
     *  (boolean)
     *  True if TASKs should be scheduled sequentially in a random fashion.
     *  Defaults to false.
     */
    public static Long serialExecution;

    /**
     *  (boolean)
     *  True if TASKs should be scheduled sequentially in a round robin fashion
     *  (with respect to the logical hydra client thread IDs).  Defaults to
     *  false.
     */
    public static Long roundRobin;

    /**
     *  (boolean)
     *  True if INITTASKs should be scheduled sequentially.  Defaults to false.
     *  @deprecated as of GemFire5.1, use the "sequential" task attribute
     *              instead, which allows individual INITTASKs and CLOSETASKs
     *              to be designated sequential.
     */
    public static Long doInitTasksSequentially;

    /**
     * (boolean)
     * True if STARTTASKs and ENDTASKs should run in lock step, so that each
     * task is completed by all appropriate clients before scheduling the
     * next task.  Defaults to false.
     */
    public static Long doStartAndEndTasksLockStep;

    /**
     * (boolean)
     * True if STARTTASKs and ENDTASKs should be versioned using the lowest
     * version in the version list for each client name. Defaults to false.
     */
    public static Long versionStartAndEndTasks;

    /**
     *  (boolean)
     *  True if ENDTASKs should be run before stopping the test, regardless of
     *  whether previous tasks had errors.  Defaults to false.
     */
    public static Long alwaysDoEndTasks;

    /**
     *  (boolean)
     *  Indicates whether master should manage the Derby server.
     *  Defaults to false.
     */
    public static Long manageDerbyServer;

    /**
     * (String)
     * The extra VM arguments, if any, to pass to the derby server startup VM.
     */
    public static Long extraDerbyServerVMArgs;

    /**
     * (boolean) Indicates whether security is turned on.
     *
     */
    public static Long testSecurity;

    /**
     *  (boolean)
     *  Indicates whether master should manage GemFire distribution locator
     *  agents.  Defaults to true.
     */
    public static Long manageLocatorAgents;

    /**
     *  (boolean)
     *  Indicates whether to start GemFire distribution locator agents before
     *  the test.  Defaults to true.
     */
    public static Long startLocatorAgentsBeforeTest;


    /**
     *  (boolean)
     *  Indicates whether to stop GemFire distribution locator agents after
     *  the test.  Defaults to true.
     */
    public static Long stopLocatorAgentsAfterTest;


    /**
     *  (boolean(s))
     *  Tells hydra whether to remove disk files after a test, if they exist.
     *  Defaults to false.
     */
    public static Long removeDiskFilesAfterTest;


    /**
     * (boolean)
     * Indicates whether to dump stacks on all processes upon a test error
     * other than a hang.  Defaults to true.
     */
    public static Long dumpStacksOnError;

    /**
     * (String)
     * Heap dump options to pass to jmap in the heapdumprun script generated
     * by hydra.  Defaults to <code>-dump:live,format=b</code> for 1.6 and
     * <code>-heap:format=b</code> for 1.5..
     */
    public static Long jmapHeapDumpOptions;

    /**
     *  (boolean)
     *  Indicates whether to stop system resources such as cache servers upon a
     *  test error other than a hang.  Defaults to true.  Setting this false
     *  causes the test (and batterytest) to halt with the resources still
     *  running, thus overriding {@link GemFirePrms#stopSystemsAfterTest}.
     */
    public static Long stopSystemsOnError;

    /**
     *  (String)
     *  Indicates what action to take with statistic archives when a test hangs.
     *  Options are {@link #STOP_ARCHIVERS} to shut down the archivers,
     *  {@link #SLOW_ARCHIVERS} to reduce the sample rate to 60 times its
     *  current value, and {@link #LEAVE_ARCHIVERS} to leave the archivers
     *  running at their current rate.  Defaults to {@link #SLOW_ARCHIVERS}.
     */
    public static Long statisticArchivesAction;
    public static String statisticArchivesAction() {
      String action = tab().stringAt( statisticArchivesAction, DEFAULT_ARCHIVER_ACTION );
      if ( action.equalsIgnoreCase( STOP_ARCHIVERS ) ) {
        return STOP_ARCHIVERS;
      } else if ( action.equalsIgnoreCase( SLOW_ARCHIVERS ) ) {
        return SLOW_ARCHIVERS;
      } else if ( action.equalsIgnoreCase( LEAVE_ARCHIVERS ) ) {
        return LEAVE_ARCHIVERS;
      } else {
        Log.getLogWriter().warning( "Illegal value for " + nameForKey( statisticArchivesAction) + ": " + action + ", defaulting to \"" + LEAVE_ARCHIVERS + "\"" );
        return LEAVE_ARCHIVERS;
      }
    }
    public static final String STOP_ARCHIVERS  = "stop";
    public static final String SLOW_ARCHIVERS  = "slow";
    public static final String LEAVE_ARCHIVERS = "leave";
    public static final String DEFAULT_ARCHIVER_ACTION = SLOW_ARCHIVERS;

    /**
     *  (int)
     *  How many seconds to sleep before shutting the test down.  Defaults to 0.
     *  Useful for keeping systems alive to see if garbage collection cleans up.
     */
    public static Long sleepBeforeShutdownSec;

/**
* Usually, this sets the maximum number of seconds that hydra should remain
* in the test task execution loop, giving jobs to clients and evaluating
* the results.  Unless <code>maxTaskExecutions</code> is exceeded, hydra
* will assume that testing should continue for at least
* <code>totalTaskTimeSec</code>.  When <code>totalTaskTimeSec</code> is
* reached, the master controller will ordinarily wait a bit for late results,
* then shut down clients and enter its final reporting and cleanup phase.
*/
    public static Long totalTaskTimeSec;

    /**
     * (int)
     * Number of seconds to sleep before starting each host agent.  Defaults to
     * {@link #DEFAULT_SLEEP_BEFORE_STARTING_HOSTAGENT_SEC}.  Useful when
     * starting large numbers of hostagents to avoid timing out from an NFS
     * overload on the master host.
     */
    public static Long sleepBeforeStartingHostAgentSec;
    public static final int DEFAULT_SLEEP_BEFORE_STARTING_HOSTAGENT_SEC = 0;

    /**
     *  (int)
     *  The maximum time, in seconds, hydra should wait for any hostagent to
     *  start up and register with the master controller.
     */
    public static Long maxHostAgentStartupWaitSec;

    /**
     *  (int)
     *  The maximum time, in seconds, hydra should wait for any hostagent to
     *  shut down.
     */
    public static Long maxHostAgentShutdownWaitSec;

    /**
     *  (String)
     *  The hostagent log level.
     */
    public static Long logLevelForHostAgent;

    /**
     *  (boolean)
     *  Whether to archive statistics in hostagents. This is turned on
     *  automatically for hostagents on hadoop cluster nodes. Has the side
     *  effect of spawning a hostagent on the master host.
     */
    public static Long archiveStatsForHostAgent;

    /**
     *  (int)
     *  The maximum time, in seconds, hydra should wait for any client vm to
     *  start up and register its clients with the master controller.
     */
    public static Long maxClientStartupWaitSec;

    /**
     *  (int)
     *  The maximum time, in seconds, hydra should wait for any client vm to
     *  shut down.
     */
    public static Long maxClientShutdownWaitSec;

    /**
     *  (int)
     *  The time, in seconds, for client threads to pause before registering
     *  with the master controller, e.g, to make it possible to attach a
     *  debugger before the action starts.  Defaults to 0.
     */
    public static Long initialClientSleepSec;

    /**
     *  (int)
     *  The time, in seconds, for client threads to pause before shutting
     *  down when requested to by the master controller, e.g, to allow time
     *  for events to arrive and be processed.  Defaults to 0.
     *  <p>
     *  Note that this value should always be smaller than
     *  {@link #maxClientShutdownWaitSec}.
     */
    public static Long finalClientSleepSec;

    /**
     *  (long)
     *  The maximum time, in seconds, hydra should wait for a thread executing
     *  a task to return a result.  For ENDTASKs, can be overridden using
     *  {@link #maxEndTaskResultWaitSec}.
     */
    public static Long maxResultWaitSec;

    /**
     *  (long)
     *  The maximum time, in seconds, hydra should wait for a thread executing
     *  an ENDTASK task to return a result.  Defaults to {@link #maxResultWaitSec}.
     */
    public static Long maxEndTaskResultWaitSec;

    /**
     *  (long)
     *  The maximum time, in seconds, hydra should wait for a thread executing
     *  a CLOSETASK task to return a result.  Defaults to {@link #maxResultWaitSec}.
     */
    public static Long maxCloseTaskResultWaitSec;

  /**
   * (String pair)
   * Shutdown hook to invoke in the event of test failure.  A hook consists of
   * a classname followed by a method name.  The method must be of type <code>
   * public static void</code> and take no arguments.  Defaults to null.
   * <p>
   * Shutdown hooks are invoked either when the first client reports task
   * failure, as part of client shutdown, or when master detects a hang, in
   * which case the hooks are attempted but not guaranteed (unfulfilled
   * requests are logged at severe level).  Shutdown hooks are not invoked on
   * a pass.
   * <p>
   * Shutdown hooks are invoked concurrently on a single representative thread
   * in each client VM and are subject to {@link #maxClientShutdownWaitSec}.
   * <p>
   * Hydra client threads, including threads with the same logical thread ID,
   * can still be executing tasks when hooks are invoked.  Use extreme care
   * when using <code>HydraThreadLocal</code>s and other state inside shutdown
   * hook methods.
   */
  public static Long clientShutdownHook;

    /**
     *  (boolean)
     *  True (default) if hydra should log a failed hydra run if a GemFire
     *       system fails to start.
     *  False if hydra should proceed silently if a GemFire system fails to
     *       start.
     */
    public static Long errorOnFailedGFStartup;

    /*
     * (String)
     * Classpath for a pure apache derby server process
     */
    public static Long derbyServerClassPath;

    static {
        setValues( Prms.class );
    }

    public static void main( String args[] ) {
        Log.createLogWriter( "prms", "info" );
        dumpKeys();
    }


}
