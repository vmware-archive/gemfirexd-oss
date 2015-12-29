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
import java.io.*;
import java.util.*;

/**
*
* A TestTask object represents a unit of work that can be transmitted 
* from a master controller to a client thread for execution.  In addition to 
* specifying work to be done (via a class name/selector combo), a TestTask
* also stores some information about the circumstances under which it should be
* executed, as well some scratchpad data about how often it's been used in the
* current test run, how many instances are currently active, and so on. 
* <p>
* To configure a test task, add an entry to the test configuration file as
* shown in <A href="hydra_grammar.txt">hydra_grammar.txt</A>.  Note that a
* test task of any type can have an arbitrary number of user-defined
* attributes.  These are available as a {@link ConfigHashtable} to a client
* running the task by invoking
* <code>RemoteTestModule.getCurrentThread().getCurrentTask()</code>.  The usual
* methods on <code>ConfigHashtable</code> are then available for reading the
* attribute values.  See <code>hydratest/checktaskattributes.conf</code> and
* methods {@link hydratest.TaskClient#tryItOutAttributes} and {@link
* hydratest.TaskClient#tryItOutAttributesOneOf} for examples.
*
*/

public class TestTask implements Cloneable, Serializable {

  protected static final int STARTTASK        = 0;
  protected static final int INITTASK         = 1;
  protected static final int DYNAMICINITTASK  = 2; // used behind the scenes
  protected static final int TASK             = 3;
  protected static final int DYNAMICCLOSETASK = 4; // used behind the scenes
  protected static final int CLOSETASK        = 5;
  protected static final int ENDTASK          = 6;
  protected static final int UNITTEST         = 7;

  /**
   *  Run mode to cause an INITTASK or CLOSETASK to run on suitable clients
   *  every time their VM starts or stops.
   */
  public static final int ALWAYS  = 0;
  /**
   *  Run mode to cause an INITTASK or CLOSETASK to run on suitable clients
   *  only when the hydra master starts or stops their VM.
   */
  public static final int ONCE    = 1;
  /**
   *  Run mode to cause an INITTASK or CLOSETASK to run on suitable clients
   *  only when a client starts or stops their VM via the {@link ClientVmMgr}
   *  interface.
   */
  public static final int DYNAMIC = 2;

  protected static final int DEFAULT_RUN_MODE = ONCE;

  //////////////////////////////////////////////////////////////////////////////
  ////  FIELDS USED BY ALL TASKS                                          //////
  //////////////////////////////////////////////////////////////////////////////

  /** This task's index in the Tasks Vector */
  private int taskIndex;
 
  /** The task type (one of the above) */ 
  private int taskType;

  /** Name of the class that should receive this task's selector */
  private String receiver;

  /** A static selector that defines the action for this task */
  private String selector;

  /** The total number of threads in all threadgroups for this task */
  private int totalThreads = -1;

  /** A configuration hashtable to store user-defined task attributes */
  private ConfigHashtable attributes = new ConfigHashtable();

  //////////////////////////////////////////////////////////////////////////////
  ////  FIELDS USED ONLY BY START AND END TASKS                           //////
  //////////////////////////////////////////////////////////////////////////////

  /** Logical client names to use for running task */
  private Vector clientNames = null;

  //////////////////////////////////////////////////////////////////////////////
  ////  FIELDS USED ONLY BY INIT, REGULAR, CLOSE TASKS                    //////
  //////////////////////////////////////////////////////////////////////////////

  private Vector tgNames;

  private Map tgs;

  //////////////////////////////////////////////////////////////////////////////
  ////  FIELDS USED ONLY BY START, INIT, CLOSE, END TASKS                 //////
  //////////////////////////////////////////////////////////////////////////////

  private boolean batch = false;

  //////////////////////////////////////////////////////////////////////////////
  ////  FIELDS USED ONLY BY INIT, CLOSE TASKS                             //////
  //////////////////////////////////////////////////////////////////////////////

  private boolean sequential = false;
  private int runMode = -1;

  //////////////////////////////////////////////////////////////////////////////
  ////  FIELDS USED ONLY BY REGULAR TASKS                                 //////
  //////////////////////////////////////////////////////////////////////////////

  /** Max number of times this task should be executed */
  private int maxTimesToRun = -1;

  /** Max number of threads that can execute this task concurrently */
  private int maxThreads = -1;

  private int weight = -1;

  private int startInterval = -1;
  private long startIntervalMs;

  private int endInterval = -1;
  private long endIntervalMs;

  /** Total number of clients that could be eligible to run this task */
  private int numClients = 0;

  //////////////////////////////////////////////////////////////////////////////
  ////  FIELDS USED AT RUNTIME                                            //////
  //////////////////////////////////////////////////////////////////////////////

  /** Number of threads currently running this task */
  private volatile int numTimesInUse = 0;

  /** Number of times this task has been executed */
  private volatile int numTimesRun = 0;

  /** Elapsed times of all executions of this task so far */
  private NumStats elapsedTimes = new NumStats();

  /** Start times (if used, there are maxThreads of these) */
  private long[] startTimes;

  /** End times (if used, there are maxThreads of these) */
  private long[] endTimes;

  /** Clients who have issued a stop scheduling order for this task */
  private Vector clients;

  //////////////////////////////////////////////////////////////////////////////
  ////  CONSTRUCTORS                                                      //////
  //////////////////////////////////////////////////////////////////////////////

  public TestTask() {
    // initialize runtime stats
    this.numTimesInUse = 0;
    this.numTimesRun = 0;
  }

  public Object clone() {
    TestTask clone;
    try {
      clone = (TestTask) super.clone();
    } catch( CloneNotSupportedException e ) {
      throw new HydraInternalException( "Unable to clone task" );
    }
    clone.elapsedTimes = new NumStats();
    return clone;
  }

  //////////////////////////////////////////////////////////////////////////////
  ////  EXECUTION                                                         //////
  //////////////////////////////////////////////////////////////////////////////

  /**
  *
  * Executes the selector on the receiver.  Returns the result, including
  * error status and elapsed time.
  * @return the result of executing the method.
  *
  */
  public TestTaskResult execute() {
    long beginTime = System.currentTimeMillis();
    MethExecutorResult result =
       MethExecutor.execute( getReceiver(), getSelector() );
    long elapsedTime = System.currentTimeMillis() - beginTime;
    return new TestTaskResult( result, elapsedTime );
  }

  //////////////////////////////////////////////////////////////////////////////
  ////  PRINTING                                                          //////
  //////////////////////////////////////////////////////////////////////////////

  /**
  *
  * Returns a String that partially describes the receiver.
  *
  */
  public String toShortString() {
    if ( this.cachedShortString == null ) {
      initCachedShortString();
    }
    return this.cachedShortString;
  }
  private synchronized void initCachedShortString() {
    StringBuffer buf = new StringBuffer(100);
    buf.append( getTaskTypeString() ).append( "[" );
    buf.append( getTaskIndex() ).append( "] " );
    buf.append( getReceiver() ).append( "." );
    buf.append( getSelector() );
    this.cachedShortString = buf.toString();
  }
  private String cachedShortString;

  public SortedMap toSortedMap() {
    SortedMap map = new TreeMap();
    int n = this.getTaskType();
    String header = null;
    if (n == UNITTEST) {
      header = this.getClass().getName() + "."
             + this.getTaskTypeString() + "." + getTaskIndex() + ".";
      map.put( header + "taskClass", getReceiver() );
      map.put( header + "taskMethod", getSelector() );
      map.put( header + "threadGroups", getThreadGroupNames() );
    }
    else if (n == STARTTASK || n == ENDTASK) {
      header = this.getClass().getName() + "."
             + this.getTaskTypeString() + "." + getTaskIndex() + ".";
      map.put( header + "taskClass", getReceiver() );
      map.put( header + "taskMethod", getSelector() );
      map.put( header + "clientNames", getClientNames() );
    }
    else if (n == INITTASK || n == CLOSETASK ||
             n == DYNAMICINITTASK || n == DYNAMICCLOSETASK) {
      header = this.getClass().getName() + "."
             + this.getTaskTypeString() + "." + getTaskIndex() + "."
             + getRunModeString() + ".";
      map.put( header + "taskClass", getReceiver() );
      map.put( header + "taskMethod", getSelector() );
      map.put( header + "threadGroups", getThreadGroupNames() );
      map.put( header + "sequential", Boolean.valueOf(sequential()) );
      map.put( header + "batch", Boolean.valueOf(batch()) );
    }
    else if (n == TASK) {
      header = this.getClass().getName() + "."
             + this.getTaskTypeString() + "." + getTaskIndex() + ".";
      map.put( header + "taskClass", getReceiver() );
      map.put( header + "taskMethod", getSelector() );
      map.put( header + "threadGroups", getThreadGroupNames() );
      map.put( header + "maxTimesToRun", String.valueOf( getMaxTimesToRun() ) );
      map.put( header + "maxThreads", String.valueOf( getMaxThreads() ) );
      map.put( header + "weight", String.valueOf( getWeight() ) );
      map.put( header + "startInterval", String.valueOf( getStartInterval() ) );
      map.put( header + "endInterval", String.valueOf( getEndInterval() ) );
    }
    SortedMap tab = this.attributes.toSortedMap();
    for ( Iterator i = tab.keySet().iterator(); i.hasNext(); ) {
      String key = (String) i.next();
      Object value = tab.get( key );
      map.put( header + key, value );
    }
    return map;
  }
  public String toString() {
    StringBuffer buf = new StringBuffer();
    SortedMap map = this.toSortedMap();
    for ( Iterator i = map.keySet().iterator(); i.hasNext(); ) {
      String key = (String) i.next();
      Object val = map.get( key );
      buf.append( key + "=" + val + "\n" );
    }
    return buf.toString();
  }

  //////////////////////////////////////////////////////////////////////////////
  ////  ACCESSORS                                                         //////
  //////////////////////////////////////////////////////////////////////////////

  public int getTaskIndex() {
    return taskIndex;
  }
  public void setTaskIndex(int anIndex) {
    taskIndex = anIndex;
  }
  public int getTaskType() {
    return taskType;
  }
  public String getTaskTypeString() {
    switch( this.taskType ) {
      case STARTTASK:        return "STARTTASK";
      case INITTASK:         return "INITTASK";
      case DYNAMICINITTASK:  return "INITTASK";
      case TASK:             return "TASK";
      case DYNAMICCLOSETASK: return "CLOSETASK";
      case CLOSETASK:        return "CLOSETASK";
      case ENDTASK:          return "ENDTASK";
      case UNITTEST:         return "UNITTEST";
      default: throw new HydraInternalException( "Unknown task type: " + this.taskType );
    }
  }
  public void setTaskType (int aType) {
    taskType = aType;
  }
  public String getReceiver() {
    return receiver;
  }
  public void setReceiver (String aStr) {
    receiver = aStr;
  }
  public String getSelector() {
    return selector;
  }
  public void setSelector(String aStr) {
    selector = aStr;
  }
  public ConfigHashtable getTaskAttributes() {
    return this.attributes;
  }
  protected void setTaskAttribute( Long key, Object value ) {
    this.attributes.put( key, value );
  }
  public int getRunMode() {
    return this.runMode;
  }
  public void setRunMode(int mode) {
    this.runMode = mode;
  }
  public String getRunModeString() {
    switch( this.runMode ) {
      case ALWAYS:  return "always";
      case ONCE:    return "once";
      case DYNAMIC: return "dynamic";
      default:      return "not set yet";
    }
  }
  public boolean sequential() {
    return this.sequential;
  }
  public void setSequential(boolean flag) {
    this.sequential = flag;
  }
  public boolean batch() {
    return this.batch;
  }
  public void setBatch(boolean flag) {
    this.batch = flag;
  }
  public Vector getClientNames() {
    return clientNames;
  }
  public void setClientNames(Vector names) {
    clientNames = names;
  }
  public int getMaxTimesToRun() {
    return maxTimesToRun;
  }
  public void setMaxTimesToRun(int anInt) {
    maxTimesToRun = anInt;
  }
  public int getMaxThreads() {
    return maxThreads;
  }
  public void setMaxThreads(int anInt) {
    maxThreads = anInt;
  }
  public int getWeight() {
    return weight;
  }
  public void setWeight( int n ) {
    weight = n;
  }
  public int getStartInterval() {
    return startInterval;
  }
  public void setStartInterval( int n ) {
    startInterval = n;
    startIntervalMs = n*1000;
  }
  public int getEndInterval() {
    return endInterval;
  }
  public void setEndInterval( int n ) {
    endInterval = n;
    endIntervalMs = n*1000;
  }
  protected void setNumClients( int numClients ) {
    this.numClients = numClients;
  }

  //////////////////////////////////////////////////////////////////////////////
  ////  THREADGROUPS                                                      //////
  //////////////////////////////////////////////////////////////////////////////

  public Vector getThreadGroupNames() {
    return this.tgNames;
  }
  protected void setThreadGroupNames( Vector tgNames ) {
    this.tgNames = tgNames;
  }
  protected void addThreadGroupName( String tgName ) {
    if ( this.tgNames == null )
      this.tgNames = new Vector();
    this.tgNames.add( tgName );
  }
  public Map getThreadGroups() {
    return this.tgs;
  }
  protected void addThreadGroup( HydraThreadGroup tg ) {
    if ( this.tgs == null )
      this.tgs = new HashMap();
    this.tgs.put( tg.getName(), tg );
  }
  protected boolean usesThreadGroup( String name ) {
    return this.tgs.containsKey( name );
  }
  /**
   *  Gets the total number of threads in all threadgroups for this task (lazily).
   */
  public int getTotalThreads() {
    if ( this.totalThreads == -1 ) {
      synchronized( this ) {
        if ( this.totalThreads == -1 ) {
          switch( this.taskType ) {
            case UNITTEST:
            case STARTTASK:
            case ENDTASK:
              this.totalThreads = 1;
              break;
            case INITTASK:
            case DYNAMICINITTASK:
            case TASK:
            case DYNAMICCLOSETASK:
            case CLOSETASK:
              int tmp = 0;
              for ( Iterator i = this.tgs.values().iterator(); i.hasNext(); ) {
                HydraThreadGroup tg = (HydraThreadGroup) i.next();
                tmp += tg.getTotalThreads();
              }
              // be sure to assign atomically
              this.totalThreads = tmp;
              break;
            default:
              throw new HydraInternalException( "Should not happen" );
          }
        }
      }
    }
    return this.totalThreads;
  }
  /**
   *  Returns a unique ID for each thread in a threadgroup for this task,
   *  numbered 0 to N-1, where N is the total number of threads in all
   *  threadgroups for the task.
   */
  public int getTaskThreadGroupId( String tgName, int tgid ) {
    int tgnum = this.tgNames.indexOf( tgName );
    int predecessors = 0;
    for ( int i = 0; i < tgnum; i++ ) {
      String name = (String) this.tgNames.get( i );
      HydraThreadGroup tg = (HydraThreadGroup) this.tgs.get( name );
      predecessors += tg.getTotalThreads();
    }
    return predecessors + tgid;
  }

  //////////////////////////////////////////////////////////////////////////////
  ////  RUNTIME STATS                                                     //////
  //////////////////////////////////////////////////////////////////////////////

  public void setStartTimes( long t ) {
    if ( this.startTimes == null )
      initStartTimes();
    if ( this.startInterval != 0 ) {
      synchronized( this.startTimes ) {
        for ( int i = 0; i < this.startTimes.length; i++ ) {
          this.startTimes[i] = t;
        }
      }
    }
  }
  private synchronized void initStartTimes() {
    if ( this.startTimes == null ) {
      int totalThreads = 0;
      for ( Iterator i = getThreadGroups().values().iterator(); i.hasNext(); ) {
        HydraThreadGroup tg = (HydraThreadGroup) i.next();
        totalThreads += tg.getTotalThreads();
      }
      this.startTimes = new long[ totalThreads ];
    }
  }
  public void setEndTimes( long t ) {
    if ( this.endTimes == null )
      initEndTimes();
    if ( this.endInterval != 0 ) {
      synchronized( this.endTimes ) {
        for ( int i = 0; i < this.endTimes.length; i++ )
          this.endTimes[i] = t;
      }
    }
  }
  private synchronized void initEndTimes() {
    if ( this.endTimes == null ) {
      int totalThreads = 0;
      for ( Iterator i = getThreadGroups().values().iterator(); i.hasNext(); ) {
        HydraThreadGroup tg = (HydraThreadGroup) i.next();
        totalThreads += tg.getTotalThreads();
      }
      this.endTimes = new long[ totalThreads ];
    }
  }

  /**
  * Updates the earliest known start time for this task with the current time.
  * Invoked when this task is assigned to a client.
  */
  public void updateStartTimes( long now ) {
    if ( this.taskType == TASK ) {
      if ( this.startInterval != 0 ) {
        synchronized( this.startTimes ) {
          int i = indexWithMinValue( this.startTimes );
          this.startTimes[ i ] = now;
        }
      }
    }
  }

  /**
  * Updates the earliest known end time for this task with the current time.
  * Invoked when a client completes this task.
  */
  public void updateEndTimes( long now ) {
    if ( this.taskType == TASK ) {
      if ( this.endInterval != 0 ) {
        synchronized( this.endTimes ) {
          int i = indexWithMinValue( this.endTimes );
          this.endTimes[ i ] = now;
        }
      }
    }
  }

  // @todo lises map this on a per-thread basis if threads are fixed
  public boolean satisfiesStartInterval( long now ) {
    if ( this.startInterval == 0 ) {
      return true;
    } else {
      long diff = now - this.startIntervalMs;
      synchronized( this.startTimes ) {
        for ( int i = 0; i < this.startTimes.length; i++ ) {
          if ( diff > this.startTimes[i] ) {
            return true;
          }
        }
      }
    }
    return false;
  }

  // @todo lises map this on a per-thread basis if threads are fixed
  public boolean satisfiesEndInterval( long now ) {
    if ( this.endInterval == 0 ) {
      return true;
    } else {
      long diff = now - this.endIntervalMs;
      synchronized( this.endTimes ) {
        for ( int i = 0; i < this.endTimes.length; i++ )
          if ( diff > this.endTimes[i] )
            return true;
      }
    }
    return false;
  }

  // compute the array index containing the smallest value
  private int indexWithMinValue( long[] arr ) {
    if ( arr.length == 0 )
      throw new HydraInternalException( "Unexpectedly empty array" );
    long minValue = arr[0];
    int index = 0;
    for ( int i = 1; i < arr.length; i++ )
      if ( arr[i] < minValue )
        index = i;
    return index;
  }

  public int getNumTimesRun() {
    return numTimesRun;
  }
  public void incrementNumTimesRun() {
    ++numTimesRun;
  }
  public int getNumTimesInUse() {
    return numTimesInUse;
  }
  public void incrementNumTimesInUse() {
    ++numTimesInUse;
  }
  public void decrementNumTimesInUse() {
    --numTimesInUse;
  }
  public NumStats getElapsedTimes() {
    synchronized( this.elapsedTimes ) {
      return this.elapsedTimes;
    }
  }
  public void addElapsedTime( long t ) {
    synchronized( this.elapsedTimes ) {
      elapsedTimes.add( t );
    }
  }
  protected boolean acceptsStopSchedulingOrder() {
    return this.taskType == TASK;
  }
  protected boolean acceptsStopSchedulingTaskOnClientOrder() {
    return this.taskType != STARTTASK && this.taskType != ENDTASK;
  }
  protected boolean receivedStopSchedulingTaskOnClientOrder() {
    synchronized( this ) {
      return this.clients != null;
    }
  }
  public boolean receivedStopSchedulingTaskOnClientOrder( ClientRecord client ) {
    synchronized( this ) {
      return this.clients != null && this.clients.contains( client );
    }
  }
  protected void receiveStopSchedulingTaskOnClientOrder( ClientRecord client ) {
    synchronized( this ) {
      if ( this.clients == null ) {
        this.clients = new Vector();
      }
      if ( this.clients.contains( client ) ) {
        String s = client + " already threw a StopSchedulingTaskOnClientOrder";
        throw new HydraInternalException( s );
      } else {
        this.clients.add( client );
      }
    }
  }
  public boolean receivedAllStopSchedulingTaskOnClientOrders() {
    synchronized( this ) {
      return this.clients != null && this.clients.size() == this.numClients;
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  ////  LOGGING                                                             ////
  //////////////////////////////////////////////////////////////////////////////

  protected void logTaskResult( ClientRecord client, TestTaskResult result ) {
    ResultLogger.logTaskResult( client, this, result );
  }
  protected void logHangResult( ClientRecord client, TestTaskResult result ) {
    ResultLogger.logHangResult( client, this, result );
  }
  public void logHangResult( ClientRecord client, String msg ) {
    ResultLogger.logHangResult( client, this, msg );
    if (client.vm().getClientName().equals(UnitTestController.NAME)) {
      // The unit test controller VM has timed out
      try {
        File dir = new File("failures");
        dir.mkdir();
        
        File file = new File(dir, "HungDUnitTest.txt");
        file.createNewFile();
        FileWriter fw = new FileWriter(file, true /* append */);
        PrintWriter pw = new PrintWriter(fw, true);
        pw.println("Some DUnit test HUNG.  See end of dunit-progress*.txt");
        pw.flush();
        pw.close();
        
      } catch (IOException ex) {
        String s = "Couldn't log hung DUnit test";
        Log.getLogWriter().severe(s, ex);
      }
    }
  }
  public void logUnitTestResult( TestTaskResult result ) {
    if ( result.getErrorStatus() )
      log().severe( "Unit test result: FAILED ==> " + result );
    else
      log().info( "Unit test result: PASSED ==> " + result );
  }
  private LogWriter log() {
    return Log.getLogWriter();
  }
}
