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

import com.gemstone.gemfire.*;
import com.gemstone.gemfire.internal.HostStatHelper;
import com.gemstone.gemfire.internal.PureJavaMode;
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.distributed.internal.*;
import java.io.*;
import java.rmi.RemoteException;
import java.util.*;
import util.TestHelper;

/**
 * Monitors the conditions in {@link hydra.StatMonitorPrms#conditions}
 * and halts the test if one of the conditions is violated.  Hydra automatically
 * starts a monitor in each remote hostagent, plus the hydra master, which
 * serves as the local hostagent.  See {@link hydra.StatMonitorPrms} for other
 * configuration options.
 * <p>
 * To start a monitor, invoke {@link StatMonitor#start(HostDescription,
 * MasterProxyIF, TestConfig)}.  To stop the monitor, invoke {@link
 * StatMonitor#stop}.
 * <p>
 * StatMonitor is not suitable for use by hydra clients, since it connects to a
 * distributed system outside the normal hydra channels.  Also, all monitors,
 * including those started automatically by hydra, share the same conditions.
 * Speak up if you would like the monitor to be made suitable for clients.
 *
 * @author lises
 * @since 5.0
 */
public class StatMonitor implements Runnable {

  //----------------------------------------------------------------------------
  //  STATIC FIELDS
  //----------------------------------------------------------------------------

  /** whether stacks have been dumped for this VM */
  private static boolean DumpedStacks = false;

  /** log */
  private static LogWriter log;

  /** test configuration (regular or hostagent version) */
  private static TestConfig testConfig;

  //----------------------------------------------------------------------------
  //  INSTANCE FIELDS
  //----------------------------------------------------------------------------

  /** conditions monitored by this instance */
  private Vector conditions;

  /** type of host on which this thread is running */
  private String os;

  /** master proxy used to report violations */
  private MasterProxyIF master;

  /** host description for the host this thread is running on */
  private HostDescription hd;

  //----------------------------------------------------------------------------
  //  CONSTRUCTORS
  //----------------------------------------------------------------------------

  public StatMonitor(Vector conditions, HostDescription hd,
                                        MasterProxyIF master,
                                        TestConfig config) {
     this.conditions = conditions;
     this.hd = hd;
     this.master = master;
     StatMonitor.testConfig = config;
     StatMonitor.log = Log.getLogWriter();
  }

  //----------------------------------------------------------------------------
  //  RUN
  //----------------------------------------------------------------------------

  /**
   * Runs the statistics monitor.
   */
  public void run() {
    Thread.currentThread().setName("StatMonitor");
    try {
      StatMonitor.log.info("Statistics monitor is running");
      monitor();
      StatMonitor.log.info("Statistics monitor is stopped");
    } 
    catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    }
    catch (Throwable t) {
      try {
        this.master.reportStatMonitorError(t);
      } catch (RemoteException e) {
        StatMonitor.log.info(TestHelper.getStackTrace(e));
        throw new HydraRuntimeException("Could not reach master", e);
      }
    }
  }

  /**
   * Monitors conditions by checking values of statistics.
   */
  private void monitor() {
    int sampleRate = StatMonitorPrms.getSampleRateMs(testConfig);
    boolean archiveStats = StatMonitorPrms.archiveStats(testConfig);
    boolean verbose = StatMonitorPrms.verbose(testConfig);
    StatisticsFactory factory = getStatisticsFactory(sampleRate, archiveStats);
    while (true) {
      MasterController.sleepForMs(sampleRate);
      for (Iterator i = this.conditions.iterator(); i.hasNext();) {
        if (MonitorStop) return; // stop when asked
        Condition condition = (Condition)i.next();
        StatisticsType type = getStatisticsType(factory, condition.getType());
        Statistics[] statArray = factory.findStatisticsByType(type);
        for (int j = 0; j < statArray.length; j++) {
          // check condition for each instance
          Statistics stats = statArray[j];
          Number n = null;
          try {
            n = stats.get(condition.getStat());
            if (n == null) {
              String s = condition.toShortString() + " has no value";
              StatMonitor.log.warning(s);
              i.remove();
            }
          } catch (IllegalArgumentException e) {
            String s = condition.toShortString() + " " + e.getMessage();
            StatMonitor.log.warning(s);
            i.remove();
          }
          if (n != null) {
            double currentValue = n.doubleValue();
            if (!condition.verify(currentValue)) {
              int action = condition.getAction();
              String s = null;
              switch (action) {
                case _HALT:
                  s = "Value " + currentValue + " violates " + condition;
                  throw new HydraRuntimeException(s);
                case _DUMP:
                  if (!DumpedStacks) {
                    s = "Value " + currentValue + " violates " + condition
                      + ", dumping stacks on this host";
                    StatMonitor.log.severe(s);
                    printLocalProcessStacks();
                    MasterController.sleepForMs(5000);
                    StatMonitor.log.severe(s + " 5 seconds later");
                    printLocalProcessStacks();
                    DumpedStacks = true;
                  }
                  break;
                case _WARN:
                  s = "Value " + currentValue + " violates " + condition;
                  StatMonitor.log.warning(s);
                  break;
                default:
                  throw new HydraInternalException("Should not happen: " + action);
              }
            } else if (verbose) {
              String s = "Value " + currentValue + " satisfies " + condition;
              StatMonitor.log.info(s);
            }
          }
        }
      }
    }
  }

  /**
   * Returns a statistics factory for this VM.
   */
  private StatisticsFactory getStatisticsFactory(int sampleRate,
                                                 boolean archiveStats) {
    // connect to a loner distributed system with a statistics sampler and
    // optional archive
    Properties p = new Properties();
    if (archiveStats) {
      String dirName = System.getProperty("user.dir") + "/statmon_"
                     + ProcessMgr.getProcessId();
      FileUtil.mkdir(new File(dirName));
      try {
        this.master.recordDir(this.hd, "statmon", dirName);
      } catch (RemoteException e) {
        String s = "Unable to record system directory with master: " + dirName;
        throw new HydraRuntimeException(s, e);
      }
      p.setProperty(DistributionConfig.STATISTIC_ARCHIVE_FILE_NAME,
                    dirName + "/statArchive.gfs");
    }
    p.setProperty(DistributionConfig.STATISTIC_SAMPLING_ENABLED_NAME, "true");
    p.setProperty(DistributionConfig.STATISTIC_SAMPLE_RATE_NAME,
                                                  String.valueOf(sampleRate));
    p.setProperty(DistributionConfig.LOCATORS_NAME, "");
    p.setProperty(DistributionConfig.MCAST_PORT_NAME, String.valueOf(0));

    DistributedSystem ds = DistributedSystem.connect(p);
    return (StatisticsFactory)ds;
  }

  /**
   * Gets the statistics instance from the factory with the specified type.
   */
  private static StatisticsType getStatisticsType(StatisticsFactory factory,
                                                  String name) {
    StatisticsType type = (StatisticsType)AllTypes.get(name);
    if (type == null) {
      synchronized (AllTypes) {
        type = factory.findType(name);
        if (type == null) { // these are lazily created, so look again
          type = factory.findType(name);
          if (type == null) {
            throw new HydraConfigException("No statistic with type " + name);
          }
        }
        AllTypes.put(name, type);
      }
    }
    return type;
  }

  /**
   * Maps statistics types to the instance of that type.
   */
  private static Map AllTypes = new HashMap();

  //----------------------------------------------------------------------------
  //  STATICS
  //----------------------------------------------------------------------------

  private static Thread Monitor = null;
  private static boolean MonitorStop = false;

  protected static Vector parseConditions(Vector conditionsIn) {
    Vector conditionsOut = null;
    if (conditionsIn != null) {
      conditionsOut = new Vector();
      for (Iterator i = conditionsIn.iterator(); i.hasNext();) {
        Vector conditionIn = (Vector)i.next();
        conditionsOut.add(new Condition(conditionIn));
      }
    }
    return conditionsOut;
  }

  /**
   * Starts the statistics monitor in this VM, using the supplied master for
   * reporting violations.  Only starts a monitor if there are conditions.
   */
  public static void start(HostDescription hd, MasterProxyIF master, TestConfig tc) {
    if (! PureJavaMode.osStatsAreAvailable()) {
      Log.getLogWriter().warning("Skipping statistics monitoring due to running in pure Java mode");
      return;
    }
    Vector conditions = StatMonitorPrms.getConditions(tc);
    boolean archiveStats = StatMonitorPrms.archiveStats(tc);
    if (conditions != null || archiveStats) {
      StatMonitor statmon = new StatMonitor(conditions, hd, master, tc);
      Log.getLogWriter().info("Starting statistics monitor with conditions " + conditions + " and archiving set " + archiveStats);
      Monitor = new Thread(statmon);
      Monitor.start();
      Log.getLogWriter().info("Started statistics monitor with conditions " + conditions + " and archiving set " + archiveStats);
    }
  }

  /**
   * Stops the statistics monitor in this VM.
   */
  public static void stop() {
    if (Monitor != null) {
      Log.getLogWriter().info("Stopping statistics monitor");
      MonitorStop = true;
      Monitor.interrupt();
      Monitor = null;
    }
  }

  /**
   * Prints stack dumps for all processes on this host.
   */
  private void printLocalProcessStacks() {
    String fn = System.getProperty("user.dir") + "/dumprun.sh";
    String localhost = HostHelper.getLocalHost();
    String masterhost = testConfig.getMasterDescription().getVmDescription()
                          .getHostDescription().getHostName();
    try {
      String line;
      FileInputStream fis = new FileInputStream(fn);
      BufferedReader br = new BufferedReader(new InputStreamReader(fis));
      while((line = br.readLine()) != null) {
        if (localhost.equals(masterhost) && line.startsWith("kill")
        || !line.startsWith("#") && (line.indexOf(localhost) != -1)) {
          String pidStr = line.substring(line.indexOf("QUIT") + 5);
          int pid = Integer.valueOf(pidStr).intValue();
          ProcessMgr.printProcessStacks(localhost, pid);
        }
      }
      br.close();
    } catch(FileNotFoundException e) {
      throw new HydraRuntimeException("Unable to find file: " + fn, e);
    } catch(IOException e) {
      throw new HydraRuntimeException("Unable to process file: " + fn, e);
    }
  }

    /** Op: The current value and the initial value differ by less than the amount */
    public static final String HAS_CHANGED_BY_LESS_THAN = "hasChangedByLessThan";
    /** Op: The current value and the initial value do not differ (the amount is ignored) */
    public static final String IS_UNCHANGED = "isUnchanged";
    /** Op: The current value is less than the amount */
    public static final String IS_LESS_THAN = "isLessThan";
    /** Op: The current value is greater than the amount */
    public static final String IS_GREATER_THAN = "isGreaterThan";
    /** Op: The current value is equal to the amount */
    public static final String IS_EQUAL_TO = "isEqualTo";

    /** Action: Halt the test when condition is violated. */
    public static final String HALT = "halt";
    /** Action: Do stack dumps on the test when condition is violated. */
    public static final String DUMP = "dump";
    /** Action: Warn the test when condition is violated. */
    public static final String WARN = "warn";

    // action codes
    private static final int _HALT = 0; // halt test execution
    private static final int _DUMP = 1; // do stack dumps
    private static final int _WARN = 2; // log warning

  /**
   * A user-defined condition.
   */
  private static class Condition implements Serializable {

    // ops
    private static final int _HAS_CHANGED_BY_LESS_THAN = 0;
    private static final int _IS_UNCHANGED             = 1;
    private static final int _IS_LESS_THAN             = 2;
    private static final int _IS_GREATER_THAN          = 3;
    private static final int _IS_EQUAL_TO              = 4;

    String type;
    String stat;
    int op;
    double amount;
    int action;
    double value = -1;
    protected Condition(Vector condition) {
      if (condition.size() != 5) {
        String s = "Statistic requires 5 fields: type stat op amount action, for "
               + "example, SystemStats pagesSwappedIn hasChangedByLessThan 5000 halt";
        throw new HydraConfigException(s);
      }
      this.type = (String)condition.get(0);
      this.stat = (String)condition.get(1);
      if (this.type.equals("SystemStats") || this.type.equals("ProcessStats")) {
        this.type = getOS() + this.type; // handle o/s dependencies
      }
      this.op = toOp((String)condition.get(2));
      try {
        this.amount = Double.valueOf((String)condition.get(3)).doubleValue();
      } catch (NumberFormatException e) {
        String s = "Statistic amount is not a number: " + condition.get(3);
        throw new HydraConfigException(s);
      }
      this.action = toAction((String)condition.get(4));
    }
    public String getType() {
      return this.type;
    }
    public String getStat() {
      return this.stat;
    }
    public int getOp() {
      return this.op;
    }
    public double getAmount() {
      return this.amount;
    }
    public int getAction() {
      return this.action;
    }
    public double getValue() {
      return this.value;
    }
    public boolean verify(double currentValue) {
      switch (this.op) {
        case _HAS_CHANGED_BY_LESS_THAN:
                 return verifyHasChangedByLessThan(currentValue);
        case _IS_UNCHANGED:
                 return verifyIsUnchanged(currentValue);
        case _IS_LESS_THAN:
                 return verifyIsLessThan(currentValue);
        case _IS_GREATER_THAN:
                  return verifyIsGreaterThan(currentValue);
        case _IS_EQUAL_TO:
                  return verifyIsEqualTo(currentValue);
        default:
          throw new HydraInternalException("Unhandled op: " + this.op);
      }
    }
    private boolean verifyHasChangedByLessThan(double currentValue) {
      if (this.value == -1) {
        this.value = currentValue;
      }
      return Math.abs(currentValue - this.value) < amount;
    }
    private boolean verifyIsUnchanged(double currentValue) {
      if (this.value == -1) {
        this.value = currentValue;
      }
      return currentValue == this.value;
    }
    private boolean verifyIsLessThan(double currentValue) {
      return currentValue < amount;
    }
    private boolean verifyIsGreaterThan(double currentValue) {
      return currentValue > amount;
    }
    private boolean verifyIsEqualTo(double currentValue) {
      return currentValue == amount;
    }
    public String toString() {
      String s = this.type + " " + this.stat + " "
               + toOpStr(this.op) + " " + this.amount + " "
               + toActionStr(this.action);
      if (this.value != -1) {
        s += " with value " + this.value;
      }
      return s;
    }
    public String toShortString() {
      return this.type + " " + this.stat;
    }
    private int toOp(String opStr) {
      if (opStr.equalsIgnoreCase(HAS_CHANGED_BY_LESS_THAN)) {
        return _HAS_CHANGED_BY_LESS_THAN;
      } else if (opStr.equalsIgnoreCase(IS_UNCHANGED)) {
        return _IS_UNCHANGED;
      } else if (opStr.equalsIgnoreCase(IS_LESS_THAN)) {
        return _IS_LESS_THAN;
      } else if (opStr.equalsIgnoreCase(IS_GREATER_THAN)) {
        return _IS_GREATER_THAN;
      } else if (opStr.equalsIgnoreCase(IS_EQUAL_TO)) {
        return _IS_EQUAL_TO;
      } else {
        String s = "Illegal op: " + opStr;
        throw new HydraConfigException(s);
      }
    }
    private String toOpStr(int opType) {
      switch (opType) {
        case _HAS_CHANGED_BY_LESS_THAN:
              return HAS_CHANGED_BY_LESS_THAN;
        case _IS_UNCHANGED:
              return IS_UNCHANGED;
        case _IS_LESS_THAN:
              return IS_LESS_THAN;
        case _IS_GREATER_THAN:
              return IS_GREATER_THAN;
        case _IS_EQUAL_TO:
              return IS_EQUAL_TO;
        default:
          throw new HydraInternalException("Should not happen");
      }
    }
    private int toAction(String actionStr) {
      if (actionStr.equalsIgnoreCase(HALT)) {
        return _HALT;
      } else if (actionStr.equalsIgnoreCase(DUMP)) {
        return _DUMP;
      } else if (actionStr.equalsIgnoreCase(WARN)) {
        return _WARN;
      } else {
        String s = "Illegal action: " + actionStr;
        throw new HydraConfigException(s);
      }
    }
    private String toActionStr(int actionType) {
      switch (actionType) {
        case _HALT:
              return HALT;
        case _DUMP:
              return DUMP;
        case _WARN:
              return WARN;
        default:
          throw new HydraInternalException("Should not happen");
      }
    }
    private String getOS() {
       String o = System.getProperty("os.name");
       if (o.contains("Linux")) return "Linux";
       else if (o.contains("SunOS")) return "Solaris";
       else if (o.contains("Windows")) return "Windows";
       else throw new HydraRuntimeException("Unsupported O/S: " + o);
    }
  }
}
