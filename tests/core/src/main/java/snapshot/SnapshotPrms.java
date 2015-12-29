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
package snapshot;

import java.util.Vector;

import util.TestException;
import hydra.BasePrms;
import hydra.HydraConfigException;

public class SnapshotPrms extends BasePrms {

  /**
   * (String) String to indicate the export strategy used for tests exercise export/import
   * Possible strategies: cmdLineTool, apiFromOneVm, apiFromAllVms
   *              
   */
  static final String exportUsingCmdLineTool = "cmdLineTool";
  static final String exportUsingApiFromOneVm = "apiFromOneVm";
  static final String exportUsingApiFromAllVms = "apiFromAllVms";
  static final String exportUsingCli = "useCli";

  public static Long exportStrategy;
  public static String getExportStrategy() {
    Long key = exportStrategy;
    String value = tab().stringAt(key);
    return value;
  }

  /**
   * (String) String to indicate the restrictions on which types of VMs can do the 
   * Snapshot save/load operations.
   * Possible restrictions: PRAccessor, edgeClient
   *              
   */
  static final String PRAccessors = "PRAccessors";
  static final String EdgeClients = "edgeClients";

  public static Long restrictSnapshotOperationsTo;
  public static String getRestrictSnapshotOperationsTo() {
    Long key = restrictSnapshotOperationsTo;
    String value = tab().stringAt(key, "none");
    return value;
  }

  /** (boolean) whether or not to use a SnapshotFilter on export.
   *
   */
  public static Long useFilterOnExport;
  public static boolean useFilterOnExport() {
    Long key = useFilterOnExport;
    boolean value = tab().booleanAt(key, false);
    return value;
  }

  /** (boolean) whether or not to use a SnapshotFilter on import.
   *
   */
  public static Long useFilterOnImport;
  public static boolean useFilterOnImport() {
    Long key = useFilterOnImport;
    boolean value = tab().booleanAt(key, false);
    return value;
  }

  /** (int) The number of FilterObjects to create per region.
   *  Defaults to 1000.
   */
  public static Long numFilterObjects;
  public static int numFilterObjects() {
    Long key = numFilterObjects;
    int value = tab().intAt(key, 1000);
    return value;
  }

  /** (int) numColocatedRegions (for colocatedWith testing)
   *  Defaults to 1.
   */
  public static Long numColocatedRegions;
  public static int getNumColocatedRegions() {
    Long key = numColocatedRegions;
    int val = tasktab().intAt( key, tab().intAt(key, 1) );
    return val;
  }

  /** (boolean) whether or not to execute concurrentOps with snapshot save/load
   *
   */
  public static Long executeConcurrentOps;
  public static boolean executeConcurrentOps() {
    Long key = executeConcurrentOps;
    boolean value = tab().booleanAt(key, false);
    return value;
  }

  /** (boolean) whether or not to execute concurrentOps with snapshot save/load
   *
   */
  public static Long executeConcurrentRebalance;
  public static boolean executeConcurrentRebalance() {
    Long key = executeConcurrentRebalance;
    boolean value = tab().booleanAt(key, false);
    return value;
  }

  /** (String) Operations used for regions in servers or data hosts when
   *           the region size is > lowerThreshold and < upperThreshold.
   */
  public static Long operations;
  public static String getOperations() {
    Long key = operations;
    String value = tasktab().stringAt(key, tab().stringAt(key, null));
    return value;
  }

  /** (String) Operations used for regions in servers or data hosts when
   *           the region size is >=  upperThreshold.
   */
  public static Long upperThresholdOperations;
  public static String getUpperThresholdOperations() {
    Long key = upperThresholdOperations;
    String value = tasktab().stringAt(key, tab().stringAt(key, null));
    return value;
  }

  /** (String) Operations used for regions in servers or data hosts when
   *           the region size is >=  upperThreshold.
   */
  public static Long lowerThresholdOperations;
  public static String getLowerThresholdOperations() {
    Long key = upperThresholdOperations;
    String value = tasktab().stringAt(key, tab().stringAt(key, null));
    return value;
  }

  /** (int) The upper threshold that will trigger use of upperThresholdOperations
   *        in servers or data host vms.
   */
  public static Long upperThreshold;
  public static int getUpperThreshold() {
    Long key = upperThreshold;
    int value = tasktab().intAt(key, tab().intAt(key));
    return value;
  }

  /** (int) The lower threshold that will trigger use of lowerThresholdOperations.
   *        in servers or data host vms.
   */
  public static Long lowerThreshold;
  public static int getLowerThreshold() {
    Long key = lowerThreshold;
    int value = tasktab().intAt(key, tab().intAt(key));
    return value;
  }

  /** (int) The number of new keys to be included in the the map argument to putAll. 
   */
  public static Long numPutAllNewKeys;  
  public static int getNumPutAllNewKeys() {
    Long key = numPutAllNewKeys;
    int value = tasktab().intAt(key, tab().intAt(key));
    return value;
  }

  /** (int) The number of new keys to be included in the the map argument to putAll. 
   */
  public static Long numPutAllExistingKeys;  
  public static int getNumPutAllExistingKeys() {
    Long key = numPutAllExistingKeys;
    int value = tasktab().intAt(key, tab().intAt(key));
    return value;
  }

  /** (int) The number of seconds to run a concurrent test.
   *           We cannot use hydra's taskTimeSec parameter
   *           because of a small window of opportunity for the test to hang due
   *           to the test's "concurrent round robin" type of strategy.
   */
  public static Long secondsToRun;  
  public static int getSecondsToRun() {
    Long key = secondsToRun;
    int value = tasktab().intAt(key, tab().intAt(key));
    return value;
  }

  /** (boolean) If true, then each thread will only write a unique set of keys.
   * 
   */
  public static Long useUniqueKeys;
  public static boolean getUseUniqueKeys() {
    Long key = useUniqueKeys;
    boolean value = tasktab().booleanAt(key, tab().booleanAt(key, true));
    return value;
  }

  /** (boolean) If true use AdminDistributedSystem.shutDownAllMembers to stop vms. 
   * 
   */
  public static Long useShutDownAll;
  public static boolean getUseShutDownAll() {
    Long key = useShutDownAll;
    boolean value = tasktab().booleanAt(key, tab().booleanAt(key, false));
    return value;
  }

  /** (String) Class name of value to put into the region. Currently
   *  supported values are:
   *     util.ValueHolder (default)
   *     util.VHDataSerializable
   *     util.VHDataSerializableInstantiator
   */
  public static Long valueClassName;
  public static String getValueClassName() {
    Long key = valueClassName;
    String value = tasktab().stringAt(key, tab().stringAt(key, "util.ValueHolder"));
    return value;
  }

  /** (boolean) If true, then register the class VHDataSerializer, otherwise
   *            do not register this class.
   */
  public static Long registerSerializer;
  public static boolean getRegisterSerializer() {
    Long key = registerSerializer;
    boolean value = tasktab().booleanAt(key, tab().booleanAt(key, false));
    return value;
  }

  /** (int) The number of entries to load for each thread running the task.
   */
  public static Long numToLoad;  
  public static int getNumToLoad() {
    Long key = numToLoad;
    int value = tasktab().intAt(key, tab().intAt(key));
    return value;
  }
   
  /** (int) The number of seconds to sleep before proceeding with a shutDownAll.
   *        This allows async ops to pile up before shutDownAll is called. 
   */
  public static Long sleepSecBeforeShutDownAll;  
  public static int getSleepSecBeforeShutDownAll() {
    Long key = sleepSecBeforeShutDownAll;
    int value = tasktab().intAt(key, tab().intAt(key, 0));
    return value;
  }

  //------------------------------------------------------------------------
  // Utility methods
  //------------------------------------------------------------------------

  private static Object instantiate(Long key, String classname) {
    if (classname == null) {
      return null;
    }
    try {
      Class cls = Class.forName(classname);
      return cls.newInstance();
    } catch (Exception e) {
      throw new HydraConfigException("Illegal value for " + nameForKey(key) + ": cannot instantiate " + classname, e);
    }
  }

  // ================================================================================
  static {
    BasePrms.setValues(SnapshotPrms.class);
  }
}
