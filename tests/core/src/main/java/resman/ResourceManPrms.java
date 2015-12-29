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
package resman;

import hydra.BasePrms;
import hydra.ClientVmMgr;
import hydra.HydraConfigException;
import hydra.Log;
import hydra.TestConfig;

import java.util.Vector;

import com.gemstone.persistence.admin.Logger;

public class ResourceManPrms extends BasePrms {

  public static volatile Long taskNumberOfPuts;
  public static int getTaskNumberOfPuts() {
    return tasktab().intAt(taskNumberOfPuts);
  }

  public static volatile Long taskMinimumPutPercentage;
  public static float getTaskMinimumPutPercentage() {
    return (float)tasktab().doubleAt(taskMinimumPutPercentage);
  }

  public static volatile Long taskTolerateLowMemSec;
  public static int getTaskTolerateLowMemSec() {
    return tab().intAt(taskTolerateLowMemSec, tasktab().intAt(taskTolerateLowMemSec));
  }

  public static Long waitForLowMemSec;
  public static int getTaskWaitForLowMemSec() {
    return tasktab().intAt(waitForLowMemSec);
  }

  public static volatile Long taskEvictionPercentage;
  public static double getTaskEvictionPercentage() {
    return tasktab().doubleAt(taskEvictionPercentage);
  }

  /**
   *  (int) The target counter value for verify off-heap memory to wait for to coordinate
   *        quietness in the system when off-heap validation is running.
   */
  public static Long offHeapVerifyTargetCount;
  public static int getOffHeapVerifyTargetCount() {
    Long key = offHeapVerifyTargetCount;
    int value = tab().intAt(key, 0);
    return value;
  }
  
  /** (int) numRegions (for multipleRegions and/or colocatedWith testing)
   *  Defaults to 1.
   */
  public static volatile Long numRegions;
  public static int getNumRegions() {
    final Long key = numRegions;
    return tasktab().intAt( key, tab().intAt(key, 1) );
  }

  public static volatile Long threadGroupNamesDoingEntryOps;
  @SuppressWarnings("unchecked")
  public static Vector<String> getTaskThreadGroupNamesDoingEntryOps() {
    return tasktab().vecAt(threadGroupNamesDoingEntryOps);
  }

  /** (int) The local max memory setting for the PR.
   *  Valid values:
   */
  public static Long putAllSize;
  public static int getPutAllSize() {
    final Long key = putAllSize;
    final int val = tasktab().intAt( key, tab().intAt(key, -1) );
    return val;
  }

  /**
   * The set of stop modes to use
   */
  public static Long stopModes;
  public static int getTaskStopMode() {
    return ClientVmMgr.toStopMode( tasktab().stringAt(stopModes) );
  }

  /**
   * The number of seconds to remain critical
   */
  public static Long remainCriticalSeconds;
  public static long getTaskRemainCriticalSeconds() {
    return tasktab().longAt(remainCriticalSeconds);
  }

  /**
   * Total number of datastore VMs (which could go critical)
   */
  public static Long totalDatastoreCount;
  public static int getTotalDatastoreCount() {
    return tasktab().intAt(totalDatastoreCount);
  }
  
  public static Long inspectorType;
  public static String getInspectorType() {
    return tab().stringAt(inspectorType, "");
  }

  /*--------------------------- Utility methods ------------------*/

  private static Object instantiate( final Long key, final String classname ) {
    if ( classname == null ) {
      return null;
    }

    try {
      final Class cls = Class.forName( classname );
      return cls.newInstance();
    } catch( final Exception e ) {
      throw new HydraConfigException( "Illegal value for " + nameForKey( key
      ) + ": cannot instantiate " + classname, e );
    }
  }

  // ================================================================================
  static {
    BasePrms.setValues(ResourceManPrms.class);
  }
}
