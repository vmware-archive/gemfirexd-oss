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
package getInitialImage;

import hydra.BasePrms;

public class InitImagePrms extends BasePrms {

  /** (int) The number of keys in the region. */
  public static Long numKeys;

  /** (int) The number of new keys to add to the region. */
  public static Long numNewKeys;

  /** (boolean) True if a new entry should appear via a cacheLoader, false otherwise. */
  public static Long useCacheLoader;

  /**
   * (String) Indicates the dataPolicy attributes of the 2 distributed caches that getInitialImage will use as its source.
   * Currently supported dataPolicies: empty, normal, persistentReplicate, replicate.  Tests will need additions for new
   * SubscriptionAttributes (InterestPolicy).
   */
  public static Long giiSourceDataPolicy;

  /** (boolean) True indicates that the destroyRegion is to occur concurrently with the getInitialImage on that region. */
  public static Long destroyConcurrently;

  /**
   * (boolean) True indicates that we will attempt to destroy the parent region of the region under construction via
   * getInitialImage rather than the region being constructed.
   */
  public static Long destroyParentRegion;

  /** (String) The fully qualified name of the cache loader to use if useCacheLoader is true. */
  public static Long cacheLoaderClass;

  /** (boolean) True if the region should be created with a cache writer, false otherwise */
  public static Long useCacheWriter;

  /** (boolean) True if test is a function execution test */
  public static Long isFunctionExecutionTest;

  /**
   * (int) The number of millseconds to sleep for each entry processed by gii. This is used to tune the tests to make gii
   * long enough so that operations can occur while gii is in progress.
   */
  public static Long giiPerEntrySleepMS;

  /**
   * (boolean) Whether to validate destruction after destroy .  Defaults to false. The destroy is local by default, but
   * can be distributed using {@link #destroyLocally}.  Not for use with oneof or range. Used in getInitialImage tests
   * to verify that the cache under construction will not return a destroyed value while the getInitialImage is in
   * progress
   */
  public static Long validateDestroyed;

  public static boolean validateDestroyed() {
    Long key = validateDestroyed;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  /**
   * (boolean) True if the test execute entry operations within a single transaction in the HydraTask_doOps() method.
   * Defaults to false
   */
  public static Long useTransactions;

  public static boolean useTransactions() {
    Long key = useTransactions;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  /** (boolean) True if the test uses replicated regions (for function execution). Defaults to false */
  public static Long useReplicatedRegions;

  public static boolean useReplicatedRegions() {
    Long key = useReplicatedRegions;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  //----------------------------------------------------------------------------
  //  getInitialImage verification params
  //----------------------------------------------------------------------------

  /**
   * (String) operation name to use in statspec when verifying nonblocking behavior of getInitialImage (e.g. get put ).
   * Defaults to null. Used to enforce minimum number of 'ops' that must be executed while the openCache is in progress
   * in order to validate nonblocking behavior.  Used by {@link CachePerfClient#verifyOpsNonBlocking}.
   */
  public static Long opStatName;

  public static String opStatName() {
    Long key = opStatName;
    return tasktab().stringAt(key, tab().stringAt(key, null));
  }

  /**
   * (int) The minimum number of operations deemed acceptable when verifying nonblocking behavior of getInitialImage.
   * Default is 100.
   */
  public static Long minOps;

  public static int getMinOps() {
    Long key = minOps;
    int val = tasktab().intAt(key, tab().intAt(key, 100));
    return val;
  }

  /**
   * (boolean) Whether to wait for getInitialImagesInProgress == true before closing the cache. Defaults to false. Not
   * for use with oneof or range.
   */
  public static Long waitForGetInitialImageStart;

  public static boolean waitForGetInitialImageStart() {
    Long key = waitForGetInitialImageStart;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  /**
   * (boolean) Whether to wait for getInitialImagesInProgress == true before closing the cache. Defaults to false. Not
   * for use with oneof or range.
   */
  public static Long giiHasSourceMember;

  public static boolean giiHasSourceMember() {
    Long key = giiHasSourceMember;
    return tasktab().booleanAt(key, tab().booleanAt(key, true));
  }

  // ================================================================================
  static {
    BasePrms.setValues(InitImagePrms.class);
  }

  /** (boolean) If true, expect restarted vms to do a delta gii */
  public static Long expectDeltaGII;
  public static boolean expectDeltaGII() {
    Long key = expectDeltaGII;
    boolean value = tasktab().booleanAt(key, tab().booleanAt(key, false));
    return value;
  }
}
