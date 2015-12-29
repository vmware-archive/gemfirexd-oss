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
package deltagii;

import hydra.BasePrms;
import util.TestException;
import com.gemstone.gemfire.internal.cache.InitialImageOperation.GIITestHookType;

public class DeltaGIIPrms extends BasePrms {

  /**
   * (int) The number of entries in the test region
   */
  public static Long numEntriesToLoad;
  public static int numEntriesToLoad() {
    Long key = numEntriesToLoad;
    int value = tasktab().intAt(key, tab().intAt(key, 100));
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

  /** (String) Operations used for regions in edge clients
   *           the region size is > lowerThresholdClient and < upperThresholdClient.
   */
  public static Long clientOperations;
  public static String getClientOperations() {
    Long key = clientOperations;
    String value = tasktab().stringAt(key, tab().stringAt(key, null));
    return value;
  }

  /** (String) Operations used for regions in edge clients
   *           the region size is >=  upperThresholdClient.
   */
  public static Long upperThresholdClientOperations;
  public static String getUpperThresholdClientOperations() {
    Long key = upperThresholdClientOperations;
    String value = tasktab().stringAt(key, tab().stringAt(key, null));
    return value;
  }

  /** (String) Operations used for regions in servers or data hosts when
   *           the region size is >=  upperThreshold.
   */
  public static Long lowerThresholdClientOperations;
  public static String getLowerThresholdClientOperations() {
    Long key = upperThresholdClientOperations;
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

  /** (int) The upper threshold that will trigger use of upperThresholdClientOperations
   *        in edge clients.
   */
  public static Long upperThresholdClient;
  public static int getUpperThresholdClient() {
    Long key = upperThresholdClient;
    int value = tasktab().intAt(key, tab().intAt(key));
    return value;
  }

  /** (int) The lower threshold that will trigger use of lowerThresholdClientOperations.
   *        in edge clients.
   */
  public static Long lowerThresholdClient;
  public static int getLowerThresholdClient() {
    Long key = lowerThresholdClient;
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

  /** (int) The number of rounds of execution (startup, doOps, stop, [provider does ops], restart and validate)
   */
  public static Long numExecutions;  
  public static int numExecutions() {
    Long key = numExecutions;
    int value = tasktab().intAt(key, tab().intAt(key));
    return value;
  }

  /** (boolean) If true, then the provider VMs will execute entry operations
   *  while the (future image requester vms) are offline.
   * 
   */
  public static Long providerDoesOps;
  public static boolean providerDoesOps() {
    Long key = providerDoesOps;
    boolean value = tasktab().booleanAt(key, tab().booleanAt(key, true));
    return value;
  }

  /** (boolean) If true, then the provider will sleep for 30 seconds to allow for a tombstone-gc.
   *   while the (future image requester vms) are offline.
   * 
   */
  public static Long doTombstoneGC;
  public static boolean doTombstoneGC() {
    Long key = doTombstoneGC;
    boolean value = tasktab().booleanAt(key, tab().booleanAt(key, false));
    return value;
  }

  /** (String) entry operations to be executed by the provider vm while the 
   *           (future) image requesters are offline.
   */
  public static Long providerOperations;
  public static String getProviderOperations() {
    Long key = providerOperations;
    String value = tasktab().stringAt(key, tab().stringAt(key, null));
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

  /** (boolean) If true, expect restarted vms to do a delta gii */
  public static Long expectDeltaGII;

  public static boolean expectDeltaGII() {
    Long key = expectDeltaGII;
    boolean value = tasktab().booleanAt(key, tab().booleanAt(key, false));
    return value;
  }

  /** support for concurrentHA tests (test hook to kill provider or requester during GII 
   *  (String) to allow us to determine at what point during gii we want to invoke our gii test hook
   */
  public static Long giiState;

  static final String BEFORE_REQUEST_RVV              = "beforeRequestRVV";
  static final String AFTER_REQUEST_RVV               = "afterRequestRVV";
  static final String AFTER_CALCULATED_UNFINISHED_OPS = "afterCalculatedUnfinishedOps";
  static final String BEFORE_SAVED_RECEIVED_RVV       = "beforeSavedReceivedRVV";
  static final String AFTER_SAVED_RECEIVED_RVV        = "afterSavedReceivedRVV";
  static final String AFTER_SENT_REQUEST_IMAGE        = "afterSentRequestImage";
  static final String AFTER_RECEIVED_IMAGE_REPLY      = "afterReceivedImageReply";
  static final String DURING_APPLY_DELTA              = "duringApplyDelta";
  static final String BEFORE_CLEAN_EXPIRED_TOMBSTONES = "beforeCleanExpiredTombstones";
  static final String AFTER_SAVED_RVV_END             = "afterSavedRvvEnd";
  static final String AFTER_RECEIVED_REQUEST_IMAGE    = "afterReceivedRequestImage";
  static final String DURING_PACKING_IMAGE            = "duringPackingImage";
  static final String AFTER_SENT_IMAGE_REPLY          = "afterSentImageReply";
  
  public static GIITestHookType getGIITestHookType(String giiStatePrm) {
    GIITestHookType giiTestHookType;

    if (giiStatePrm.equalsIgnoreCase(BEFORE_REQUEST_RVV)) {
      giiTestHookType = GIITestHookType.BeforeRequestRVV;
    } else if (giiStatePrm.equalsIgnoreCase(AFTER_REQUEST_RVV)) {
      giiTestHookType = GIITestHookType.AfterRequestRVV;
    } else if (giiStatePrm.equalsIgnoreCase(AFTER_CALCULATED_UNFINISHED_OPS)) {
      giiTestHookType = GIITestHookType.AfterCalculatedUnfinishedOps;
    } else if (giiStatePrm.equalsIgnoreCase(BEFORE_SAVED_RECEIVED_RVV)) {
      giiTestHookType = GIITestHookType.BeforeSavedReceivedRVV;
    } else if (giiStatePrm.equalsIgnoreCase(AFTER_SAVED_RECEIVED_RVV)) {
      giiTestHookType = GIITestHookType.AfterSavedReceivedRVV;
    } else if (giiStatePrm.equalsIgnoreCase(AFTER_SENT_REQUEST_IMAGE)) {
      giiTestHookType = GIITestHookType.AfterSentRequestImage;
    } else if (giiStatePrm.equalsIgnoreCase(AFTER_RECEIVED_IMAGE_REPLY)) {
      giiTestHookType = GIITestHookType.AfterReceivedImageReply;
    } else if (giiStatePrm.equalsIgnoreCase(DURING_APPLY_DELTA)) {
      giiTestHookType = GIITestHookType.DuringApplyDelta;
    } else if (giiStatePrm.equalsIgnoreCase(BEFORE_CLEAN_EXPIRED_TOMBSTONES)) {
      giiTestHookType = GIITestHookType.BeforeCleanExpiredTombstones;
    } else if (giiStatePrm.equalsIgnoreCase(AFTER_SAVED_RVV_END)) {
      giiTestHookType = GIITestHookType.AfterSavedRVVEnd;;
    } else if (giiStatePrm.equalsIgnoreCase(AFTER_RECEIVED_REQUEST_IMAGE)) {
      giiTestHookType = GIITestHookType.AfterReceivedRequestImage;
    } else if (giiStatePrm.equalsIgnoreCase(DURING_PACKING_IMAGE)) {
      giiTestHookType = GIITestHookType.DuringPackingImage;
    } else if (giiStatePrm.equalsIgnoreCase(AFTER_SENT_IMAGE_REPLY)) {
      giiTestHookType = GIITestHookType.AfterSentImageReply;
    } else {
        throw new TestException("Unknown giiStatePrm " + giiStatePrm);
    }
    return giiTestHookType;
  }

  public static String toStringForGIITestHookType(GIITestHookType giiTestHookType) {
    String s = null;

    switch (giiTestHookType) {
      // requester triggers
      case BeforeRequestRVV:
        s = BEFORE_REQUEST_RVV;
        break;
      case AfterRequestRVV:
        s = AFTER_REQUEST_RVV;
        break;
      case AfterCalculatedUnfinishedOps:
        s = AFTER_CALCULATED_UNFINISHED_OPS;
        break;
      case BeforeSavedReceivedRVV:
        s = BEFORE_SAVED_RECEIVED_RVV;
        break;
      case AfterSavedReceivedRVV:
        s= AFTER_SAVED_RECEIVED_RVV;
        break;
      case AfterSentRequestImage:
        s = AFTER_SENT_REQUEST_IMAGE;
        break;
      case AfterReceivedImageReply:
        s = AFTER_RECEIVED_IMAGE_REPLY;
        break;
      case DuringApplyDelta:
        s = DURING_APPLY_DELTA;
        break;
      case BeforeCleanExpiredTombstones:
        s = BEFORE_CLEAN_EXPIRED_TOMBSTONES;
        break;
      case AfterSavedRVVEnd:
        s = AFTER_SAVED_RVV_END;
        break;
      // provider triggers
      case AfterReceivedRequestImage:
        s = AFTER_RECEIVED_REQUEST_IMAGE;
        break;
      case DuringPackingImage:
        s = DURING_PACKING_IMAGE;
        break;
      case AfterSentImageReply:
        s = AFTER_SENT_IMAGE_REPLY;
        break;
      default:
        s = "Invalid giiTestHookType (" + giiTestHookType + ")";
    }
    return s;
  }

  // ================================================================================
  static {
    BasePrms.setValues(DeltaGIIPrms.class);
  }

}
