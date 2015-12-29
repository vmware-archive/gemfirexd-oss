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

package cacheperf.comparisons.replicated.delta;

import hydra.BasePrms;

/**
 *  A class used to store keys for test configuration settings.
 */
public class DeltaPrms extends BasePrms {

  static {
    setValues(DeltaPrms.class);
  }

  /**
   * (boolean)
   * Whether to obtain an object to update by getting it from the cache
   * (default) or by instantiating an uninitialized instance.  Either way,
   * the object must be an {@link objects.UpdatableObject} and, if
   * uninitialized, must also implement {@link com.gemstone.gemfire.Delta}.
   * Defaults to true.
   * <p>
   * False is used for "full delta", where, for example, an empty peer
   * or partitioned accessor can feed deltas to entries without the overhead
   * of a get by creating a dummy object and setting only delta fields.  Full
   * delta does not work in the general case.  It will cause object payload
   * loss and cache inconsistency if the delta sender has a local cache, or
   * if a delta receiver has to go back to the delta sender for the full
   * object, since the dummy object will end up in one or more caches.
   * <p>
   * Used by {@link cacheperf.comparisons.replicated.delta.DeltaClient
   * #updateDeltaDataTask}.
   * <p>
   * Not intended for randomization.
   */
  public static Long getBeforeUpdate;
  public static boolean getBeforeUpdate() {
    Long key = getBeforeUpdate;
    return tasktab().booleanAt(key, tab().booleanAt(key, true));
  }

  /**
   * (boolean)
   * Whether to complain if a test tries to use an object that does not
   * implement {@link com.gemstone.gemfire.Delta}.  Defaults to true,
   * so that it is harder to misconfigure a delta test.
   * <p>
   * This is for comparing delta to non-delta with the same delta client
   * code.
   * <p>
   * Used by {@link cacheperf.comparisons.replicated.delta.DeltaClient
   * #updateDeltaDataTask}.
   * <p>
   * Not intended for randomization.
   */
  public static Long enforceDeltaObjectsOnly;
  public static boolean enforceDeltaObjectsOnly() {
    Long key = enforceDeltaObjectsOnly;
    return tasktab().booleanAt(key, tab().booleanAt(key, true));
  }
}
