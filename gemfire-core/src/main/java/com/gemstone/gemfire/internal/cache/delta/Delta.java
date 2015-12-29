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

package com.gemstone.gemfire.internal.cache.delta;

import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;

/**
 * Represents changes to apply to a region entry instead of a new value.
 * A Delta is passed as the new value in a put operation on a Region
 * and knows how to apply itself to an old value.
 *
 * Internal Note: When an update message carries a Delta as a payload,
 * it makes sure it gets deserialized before being put into the region.
 *
 * @author Eric Zoerner
 * @since 5.5
 * @see com.gemstone.gemfire.internal.cache.UpdateOperation
 */
public interface Delta {

  /**
   * Apply delta to the old value from the provided EntryEvent. If the delta
   * cannot be applied for any reason then an (unchecked) exception should be
   * thrown. If the put is being applied in a distributed-ack scope, then the
   * exception will be propagated back to the originating put call, but will not
   * necessarily cause puts in other servers to fail.
   * 
   * @param putEvent
   *          the EntryEvent for the put operation, from which the old value can
   *          be obtained (as well as other information such as the key and
   *          region being operated on)
   * 
   * @return the new value to be put into the region
   */
  Object apply(EntryEvent<?, ?> putEvent);

  /**
   * Apply delta to the given old value. If the delta cannot be applied for any
   * reason then an (unchecked) exception should be thrown. If the put is being
   * applied in a distributed-ack scope, then the exception will be propagated
   * back to the originating put call, but will not necessarily cause puts in
   * other servers to fail.
   * 
   * @param region
   *          the Region for the put operation
   * @param key
   *          the key for the entry
   * @param oldValue
   *          the current value for the entry
   * @param prepareForOffHeap  Used for gfxd update with row having LOB columns. Indicates whether the metadata 
   * of the modified columns should be contained in the resulting value.         
   * 
   * @return the new value to be put into the region
   */
  Object apply(Region<?, ?> region, Object key, Object oldValue, boolean prepareForOffHeap);

  /**
   * Merge two deltas into a single delta. The return value can be "this" or a
   * new Delta.
   */
  Delta merge(Region<?, ?> region, Delta toMerge);

  /**
   * Clone the given delta.
   */
  Delta cloneDelta();

  /**
   * Allow a Delta to be put in region even if there is no existing value.
   */
  boolean allowCreate();
  
  /**
   * @param versionTag
   *          the versionTag to set
   */
  public void setVersionTag(VersionTag versionTag);

  /**
   * @return the concurrency versioning tag for this event, if any
   */
  public VersionTag getVersionTag();
}
