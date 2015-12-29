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
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;
import com.gemstone.gemfire.internal.offheap.annotations.Unretained;

/**
 * Internal {@link Delta} implementations can extend this interface for
 * convenience.
 * 
 * @since 7.0
 */
public abstract class AbstractDelta implements Delta {

  /**
   * @see Delta#apply(EntryEvent)
   */
  public Object apply(final EntryEvent<?, ?> putEvent) {
    return apply(putEvent.getRegion(), putEvent.getKey(),
        ((EntryEventImpl)putEvent).getOldValueAsOffHeapDeserializedOrRaw(), putEvent.getTransactionId() == null);
  }

  /**
   * @see Delta#apply(Region, Object, Object, boolean)
   */
  public abstract Object apply(Region<?, ?> region, Object key,
      @Unretained Object oldValue, boolean prepareForOffHeap);

  /**
   * @see Delta#merge(Region, Delta)
   */
  public Delta merge(Region<?, ?> region, Delta toMerge) {
    throw new UnsupportedOperationException("Delta#merge not implemented by "
        + getClass().getName());
  }

  /**
   * @see Delta#cloneDelta()
   */
  public Delta cloneDelta() {
    throw new UnsupportedOperationException(
        "Delta#cloneDelta not implemented by " + getClass().getName());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean allowCreate() {
    return false;
  }
  
  public void setVersionTag(VersionTag versionTag) {
    
  }

  public VersionTag getVersionTag() {
    return null;
  }
}
