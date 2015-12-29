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
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.util.TimestampedEntryEvent;

/**
 * A subclass of EntryEventImpl used in WAN conflict resolution
 * 
 * @author Bruce Schuchardt
 */
public final class TimestampedEntryEventImpl extends EntryEventImpl implements
    TimestampedEntryEvent {

  private final int newDSID;
  private final int oldDSID;
  private final long newTimestamp;
  private final long oldTimestamp;

  public TimestampedEntryEventImpl(EntryEventImpl event, int newDSID,
      int oldDSID, long newTimestamp, long oldTimestamp) {
    super(event);
    this.newDSID = newDSID;
    this.oldDSID = oldDSID;
    this.newTimestamp = newTimestamp;
    this.oldTimestamp = oldTimestamp;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.util.TimestampedEntryEvent#getNewDistributedSystemID()
   */
  @Override
  public final int getNewDistributedSystemID() {
    return this.newDSID;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.util.TimestampedEntryEvent#getOldDistributedSystemID()
   */
  @Override
  public final int getOldDistributedSystemID() {
    return this.oldDSID;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.util.TimestampedEntryEvent#getNewTimestamp()
   */
  @Override
  public final long getNewTimestamp() {
    return this.newTimestamp;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.util.TimestampedEntryEvent#getOldTimestamp()
   */
  @Override
  public final long getOldTimestamp() {
    return this.oldTimestamp;
  }
}
