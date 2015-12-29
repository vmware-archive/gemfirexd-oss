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

package com.pivotal.gemfirexd.internal.engine.sql.conn;

import com.gemstone.gemfire.internal.shared.ClientSharedUtils;
import com.gemstone.gemfire.internal.util.ArrayUtils;

/**
 * Encapsulates the {@link ConnectionState} provided and the end-time for the
 * wait of batch-size to complete for it. Also implements {@link Comparable} to
 * first account for connections that need to be processed immediately
 * (preferring those that have exceeded the batch-size most), then to account
 * for end-times of those that have to be processed later.
 * 
 * @author swale
 */
final class ConnectionStateKey implements Comparable<ConnectionStateKey> {

  /** the {@link ConnectionState} to be processed */
  private final ConnectionState state;

  /**
   * the time limit when the wait for filling up of batch-size has to end in
   * every case
   */
  private final long endWaitTime;

  /** token to indicate that no wait is required for this {@link #state} */
  static final long TIME_IMMEDIATE = -1;

  /**
   * Construct a new key given a {@link ConnectionState} to be processed that is
   * suitable to be inserted as a key into sorted data-structures. Also keeps
   * account of the end-time for wait of batch-size to fill up from the time
   * when this constructor is invoked.
   */
  ConnectionStateKey(ConnectionState connState) {
    this.state = connState;
    if (this.state.numChanges() < this.state.minBatchSize()) {
      this.endWaitTime = System.currentTimeMillis() + this.state.waitMillis();
    }
    else {
      this.endWaitTime = TIME_IMMEDIATE;
    }
  }

  /** Get the {@link ConnectionState} that this key object encapsulates. */
  final ConnectionState connectionState() {
    return this.state;
  }

  /**
   * Get the time limit when the wait for filling up of batch-size has to end
   * for this {@link ConnectionState}.
   */
  final long endWaitTime() {
    return this.endWaitTime;
  }

  /**
   * Return true if the {@link ConnectionState} object in this key needs to be
   * processed immediately i.e. either the batch-size has filled up or the max
   * wait time ({@link ConnectionState#waitMillis()}) has gotten exhausted.
   */
  final boolean processNow() {
    // process immediately if either endtime has been reached, or number of
    // changes has equalled or exceeded the required batch size; however, no
    // processing is to be done if the number of changes is zero
    final int numChanges = this.state.numChanges();
    return this.endWaitTime == TIME_IMMEDIATE
        || (numChanges != 0 && (numChanges >= this.state.minBatchSize()
            || System.currentTimeMillis() >= this.endWaitTime));
  }

  /**
   * Compare this key object against another ordering by the ones that need to
   * be processed first.
   * 
   * @see Comparable#compareTo(Object)
   */
  @Override
  public final int compareTo(ConnectionStateKey conn2) {
    if (this == conn2) {
      return 0;
    }
    int remainingChanges1 = this.state.minBatchSize() - this.state.numChanges();
    int remainingChanges2 = conn2.state.minBatchSize()
        - conn2.state.numChanges();
    // first sort criteria is the number of remaining changes required to fill
    // the batch-size; if no more changes required for any of the two then
    // that one is to be preferred
    if (remainingChanges1 <= 0 || remainingChanges2 <= 0) {
      if (remainingChanges1 != remainingChanges2) {
        return (remainingChanges1 < remainingChanges2 ? -1 : 1);
      }
    }
    else {
      final long endTime1 = this.endWaitTime;
      final long endTime2 = conn2.endWaitTime;
      if (endTime1 != endTime2) {
        return (endTime1 < endTime2 ? -1 : 1);
      }
    }
    // prefer existing object
    return ArrayUtils.objectEquals(this.state, conn2.state) ? 0 : 1;
  }

  @Override
  public final String toString() {
    final StringBuilder sb = new StringBuilder().append(this.state).append(
        " [endTime: ");
    ClientSharedUtils.formatDate(this.endWaitTime, sb);
    sb.append(']');
    return sb.toString();
  }
}
