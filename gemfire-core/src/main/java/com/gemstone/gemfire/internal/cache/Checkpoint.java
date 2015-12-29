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

/**
 * Expresses changes in the state of a structure after some instant of time.
 * 
 * @author swale
 * @since 7.0
 */
public interface Checkpoint {

  /**
   * Enumeration for the possible object states for changed objects in a
   * checkpoint.
   */
  public enum ObjectState {
    ADDED, DELETED, UPDATED
  }

  /**
   * Attempt to acquire the lock on underlying data so no further changes to
   * that are possible. Typically if the underlying data is being concurrently
   * modified by other threads, then this lock will be required to ensure a
   * consistent view of the data changed after this {@link Checkpoint} was
   * captured.
   * 
   * @param msecs
   *          maximum time to wait for the lock; a -ve value indicates infinite
   *          wait
   */
  public boolean attemptLock(long msecs);

  /**
   * Release the lock acquired on the underlying data using
   * {@link #attemptLock(long)}.
   */
  public void releaseLock();

  /**
   * Return the changed element at given 0-based index.
   */
  public Object elementAt(int index);

  /**
   * Return the {@link ObjectState} for the changed element at given 0-based
   * index.
   */
  public ObjectState elementState(int index);

  /**
   * Return the number of elements changed after this checkpoint was captured.
   */
  public int numChanged();

  /**
   * Remove the changed element at the given 0-based index.
   */
  public Object removeAt(int index);

  /**
   * Remove all new elements changed after this checkpoint was captured.
   */
  public void removeAllChanged();

  /**
   * Update the checkpoint to point to the current end of the source.
   */
  public void updateToEnd();
}
