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

import java.io.Serializable;

/**
 * Interface <code>Conflatable</code> is used by the bridge server client
 * notification mechanism to conflate messages being sent from the server to the
 * client.
 * 
 * @author Barry Oglesby
 * 
 * @since 4.2
 */
public interface Conflatable extends Serializable {

  /**
   * Returns whether the object should be conflated.
   * 
   * Note that in GemFireXD the DDL conflation semantics mean that if this method
   * returns true, then all the matching entries including this one will be
   * removed from the queue. Cases of merging two or more entries are now
   * handled by {@link #merge(Conflatable)}.
   * 
   * @return whether the object should be conflated
   */
  public boolean shouldBeConflated();

  /**
   * Returns true if the object should be merged with other using
   * {@link #merge(Conflatable)} rather than a simple conflation.
   * 
   * @return whether object should be merged
   */
  public boolean shouldBeMerged();

  /**
   * Merges the given {@link Conflatable} object into this object if possible.
   * Invoked only when {@link #shouldBeMerged()} returns true.
   * 
   * @param existing
   *          the other existing conflatable object that should be merged into
   *          this object
   * 
   * @return true if merge was successful else false
   */
  public boolean merge(Conflatable existing);

  /**
   * Returns the name of the region for this <code>Conflatable</code>
   * 
   * @return the name of the region for this <code>Conflatable</code>
   */
  public String getRegionToConflate();

  /**
   * Returns the key for this <code>Conflatable</code>
   * 
   * @return the key for this <code>Conflatable</code>
   */
  public Object getKeyToConflate();

  /**
   * Returns the value for this <code>Conflatable</code>
   * 
   * @return the value for this <code>Conflatable</code>
   */
  public Object getValueToConflate();

  /**
   * Sets the latest value for this <code>Conflatable</code>
   * 
   * @param value
   *          The latest value
   */
  public void setLatestValue(Object value);

  /**
   * Return this event's identifier
   * 
   * @return EventID object uniquely identifying the Event
   */
  public EventID getEventId();
}
