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
package com.gemstone.gemfire.cache.query;

//import java.util.*;

/**
 * Provides statistics about a GemFire cache {@link Index}.
 *
 * @since 4.0
 */
public interface IndexStatistics {

  /** Return the total number of times this index has been updated
   */
  public long getNumUpdates();
  
  /** Returns the total amount of time (in nanoseconds) spent updating
   *  this index.
   */
  public long getTotalUpdateTime();
  
  /**
   * Returns the total number of times this index has been accessed by
   * a query.
   */
  public long getTotalUses();

  /**
   * Returns the number of keys in this index.
   */
  public long getNumberOfKeys();

  /**
   * Returns the number of values in this index.
   */
  public long getNumberOfValues();


  /** Return the number of values for the specified key
   *  in this index.
   */
  public long getNumberOfValues(Object key);
}
