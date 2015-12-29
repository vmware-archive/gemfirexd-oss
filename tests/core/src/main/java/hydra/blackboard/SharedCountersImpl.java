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

package hydra.blackboard;

//import java.util.*;

/**
 *
 *  Manages a set of indexed counters.  Methods are not thread-safe.
 *  Instances of this class are intended only for use by other classes
 *  such as ({@link RmiSharedCountersImpl}
 *  that overlay a transport type for cross-vm access and provide
 *  synchronization.
 *
 */

public class SharedCountersImpl implements SharedCounters {

  private long[] counters;
   
  /**
   *  Creates an instance with its counters set to the provided initial values.
   *  The number of counters managed by this instance is equal to the number of
   *  initial values.
   */
  public SharedCountersImpl( long[] initialValues ) {
    this.counters = initialValues;
  }    

  /**
   *  Implements {@link SharedCounters#read(int)}.
   */
  public long read( int index ) {
    return counters[index];
  }
  /**
   *  Implements {@link SharedCounters#add(int,long)}.
   */
  public long add( int index, long i ) {
    counters[index] += i;
    return counters[index];
  }
  /**
   *  Implements {@link SharedCounters#subtract(int,long)}.
   */
  public long subtract( int index, long i ) {
    counters[index] -= i;
    return counters[index];
  }
  /**
   *  Implements {@link SharedCounters#increment(int)}.
   */
  public void increment( int index ) {
    ++counters[index];
  }
  /**
   *  Implements {@link SharedCounters#decrement(int)}.
   */
  public void decrement( int index ) {
    --counters[index];
  }
  /**
   *  Increments the counter at index by 1 and returns the new count.
   */
  public long incrementAndRead( int index ) {
    ++counters[index];
    return counters[index];
  }
  /**
   *  Implements {@link SharedCounters#incrementAndRead(int)}.
   */
  public long decrementAndRead( int index ) {
    --counters[index];
    return counters[index];
  }
  /**
   *  Implements {@link SharedCounters#zero(int)}.
   */
  public void zero( int index ) {
    counters[index] = 0;
  }
  /**
   *  Implements {@link SharedCounters#setIfLarger(int,long)}.
   */
  public void setIfLarger( int index, long i ) {
    if ( i > counters[index] ) counters[index] = i;
  }
  /**
   *  Implements {@link SharedCounters#setIfSmaller(int,long)}.
   */
  public void setIfSmaller( int index, long i ) {
    if ( i < counters[index] ) counters[index] = i;
  }
  /**
   *  Implements {@link SharedCounters#getCounterValues}.
   */
  public long[] getCounterValues() {
    return this.counters;
  }
}
