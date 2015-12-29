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

public interface SharedCounters {

  /**
   *  Returns the current value of the counter at index.
   */
  public long read( int index );

  /**
   *  Adds <code>i</code> to the counter at <code>index</code>.
   */
  public long add( int index, long i );

  /**
   *  Subtracts <code>i</code> from the counter at <code>index</code>.
   */
  public long subtract( int index, long i );

  /**
   *  Increments the counter at <code>index</code> by 1.
   */
  public void increment( int index );

  /**
   *  Decrements the counter at <code>index</code> by 1.
   */
  public void decrement( int index );

  /**
   *  Increments the counter at <code>index</code> by 1 and returns the new count.
   */
  public long incrementAndRead( int index );

  /**
   *  Decrements the counter at <code>index</code> by 1 and returns the new count.
   */
  public long decrementAndRead( int index );

  /**
   *  Sets the counter at <code>index</code> to 0.
   */
  public void zero( int index );

  /**
   *  Sets the counter at <code>index</code> to <code>i</code> only if
   *  <code>i</code> is greater than the current count.  This is a convenient
   *  way to track a maximum value.
   */
  public void setIfLarger( int index, long i );

  /**
   *  Sets the counter at <code>index</code> to <code>i</code> only if
   *  <code>i</code> is less than the current count.  This is a convenient
   *  way to track a minimum value.
   */
  public void setIfSmaller( int index, long i );

  /**
   *  Returns the values of the counters in an array.
   */
  public long[] getCounterValues();
}
