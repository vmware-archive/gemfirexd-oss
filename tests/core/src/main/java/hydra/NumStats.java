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

package hydra;

import java.io.Serializable;

/**
 *  Keeps a running min, max, and mean.  Omits standard deviation to avoid
 *  having to hold onto each value.  Very efficient in terms of time and space.
 *  Includes method for fetching stats.
 */

public class NumStats implements Serializable {

  double min = Double.MAX_VALUE;
  double max = Double.MIN_VALUE;
  double total = 0.0;
  long count = 0;

  public NumStats() {
  }

  /**
   *  Adds a double to the stat.
   */
  public void add(double arg) { 
    if ( arg < this.min ) this.min = arg;
    if ( arg > this.max ) this.max = arg;
    this.total += arg;
    ++this.count;
  }
  /**
   *  Adds an int to the stat.
   */
  public void add(int arg) { 
    add((double)arg);
  }
  /**
   *  Adds a long to the stat.
   */
  public void add(long arg) { 
    add((double)arg);
  }
  /**
   *  Returns the min as a double.
   */
  public double min() {
    return this.min;
  }
  /**
   *  Returns the max as a double.
   */
  public double max() {
    return this.max;
  }
  /**
   *  Returns the mean as a double.
   */
  public double mean() {
    return this.total/this.count; 
  }
  /**
   *  Returns min, max, mean considered as times in milliseconds, as a string.
   *  Returns null if no values have been recorded.
   */
  public String printStatsAsTimes() {
    if ( this.count == 0 )
      return null;

    return "Number of elapsed times: " + this.count +
           "\nMin:  " + (float) this.min + " ms" +
           "\nMax:  " + (float) this.max + " ms" +
           "\nMean: " + (float) mean() + " ms";
  }
  /**
   *  A test routine.
   */
  public static void main(String[] args) {
    NumStats stats = new NumStats();
    for ( int i = 0; i <= 100; i++ ) {
      stats.add(i);
    }
    System.out.println( stats.printStatsAsTimes() );
  } 
}


