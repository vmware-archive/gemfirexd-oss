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

package perffmwk;

import java.util.*;

/**
 * Represents the statistics-related information from a test run.
 *
 * @see perffmwk.StatConfig
 */
public class ValueComparator {

  /** The index of this instance. */
  private int index;

  /** The test run. */
  private Test test;

  /** The values of the interesting statistics from the test run. */
  private SortedMap statValues;

  /**
   * Creates a new instance for the given test.
   */
  public ValueComparator(int index, Test test) {
    this.index = index;
    this.test = test;
  }

  /**
   * Returns the index of this comparator.
   */
  public int getIndex() {
    return this.index;
  }

  /**
   * Returns the test.
   */
  public Test getTest() {
    return this.test;
  }

  /**
   * Reads the statistic archives for the test run and creates the {@link
   * PerfStatValue}s.
   */
  public void readArchives() {
    this.statValues = PerfReporter.getStatValues(this.test.getStatConfig());
  }

  /**
   * Returns the stat values for this instance.
   */
  public SortedMap getStatValues() {
    return this.statValues;
  }

  public String toString() {
    return this.index + ":" + this.test;
  }
}
