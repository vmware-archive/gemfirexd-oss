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

import com.gemstone.gemfire.LogWriter;
import hydra.*;
import java.util.*;

/**
 * Contains data and behavior common to all <code>TestComparison</code>s. 
 */
public abstract class BaseComparison
implements TestComparison, ComparisonConstants {

  /** The test runs to compare */
  protected List comparators;

  /** Whether to mark failed tests with a special value */
  protected boolean markFailedTests;

  /** The names of the statistics spec in each "row" of data */
  protected List statspecColumn;

  /** The names of the operation for each "row" of data */
  protected List opColumn;

  /** The statistics values for the comparison test runs */
  protected List[] valColumns;

  /** Logging */
  protected LogWriter log;

  /**
   * Creates a new <code>BaseComparison</code> for the given set of test runs.
   */
  BaseComparison(List comparators, boolean markFailedTests) {
    this.comparators = comparators;
    this.markFailedTests = markFailedTests;
    this.statspecColumn = new ArrayList();
    this.opColumn = new ArrayList();
    this.valColumns = new ArrayList[this.comparators.size()];
    for (int i = 0; i < this.comparators.size(); i++) {
      this.valColumns[i] = new ArrayList();
    }
    this.log = Log.getLogWriter();
  }

  public List getStatSpecColumn() {
    return this.statspecColumn;
  }

  public List getOpColumn() {
    return this.opColumn;
  }

  public List[] getValColumns() {
    return this.valColumns;
  }

  /**
   * Returns the value of a given operation on a given stat value.
   */
  protected double getOpValue(String op, PerfStatValue psv) {
    if (op.equals(MIN_OP)) return psv.getMin();
    else if (op.equals(MAX_OP)) return psv.getMax();
    else if (op.equals(AVG_OP)) return psv.getMean();
    else if (op.equals(MMM_OP)) return psv.getMaxMinusMin();
    else if (op.equals(STD_OP)) return psv.getStddev();
    else throw new HydraInternalException("Should not happen: " + op);
  }

  /**
   * Creates a "table" of statistic data consisting of a "row" per stat spec
   * and a column per test run.
   */
  protected void fillInRows() {
    SortedMap statspecs = null;
    for (Iterator i = this.comparators.iterator(); i.hasNext();) {
      ValueComparator comparator = (ValueComparator)i.next();
      if (comparator != null) {
        statspecs = comparator.getTest().getStatSpecs();
        break; // they all use the same statspec
      }
    }
    for (Iterator i = statspecs.values().iterator(); i.hasNext();) {
      StatSpec statspec = (StatSpec)i.next();
      String statspecName = statspec.getName();

      if (statspec.getMinCompare())         fillInRow(MIN_OP, statspecName);
      if (statspec.getMaxCompare())         fillInRow(MAX_OP, statspecName);
      if (statspec.getMeanCompare())        fillInRow(AVG_OP, statspecName);
      if (statspec.getMaxMinusMinCompare()) fillInRow(MMM_OP, statspecName);
      if (statspec.getStddevCompare())      fillInRow(STD_OP, statspecName);
    }
  }

  protected abstract void fillInRow(String op, String statspecName);

  public String toString() {
    StringBuffer buf = new StringBuffer();
    for (Iterator i = this.statspecColumn.iterator(); i.hasNext();) {
      buf.append("\nStat spec: " + i.next());
    }
    for (Iterator i = this.opColumn.iterator(); i.hasNext();) {
      buf.append("\nOperation: " + i.next());
    }
    for (int i = 0; i < this.valColumns.length; i++) {
      buf.append("\nValues: " + this.valColumns[i]);
    }
    return buf.toString();
  }
}
