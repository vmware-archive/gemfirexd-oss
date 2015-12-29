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
 * A {@link TestComparison} whose data table contains values that are ratio
 * comparisons to a base value.
 */
public class RatioComparison extends BaseComparison
                             implements ComparisonConstants {

  /** Ratio threshold under which a variation is considered insignificant. */
  double ratioThreshold;

  RatioComparison(List comparators, double ratioThreshold, boolean markFailedTests) {
    super(comparators, markFailedTests);
    this.ratioThreshold = ratioThreshold;
    fillInRows();
  }

  /**
   * Creates a single "row" of data for the given stat spec and operation, with
   * a value for each test run.  Each value is a ratio between the value for
   * that test run and the value for the base test run.  The base is the first
   * run that has a valid value for the statistic.
   */
  protected void fillInRow(String op, String statSpecName) {
    if (this.log.finerEnabled()) {
      this.log.finer("Filling in row for " + statSpecName + " " + op);
    }
    this.statspecColumn.add(statSpecName);
    this.opColumn.add(op);

    double baseVal = -1;
    for (int i = 0; i < this.comparators.size(); i++) {
      ValueComparator comparator = (ValueComparator)this.comparators.get(i);
      Object val;
      if (comparator == null) { // test is missing
        val = MISSING_VAL;
      } else if (markFailedTests &&
                (comparator.getTest().getTestResult().equals(HANG) ||
                 comparator.getTest().getTestResult().equals(FAIL))) {
        val = FAIL_VAL;
      } else {
        SortedMap statValues = comparator.getStatValues();
        if (statValues == null) { // no archives
          if (markFailedTests) {
            val = FAIL_VAL;
          } else {
            val = MISSING_VAL;
          }
        } else {
          List psvs = (List)statValues.get(statSpecName);
          if (psvs == null) { // stat is missing from spec
            val = MISSING_VAL;
          } else if (psvs.size() == 0) { // stat is missing from archive
            val = MISSING_VAL;
          } else if (psvs.size() == 1) {
            PerfStatValue psv = (PerfStatValue)psvs.get(0);
            double compareVal = getOpValue(op, psv);
            double ratioVal =
              getRatioValue(baseVal, compareVal, psv.isLargerBetter());
            if (ratioVal == Double.MAX_VALUE) {
              val = PLUS_INFINITY;
            } else if (ratioVal == Double.MIN_VALUE) {
              val = MINUS_INFINITY;
            } else if (Math.abs(ratioVal) < this.ratioThreshold) {
              val = NIL_VAL;
            } else if (Math.abs(ratioVal) < 1.01) {
              val = NIL_VAL;
            } else {
              val = new Double(ratioVal);
            }
            // set the base to the first good value
            if (baseVal == -1) {
              baseVal = compareVal;
            }
          } else { // stat is not combined into one value
            val = ERR_VAL;
          }
        }
      }
      this.valColumns[i].add(val);
      if (this.log.finerEnabled()) {
        if (comparator == null) {
          this.log.finer(i + ": has " + statSpecName + " " + op + " " + val);
        } else {
          this.log.finer(i + ": value comparator " + comparator.getIndex()
                           + " at " + comparator.getTest().getTestDir()
                           + " for " + comparator.getTest().getTestId()
                           + " has " + statSpecName + " " + op + " " + val);
        }
      }
    }
  }

  /**
   * Returns the ratio of the base value to the comparison value taking into
   * account whether or not a large value is "better".  If there is no base
   * value yet (that is, this will become the base value), the ratio is 1.0.
   */
  protected double getRatioValue(double baseVal, double compareVal,
                                 boolean largerIsBetter) {
    double ratio;
    if (baseVal == -1 || baseVal == compareVal) {
      ratio = 1.0d;

    } else if (baseVal == 0 || compareVal == 0) {
      ratio = (( baseVal == 0 && largerIsBetter) ||
                (compareVal == 0 && ! largerIsBetter))
        ? Double.MAX_VALUE : Double.MIN_VALUE;

    } else {
      double max = Math.max(baseVal, compareVal);
      double min = Math.min(baseVal, compareVal);
      ratio = max / min;
      ratio = (( baseVal < compareVal && largerIsBetter) ||
                (baseVal > compareVal && ! largerIsBetter))
        ? ratio : 0 - ratio;
    }
    if (this.log.finerEnabled()) {
      this.log.finer("Ratio for base=" + baseVal + " compare=" + compareVal
                    + " " + largerIsBetter + " ==> " + ratio);
    }
    return ratio;
  }
}
