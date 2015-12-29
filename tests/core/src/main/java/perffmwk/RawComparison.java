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
 *  A {@link TestComparison} whose data table consists of raw statistics values
 *  obtained directly from the {@link PerfStatValue}. 
 */
public class RawComparison extends BaseComparison
                           implements ComparisonConstants {

  RawComparison(List comparators, boolean markFailedTests) {
    super(comparators, markFailedTests);
    fillInRows();
  }

  /**
   * Creates a single "row" of data for the given stat spec and operation, with
   * a column for each test run.
   */
  protected void fillInRow(String op, String statSpecName) {
    if (this.log.finerEnabled()) {
      this.log.finer("Filling in row for " + statSpecName + " " + op);
    }
    this.statspecColumn.add(statSpecName);
    this.opColumn.add(op);

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
          val = MISSING_VAL;
        } else {
          List psvs = (List)statValues.get(statSpecName);
          if (psvs == null) { // stat is missing from spec
            val = MISSING_VAL;
          } else if (psvs.size() == 0) { // stat is missing from archive
            val = MISSING_VAL;
          } else if (psvs.size() == 1) {
            PerfStatValue psv = (PerfStatValue)psvs.get(0);
            val = new Double(getOpValue(op, psv));
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
}
