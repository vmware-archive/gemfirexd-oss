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
package gfxdperf.tpch;

import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.internal.NanoTimer;

import gfxdperf.PerfTestException;

import perffmwk.PerformanceStatistics;

/**
 * Implements statistics related to TPCH.
 */
public class TPCHStats extends PerformanceStatistics {

  protected static final String Q1 = "q1";
  protected static final String Q1_TIME = "q1Time";
  protected static final String Q1_RESULTS = "q1Results";

  protected static final String Q2 = "q2";
  protected static final String Q2_TIME = "q2Time";
  protected static final String Q2_RESULTS = "q2Results";

  protected static final String Q3 = "q3";
  protected static final String Q3_TIME = "q3Time";
  protected static final String Q3_RESULTS = "q3Results";

  protected static final String Q4 = "q4";
  protected static final String Q4_TIME = "q4Time";
  protected static final String Q4_RESULTS = "q4Results";

  protected static final String Q5 = "q5";
  protected static final String Q5_TIME = "q5Time";
  protected static final String Q5_RESULTS = "q5Results";

  protected static final String Q6 = "q6";
  protected static final String Q6_TIME = "q6Time";
  protected static final String Q6_RESULTS = "q6Results";

  protected static final String Q7 = "q7";
  protected static final String Q7_TIME = "q7Time";
  protected static final String Q7_RESULTS = "q7Results";

  protected static final String Q8 = "q8";
  protected static final String Q8_TIME = "q8Time";
  protected static final String Q8_RESULTS = "q8Results";

  protected static final String Q9 = "q9";
  protected static final String Q9_TIME = "q9Time";
  protected static final String Q9_RESULTS = "q9Results";

  protected static final String Q10 = "q10";
  protected static final String Q10_TIME = "q10Time";
  protected static final String Q10_RESULTS = "q10Results";

  protected static final String Q11 = "q11";
  protected static final String Q11_TIME = "q11Time";
  protected static final String Q11_RESULTS = "q11Results";

  protected static final String Q12 = "q12";
  protected static final String Q12_TIME = "q12Time";
  protected static final String Q12_RESULTS = "q12Results";

  protected static final String Q13 = "q13";
  protected static final String Q13_TIME = "q13Time";
  protected static final String Q13_RESULTS = "q13Results";

  protected static final String Q14 = "q14";
  protected static final String Q14_TIME = "q14Time";
  protected static final String Q14_RESULTS = "q14Results";

  protected static final String Q15 = "q15";
  protected static final String Q15_TIME = "q15Time";
  protected static final String Q15_RESULTS = "q15Results";

  protected static final String Q16 = "q16";
  protected static final String Q16_TIME = "q16Time";
  protected static final String Q16_RESULTS = "q16Results";

  protected static final String Q17 = "q17";
  protected static final String Q17_TIME = "q17Time";
  protected static final String Q17_RESULTS = "q17Results";

  protected static final String Q18 = "q18";
  protected static final String Q18_TIME = "q18Time";
  protected static final String Q18_RESULTS = "q18Results";

  protected static final String Q19 = "q19";
  protected static final String Q19_TIME = "q19Time";
  protected static final String Q19_RESULTS = "q19Results";

  protected static final String Q20 = "q20";
  protected static final String Q20_TIME = "q20Time";
  protected static final String Q20_RESULTS = "q20Results";

  protected static final String Q21 = "q21";
  protected static final String Q21_TIME = "q21Time";
  protected static final String Q21_RESULTS = "q21Results";

  protected static final String Q22 = "q22";
  protected static final String Q22_TIME = "q22Time";
  protected static final String Q22_RESULTS = "q22Results";
  /**
   * Returns the statistic descriptors for <code>TPCHStats</code>.
   */
  public static StatisticDescriptor[] getStatisticDescriptors() {
    boolean largerIsBetter = true;
    return new StatisticDescriptor[] {
      factory().createIntCounter
      (
        Q1,
        "Number of executions of Q1.",
        "rows",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        Q1_TIME,
        "Total time spent executing Q1.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        Q1_RESULTS,
        "Number of results found executing Q1.",
        "results",
        !largerIsBetter
      ),

      factory().createIntCounter
      (
        Q2,
        "Number of executions of Q2.",
        "rows",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        Q2_TIME,
        "Total time spent executing Q2.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        Q2_RESULTS,
        "Number of results found executing Q2.",
        "results",
        !largerIsBetter
      ),

      factory().createIntCounter
      (
        Q3,
        "Number of executions of Q3.",
        "rows",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        Q3_TIME,
        "Total time spent executing Q3.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        Q3_RESULTS,
        "Number of results found executing Q3.",
        "results",
        !largerIsBetter
      ),

      factory().createIntCounter
      (
        Q4,
        "Number of executions of Q4.",
        "rows",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        Q4_TIME,
        "Total time spent executing Q4.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        Q4_RESULTS,
        "Number of results found executing Q4.",
        "results",
        !largerIsBetter
      ),

      factory().createIntCounter
      (
        Q5,
        "Number of executions of Q5.",
        "rows",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        Q5_TIME,
        "Total time spent executing Q5.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        Q5_RESULTS,
        "Number of results found executing Q5.",
        "results",
        !largerIsBetter
      ),

      factory().createIntCounter
      (
        Q6,
        "Number of executions of Q6.",
        "rows",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        Q6_TIME,
        "Total time spent executing Q6.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        Q6_RESULTS,
        "Number of results found executing Q6.",
        "results",
        !largerIsBetter
      ),

      factory().createIntCounter
      (
        Q7,
        "Number of executions of Q7.",
        "rows",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        Q7_TIME,
        "Total time spent executing Q7.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        Q7_RESULTS,
        "Number of results found executing Q7.",
        "results",
        !largerIsBetter
      ),

      factory().createIntCounter
      (
        Q8,
        "Number of executions of Q8.",
        "rows",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        Q8_TIME,
        "Total time spent executing Q8.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        Q8_RESULTS,
        "Number of results found executing Q8.",
        "results",
        !largerIsBetter
      ),

      factory().createIntCounter
      (
        Q9,
        "Number of executions of Q9.",
        "rows",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        Q9_TIME,
        "Total time spent executing Q9.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        Q9_RESULTS,
        "Number of results found executing Q9.",
        "results",
        !largerIsBetter
      ),

      factory().createIntCounter
      (
        Q10,
        "Number of executions of Q10.",
        "rows",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        Q10_TIME,
        "Total time spent executing Q10.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        Q10_RESULTS,
        "Number of results found executing Q10.",
        "results",
        !largerIsBetter
      ),

      factory().createIntCounter
      (
        Q11,
        "Number of executions of Q11.",
        "rows",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        Q11_TIME,
        "Total time spent executing Q11.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        Q11_RESULTS,
        "Number of results found executing Q11.",
        "results",
        !largerIsBetter
      ),

      factory().createIntCounter
      (
        Q12,
        "Number of executions of Q12.",
        "rows",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        Q12_TIME,
        "Total time spent executing Q12.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        Q12_RESULTS,
        "Number of results found executing Q12.",
        "results",
        !largerIsBetter
      ),

      factory().createIntCounter
      (
        Q13,
        "Number of executions of Q13.",
        "rows",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        Q13_TIME,
        "Total time spent executing Q13.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        Q13_RESULTS,
        "Number of results found executing Q13.",
        "results",
        !largerIsBetter
      ),

      factory().createIntCounter
      (
        Q14,
        "Number of executions of Q14.",
        "rows",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        Q14_TIME,
        "Total time spent executing Q14.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        Q14_RESULTS,
        "Number of results found executing Q14.",
        "results",
        !largerIsBetter
      ),

      factory().createIntCounter
      (
        Q15,
        "Number of executions of Q15.",
        "rows",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        Q15_TIME,
        "Total time spent executing Q15.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        Q15_RESULTS,
        "Number of results found executing Q15.",
        "results",
        !largerIsBetter
      ),

      factory().createIntCounter
      (
        Q16,
        "Number of executions of Q16.",
        "rows",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        Q16_TIME,
        "Total time spent executing Q16.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        Q16_RESULTS,
        "Number of results found executing Q16.",
        "results",
        !largerIsBetter
      ),

      factory().createIntCounter
      (
        Q17,
        "Number of executions of Q17.",
        "rows",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        Q17_TIME,
        "Total time spent executing Q17.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        Q17_RESULTS,
        "Number of results found executing Q17.",
        "results",
        !largerIsBetter
      ),

      factory().createIntCounter
      (
        Q18,
        "Number of executions of Q18.",
        "rows",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        Q18_TIME,
        "Total time spent executing Q18.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        Q18_RESULTS,
        "Number of results found executing Q18.",
        "results",
        !largerIsBetter
      ),

      factory().createIntCounter
      (
        Q19,
        "Number of executions of Q19.",
        "rows",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        Q19_TIME,
        "Total time spent executing Q19.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        Q19_RESULTS,
        "Number of results found executing Q19.",
        "results",
        !largerIsBetter
      ),

      factory().createIntCounter
      (
        Q20,
        "Number of executions of Q20.",
        "rows",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        Q20_TIME,
        "Total time spent executing Q20.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        Q20_RESULTS,
        "Number of results found executing Q20.",
        "results",
        !largerIsBetter
      ),

      factory().createIntCounter
      (
        Q21,
        "Number of executions of Q21.",
        "rows",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        Q21_TIME,
        "Total time spent executing Q21.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        Q21_RESULTS,
        "Number of results found executing Q21.",
        "results",
        !largerIsBetter
      ),

      factory().createIntCounter
      (
        Q22,
        "Number of executions of Q22.",
        "rows",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        Q22_TIME,
        "Total time spent executing Q22.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        Q22_RESULTS,
        "Number of results found executing Q22.",
        "results",
        !largerIsBetter
      )
    };
  }

  public static TPCHStats getInstance() {
    return (TPCHStats)getInstance(TPCHStats.class, THREAD_SCOPE);
  }

//------------------------------------------------------------------------------
// constructors

  public TPCHStats(Class cls, StatisticsType type, int scope,
                   String instanceName, String trimspecName) {
    super(cls, type, scope, instanceName, trimspecName);
  }

//------------------------------------------------------------------------------
// operations

  public long startQuery() {
    return NanoTimer.getTime();
  }

  public long endQuery(int queryNum, long start, int results) {
    long elapsed = NanoTimer.getTime() - start;
    switch (queryNum) {
      case 1:
        statistics().incInt(Q1, 1);
        statistics().incLong(Q1_TIME, elapsed);
        statistics().incInt(Q1_RESULTS, results);
        break;
      case 2:
        statistics().incInt(Q2, 1);
        statistics().incLong(Q2_TIME, elapsed);
        statistics().incInt(Q2_RESULTS, results);
        break;
      case 3:
        statistics().incInt(Q3, 1);
        statistics().incLong(Q3_TIME, elapsed);
        statistics().incInt(Q3_RESULTS, results);
        break;
      case 4:
        statistics().incInt(Q4, 1);
        statistics().incLong(Q4_TIME, elapsed);
        statistics().incInt(Q4_RESULTS, results);
        break;
      case 5:
        statistics().incInt(Q5, 1);
        statistics().incLong(Q5_TIME, elapsed);
        statistics().incInt(Q5_RESULTS, results);
        break;
      case 6:
        statistics().incInt(Q6, 1);
        statistics().incLong(Q6_TIME, elapsed);
        statistics().incInt(Q6_RESULTS, results);
        break;
      case 7:
        statistics().incInt(Q7, 1);
        statistics().incLong(Q7_TIME, elapsed);
        statistics().incInt(Q7_RESULTS, results);
        break;
      case 8:
        statistics().incInt(Q8, 1);
        statistics().incLong(Q8_TIME, elapsed);
        statistics().incInt(Q8_RESULTS, results);
        break;
      case 9:
        statistics().incInt(Q9, 1);
        statistics().incLong(Q9_TIME, elapsed);
        statistics().incInt(Q9_RESULTS, results);
        break;
      case 10:
        statistics().incInt(Q10, 1);
        statistics().incLong(Q10_TIME, elapsed);
        statistics().incInt(Q10_RESULTS, results);
        break;
      case 11:
        statistics().incInt(Q11, 1);
        statistics().incLong(Q11_TIME, elapsed);
        statistics().incInt(Q11_RESULTS, results);
        break;
      case 12:
        statistics().incInt(Q12, 1);
        statistics().incLong(Q12_TIME, elapsed);
        statistics().incInt(Q12_RESULTS, results);
        break;
      case 13:
        statistics().incInt(Q13, 1);
        statistics().incLong(Q13_TIME, elapsed);
        statistics().incInt(Q13_RESULTS, results);
        break;
      case 14:
        statistics().incInt(Q14, 1);
        statistics().incLong(Q14_TIME, elapsed);
        statistics().incInt(Q14_RESULTS, results);
        break;
      case 15:
        statistics().incInt(Q15, 1);
        statistics().incLong(Q15_TIME, elapsed);
        statistics().incInt(Q15_RESULTS, results);
        break;
      case 16:
        statistics().incInt(Q16, 1);
        statistics().incLong(Q16_TIME, elapsed);
        statistics().incInt(Q16_RESULTS, results);
        break;
      case 17:
        statistics().incInt(Q17, 1);
        statistics().incLong(Q17_TIME, elapsed);
        statistics().incInt(Q17_RESULTS, results);
        break;
      case 18:
        statistics().incInt(Q18, 1);
        statistics().incLong(Q18_TIME, elapsed);
        statistics().incInt(Q18_RESULTS, results);
        break;
      case 19:
        statistics().incInt(Q19, 1);
        statistics().incLong(Q19_TIME, elapsed);
        statistics().incInt(Q19_RESULTS, results);
        break;
      case 20:
        statistics().incInt(Q20, 1);
        statistics().incLong(Q20_TIME, elapsed);
        statistics().incInt(Q20_RESULTS, results);
        break;
      case 21:
        statistics().incInt(Q21, 1);
        statistics().incLong(Q21_TIME, elapsed);
        statistics().incInt(Q21_RESULTS, results);
        break;
      case 22:
        statistics().incInt(Q22, 1);
        statistics().incLong(Q22_TIME, elapsed);
        statistics().incInt(Q22_RESULTS, results);
        break;
      default:
        String s = "Unsupported query number: " + queryNum;
        throw new PerfTestException(s);
    }
    return elapsed;
  }
}
