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
package util;

/** Class to specify a blackboard counter and its expected value.
 */
public class ExpCounterValue {
   
private int counterIndex;
private String counterName1 = null;  
private String counterName2 = null;
private long expectedValue = 0;
private boolean exact = true;

/** Create an ExpCounterValue specifying a shared counter name and
  * its exact expected value
  */
public ExpCounterValue(String counterName, long counterValue) {
   if (counterName == null)
      throw new TestException("counterName " + counterName + " must be non-null");
   counterName1 = counterName;
   expectedValue = counterValue;
   exact = true;
}

/** Create an ExpCounterValue specifying a shared counter name and
  * its expected value, and whether the expected value is exact
  * or a minimum expected value.
  */
public ExpCounterValue(String counterName, long counterValue, boolean exact) {
   if (counterName == null)
      throw new TestException("counterName " + counterName + " must be non-null");
   counterName1 = counterName;
   expectedValue = counterValue;
   this.exact = exact;
}

/** Create an ExpCounterValue specifying two shared counter names and
  * its expected value. The counters values will be summed before comparing
  * to the exact expected counterValue
  */
public ExpCounterValue(String counterName1, String counterName2, long counterValue) {
   if (counterName1 == null)
      throw new TestException("counterName " + counterName1 + " must be non-null");
   if (counterName2 == null)
      throw new TestException("counterName " + counterName2 + " must be non-null");
   this.counterName1 = counterName1;
   this.counterName2 = counterName2;
   expectedValue = counterValue;
}

public String getCounterName1() {
   return counterName1;
}

public String getCounterName2() {
   return counterName2;
}

public long getExpectedValue() {
   return expectedValue;
}

public boolean getExact() {
   return exact;
}

/** Create an ExpCounterValue specifying a shared counter index and 
 *  its exact expected value
 */
public ExpCounterValue(int counterIndex, long counterValue) {
   this.counterIndex = counterIndex;
   expectedValue = counterValue;
   exact = true;
}

public int getCounterIndex() {
   return counterIndex;
}

}
