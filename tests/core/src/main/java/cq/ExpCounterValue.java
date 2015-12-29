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
package cq;

import util.TestException;
import hydra.Log;

/** Class to specify a named pair of expected and actual event counters
 */
public class ExpCounterValue {
   
private String eventType = null;  
private long expectedValue = 0;
private long actualValue = 0;

/** Create an ExpCounterValue instance, specifying eventType (for logging only) and
  * the expected and actual values for the counter.
  */
public ExpCounterValue(String eventType, long expected, long actual) {
   if (eventType == null)
      throw new TestException("eventType" + eventType + " must be non-null");

   Log.getLogWriter().info("Creating ExpCounterValue( " + eventType + ", " + expected + ", " + actual + ")");

   this.eventType = eventType;
   this.expectedValue = expected;
   this.actualValue = actual;
}

public String getEventType() {
   return this.eventType;
}

public long getExpectedValue() {
   return this.expectedValue;
}

public long getActualValue() {
   return this.actualValue;
}

}
