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

package cacheperf.comparisons.cq;

import com.gemstone.gemfire.cache.query.CqListener;
import hydra.*;

/**
 * A class used to store keys for test configuration settings.
 */
public class CQPrms extends BasePrms {

  /**
   * (String) Classname for CQ listener.
   */
  public static Long cqListener;
  public static CqListener getCQListener() {
    Long key = cqListener;
    String val = tasktab().stringAt(key, tab().stringAt(key, null));
    if (val == null) {
      String s = "Missing value for " + nameForKey(key);
      throw new HydraConfigException(s);
    } else {
      try {
        return (CqListener)RegionDescription.getInstance(key, val);
      } catch (ClassCastException e) {
        String s = nameForKey(key) + " does not implement CqListener";
        throw new HydraConfigException(s);
      }
    }
  }

  /**
   * (int) Number of continuous queries to register.  Defaults to 1.
   */
  public static Long numCQs;
  public static int getNumCQs() {
    Long key = numCQs;
    int val = tasktab().intAt(key, tab().intAt(key, 1));
    if (val <= 0) {
      String s = nameForKey(key) + " must be positive: " + val;
      throw new HydraConfigException(s);
    }
    return val;
  }

  /**
   * (int)
   * Number of operations per second to throttle to.  Defaults to 0, which is
   * no throttling.
   */
  public static Long throttledOpsPerSec;
  public static int getThrottledOpsPerSec() {
    Long key = throttledOpsPerSec;
    return tasktab().intAt(key, tab().intAt(key, 0));
  }

  /**
   * (boolean)
   * Whether to execute with initial results or just execute.  Defaults to true.
   */
  public static Long executeWithInitialResults;
  public static boolean executeWithInitialResults() {
    Long key = executeWithInitialResults;
    return tasktab().booleanAt(key, tab().booleanAt(key, true));
  }

  
  /**
   * (boolean)
   * Whether to use multiple where conditions in CQs.
   */
  public static Long useMultipleWhereConditionsInCQs;
  public static boolean useMultipleWhereConditionsInCQs() {
    Long key = useMultipleWhereConditionsInCQs;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }
  //----------------------------------------------------------------------------
  //  Required stuff
  //----------------------------------------------------------------------------

  static {
    setValues(CQPrms.class);
  }
  public static void main(String args[]) {
      dumpKeys();
  }
}
