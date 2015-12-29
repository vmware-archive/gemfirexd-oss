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

package cacheperf.poc.useCase3;

import hydra.*;

/**
 * A class used to store keys for test configuration settings.
 */
public class UseCase3Prms extends BasePrms {

  static {
    setValues(UseCase3Prms.class);
  }

  /**
   * (int)
   * The number of puts to do between queries.  Defaults to 0.
   */
  public static Long numPutsBetweenQueries;
  public static int getNumPutsBetweenQueries() {
    Long key = numPutsBetweenQueries;
    int val = tasktab().intAt(key, tab().intAt(key, 0));
    return val;
  }

  /**
   * (boolean)
   * Whether to log query results.  Defaults to false.
   */
  public static Long logQueryResults;
  public static boolean logQueryResults() {
    Long key = logQueryResults;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }
}
