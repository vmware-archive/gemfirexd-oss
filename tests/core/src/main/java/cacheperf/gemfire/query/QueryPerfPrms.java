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

package cacheperf.gemfire.query;

import hydra.*;
import java.util.Iterator;
import java.util.Vector;

import cacheperf.CachePerfPrms;

/**
 *
 *  A class used to store keys for test configuration settings.
 *
 */
public class QueryPerfPrms extends CachePerfPrms {

  private static final String QUERY_PERF_NONE = "none";

  //----------------------------------------------------------------------------
  //  Queries
  //----------------------------------------------------------------------------

 
  /**
   * (String)
   * The query string.  Handles +=.  Defaults to null.
   */
  public static Long query;
  public static String getQuery() {
    Long key = query;
    return getPlusEqualsString(key);
  }

  private static String getPlusEqualsString(Long key) {
    Vector v = tab().vecAt(key, null);
    if (v == null) {
      return null;
    } else {
      String s = "";
      for (Iterator i = v.iterator(); i.hasNext();) {
        s += (String)i.next();
      }
      return s;
    }
  }

  /**
   * (long)
   * The minimum random value for a query range.  Defaults to 0.
   */
  public static Long queryRangeMin;
  public static long getQueryRangeMin() {
    Long key = queryRangeMin;
    long val = tasktab().longAt(key, tab().longAt(key, 0));
    if (val < 0.0 ) {
      String s = "Illegal value for " + nameForKey(key) + ": " + val;
      throw new HydraConfigException(s);
    }
    return val;
  }

  /**
   * (long)
   * The maximum random value for a query range.  Defaults to 0.
   */
  public static Long queryRangeMax;
  public static long getQueryRangeMax() {
    Long key = queryRangeMax;
    long val = tasktab().longAt(key, tab().longAt(key, 0));
    if (val < 0.0 ) {
      String s = "Illegal value for " + nameForKey(key) + ": " + val;
      throw new HydraConfigException(s);
    }
    return val;
  }

  /**
   * (long)
   * The size of a query range.  Defaults to 0.
   */
  public static Long queryRangeSize;
  public static long getQueryRangeSize() {
    Long key = queryRangeSize;
    long val = tasktab().longAt(key, tab().longAt(key, 0));
    if (val < 0.0 ) {
      String s = "Illegal value for " + nameForKey(key) + ": " + val;
      throw new HydraConfigException(s);
    }
    return val;
  }

  static {
    setValues( QueryPerfPrms.class );
  }
  public static void main( String args[] ) {
      dumpKeys();
  }
}
