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

public interface ComparisonConstants {

//------------------------------------------------------------------------------
// Test results
//------------------------------------------------------------------------------

  /** Passed */
  public static final String PASS  = "P";

  /** Failed */
  public static final String FAIL  = "F";

  /** Hung */
  public static final String HANG  = "H";

  /** Missing latest.prop */
  public static final String BOGUS = "bogus";

//------------------------------------------------------------------------------
// Statistics operations
//------------------------------------------------------------------------------

  public static final String MIN_OP = "min";
  public static final String MAX_OP = "max";
  public static final String AVG_OP = "avg";
  public static final String MMM_OP = "del";
  public static final String STD_OP = "std";
  public static final String ERR_OP = "err";

//------------------------------------------------------------------------------
// Statistics values
//------------------------------------------------------------------------------

  public static final String MISSING_VAL = "";
  public static final String MISSING_VAL_DOC =
    "Statistic value is unavailable or omitted";

  public static final String NIL_VAL = "---";
  public static final String NIL_VAL_DOC =
    "Statistic value is less than the ratio threshold";

  public static final String ERR_VAL = "err";
  public static final String ERR_VAL_DOC =
    "Statistic has multiple values instead of one";

  public static final String FAIL_VAL = "xxx";
  public static final String FAIL_VAL_DOC =
    "The test failed or hung";

  public static final String PLUS_INFINITY = "+inf";
  public static final String PLUS_INFINITY_DOC =
    "Statistic value went from zero to non-zero or vice versa and this is good";

  public static final String MINUS_INFINITY = "-inf";
  public static final String MINUS_INFINITY_DOC =
    "Statistic value went from zero to non-zero or vice versa and this is bad";
}
