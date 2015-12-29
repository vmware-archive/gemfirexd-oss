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

/** 
 *
 *  Holds tokens for the statistics specification grammar.
 *
 */

public class StatSpecTokens {

  protected static final String INCLUDE          = "include";
  protected static final String INCLUDEIFPRESENT = "includeIfPresent";
  protected static final String STATSPEC         = "statspec";
  protected static final String EXPR             = "expr";

  public static final String FILTER_TYPE   = "filter";
  public static final String FILTER_PERSEC = "perSecond";
  public static final String FILTER_NONE   = "none";

  public static final String COMBINE_TYPE            = "combine";
  public static final String RAW                     = "raw";
  public static final String COMBINE                 = "combine";
  public static final String COMBINE_ACROSS_ARCHIVES = "combineAcrossArchives";

  public static final String OP_TYPES    = "ops";
  public static final String MIN         = "min";
  public static final String MAX         = "max";
  public static final String MAXMINUSMIN = "max-min";
  public static final String MEAN        = "mean";
  public static final String STDDEV      = "stddev";
  public static final String MIN_E         = "min!";
  public static final String MAX_E         = "max!";
  public static final String MAXMINUSMIN_E = "max-min!";
  public static final String MEAN_E        = "mean!";
  public static final String STDDEV_E      = "stddev!";
  public static final String MIN_C         = "min?";
  public static final String MAX_C         = "max?";
  public static final String MAXMINUSMIN_C = "max-min?";
  public static final String MEAN_C        = "mean?";
  public static final String STDDEV_C      = "stddev?";

  public static final String TRIM_SPEC_NAME = "trimspec";

  protected static final String FCN              = "fcn";
  protected static final String NCF              = "ncf";

  protected static final String EQUALS           = "=";
  protected static final String COMMA            = ",";
  protected static final String SEMI             = ";";
  protected static final String SPACE            = " ";

  protected static final String DIV  = "/";
}
