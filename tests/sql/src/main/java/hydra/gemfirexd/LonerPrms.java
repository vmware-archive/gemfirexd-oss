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

package hydra.gemfirexd;

import hydra.BasePrms;

/**
 * A class used to store keys for loner configuration settings.
 * The settings are used to create instances of {@link LonerDescription}.
 * <p>
 * The number of description instances is gated by {@link #names}.  For other
 * parameters, if fewer values than names are given, the remaining instances
 * will use the last value in the list.  See $JTESTS/hydra/hydra.txt for more
 * details.
 * <p>
 * Unused parameters default to null, except where noted.  This uses the
 * product default, except where noted.
 * <p>
 * Values and fields of a parameter can be set to {@link #DEFAULT},
 * except where noted.  This uses the product default, except where noted.
 * <p>
 * Values, fields, and subfields can be set to {@link #NONE} where noted, with
 * the documented effect.
 * <p>
 * Values and fields of a parameter can use oneof, range, or robing
 * except where noted, but each description created will use a fixed value
 * chosen at test configuration time.  Use as a task attribute is illegal.
 */
public class LonerPrms extends BasePrms {

  static {
    setValues(LonerPrms.class);
  }

  public static final String LONER = "loner";

  /**
   * (String(s))
   * Logical names of the loner descriptions.  Each name must be unique.
   * Defaults to null.  Not for use with oneof, range, or robing.
   */
  public static Long names;
  
  /**
   * (int(s))
   * The "archive-disk-space-limit" property, in megabytes.
   */
  public static Long archiveDiskSpaceLimit;

  /**
   * (int(s))
   * The "archive-file-size-limit" property, in megabytes.
   */
  public static Long archiveFileSizeLimit;

  /**
   * (Comma-separated Lists of String(s))
   * Names of logical hydra client configurations, as found in {@link
   * hydra.ClientPrms#names}.  No client name can be listed more than once.
   * This is a required field.
   * <p>
   * This parameter is used to wire each logical loner description
   * to specific hydra clients.
   */
  public static Long clientNames;

  /**
   * (boolean(s))
   * The "enable-time-statistics" property.  Defaults to true, which overrides
   * the product default.
   */
  public static Long enableTimeStatistics;

  /**
   * (int(s))
   * The "log-disk-space-limit" property, in megabytes.
   */
  public static Long logDiskSpaceLimit;

  /**
   * (int(s))
   * The "log-file-size-limit" property, in megabytes.
   */
  public static Long logFileSizeLimit;

  /**
   * (String(s))
   * The "log-level" property.
   */
  public static Long logLevel;

  /**
   * (int(s))
   * The "statistic-sample-rate" property, in milliseconds.
   */
  public static Long statisticSampleRate;

  /**
   * (boolean(s))
   * The "statistic-sampling-enabled" property.  Defaults to true, which
   * overrides the product default.
   */
  public static Long statisticSamplingEnabled;
}
