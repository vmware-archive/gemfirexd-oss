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

package cacheperf.comparisons.connection.preload;

import hydra.BasePrms;
import hydra.EnvHelper;

public class PreloadPrms extends BasePrms {

  static {
    setValues(PreloadPrms.class);
  }

  public static final String DEFAULT_RESOURCE = "$GEMFIRE/lib/gemfire.jar";

  /**
   * (String)
   * Path to a resource to preload.  Defaults to {@link #DEFAULT_RESOURCE}.
   * The resource can be either a jar or a text file containing a list of
   * classnames.
   */
  public static Long resourceFile;
  public static String getResourceFile() {
    Long key = resourceFile;
    String val = tasktab().stringAt(key, tab().stringAt(key, DEFAULT_RESOURCE));
    return EnvHelper.expandEnvVars(val);
  }

  /**
   * (String)
   * Path to the file for logging class names and load times.  Defaults to
   * "preload.txt" in the working directory.
   */
  public static Long outputFile;
  public static String getOutputFile() {
    Long key = outputFile;
    return tasktab().stringAt(key, tab().stringAt(key, "preload.txt"));
  }

  /**
   * (boolean)
   * Whether to use the timing class loader.  Defaults to false.
   */
  public static Long useTimingClassLoader;
  public static boolean useTimingClassLoader() {
    Long key = useTimingClassLoader;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  /**
   * (boolean)
   * Whether to initialize classes as they are preloaded.  Defaults to true.
   */
  public static Long initializeClasses;
  public static boolean initializeClasses() {
    Long key = initializeClasses;
    return tasktab().booleanAt(key, tab().booleanAt(key, true));
  }

  /**
   * (boolean)
   * Whether to use verbose logging.  Defaults to false.
   */
  public static Long verbose;
  public static boolean verbose() {
    Long key = verbose;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }
}
