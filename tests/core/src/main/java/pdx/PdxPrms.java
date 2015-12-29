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
package pdx;

import java.util.List;

import hydra.BasePrms;
import hydra.TestConfig;

public class PdxPrms extends BasePrms {

  /** (boolean) If true then this thread should initialize a class loader
   *            for versioned domain objects, false otherwise.
   */
  public static Long initClassLoader;
  public static boolean getInitClassLoader() {
    Long key = initClassLoader;
    boolean val = tasktab().booleanAt(key, tab().booleanAt(key, false));
    return val;
  }

  /** (boolean) If true then log stack traces on toData/fromData to help
   *            with debugging, if false then don't log stack traces.
   */
  public static Long logStackTrace;
  public static boolean getLogStackTrace() {
    Long key = logStackTrace;
    boolean val = tasktab().booleanAt(key, tab().booleanAt(key, false));
    return val;
  }
  
  /** (boolean) If true then log calls to toData/fromData for PdxSerializables
   *            that consider this flag.
   */
  public static Long logToAndFromData;
  public static boolean getLogToAndFromData() {
    Long key = logToAndFromData;
    boolean val = tasktab().booleanAt(key, tab().booleanAt(key, true));
    return val;
  }
  
  /** (String) The name of the PdxSerializer class to use.
   */
  public static Long pdxSerializerClassName;
  public static String getPdxSerializerClassName() {
    Long key = pdxSerializerClassName;
    String val = tasktab().stringAt(key, tab().stringAt(key));
    return val;
  }

  /** (boolean) If true, create empty or accessor regions, otherwise create
   *            regions that host data.
   */
  public static Long createProxyRegions;
  public static boolean getCreateProxyRegions() {
    Long key = createProxyRegions;
    boolean val = tasktab().booleanAt(key, tab().booleanAt(key, false));
    return val;
  }
  
  /** (int) The number of entries to load into each region.
   */
  public static Long numToLoadPerRegion;
  public static int getNumToLoadPerRegion() {
    Long key = numToLoadPerRegion;
    int val = tasktab().intAt(key, tab().intAt(key));
    return val;
  }
  
  /** (boolean) If true then include shutDownAll in test.
   */
  public static Long shutDownAll;
  public static boolean getShutDownAll() {
    Long key = shutDownAll;
    boolean val = tasktab().booleanAt(key, tab().booleanAt(key, false));
    return val;
  }
  
  /** (boolean) For 662 do enum validation (new api added in 662 for pdx
   *            enums. Prior to 662 we cannot do the validation.
   */
  public static Long doEnumValidation;
  public static boolean getDoEnumValidation() {
    Long key = doEnumValidation;
    boolean val = tasktab().booleanAt(key, tab().booleanAt(key, true));
    return val;
  }
  
  // ================================================================================
  static {
    BasePrms.setValues(PdxPrms.class);
  }

}
