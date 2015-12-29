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
package vsphere.vijava;

import delta.DeltaPropagationPrms;
import hydra.BasePrms;

public class VIJavaPrms extends BasePrms {

  public static Long wsUrl;

  public static Long username;

  public static Long password;

  public static Long hostNames;

  public static Long vmNames;

  public static Long vMotionIntervalSec;

  public static Long targetHost;

  public static Long targetDatastore;

  /**
   * startInterval for the vMotion task to trigger vMotion. Can be any of the specified values e.g. 90
   */
  public static Long delayVMotionStartTime;

  /**
   * (String) The possible stop modes for stopping a VM. Can be any of
   * MEAN_EXIT, MEAN_KILL, NICE_EXIT, NICE_KILL.
   */
  public static Long stopMode;

  public static Long vMotionEnabled;

  static {
    setValues(VIJavaPrms.class);
  }

}
