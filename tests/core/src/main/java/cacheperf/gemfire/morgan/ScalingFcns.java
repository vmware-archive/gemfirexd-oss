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

package cacheperf.gemfire.morgan;

import hydra.HydraConfigException;

/**
 *  Functions to scale things.
 */
public class ScalingFcns {

  /**
   * Minimum size of a VM.
   */
  public static final int MINIMUM_VM_SIZE = 256;

  /**
   * Maximum size of a VM.
   */
  public static final int MAXIMUM_VM_SIZE = 1500;

  /**
   * Amount of padding for a VM.
   */
  public static final int VM_PADDING_SIZE = 128;

  /**
   * Returns a string containing the VM sizing parameters Xms and Xmx scaled
   * for spreading maxKeys objects of size dataSize (in bytes) across numHosts
   * hosts.  The minimum size returned is {@link #MINIMUM_VM_SIZE} megabytes.
   * Computed sizes are padded by {@link VM_PADDING_SIZE} megabytes.  An error
   * is thrown if the value exceeds a maximum of {@link #MAXIMUM_VM_SIZE}
   * megabytes.
   */
  public static String scaleVmSize(int maxKeys, int dataSize, int numHosts) {
    long sz = (long)maxKeys * (long)dataSize; // total bytes
    sz = sz/numHosts; // bytes per host
    sz = sz/1000000; // megabytes per host
    sz = sz + VM_PADDING_SIZE; // include padding to cover overhead
    sz = Math.max(sz, MINIMUM_VM_SIZE);
    if (sz > MAXIMUM_VM_SIZE) {
      String s = "Trying to scale VM size past " + MAXIMUM_VM_SIZE + "m."
               + " Reduce maxKeys=" + maxKeys + " or dataSize=" + dataSize
               + " or increase numHosts=" + numHosts;
      throw new HydraConfigException(s);
    }             
    return "-Xms" + sz + "m -Xmx" + sz + "m";
  }
}
