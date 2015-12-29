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

package hydra;

/**
 * A class used to store keys for cache configuration settings related to
 * resource management.  The settings are used to create instances of {@link
 * ResourceManagerDescription}, and can be referenced from a cache configuration
 * via {@link CachePrms#resourceManagerName}.
 * <p>
 * The number of description instances is gated by {@link #names}.  For other
 * parameters, if fewer values than names are given, the remaining instances
 * will use the last value in the list.  See $JTESTS/hydra/hydra.txt for more
 * details.
 * <p>
 * Unused parameters default to null, except where noted.  This uses the
 * product default, except where noted.
 * <p>
 * Values can be set to {@link #NONE} where noted, with the documented effect.
 * <p>
 * Values of a parameter can be set to {@link #DEFAULT}, except where noted.
 * This uses the product default, except where noted.
 */
public class ResourceManagerPrms extends BasePrms {

  /**
   * (String(s))
   * Logical names of the resource manager descriptions.  Each name must be
   * unique.  Defaults to null.  Not for use with oneof, range, or robing.
   */
  public static Long names;

  /**
   * (float(s))
   * Critical heap percentage for each cache.
   * <p>
   * The product default is computed at runtime.
   */
  public static Long criticalHeapPercentage;
  
  /**
   * (float(s))
   * Critical off-heap percentage for each cache.
   * <p>
   * The product default is computed at runtime.
   */
  public static Long criticalOffHeapPercentage;

  /**
   * (float(s))
   * Eviction heap percentage for each cache.
   * <p>
   * The product default is computed at runtime.
   */
  public static Long evictionHeapPercentage;
  
  /**
   * (float(s))
   * Eviction off-heap percentage for each cache.
   * <p>
   * The product default is computed at runtime.
   */
  public static Long evictionOffHeapPercentage;

//------------------------------------------------------------------------------
// parameter setup
//------------------------------------------------------------------------------

  static {
    setValues(ResourceManagerPrms.class);
  }
}
