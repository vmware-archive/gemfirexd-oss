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
 * A class used to store keys for cache configuration settings.
 * The settings are used to create instances of {@link CacheDescription}.
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
 * Values and fields of a parameter can use oneof, range, or robing
 * except where noted, but each description created will use a fixed value
 * chosen at test configuration time.  Use as a task attribute is illegal.
 */
public class CachePrms extends BasePrms {

  /**
   * (String(s))
   * Logical names of the cache descriptions.  Each name must be unique.
   * Defaults to null.  Not for use with oneof, range, or robing.
   */
  public static Long names;

  /**
   * (boolean(s))
   * Copy on read for each cache.
   */
  public static Long copyOnRead;

  /**
   * (String(s))
   * Class name of gateway conflict resolver for each cache. Can be specified
   * as {@link #NONE}.
   */
  public static Long gatewayConflictResolver;

  /**
   * (int(s))
   * Lock lease for each cache, in seconds.
   */
  public static Long lockLease;

  /**
   * (int(s))
   * Lock timeout for each cache, in seconds.
   */
  public static Long lockTimeout;

  /**
   * (int(s))
   * Message sync interval for each cache, in seconds.
   */
  public static Long messageSyncInterval;

  /**
   * (String(s))
   * Name of logical disk store configuration (and actual disk store name)
   * to use for the pdx serializer for each cache, as found in {@link
   * DiskStorePrms#names}.  Can be specified as {@link #NONE} (default).
   *
   * @see {@link #pdxSerializer}.
   */
  public static Long pdxDiskStoreName;

  /**
   * (boolean(s))
   * PDX ignore unread fields for each cache.
   */
  public static Long pdxIgnoreUnreadFields;

  /**
   * (boolean(s))
   * PDX persistent for each cache.
   *
   * @see {@link #pdxSerializer}.
   */
  public static Long pdxPersistent;

  /**
   * (boolean(s))
   * PDX read serialized for each cache.
   *
   * @see {@link #pdxSerializer}.
   */
  public static Long pdxReadSerialized;

  /**
   * (String pair(s))
   * Class and method used to instantiate a pdx serializer for each cache.
   * Can be specified as {@link #NONE} (default).
   * <p>
   * Hydra creates a singleton instance for each cache description.
   */
  public static Long pdxSerializerInstantiator;

  /**
   * (String(s))
   * Name of logical resource manager configuration for each cache, as found in
   * {@link ResourceManagerPrms#names}.  Can be specified as {@link #NONE}
   * (default).
   */
  public static Long resourceManagerName;

  /**
   * (int(s))
   * Search timeout for each cache, in seconds.
   */
  public static Long searchTimeout;

//------------------------------------------------------------------------------
// parameter setup
//------------------------------------------------------------------------------

  static {
    setValues(CachePrms.class);
  }

  public static void main(String args[]) {
    Log.createLogWriter("cacheprms", "info");
    dumpKeys();
  }
}
