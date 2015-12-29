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
 * A class used to store keys for client cache configuration settings.
 * The settings are used to create instances of {@link ClientCacheDescription}.
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
public class ClientCachePrms extends BasePrms {

  /**
   * (String(s))
   * Logical names of the client cache descriptions.  Each name must be unique.
   * Defaults to null.  Not for use with oneof, range, or robing.
   */
  public static Long names;

  /**
   * (String(s))
   * Name of the logical configuration for the default disk store for each
   * client cache, as found in {@link DiskStorePrms#names}.  Can be specified
   * as {@link #NONE} (default).  The name of this disk store is "DEFAULT".
   * <p>
   * NOTE: If no default disk store is specified, it is up to the test to either
   * create a disk store named "DEFAULT" that the product can discover and use
   * as the default disk store or specify a non-default disk store name
   * whereever disk stores are needed.  Otherwise, the default disk store will
   * use disk directories based on the master controller working directory.
   */
  public static Long defaultDiskStoreName;

  /**
   * (String(s))
   * Name of the logical configuration for the default pool for each client
   * cache, as found in {@link PoolPrms#names}.  Can be specified as {@link
   * #NONE} (default).  The product names this pool "DEFAULT" if no pool by
   * that name already exists (otherwise it adds a numeric suffix).
   * <p>
   * NOTE: If no default pool is specified, it is up to the test to either
   * precreate a single pool that the product can discover and use as the
   * default pool or specify a non-default pool name when creating regions.
   * Otherwise, it creates a default pool listening on localhost and the
   * default port;
   */
  public static Long defaultPoolName;

  /**
   * (String(s))
   * Name of logical disk store configuration (and actual disk store name)
   * to use for the pdx serializer for each client cache, as found in {@link
   * DiskStorePrms#names}.  Can be specified as {@link #NONE} (default).
   *
   * @see {@link #pdxSerializer}.
   */
  public static Long pdxDiskStoreName;

  /**
   * (boolean(s))
   * PDX ignore unread fields for each client cache.
   */
  public static Long pdxIgnoreUnreadFields;

  /**
   * (boolean(s))
   * PDX persistent for each client cache.
   *
   * @see {@link #pdxSerializer}.
   */
  public static Long pdxPersistent;

  /**
   * (boolean(s))
   * PDX read serialized for each client cache.
   *
   * @see {@link #pdxSerializer}.
   */
  public static Long pdxReadSerialized;

  /**
   * (String pair(s))
   * Class and method used to instantiate a pdx serializer for each client
   * cache.  Can be specified as {@link #NONE} (default).
   * <p>
   * Hydra creates a singleton instance for each client cache description.
   */
  public static Long pdxSerializerInstantiator;

//------------------------------------------------------------------------------
// parameter setup
//------------------------------------------------------------------------------

  static {
    setValues(ClientCachePrms.class);
  }

  public static void main(String args[]) {
    Log.createLogWriter("clientcacheprms", "info");
    dumpKeys();
  }
}
