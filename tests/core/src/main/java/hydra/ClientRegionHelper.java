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

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.client.*;

/**
 * Helps clients use {@link ClientRegionDescription}.  Methods are thread-safe.
 */
public class ClientRegionHelper {

//------------------------------------------------------------------------------
// ClientRegion
//------------------------------------------------------------------------------

  /**
   * Creates the client region using the given client region configuration
   * in the current cache.  The region is configured using the {@link
   * ClientRegionDescription} corresponding to the client region configuration
   * from {@link ClientRegionPrms#names}.  The region name is the corresponding
   * value of {@link ClientRegionPrms#regionName}.
   * <p>
   * Lazily creates the disk store specified by {@link ClientRegionPrms
   * #diskStoreName}, if needed, using {@link DiskStoreHelper
   * #createDiskStore(String)}.
   * <p>
   * Lazily creates the pool specified by {@link ClientRegionPrms#poolName},
   * if needed, using {@link PoolHelper#createPool(String)}.
   */
  public static synchronized Region createRegion(String regionConfig) {
    ClientRegionDescription rd = getClientRegionDescription(regionConfig);
    String regionName = rd.getRegionName();
    return createRegion(regionName, regionConfig);
  }

  /**
   * Creates the client region using the given name and client region
   * configuration in the client cache.  The region is configured using the
   * {@link ClientRegionDescription} corresponding to the client region
   * configuration from {@link ClientRegionPrms#names}.
   * Creates the client region with the given name using the given client region
   * configuration in the current cache.  The region is configured using the
   * {@link ClientRegionDescription} corresponding to the client region
   * configuration from {@link ClientRegionPrms#names}.
   * <p>
   * Lazily creates the disk store specified by {@link ClientRegionPrms
   * #diskStoreName}, if needed, using {@link DiskStoreHelper
   * #createDiskStore(String)}.
   * <p>
   * Lazily creates the pool specified by {@link ClientRegionPrms#poolName},
   * if needed, using {@link PoolHelper#createPool(String)}.
   */
  public static synchronized Region createRegion(String regionName,
                                                 String regionConfig) {
    if (regionName == null) {
      throw new IllegalArgumentException("regionName cannot be null");
    }
    if (regionConfig == null) {
      throw new IllegalArgumentException("regionConfig cannot be null");
    }
    ClientCache cache = ClientCacheHelper.getCache();
    Region region = cache.getRegion(regionName);
    if (region == null) {
      log("Creating client region named: " + regionName);
      ClientRegionDescription crd = getClientRegionDescription(regionConfig);
      DiskStoreDescription dsd = crd.getDiskStoreDescription();
      if (dsd != null) {
        DiskStoreHelper.createDiskStore(dsd.getName());
      }
      PoolDescription pd = crd.getPoolDescription();
      if (pd != null) {
        PoolHelper.createPool(pd.getName());
      }
      ClientRegionShortcut shortcut = crd.getClientRegionShortcut();
      ClientRegionFactory factory = cache.createClientRegionFactory(shortcut);
      crd.configure(factory, true);
      region = factory.create(regionName);
      log("Created region named " + regionName + " with attributes "
         + RegionHelper.regionAttributesToString(region.getAttributes()));
    }
    return region;
  }

  /**
   * Returns the client region with the given name in the current cache,
   * or null if no region with that name exists.
   */
  public static synchronized Region getRegion(String regionName) {
    if (regionName == null) {
      throw new IllegalArgumentException("regionName cannot be null");
    }
    return ClientCacheHelper.getCache().getRegion(regionName);
  }

//------------------------------------------------------------------------------
// ClientRegionDescription
//------------------------------------------------------------------------------

  /**
   * Returns the {@link ClientRegionDescription} with the given configuration
   * name from {@link ClientRegionPrms#names}.
   */
  public static ClientRegionDescription getClientRegionDescription(
                                                 String regionConfig) {
    if (regionConfig == null) {
      throw new IllegalArgumentException("regionConfig cannot be null");
    }
    log("Looking up client region config: " + regionConfig);
    ClientRegionDescription crd = TestConfig.getInstance()
                                     .getClientRegionDescription(regionConfig);
    if (crd == null) {
      String s = regionConfig + " not found in "
               + BasePrms.nameForKey(ClientRegionPrms.names);
      throw new HydraRuntimeException(s);
    }
    log("Looked up client region config:\n" + crd);
    return crd;
  }

//------------------------------------------------------------------------------
// RegionAttributes
//------------------------------------------------------------------------------

  /**
   * Returns the given region attributes as a string.
   */
  public static String regionAttributesToString(RegionAttributes attributes) {
    return RegionHelper.regionAttributesToString(attributes);
  }

//------------------------------------------------------------------------------
// Log
//------------------------------------------------------------------------------

  private static LogWriter log;
  private static synchronized void log(String s) {
    if (log == null) {
      log = Log.getLogWriter();
    }
    if (log.infoEnabled()) {
      log.info(s);
    }
  }
}
