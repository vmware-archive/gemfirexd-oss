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
import hydra.HydraConfigException;
import hydra.HydraInternalException;

/**
 * A class used to store keys for test configuration settings, and to provide
 * accessors for reading the keys so that they can be set as regular parameters
 * or as task attributes.  See individual javadocs for the meaning of each key.
 * <p>
 * Values of a parameter can use oneof, range, or robing to get a different
 * value each time the parameter is accessed.
 * <p>
 * This class is offered as a convenience to test developers and is not used by
 * the hydra master controller.
 */
public class GfxdConfigPrms extends BasePrms {

  static {
    setValues(GfxdConfigPrms.class);
  }

  //----------------------------------------------------------------------------
  // networkServerConfig 
  //----------------------------------------------------------------------------

  /**
   * (String)
   * An entry in {@link NetworkServerPrms#names} giving the network server
   * configuration.
   */
  public static Long networkServerConfig;

  /**
   * Returns the value of {@link #networkServerConfig}, or null if it is not
   * set.
   * @throws HydraConfigException if not found in {@link
   *                              NetworkServerPrms#names}.
   */
  public static String getNetworkServerConfig() {
    return getString(networkServerConfig);
  }

  //----------------------------------------------------------------------------
  // diskStoreConfig
  //----------------------------------------------------------------------------

  /**
   * (String)
   * An entry in {@link hydra.gemfirexd.DiskStorePrms#names} giving the disk
   * store configuration.
   */
  public static Long diskStoreConfig;

  /**
   * Returns the value of {@link #diskStoreConfig}, or null if it is not set.
   * @throws HydraConfigException if not found in {@link DiskStorePrms#names}.
   */
  public static String getDiskStoreConfig() {
    return getString(diskStoreConfig);
  }

  //----------------------------------------------------------------------------
  // hdfsStoreConfig
  //----------------------------------------------------------------------------

  /**
   * (String)
   * An entry in {@link hydra.HDFSStorePrms#names} giving the HDFS store
   * configuration.
   */
  public static Long hdfsStoreConfig;

  /**
   * Returns the value of {@link #hdfsStoreConfig}, or null if it is not set.
   * @throws HydraConfigException if not found in {@link hydra.HDFSStorePrms#names}.
   */
  public static String getHDFSStoreConfig() {
    return getString(hdfsStoreConfig);
  }

  //----------------------------------------------------------------------------
  // Utility methods
  //----------------------------------------------------------------------------

  /**
   * Returns the string at the given key, checking task attributes as well as
   * the regular configuration hashtable.
   */
  public static String getString(Long key) {
    String val = tasktab().stringAt(key, tab().stringAt(key, null));
    if (val == null) {
      return null;
    } else {
      Object description;
      if (key == networkServerConfig) {
        description = GfxdTestConfig.getInstance().getNetworkServerDescription(val);

      } else if (key == diskStoreConfig) {
        description = GfxdTestConfig.getInstance().getDiskStoreDescription(val);

      } else if (key == hdfsStoreConfig) {
        description = GfxdTestConfig.getInstance().getHDFSStoreDescription(val);

      } else {
        throw new HydraInternalException("Unknown key: " + nameForKey(key));
      }
      if (description == null) {
        String s = "Description for configuration name \"" + nameForKey(key) + "\" not found: " + val;
        throw new HydraConfigException(s);
      }
    }
    return val;
  }
}
