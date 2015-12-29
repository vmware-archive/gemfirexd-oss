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

package wan;

import hydra.*;

/**
 * A class used to store keys for cache server configuration.
 */

public class CacheServerPrms extends BasePrms {

  /**
   * (String) A cache configuration name from {@link hydra.CachePrms#names}.
   */
  public static Long cacheConfig;

  /**
   * (String) A region configuration name from {@link hydra.RegionPrms#names}.
   */
  public static Long regionConfig;

  /**
   * (String) A bridge configuration name from {@link hydra.BridgePrms#names}.
   */
  public static Long bridgeConfig;

  /**
   * (String) A gateway hub configuration name from {@link
   *          hydra.GatewayHubPrms#names}.
   */
  public static Long gatewayHubConfig;

  /**
   * (String) A gateway configuration name from {@link hydra.GatewayPrms#names}.
   */
  public static Long gatewayConfig;

  static {
    setValues(CacheServerPrms.class);
  }
  public static void main(String args[]) {
    dumpKeys();
  }
}
