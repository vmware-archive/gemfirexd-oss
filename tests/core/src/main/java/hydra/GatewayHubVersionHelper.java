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

import com.gemstone.gemfire.cache.util.Gateway;
import com.gemstone.gemfire.cache.util.GatewayHub;
import java.util.List;

/**
 * Provides version-dependent support for {@link GatewayHubHelper}.
 */
public class GatewayHubVersionHelper {

  /**
   * Returns true if the hub is running.
   */
  protected static boolean isRunning(GatewayHub hub) {
    return hub.getCancelCriterion().cancelInProgress() == null;
  }

  /**
   * Creates the gateway disk stores for the hub.
   */
  protected static void createDiskStores(GatewayHub hub) {
    List<Gateway> gateways = hub.getGateways();
    if (gateways != null) {
      for (Gateway gateway : gateways) {
        String diskStoreName = gateway.getQueueAttributes().getDiskStoreName();
        if (diskStoreName != null) {
          // the name and the config name are the same
          DiskStoreHelper.createDiskStore(diskStoreName);
        }
      }
    }
  }

  /**
   * Adds the gateway with the given id to the hub.
   */
  protected static Gateway addGateway(GatewayHub hub, String id,
                                      GatewayDescription gd) {
    return hub.addGateway(id, gd.getConcurrencyLevel());
  }
}
