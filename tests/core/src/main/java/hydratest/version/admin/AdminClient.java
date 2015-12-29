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

package hydratest.version.admin;

import com.gemstone.gemfire.admin.AdminDistributedSystem;
import com.gemstone.gemfire.internal.GemFireVersion;
import hydra.*;

public class AdminClient {

  public static void reportAdminConfigTask() {
    AdminDistributedSystem ads =
         DistributedSystemHelper.getAdminDistributedSystem();
    if (ads == null) {
      throw new HydraConfigException("Admin distributed system not configured");
    }
    else {
      String s = "In GemFire version " + GemFireVersion.getGemFireVersion()
               + " AdminPrms refreshInterval is "
               + VersionHelper.getRefreshInterval(ads.getConfig());
      Log.getLogWriter().info(s);
    }
  }
}
