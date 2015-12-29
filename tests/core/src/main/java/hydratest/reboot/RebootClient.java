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
package hydratest.reboot;

import com.gemstone.gemfire.LogWriter;
import hydra.*;

/**
 * A client that tests the hydra {@link hydra.HostMgr} API.
 */
public class RebootClient {

  public static void rebootTask()
  throws ClientVmNotFoundException, RebootHostNotFoundException {
    String logicalHost = RebootPrms.getLogicalHostToReboot();
    HostDescription hd = TestConfig.getInstance()
                                   .getHostDescription(logicalHost);
    String host = hd.getHostName();

    Log.getLogWriter().info("Rebooting host " + host);
    RebootInfo info = RebootMgr.reboot("testing reboot", host, true);
    Log.getLogWriter().info("Rebooted host " + host);

    RebootMgr.clearInfo(host);

    Log.getLogWriter().info("Restarting hadoops: " + info.getHadoops());
    HadoopHelper.startHadoopProcesses(info.getHadoops());
    Log.getLogWriter().info("Restarted hadoops");

    Log.getLogWriter().info("Restarting clients: " + info.getClients());
    for (ClientInfo client : info.getClients()) {
      ClientVmMgr.start("testing client restart", client.getClientVmInfo());
    }
    Log.getLogWriter().info("Restarted clients");

    MasterController.sleepForMs(15000);
  }

  public static void workTask() {
    MasterController.sleepForMs(37000); // sleep for 37 seconds
  }
}
