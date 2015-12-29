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
package hydra.training;

import hydra.*;
import java.io.IOException;

/**
 * Hydra tasks for working with the {@link Server}.
 *
 * @author Lise Storc updated with API for stopping VMs
 * @author David Whitlock
 * @since 4.0
 */
public class ServerTasks {

  /** The Server that runs in this VM */
  private static Server server;

  ///////////////////////  Static Methods  ///////////////////////

  /**
   * A Hydra INIT task that starts the server in this VM
   */
  public static void startServer() {
    if (server != null) {
      String s = "A single VM cannot host more than one server";
      throw new IllegalArgumentException(s);
    }

    server = new Server(ServerPrms.getPort());
    server.start();
  }

  /**
   * A Hydra INIT task that starts the failover server in this VM
   */
  public static void startFailoverServer() {
    if (server != null) {
      String s = "A single VM cannot host more than one server";
      throw new IllegalArgumentException(s);
    }

    server = new Server(ServerPrms.getFailoverPort());
    server.start();
  }

  /**
   * A Hydra TASK that simulates this server failing by executing a
   * {@link hydra.ClientVmMgr.stopAsync(String,int,long)}.
   */
  public static void fail() throws ClientVmNotFoundException {
    ClientVmMgr.stopAsync( "Server has failed",
      ClientVmMgr.MEAN_KILL,  // kill -TERM
      ClientVmMgr.NEVER       // never allow this VM to restart
    );
  }

  /**
   * A Hydra CLOSE task that stops the server
   */
  public static void stopServer()
    throws IOException, InterruptedException {

    server.close();
  }

}
