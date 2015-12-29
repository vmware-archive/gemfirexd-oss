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

import hydra.Log;
import java.io.IOException;

/**
 * Hydra tasks for working with the {@link Client}.
 *
 * @author David Whitlock
 * @since 4.0
 */
public class ClientTasks {

  /** Did a client fail over */
  private static boolean failedOver = false;

  /////////////////////// Static Methods  ///////////////////////

  /**
   * A Hydra TASK that connects to a server
   */
  public static void connect() throws IOException {
    Client client = new Client();
    client.connect(ServerPrms.getPort());
  }

  /**
   * A Hydra TASK that connects to a server and, if that connection
   * goes away, will fail over to the "failover" server.
   */
  public static void connectAndFailover() throws IOException {
    Client client = new Client();

    try {
      // Try the primary server
      client.connect(ServerPrms.getPort());

    } catch (IOException ex) {
      // Okay, let's fail over
      failedOver = true;
      if (ServerPrms.debug()) {
        Log.getLogWriter().info("Client failing over", ex);
      }
      client.connect(ServerPrms.getFailoverPort());
    }
  }

  /**
   * A CLOSE task that asserts that a <code>Client</code> failed
   * over. 
   */
  public static void checkFailover() {
    if (!failedOver) {
      String s = "Client did not fail over";
      throw new IllegalStateException(s);
    }
  }

}
