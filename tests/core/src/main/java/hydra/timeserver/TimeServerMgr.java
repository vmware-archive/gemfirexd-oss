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

package hydra.timeserver;

import hydra.*;
import java.io.IOException;

/** 
 * Manages the time server for the hydra master.
 */

public class TimeServerMgr {

  /**
   * The time server.
   */
  private static TimeServer TheTimeServer;

  /**
   * Starts the time server thread.
   */
  public static synchronized TimeServer startTimeServer() {
    Log.getLogWriter().info("Starting the time server");
    if (TheTimeServer != null) {
      throw new HydraRuntimeException("Time server already started");
    }
    try {
      TheTimeServer = new TimeServer();
    } catch (IOException e) {
      throw new HydraRuntimeException("Unable to start time server", e);
    }
    TheTimeServer.start();
    Log.getLogWriter().info("Started the time server: " + TheTimeServer);

    return TheTimeServer;
  }

  /**
   * Gets the time server.
   */
  public static TimeServer getTimeServer() {
    return TheTimeServer;
  }
}
