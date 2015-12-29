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

import java.io.Serializable;
import java.rmi.RemoteException;

/**
 * Supports hydra client access to the master-managed derby server.
 */
public class DerbyServerHelper {

//------------------------------------------------------------------------------
// Endpoints
//------------------------------------------------------------------------------

  /**
   * Returns the endpoint for the derby server if {@link Prms#manageDerbyServer}
   * is true, else null.
   */
  public static DerbyServerHelper.Endpoint getEndpoint() {
    try {
      return RemoteTestModule.Master.getDerbyServerEndpoint();
    } catch (RemoteException e) {
      String s = "Unable to get derby server endpoint from master";
      throw new HydraRuntimeException(s, e);
    }
  }

  /**
   * Represents the endpoint for the derby server.
   */
  public static class Endpoint implements Serializable {
    String host;
    int port, pid;

    /**
     * Creates an endpoint for the derby server.
     *
     * @param host the derby server host.
     * @param port the derby server port.
     * @param pid  the derby server pid.
     */
    public Endpoint(String host, int port, int pid) {
      if (host == null) {
        throw new IllegalArgumentException("host cannot be null");
      }

      this.host = host;
      this.port = port;
      this.pid  = pid;
    }

    /**
     * Returns the derby server host.
     */
    public String getHost() {
      return this.host;
    }

    /**
     * Returns the derby server port.
     */
    public int getPort() {
      return this.port;
    }

    /**
     * Returns the derby server pid.
     */
    public int getPid() {
      return this.pid;
    }

    /**
     * Returns the endpoint info as a string.
     */
    public String toString() {
      return "host=" + this.host + " port=" + this.port + " pid=" + this.pid;
    }
  }
}
