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
import java.util.List;

/**
 * This class returns objects that describe the hydra clients and hydra-managed
 * Hadoop processes running on a rebooted host. See the {@link RebootMgr} API.
 */
public class RebootInfo implements Serializable {

  String host;
  private List<ClientInfo> clients;
  private List<HadoopInfo> hadoops;

  protected RebootInfo(String host, List<ClientInfo> clients,
                                    List<HadoopInfo> hadoops) {
    this.host = host;
    this.clients = clients;
    this.hadoops = hadoops;
  }

  /**
   * Returns the host that is the target of the reboot.
   */
  public String getHost() {
    return this.host;
  }

  /**
   * Returns the client info for each hydra client running on this host.
   */
  public List<ClientInfo> getClients() {
    return this.clients;
  }

  /**
   * Returns the Hadoop info for each Hadoop process running on this host.
   */
  public List<HadoopInfo> getHadoops() {
    return this.hadoops;
  }

  public String toProcessList() {
    StringBuilder sb = new StringBuilder();
    if (clients.size() > 0) {
      sb.append(" clients: ").append(clients);
    }
    if (hadoops.size() > 0) {
      sb.append(" hadoops: ").append(hadoops);
    }
    return sb.toString();
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("RebootInfo for ").append(this.host);
    sb.append("\n    clients: ").append(this.clients);
    sb.append("\n    hadoops: ").append(this.hadoops);
    return sb.toString();
  }
}
