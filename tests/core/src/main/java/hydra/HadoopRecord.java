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

import hydra.HadoopDescription.NodeDescription;

/**
 * A HadoopRecord represents a Hadoop process running in a hydra test. Only
 * the standard cluster processes started with {@link HadoopHelper} have
 * records.
 */
public class HadoopRecord {

  /**
   * Indicates that the process has no valid PID.
   */
  protected static int NO_PID = -1;

  HadoopDescription hdd;
  HadoopDescription.NodeDescription nd;
  int pid;
  boolean secure;

  public HadoopRecord(HadoopDescription hdd, HadoopDescription.NodeDescription nd,
                      int pid, boolean secure) {
    this.hdd = hdd;
    this.nd = nd;
    this.pid = pid;
    this.secure = secure;
  }

  protected HadoopDescription getHadoopDescription() {
    return this.hdd;
  }

  protected NodeDescription getNodeDescription() {
    return this.nd;
  }

  protected int getPID() {
    return this.pid;
  }

  protected void setPID(int p) {
    if (this.pid != NO_PID) {
      String s = "HadoopRecord for " + nd.getName() + " already has a PID: "
               + this.pid + ", cannot set it to " + p;
      throw new HydraInternalException(s);
    }
    this.pid = p;
  }

  protected void unsetPID(int p) {
    if (this.pid != p) {
      String s = "HadoopRecord for " + nd.getName() + " has a PID: " + this.pid
             + ", was expecting PID: " + this.pid;
      throw new HydraInternalException(s);
    }
    this.pid = NO_PID;
  }

  protected boolean isLive() {
    return this.pid != NO_PID;
  }

  protected boolean getSecure() {
    return this.secure;
  }
}
