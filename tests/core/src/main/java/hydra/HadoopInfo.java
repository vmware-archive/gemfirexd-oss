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

import java.io.Serializable;

/**
 * A HadoopInfo represents a Hadoop process affected by a reboot action
 * through the {@link RebootMgr} API. It holds information needed for using
 * {@link HadoopHelper#startHadoopProcess} to restart the process after reboot.
 */
public class HadoopInfo implements Serializable {

  private HadoopDescription hdd;
  private NodeDescription nd;

  public HadoopInfo(HadoopDescription hdd, NodeDescription nd) {
    this.hdd = hdd;
    this.nd = nd;
  }

  /**
   * Returns the {@link HadoopDescription} for this process.
   */
  public HadoopDescription getHadoopDescription() {
    return this.hdd;
  }

  /**
   * Returns the {@link HadoopDescription.NodeDescription} for this process.
   */
  public NodeDescription getNodeDescription() {
    return this.nd;
  }

  public String toString() {
    return nd.getClusterName() + "_" + nd.getNodeType();
  }
}
