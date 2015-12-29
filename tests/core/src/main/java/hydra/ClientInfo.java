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

/**
 * A ClientInfo represents a hydra client JVM affected by a reboot action
 * through the {@link RebootMgr} API. It holds information needed for using
 * {@link ClientVmMgr} to restart the JVM after reboot.
 */
public class ClientInfo implements Serializable {

  private int vmid;
  private ClientDescription cd;

  public ClientInfo(int vmid, ClientDescription cd) {
    this.vmid = vmid;
    this.cd = cd;
  }

  /**
   * Returns the hydra client VMID for this JVM.
   */
  public int getVmid() {
    return this.vmid;
  }

  /**
   * Returns the {@link ClientDescription} for this JVM.
   */
  public ClientDescription getClientDescription() {
    return this.cd;
  }

  /**
   * Returns the logical client for this JVM, as specified in {@link
   * hydra.ClientPrms#names}.
   */
  public String getClientName() {
    return this.cd.getName();
  }

  /**
   * Returns the logical host for this JVM, as specified in {@link
   * hydra.HostPrms#names}.
   */
  protected String getLogicalHost() {
    return this.cd.getVmDescription().getHostDescription().getName();
  }

  /**
   * Returns the physical host for this JVM, as specified in {@link
   * hydra.HostPrms#hostNames}.
   */
  protected String getPhysicalHost() {
    return this.cd.getVmDescription().getHostDescription().getHostName();
  }

  /**
   * Returns a fully specified {@link ClientVmInfo} for this JVM. This is
   * handy for using with the {@link ClientVmMgr} API.
   */
  public ClientVmInfo getClientVmInfo() {
    return new ClientVmInfo(getVmid(), getClientName(), getLogicalHost());
  }

  public String toString() {
    return "vm_" + getVmid() + "_" + getClientName();
  }
}
