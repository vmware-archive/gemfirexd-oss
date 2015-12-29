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
package dunit;

import java.io.*;
import java.util.*;

/**
 * This class represents a GemFire system that runs on some host.
 *
 * @see VM#setSystem
 *
 * @author David Whitlock
 *
 */
public class GemFireSystem implements Serializable {

  ////////////////////  Instance Fields  ////////////////////

  /** The host on which this system runs */
  private Host host;

  /** The name of the system directory for this system */
  private String systemDirectory;

  /** The (hydra) name of this GemFire system */
  private String name;

  //////////////////////  Constructors  //////////////////////

  /**
   * Creates a new <code>GemFireSystem</code> that represents a
   * GemFire System that runs on a given host.
   */
  GemFireSystem(String name, Host host, String systemDirectory) {
    this.name = name;
    this.host = host;
    this.systemDirectory = systemDirectory;
  }

  ///////////////////////  Accessors  ////////////////////////

  /**
   * Returns the host on which this GemFire system runs
   */
  public Host getHost() {
    return this.host;
  }

  /**
   * Returns the system directory for this GemFire system
   */
  public String getSystemDirectory() {
    return this.systemDirectory;
  }

  /**
   * Return the number of VMs associated with this system.
   */  
  public int getVMCount() {
    return getVMs().size();
  }

  /**
   * Return a list of VMs associated with this system
   */
  public List getVMs() {
    List result = new ArrayList();
    for (int i=0; i < getHost().getVMCount(); i++) {
      VM vm = getHost().getVM(i);
      if (vm.getSystem().equals(this)) result.add(vm);
    }
    return result;
  }
  
  /**
   * Returns the VM at the 0-based index i into getVMs()
   */
  public VM getVM(int i) {
    List vms = getVMs();
    if (i >= vms.size()) {
      dumpState();
      throw new IndexOutOfBoundsException(
        "Can't get VM " + i + ", this system has " + vms.size() + " VMs");
    }
    return (VM)vms.get(i);
  }
  
  private void dumpState() {
    System.err.println(this.toString());
    System.err.println("Host VMs:");
    for (int i=getHost().getVMCount(); --i >= 0;) {
      System.err.println("  " + i + ": " + getHost().getVM(i));
    }
  }

  /////////////////////  Utility Methods  /////////////////////

  /**
   * Two <code>GemFireSystem</code>s are equal if they run on the same
   * directory on the same host.
   */
  public boolean equals(Object o) {
    if (o instanceof GemFireSystem) {
      GemFireSystem other = (GemFireSystem) o;
      return this.host.equals(other.getHost()) &&
        this.systemDirectory.equals(other.getSystemDirectory());

    } else {
      return false;
    }
  }

  public String toString() {
    return "GemFire System running on " + this.getHost() + " in " +
      this.getSystemDirectory();
  }

}
