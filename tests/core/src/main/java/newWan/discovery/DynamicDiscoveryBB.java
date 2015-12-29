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
package newWan.discovery;

import hydra.RemoteTestModule;
import hydra.blackboard.Blackboard;
import util.TestException;
import util.TestHelper;

/**
 * Blackboard for dynamic discovery tests used by {@link DynamicDiscoveryTest}
 * This was written to support the Jayesh usecase
 * 
 * @author rdiyewar
 * @since 6.8
 */
public class DynamicDiscoveryBB extends Blackboard {

  private static DynamicDiscoveryBB blackboard;

  // counters
  public static int SITE_ADDED_LISTENER_INVOCATION_COUNTER;

  public static int SITE_REMOVED_LISTENER_INVOCATION_COUNTER;

  // keys for shared map
  public static final String WAN_SITES = "WanSites";

  public static final String WAN_SITES_REMOVED = "WanSitesRemoved";

  public static final String EXPECTED_ADD_PER_VM = "ExpectedAddPerVM";

  public static final String EXPECTED_REMOVE_PER_VM = "ExpectedRemovePerVM";

  /**
   * Zero-arg constructor for remote method invocations.
   */
  public DynamicDiscoveryBB() {
  }

  /**
   * Creates a WAN blackboard using the specified name and transport type.
   */
  public DynamicDiscoveryBB(String name, String type) {
    super(name, type, DynamicDiscoveryBB.class);
  }

  /**
   * Creates a WAN blackboard named "WAN" using RMI for transport.
   */
  public static synchronized DynamicDiscoveryBB getInstance() {
    if (blackboard == null) {
      blackboard = new DynamicDiscoveryBB("DynamicDiscoveryBB", "RMI");
    }
    return blackboard;
  }

  /**
   * printBlackboard: ENDTASK to print contents of blackboard
   */
  public static void printBlackboard() {
    hydra.Log.getLogWriter().info("Printing WAN Blackboard contents");
    DynamicDiscoveryBB bb = getInstance();
    bb.print();
    TestHelper.checkForEventError(bb);
  }

  public static synchronized void throwException(String msg) {
    if (getInstance().getSharedMap().get(TestHelper.EVENT_ERROR_KEY) == null) {
      getInstance().getSharedMap().put(TestHelper.EVENT_ERROR_KEY,
          msg + " in " + getMyUniqueName() + " " + TestHelper.getStackTrace());
    }

    throw new TestException(msg);
  }

  /**
   * Uses RemoteTestModule information to produce a name to uniquely identify a
   * client vm (vmid, clientName, host, pid) for the calling thread
   */
  public static String getMyUniqueName() {
    StringBuffer buf = new StringBuffer(50);
    buf.append("vm_").append(RemoteTestModule.getMyVmid());
    buf.append("_").append(RemoteTestModule.getMyClientName());
    buf.append("_").append(RemoteTestModule.getMyHost());
    buf.append("_").append(RemoteTestModule.getMyPid());
    return buf.toString();
  }

}
