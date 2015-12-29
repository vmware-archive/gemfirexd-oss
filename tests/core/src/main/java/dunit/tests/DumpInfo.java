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
package dunit.tests;

import dunit.*;

/**
 * This class tests the distributed unit testing framework's
 * facilities for examining remote hosts and their VMs.
 */
public class DumpInfo extends junit.framework.TestCase {

  public void testDumpInfo() {
    dumpInfo();
  }

  /**
   * Dumps a description of all of the hosts, VMs, and GemFire
   * systems we can find to the hydra master controller log file.
   */
  public static void dumpInfo() {
    StringBuffer sb = new StringBuffer();
    int hostCount = Host.getHostCount();
    sb.append(hostCount + " hosts:\n");

    for (int i = 0; i < hostCount; i++) {
      Host host = Host.getHost(i);
      sb.append("  ");
      sb.append(host);
      sb.append('\n');

      int systemCount = host.getSystemCount();
      sb.append("    ");
      sb.append(systemCount);
      sb.append(" systems\n");

      for (int j = 0; j < systemCount; j++) {
        GemFireSystem system = host.getSystem(j);
        sb.append("      ");
        sb.append(j);
        sb.append(") ");
        sb.append(system);
        sb.append('\n');
      }

      int vmCount = host.getVMCount();
      sb.append("    ");
      sb.append(vmCount);
      sb.append(" vms\n");

      for (int j = 0; j < vmCount; j++) {
        VM vm = host.getVM(j);
        sb.append("      ");
        sb.append(j);
        sb.append(") ");
        sb.append(vm);
        sb.append('\n');
      }
    }

    hydra.Log.getLogWriter().info(sb.toString());
  }

}
