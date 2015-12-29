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
package com.gemstone.gemfire.admin.stats;

import java.util.Properties;

import com.gemstone.gemfire.admin.AdminDUnitTestCase;
import com.gemstone.gemfire.admin.AdminDistributedSystem;
import com.gemstone.gemfire.admin.StatisticResource;
import com.gemstone.gemfire.admin.SystemMember;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;

import dunit.Host;
import dunit.SerializableRunnable;
import dunit.VM;

/**
 * Tests the functionality of the <code>StatisticResource</code> admin
 * interface 
 *
 * @author Harsh Khanna
 * @since 5.7
 */
public class GemFireMemberStatsDUnitTest
  extends AdminDUnitTestCase {

  ////////  Constructors

  /**
   * Creates a new <code>GemFireMemberStatsDUnitTest</code>
   */
  public GemFireMemberStatsDUnitTest(String name) {
    super(name);
  }
  
  protected SystemMember getMember() throws Exception  {
    getLogWriter().info("[testGetMemberStats]");

    // Create Member an Cache
    Host host = Host.getHost(0);
    VM vm = host.getVM(0);
    
    final String testName = this.getName();

    vm.invoke(new SerializableRunnable() {
        public void run() {
          Properties props = getDistributedSystemProperties();
          props.setProperty(DistributionConfig.NAME_NAME, testName);
          getSystem(props);
        }
      });
    pause(2 * 1000);
    
    getLogWriter().info("Test: Created DS");
    
    AdminDistributedSystem system = this.tcSystem;
    SystemMember[] members = system.getSystemMemberApplications();
    if (members.length != 1) {
      StringBuffer sb = new StringBuffer();
      sb.append("Expected 1 member, got " + members.length + ": ");
      for (int i = 0; i < members.length; i++) {
        SystemMember member = members[i];
        sb.append(member.getName());
        sb.append(" ");
      }

      fail(sb.toString());
    }

    getLogWriter().info("Test: Created Member");
    return members[0];
  }

  /**
   * Tests Getting a Single Stat
   */
  public void testGetMemberStats() throws Exception {
    getLogWriter().info("[testGetMemberStats]");

    // Should be null as the case is incorrect
    StatisticResource[] statRes = getMember().getStat("distributionStats");
    assertNull(statRes);

    statRes = getMember().getStat("DistributionStats");
    assertNotNull(statRes);
  }
}
