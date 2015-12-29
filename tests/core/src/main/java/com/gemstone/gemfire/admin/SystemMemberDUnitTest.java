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
package com.gemstone.gemfire.admin;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystemDisconnectedException;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import dunit.*;
import java.net.InetAddress;
import java.util.*;

/**
 * Tests the functionality of the {@link SystemMember} administration
 * API. 
 *
 * @author David Whitlock
 * @since 3.5
 */
public class SystemMemberDUnitTest extends AdminDUnitTestCase {

  /**
   * Creates a new <code>SystemMemberDUnitTest</code>
   */
  public SystemMemberDUnitTest(String name) {
    super(name);
  }

  ////////  Test Methods

  /**
   * Tests that system members have the appropriate host information
   */
  public void testHostInformation() throws Exception {
    Host host = Host.getHost(0);
    final String hostName = getServerHostName(host);
    final InetAddress addresses[] =
      InetAddress.getAllByName(hostName);
    final String testName = this.getName();
    
    assertFalse(isConnectedToDS());

    final AdminDistributedSystem system = this.tcSystem;
//     assertEquals(0, system.getSystemMemberApplications().length);

    final int vmCount = host.getVMCount();
    for (int i = 0; i < vmCount; i++) {
      VM vm = host.getVM(i);
      final int pid = vm.getPid();
      vm.invoke(new SerializableRunnable("Connect to DS") {
          public void run() {
            disconnectFromDS();
            Properties props = getDistributedSystemProperties();
            String name = testName + "_" + pid;
            props.setProperty(DistributionConfig.NAME_NAME, name);
            getSystem(props);
          }
        });
    } // for

    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        try {
          return system.getSystemMemberApplications().length >= vmCount;
        }
        catch (AdminException e) {
          fail("unexpected exception", e);
        }
        return false; // NOTREACHED
      }
      public String description() {
        return "system member applications never reached " + vmCount;
      }
    };
    DistributedTestCase.waitForCriterion(ev, 10 * 1000, 200, true);

    SystemMember[] apps = system.getSystemMemberApplications();
    assertEquals(host.getVMCount(), apps.length);
    for (int i = 0; i < apps.length; i++) {
      SystemMember app = apps[i];
      assertTrue(app.getType().isApplication());

      String appName = app.getName();
      if (appName.indexOf(testName) == -1) {
        String s = "Expected app name \"" + appName +
          "\" to contain \"" + testName + "\"";
        fail(s);
      }
      String expected = InetAddress.getByName(hostName).getCanonicalHostName();
      String actual = app.getHostAddress().getCanonicalHostName();
      if (!actual.startsWith(expected)) {
        String s = "Expected \"" + expected + "\" got \"" + actual +
          "\"";
        fail(s);
      }

      InetAddress address = app.getHostAddress();
      boolean foundAddress = false;
      for (int j = 0; j < addresses.length; j++) {
        if (addresses[j].equals(address)) {
          foundAddress = true;
          break;
        }
      }
      assertTrue(foundAddress);
    }
  }

  /**
   * Tests administration methods that return strings and such
   */
  public void testStringsAndThings() throws Exception {
    AdminDistributedSystem system = this.tcSystem;

    Collection members = new ArrayList();
    members.addAll(Arrays.asList(system.getSystemMemberApplications()));

    for (Iterator iter = members.iterator(); iter.hasNext(); ) {
      SystemMember member = (SystemMember) iter.next();
      assertNotNull(member.getId());
      assertNotNull(member.getLog());
      assertNotNull(member.getVersion());
    }
  }

  /**
   * Tests to make sure that the name of a system member with the
   * default name is the id.  See, when you've only got four more work
   * days before you leave the company, you don't really care if your
   * Javadocs make any sense or not.  See bug 32877.
   *
   * @since 4.1
   */
  public void testNameIsId() throws Exception {
    Host host = Host.getHost(0);

    assertFalse(isConnectedToDS());

    final AdminDistributedSystem system = this.tcSystem;

    final int vmCount = host.getVMCount();
    for (int i = 0; i < vmCount; i++) {
      VM vm = host.getVM(i);
//      final int pid = vm.getPid();
      vm.invoke(new SerializableRunnable("Connect to DS") {
          public void run() {
            disconnectFromDS();
            Properties props = getDistributedSystemProperties();
            String name = DistributionConfig.DEFAULT_NAME;
            props.setProperty(DistributionConfig.NAME_NAME, name);
            getSystem(props);
          }
        });
    }

    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        try {
          return system.getSystemMemberApplications().length >= vmCount;
        }
        catch (AdminException e) {
          fail("unexpected  failure", e);
        }
        return false; // NOTREACHED
      }
      public String description() {
        return "system member applications never reached " + vmCount;
      }
    };
    DistributedTestCase.waitForCriterion(ev, 20 * 1000, 200, true);

    SystemMember[] apps = system.getSystemMemberApplications();
    assertEquals(host.getVMCount(), apps.length);
    for (int i = 0; i < apps.length; i++) {
      SystemMember app = apps[i];
      assertEquals(app.getId(), app.getName());
    }

  }

  /**
   * Tests getRoles and getDistributedMember.
   * @since 5.0
   */
  public void testRolesAndDistributedMember() throws Exception {
    final String[] vmRoles = new String[] {"VM_A","VM_B","VM_C","VM_D"};
    
    assertFalse(isConnectedToDS());

    final AdminDistributedSystem system = this.tcSystem;

    final int vmCount = Host.getHost(0).getVMCount();
    for (int i = 0; i < vmCount; i++) {
      final int vm = i;
      Host.getHost(0).getVM(vm).invoke(new SerializableRunnable("Connect to DS") {
          public void run() {
            disconnectFromDS();
            Properties props = getDistributedSystemProperties();
            props.setProperty(DistributionConfig.NAME_NAME, String.valueOf(vm));
            props.setProperty(DistributionConfig.ROLES_NAME, vmRoles[vm]);
            getSystem(props);
          }
        });
    }

    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        try {
          return system.getSystemMemberApplications().length >= vmCount;
        }
        catch (AdminException e) {
          fail("unexpected exception", e);
        }
        return false; // NOTREACHED
      }
      public String description() {
        return "system member applications never reached " + vmCount;
      }
    };
    DistributedTestCase.waitForCriterion(ev, 10 * 1000, 200, true);

    SystemMember[] apps = system.getSystemMemberApplications();
    assertEquals(vmCount, apps.length);
    for (int i = 0; i < apps.length; i++) {
      SystemMember app = apps[i];
      String[] roles = app.getRoles();
      DistributedMember distMember = app.getDistributedMember();
      
      // assert has one role
      assertEquals(1, roles.length);
      
      // assert role based on the int we used for name
      String name = app.getName();
      final int whichVm = (new Integer(name)).intValue();
      assertEquals(vmRoles[whichVm], roles[0]);

      // assert distMember equals vmMember      
      assertNotNull(distMember);
      DistributedMember vmMember = 
          (DistributedMember) Host.getHost(0).getVM(whichVm).invoke(
            DistributedSystemDUnitTest.class, "getDistributedMember", 
            new Object[] {});
      assertNotNull(vmMember);
      assertEquals(vmMember, distMember);
    }
  }

  @Override
  public void tearDown2() throws Exception {
    // might get an interrupted exception in RemoteGfManagerAgent.JoinProcessor
    // during shutdown
    if (isJMX() && this.agent != null) {
      final LogWriter logger = this.agent.getLogWriter();
      logger.info("<ExpectedException action=add>"
          + RuntimeAdminException.class.getName() + "</ExpectedException>");
      logger.info("<ExpectedException action=add>"
          + DistributedSystemDisconnectedException.class.getName()
          + "</ExpectedException>");
      logger.info("<ExpectedException action=add>"
          + InterruptedException.class.getName() + "</ExpectedException>");
    }
    super.tearDown2();
  }
}
