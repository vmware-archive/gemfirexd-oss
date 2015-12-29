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

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.cache.DistributedCacheTestCase;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;

import java.util.*;

import dunit.*;

/**
 * Tests that admin distributed systems properly recuse when
 * their JVM crashes.
 *
 * @author jpenney
 */
public class SystemFailureAdminDUnitTest extends DistributedCacheTestCase {

  /**
   * Creates a new <code>AdminAndCacheDUnitTest</code> with the given
   * name.
   */
  public SystemFailureAdminDUnitTest(String name) {
    super(name);
  }

  ////////  Test Methods

  protected static void connectAdminSystem() throws Exception {
    assertTrue(!system.getConfig().getLocators().equals(""));

    DistributedSystemConfig config = 
      AdminDistributedSystemFactory.defineDistributedSystem(system, null);
    assertEquals(system.getConfig().getLocators(),
                 config.getLocators());
    assertEquals(system.getConfig().getMcastPort(),
                 config.getMcastPort());

    getLogWriter().info("Locators are: " + config.getLocators() +
                        ", mcast port is: " + config.getMcastPort());


    AdminDistributedSystem admin =
      AdminDistributedSystemFactory.getDistributedSystem(config);
    admin.connect();
    assertTrue(admin.waitToBeConnected(5 * 1000));
  }
  
  protected static ArrayList pesky;
  protected static void crashMe() {
    pesky = new ArrayList();
    
    SystemFailure.setFailureMemoryThreshold(0); // Disable watchcat
//  SystemFailure.setExitOK(true); // This VM gets _really_ sick, screws up RMI
    Runnable r = new Runnable() {
      public void run() {
        try {
          for (;;) {
            pesky.add(new int[1000]);
          }
        }
        catch (OutOfMemoryError e) {
          SystemFailure.setFailure(e); // don't throw here :-)
          
          // Wait and let watchdog notice
          try {
            Thread.sleep(15 * 1000);
          }
          catch (InterruptedException e2) {
            // eeeuw, probably won't do anything useful...
            throw new util.TestException("interrupted");
          }
        } // oome
      }
    };
    new Thread(r, "crashMe").start();
  }
  
  private void waitForDeath(GemFireCacheImpl c, int numBefore) {
    InternalDistributedSystem ids = (InternalDistributedSystem)
        c.getDistributedSystem();
    long start = System.currentTimeMillis();
    for (;;) {
      if (System.currentTimeMillis() - start > 90 * 1000) {
        fail("Peer did not recuse");
        break; // for lint
      }
      Set membersAfter = ids.getDistributionManager().getNormalDistributionManagerIds();
      int numAfter = membersAfter.size();
      if (numAfter == numBefore - 1) {
        break; // success
      }
      pause(5 * 1000);
    }
    c.getLogger().info("Passed recusal test");
  }

  /**
   * Tests that the admin API launched in an application VM can see
   * caches and regions in other members of the distributed system.
   */
  public void _testAdminRecuses() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);

    // Connect ourselves to the DS
    GemFireCacheImpl c = (GemFireCacheImpl)cache;
    InternalDistributedSystem ids = (InternalDistributedSystem)
        c.getDistributedSystem();
    
    // Create the admin system
    vm0.invoke(this.getClass(), "connectAdminSystem");
    
    // How many of us are there?  Use this healthy DS member
    // to examine...
    int numBefore = ids.getDistributionManager().getNormalDistributionManagerIds().size();

    vm0.invokeAsync(this.getClass(), "crashMe");
    waitForDeath(c, numBefore);
    resetVm(vm0);
  }

  public void testNull() {
    // TODO remove once other test is in place
    getLogWriter().info("TODO: this test needs to use VM#bounce");
  }
  
  private void resetVm(VM vm) {
    // TODO needs to be implemented
    throw new util.TestException("not implemented");
  }
}
