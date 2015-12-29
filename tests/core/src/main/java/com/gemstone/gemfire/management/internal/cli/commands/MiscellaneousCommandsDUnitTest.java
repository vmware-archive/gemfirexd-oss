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
package com.gemstone.gemfire.management.internal.cli.commands;

import java.util.List;
import java.util.Properties;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.lang.ThreadUtils;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;
import com.gemstone.gemfire.management.internal.cli.result.ResultData;
import com.gemstone.gemfire.management.internal.cli.result.TabularResultData;

import dunit.Host;
import dunit.SerializableCallable;
import dunit.SerializableRunnable;
import dunit.VM;
import management.cli.TestableGfsh;


/**
 * 
 * Dunit class for testing gemfire function commands : GC, Shutdown
 * 
 * @author apande
 *  
 * 
 */
public class MiscellaneousCommandsDUnitTest extends CliCommandTestBase {

  private static final long serialVersionUID = 1L;  

  public MiscellaneousCommandsDUnitTest(String name) {
    super(name);
  }
 
  
  public void testGCForGroup() {
    Properties localProps = new Properties();
    localProps.setProperty(DistributionConfig.NAME_NAME, "Manager");
    localProps.setProperty(DistributionConfig.GROUPS_NAME, "Group1");
    createDefaultSetup(localProps);
    String command = "gc --group=Group1";
    CommandResult cmdResult = executeCommand(command);
    cmdResult.resetToFirstLine();
    if (cmdResult != null) {
      String cmdResultStr = commandResultToString(cmdResult);
      getLogWriter().info("testGCForGroup cmdResultStr=" + cmdResultStr);
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      if (cmdResult.getType().equals(ResultData.TYPE_TABULAR)) {
        TabularResultData table = (TabularResultData) cmdResult.getResultData();
        List<String> memberNames = table
            .retrieveAllValues(CliStrings.GC__MSG__MEMBER_NAME);
        assertEquals(true, memberNames.size() == 1 ? true : false);
      } else {
        fail("testGCForGroup failed as CommandResult should be table type");
      }
    } else {
      fail("testGCForGroup failed as did not get CommandResult");
    }
  }
 
  
  
  public static String getMemberId(){
    Cache cache = new GemfireDataCommandsDUnitTest("test").getCache();
    return cache.getDistributedSystem().getDistributedMember().getId();
  }


  public void testGCForMemberID() {
    createDefaultSetup(null);
    final VM vm1 = Host.getHost(0).getVM(1);
    final String vm1MemberId = (String) vm1.invoke(
        MiscellaneousCommandsDUnitTest.class, "getMemberId");
    String command = "gc --member=" + vm1MemberId;
    CommandResult cmdResult = executeCommand(command);
    cmdResult.resetToFirstLine();
    if (cmdResult != null) {
      String cmdResultStr = commandResultToString(cmdResult);
      getLogWriter().info("testGCForMemberID cmdResultStr=" + cmdResultStr);
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      if (cmdResult.getType().equals(ResultData.TYPE_TABULAR)) {
        TabularResultData table = (TabularResultData) cmdResult.getResultData();
        List<String> memberNames = table
            .retrieveAllValues(CliStrings.GC__MSG__MEMBER_NAME);
        assertEquals(true, memberNames.size() == 1 ? true : false);
      } else {
        fail("testGCForGroup failed as CommandResult should be table type");
      }
    } else {
      fail("testGCForCluster failed as did not get CommandResult");
    }
  }
  
  public void testShowLogDefault() {
    createDefaultSetup(null);
    final VM vm1 = Host.getHost(0).getVM(0);
    final String vm1MemberId = (String) vm1.invoke(
        MiscellaneousCommandsDUnitTest.class, "getMemberId");
    String command = "show log --member=" + vm1MemberId;
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String log = commandResultToString(cmdResult);
      assertNotNull(log);
      getLogWriter().info("Show Log is" + log);
      assertEquals(Result.Status.OK, cmdResult.getStatus());
    } else {
      fail("testShowLog failed as did not get CommandResult");
    }
    
  }

  public void testShowLogNumLines() {
    createDefaultSetup(null);
    final VM vm1 = Host.getHost(0).getVM(0);
    final String vm1MemberId = (String) vm1.invoke(
        MiscellaneousCommandsDUnitTest.class, "getMemberId");
    String command = "show log --member=" + vm1MemberId  + " --lines=50";
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String log = commandResultToString(cmdResult);
      assertNotNull(log);
      getLogWriter().info("Show Log is" + log);
      assertEquals(Result.Status.OK, cmdResult.getStatus());
    } else {
      fail("testShowLog failed as did not get CommandResult");
    }
    
  }
  
  public void testGCForEntireCluster() {
    setupForGC();
    String command = "gc";
    CommandResult cmdResult = executeCommand(command);
    cmdResult.resetToFirstLine();
    if (cmdResult != null) {
      String cmdResultStr = commandResultToString(cmdResult);
      getLogWriter()
          .info("testGCForEntireCluster cmdResultStr=" + cmdResultStr);
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      if (cmdResult.getType().equals(ResultData.TYPE_TABULAR)) {
        TabularResultData table = (TabularResultData) cmdResult.getResultData();
        List<String> memberNames = table
            .retrieveAllValues(CliStrings.GC__MSG__MEMBER_NAME);
        assertEquals(true, memberNames.size() == 2 ? true : false);
      } else {
        fail("testGCForGroup failed as CommandResult should be table type");
      }
    } else {
      fail("testGCForGroup failed as did not get CommandResult");
    }
  }
  
  void setupForGC(){   
    final VM vm1 = Host.getHost(0).getVM(1);
    final VM vm2 = Host.getHost(0).getVM(2);
    
    
    createDefaultSetup(null);
    vm1.invoke(new SerializableRunnable() {
      public void run() {
        // no need to close cache as it will be closed as part of teardown2
        Cache cache = getCache();

        RegionFactory<Integer, Integer> dataRegionFactory = cache
            .createRegionFactory(RegionShortcut.PARTITION);
        Region region = dataRegionFactory.create("testRegion");
        for (int i = 0; i < 10; i++) {
          region.put("key" + (i + 200), "value" + (i + 200));
        }
      }
    });
  }

  public void testShutDownWithoutTimeout() {
    setupForShutDown();
    ThreadUtils.sleep(2500);

    MiscellaneousCommands misc = new MiscellaneousCommands();
    Result result = misc.executeFunction(getCache(), -1);

    getLogWriter().info("testShutDownWithoutTimeout result=" + result);

    verifyShutDown();

    final TestableGfsh defaultShell = getDefaultShell();

    // Need for the Gfsh HTTP enablement during shutdown to properly assess the state of the connection.
    waitForCriterion(new WaitCriterion() {
      public boolean done() {
        return !defaultShell.isConnectedAndReady();
      }
      public String description() {
        return "Waits for the shell to disconnect!";
      }
    }, 1000, 250, true);

    assertFalse(defaultShell.isConnectedAndReady());
  }

  public void testShutDownForTIMEOUT() {
    setupForShutDown();
    ThreadUtils.sleep(2500);

    MiscellaneousCommands misc = new MiscellaneousCommands();
    Result result = misc.executeFunction(getCache(), 1);

    getLogWriter().info("testShutDownForTIMEOUT result=" + result);

    verifyShutDown();

    final TestableGfsh defaultShell = getDefaultShell();

    // Need for the Gfsh HTTP enablement during shutdown to properly assess the state of the connection.
    waitForCriterion(new WaitCriterion() {
      public boolean done() {
        return !defaultShell.isConnectedAndReady();
      }
      public String description() {
        return "Waits for the shell to disconnect!";
      }
    }, 1000, 250, false);

    assertFalse(defaultShell.isConnectedAndReady());
  }

  void setupForShutDown(){   
    final VM vm0 = Host.getHost(0).getVM(0);
    final VM vm1 = Host.getHost(0).getVM(1);
    
    
    createDefaultSetup(null);
    vm1.invoke(new SerializableRunnable() {
      public void run() {
        // no need to close cache as it will be closed as part of teardown2
        Cache cache = getCache();

        RegionFactory<Integer, Integer> dataRegionFactory = cache
            .createRegionFactory(RegionShortcut.PARTITION);
        Region region = dataRegionFactory.create("testRegion");
        for (int i = 0; i < 10; i++) {
          region.put("key" + (i + 200), "value" + (i + 200));
        }
      }
    });
    

  }
  void verifyShutDown() {
    final VM vm0 = Host.getHost(0).getVM(0);
    final VM vm1 = Host.getHost(0).getVM(1);
    

    @SuppressWarnings("serial")
    final SerializableCallable connectedChecker = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        boolean cacheExists = true;
        try {
          Cache cacheInstance = CacheFactory.getAnyInstance();
          cacheExists = cacheInstance.getDistributedSystem().isConnected();
        } catch (CacheClosedException e) {
          cacheExists = false;
        }
        return cacheExists;
      }
    };

    WaitCriterion waitCriterion = new WaitCriterion() {      
      @Override
      public boolean done() {
        return Boolean.FALSE.equals(vm0.invoke(connectedChecker)) && Boolean.FALSE.equals(vm1.invoke(connectedChecker));
      }
      
      @Override
      public String description() {
        return "Wait for gfsh to get disconnected from Manager.";
      }
    };
    waitForCriterion(waitCriterion, 5000, 200, true);

    assertEquals("Connected cache still exists in vm1: "+vm1, Boolean.FALSE, vm1.invoke(connectedChecker));    
    assertEquals("Connected cache still exists in vm0: "+vm0, Boolean.FALSE, vm0.invoke(connectedChecker));
  }
}