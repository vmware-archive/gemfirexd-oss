package com.gemstone.gemfire.management.internal.cli.commands;

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

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.DistributedLockService;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.deadlock.DeadlockDetector;
import com.gemstone.gemfire.distributed.internal.deadlock.Dependency;
import com.gemstone.gemfire.distributed.internal.deadlock.GemFireDeadlockDetector;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.cli.Result.Status;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.remote.CommandProcessor;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;
import com.gemstone.gemfire.management.internal.cli.util.CommandStringBuilder;

import dunit.Host;
import dunit.SerializableCallable;
import dunit.SerializableRunnable;
import dunit.VM;
import dunit.DistributedTestCase.WaitCriterion;

/**
 * This DUnit tests uses same code as GemFireDeadlockDetectorDUnitTest
 * and uses the command processor for executing the "show deadlock" command
 *
 */
//testNoDeadlock is finding a deadlock due to a race cleaning up the deadlock from a previous test.
@org.junit.FixMethodOrder(org.junit.runners.MethodSorters.JVM)
public class ShowDeadlockDUnitTest extends CacheTestCase {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private static final Set<Thread> stuckThreads = Collections.synchronizedSet(new HashSet<Thread>());
  private static final Map<String, String> EMPTY_ENV = Collections.emptyMap();


  @Override
  public void tearDown2() throws Exception {
    invokeInEveryVM(new SerializableRunnable() {
      private static final long serialVersionUID = 1L;

      public void run() {
        for(Thread thread: stuckThreads) {
          thread.interrupt();
        }
      }
    });
  }

  public ShowDeadlockDUnitTest(String name) {
    super(name);
  }

  public void testNoDeadlock() throws ClassNotFoundException, IOException {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    //Make sure a deadlock from a previous test is cleared.
    disconnectAllFromDS();
    
    createCache(vm0);
    createCache(vm1);
    createCache(new Properties());
    
    String fileName = "dependency.txt";
    GemFireDeadlockDetector detect = new GemFireDeadlockDetector();
    assertEquals(null, detect.find().findCycle());

    CommandProcessor commandProcessor = new CommandProcessor();
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.SHOW_DEADLOCK);
    csb.addOption(CliStrings.SHOW_DEADLOCK__DEPENDENCIES__FILE, fileName);
    Result result = commandProcessor.createCommandStatement(csb.toString(), EMPTY_ENV).process();

    String deadLockOutputFromCommand = getResultAsString(result);
    
    getLogWriter().info("output = " + deadLockOutputFromCommand);
    assertEquals(true, result.hasIncomingFiles());
    assertEquals(true, result.getStatus().equals(Status.OK));
    assertEquals(true, deadLockOutputFromCommand.startsWith(CliStrings.SHOW_DEADLOCK__NO__DEADLOCK));
    result.saveIncomingFiles(null);
    File file = new File(fileName);
    assertTrue(file.exists());
    file.delete();
  }

  private static final Lock lock = new ReentrantLock();


  public void testDistributedDeadlockWithFunction() throws InterruptedException, ClassNotFoundException, IOException {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    String filename = "gfeDependency.txt";
    InternalDistributedMember member1 = createCache(vm0);
    final InternalDistributedMember member2 = createCache(vm1);
    createCache(new Properties());
    //Have two threads lock locks on different members in different orders.
    //This thread locks the lock member1 first, then member2.
    lockTheLocks(vm0, member2);
    //This thread locks the lock member2 first, then member1.
    lockTheLocks(vm1, member1);

    Thread.sleep(5000);
    CommandProcessor commandProcessor = new CommandProcessor();
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.SHOW_DEADLOCK);
    csb.addOption(CliStrings.SHOW_DEADLOCK__DEPENDENCIES__FILE, filename);
    Result result = commandProcessor.createCommandStatement(csb.toString(), EMPTY_ENV).process();

    String deadLockOutputFromCommand = getResultAsString(result);
    getLogWriter().info("Deadlock = " + deadLockOutputFromCommand);
    result.saveIncomingFiles(null);
    assertEquals(true, deadLockOutputFromCommand.startsWith(CliStrings.SHOW_DEADLOCK__DEADLOCK__DETECTED));
    assertEquals(true, result.getStatus().equals(Status.OK));
    File file = new File(filename);
    assertTrue(file.exists());
    file.delete();
    
  }


  private void createCache(Properties props) {
    getSystem(props);
    final Cache cache = getCache();
  }

  private Properties createProperties(Host host, int locatorPort) {
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
//    props.setProperty(DistributionConfig.LOCATORS_NAME, getServerHostName(host) + "[" + locatorPort + "]");
    props.setProperty(DistributionConfig.LOG_LEVEL_NAME, "info");
    props.setProperty(DistributionConfig.STATISTIC_SAMPLING_ENABLED_NAME, "true");
    props.setProperty(DistributionConfig.ENABLE_TIME_STATISTICS_NAME, "true");
    props.put(DistributionConfig.ENABLE_NETWORK_PARTITION_DETECTION_NAME, "true");
    return props;
  }

  private void lockTheLocks(VM vm0, final InternalDistributedMember member) {
    vm0.invokeAsync(new SerializableRunnable() {

      private static final long serialVersionUID = 1L;

      public void run() {
        lock.lock();
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          fail("interrupted", e);
        }
        ResultCollector collector = FunctionService.onMember(system, member).execute(new TestFunction());
        //wait the function to lock the lock on member.
        collector.getResult();
        lock.unlock();
      }
    });
  }

  public void testDistributedDeadlockWithDLock() throws InterruptedException, ClassNotFoundException, IOException {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    createCache(new Properties());

    lockTheDLocks(vm0, "one", "two");
    lockTheDLocks(vm1, "two", "one");

    final GemFireDeadlockDetector detect = new GemFireDeadlockDetector();

    LinkedList<Dependency> deadlock = null;


    for(int i =0; i < 120; i ++) {
      deadlock = detect.find().findCycle();
      if(deadlock != null) {
        break;
      }
      Thread.sleep(1000);
    }

  /*  WaitCriterion wc = new WaitCriterion() {
      public String description() {
        return "Waiting for the deadlock to be detected";
      }

      public boolean done() {
        return detect.find().findCycle() != null;
      }
    };

    waitForCriterion(wc, 100000, 1000, true);*/
   // deadlock = detect.find().findCycle();
    String fileName = "dependency.txt";
    CommandProcessor commandProcessor = new CommandProcessor();
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.SHOW_DEADLOCK);
    csb.addOption(CliStrings.SHOW_DEADLOCK__DEPENDENCIES__FILE, fileName);
    Result result = commandProcessor.createCommandStatement(csb.toString() , EMPTY_ENV).process();
    String deadLockOutputFromCommand = getResultAsString(result);
    getLogWriter().info("Deadlock = " + deadLockOutputFromCommand);
    assertTrue(deadlock != null);
    assertEquals(true , result.getStatus().equals(Status.OK));
    assertEquals(true, deadLockOutputFromCommand.startsWith(CliStrings.SHOW_DEADLOCK__DEADLOCK__DETECTED));
    result.saveIncomingFiles(".");
    File file = new File(fileName);
    assertTrue(file.exists());
    file.delete();
  }

  private void lockTheDLocks(VM vm, final String first, final String second) {
    vm.invokeAsync(new SerializableRunnable() {

      private static final long serialVersionUID = 1L;

      public void run() {
        getCache();
        DistributedLockService dls = DistributedLockService.create("deadlock_test", getSystem());
        dls.lock(first, 10 * 1000, -1);

        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        dls.lock(second, 10 * 1000, -1);
      }
    });

  }

  private InternalDistributedMember createCache(VM vm) {
    return (InternalDistributedMember) vm.invoke(new SerializableCallable() {
      /**
       * 
       */
      private static final long serialVersionUID = 1L;

      public Object call() {
        getCache();
        return getSystem().getDistributedMember();
      }
    });
  }
  
  private String getResultAsString(Result result) {
    StringBuilder sb = new StringBuilder();
    while (result.hasNextLine()) {
      sb.append(result.nextLine());
    }
    
    return sb.toString();
  }
  private static class TestFunction implements Function {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    /**
     * 
     */
    private static final int LOCK_WAIT_TIME = 1000;

    public boolean hasResult() {
      return true;
    }


    public void execute(FunctionContext context) {
      try {
        stuckThreads.add(Thread.currentThread());
        lock.tryLock(LOCK_WAIT_TIME, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        //ingore
      }
      context.getResultSender().lastResult(null);
    }

    public String getId() {
      return getClass().getCanonicalName();
    }

    public boolean optimizeForWrite() {
      return false;
    }

    public boolean isHA() {
      return false;
    }
  }

}

