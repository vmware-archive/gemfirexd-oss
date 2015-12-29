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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;

import util.ClassBuilder;

import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.FileUtil;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;
import com.gemstone.gemfire.management.internal.cli.util.CommandStringBuilder;

import dunit.Host;
import dunit.SerializableRunnable;
import dunit.VM;

/**
 * A distributed test suite of test cases for testing the queue commands that
 * are part of Gfsh.
 * 
 * @author David Hoots
 * @since 7.5
 */

public class QueueCommandsDUnitTest extends CliCommandTestBase {
  private static final long serialVersionUID = 1L;

  final List<String> filesToBeDeleted = new CopyOnWriteArrayList<String>();

  public QueueCommandsDUnitTest(final String testName) {
    super(testName);
  }

  public void testAsyncEventQueue() throws IOException {
    final String queue1Name = "testAsyncEventQueue1";
    final String queue2Name = "testAsyncEventQueue2";
    final String diskStoreName = "testAsyncEventQueueDiskStore";

    Properties localProps = new Properties();
    localProps.setProperty(DistributionConfig.GROUPS_NAME, "Group0");
    createDefaultSetup(localProps);

    CommandResult cmdResult = executeCommand(CliStrings.LIST_ASYNC_EVENT_QUEUES);
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    assertTrue(commandResultToString(cmdResult).contains("No Async Event Queues Found"));

    final VM vm1 = Host.getHost(0).getVM(1);
    final String vm1Name = "VM" + vm1.getPid();
    final File diskStoreDir = new File(new File(".").getAbsolutePath(), diskStoreName);
    this.filesToBeDeleted.add(diskStoreDir.getAbsolutePath());
    vm1.invoke(new SerializableRunnable() {
      public void run() {
        diskStoreDir.mkdirs();
        
        Properties localProps = new Properties();
        localProps.setProperty(DistributionConfig.NAME_NAME, vm1Name);
        localProps.setProperty(DistributionConfig.GROUPS_NAME, "Group1");
        getSystem(localProps);
        getCache();
      }
    });

    final VM vm2 = Host.getHost(0).getVM(2);
    final String vm2Name = "VM" + vm2.getPid();
    vm2.invoke(new SerializableRunnable() {
      public void run() {
        Properties localProps = new Properties();
        localProps.setProperty(DistributionConfig.NAME_NAME, vm2Name);
        localProps.setProperty(DistributionConfig.GROUPS_NAME, "Group2");
        getSystem(localProps);
        getCache();
      }
    });
    
    // Deploy a JAR file with an AsyncEventListener that can be instantiated on each server
    final File jarFile = new File(new File(".").getAbsolutePath(), "QueueCommandsDUnit.jar");
    QueueCommandsDUnitTest.this.filesToBeDeleted.add(jarFile.getAbsolutePath());
    
    ClassBuilder classBuilder = new ClassBuilder();
    byte[] jarBytes = classBuilder.createJarFromClassContent("com/qcdunit/QueueCommandsDUnitTestListener",
        "package com.qcdunit;" +
        "import java.util.List; import java.util.Properties;" +
        "import com.gemstone.gemfire.internal.cache.xmlcache.Declarable2; import com.gemstone.gemfire.cache.asyncqueue.AsyncEvent;" +
        "import com.gemstone.gemfire.cache.asyncqueue.AsyncEventListener;" +
        "public class QueueCommandsDUnitTestListener implements Declarable2, AsyncEventListener {" +
        "Properties props;" +
        "public boolean processEvents(List events) { return true; }" +
        "public void close() {}" +
        "public void init(final Properties props) {this.props = props;}" +
        "public Properties getConfig() {return this.props;}}");
    writeJarBytesToFile(jarFile, jarBytes);

    cmdResult = executeCommand("deploy --jar=QueueCommandsDUnit.jar");
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    
    CommandStringBuilder commandStringBuilder = new CommandStringBuilder(CliStrings.CREATE_DISK_STORE);
    commandStringBuilder.addOption(CliStrings.CREATE_DISK_STORE__NAME, diskStoreName);
    commandStringBuilder.addOption(CliStrings.CREATE_DISK_STORE__GROUP, "Group1");
    commandStringBuilder.addOption(CliStrings.CREATE_DISK_STORE__DIRECTORY_AND_SIZE, diskStoreDir.getAbsolutePath());
    cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    String stringResult = commandResultToString(cmdResult);
    assertEquals(3, countLinesInString(stringResult, false));
    assertEquals(false, stringResult.contains("ERROR"));
    assertTrue(stringContainsLine(stringResult, vm1Name + ".*Success"));
    
    commandStringBuilder = new CommandStringBuilder(CliStrings.CREATE_ASYNC_EVENT_QUEUE);
    commandStringBuilder.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__ID, queue1Name);
    commandStringBuilder.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__GROUP, "Group1");
    commandStringBuilder.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__BATCH_SIZE, "514");
    commandStringBuilder.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__PERSISTENT, "true");
    commandStringBuilder.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__DISK_STORE, diskStoreName);
    commandStringBuilder.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__MAXIMUM_QUEUE_MEMORY, "213");
    commandStringBuilder.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__LISTENER, "com.qcdunit.QueueCommandsDUnitTestListener");
    commandStringBuilder.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__LISTENER_PARAM_AND_VALUE, "param1");
    commandStringBuilder.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__LISTENER_PARAM_AND_VALUE, "param2#value2");
    cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    stringResult = commandResultToString(cmdResult);
    assertEquals(3, countLinesInString(stringResult, false));
    assertEquals(false, stringResult.contains("ERROR"));
    assertTrue(stringContainsLine(stringResult, vm1Name + ".*Success"));

     // Verify that the queue was created on the correct member
    cmdResult = executeCommand(CliStrings.LIST_ASYNC_EVENT_QUEUES);
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    stringResult = commandResultToString(cmdResult);
    assertEquals(3, countLinesInString(stringResult, false));
    assertTrue(stringContainsLine(stringResult, vm1Name + " .*" + queue1Name + " .*514 .*true .*" + diskStoreName + " .*213 .*"
        + " .*com.qcdunit.QueueCommandsDUnitTestListener" + ".*"));
    assertTrue(stringContainsLine(stringResult, vm1Name + ".*param2=value2.*"));
    assertTrue(stringContainsLine(stringResult, vm1Name + ".*param1=[^\\w].*"));
    assertFalse(stringContainsLine(stringResult, vm2Name + ".*" + queue1Name + ".*"));
    
    commandStringBuilder = new CommandStringBuilder(CliStrings.CREATE_ASYNC_EVENT_QUEUE);
    commandStringBuilder.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__ID, queue2Name);
    commandStringBuilder.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__LISTENER, "com.qcdunit.QueueCommandsDUnitTestListener");
    cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    stringResult = commandResultToString(cmdResult);
    assertEquals(5, countLinesInString(stringResult, false));
    assertTrue(stringContainsLine(stringResult, "Manager.*Success"));
    assertTrue(stringContainsLine(stringResult, vm2Name + ".*Success"));
    assertTrue(stringContainsLine(stringResult, vm1Name + ".*Success"));
   
    // Verify that the queue was created on the correct members
    cmdResult = executeCommand(CliStrings.LIST_ASYNC_EVENT_QUEUES);
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    stringResult = commandResultToString(cmdResult);
    assertEquals(6, countLinesInString(stringResult, false));
    assertTrue(stringContainsLine(stringResult, "Manager .*" + queue2Name + " .*100 .*false .*null .*100 .*"
        + " .*com.qcdunit.QueueCommandsDUnitTestListener"));
    assertTrue(stringContainsLine(stringResult, vm1Name + " .*" + queue1Name + " .*514 .*true .*" + diskStoreName + " .*213 .*"
        + " .*com.qcdunit.QueueCommandsDUnitTestListener" + ".*"));
    assertTrue(stringContainsLine(stringResult, vm1Name + " .*" + queue2Name + " .*100 .*false .*null .*100 .*"
        + " .*com.qcdunit.QueueCommandsDUnitTestListener"));
    assertTrue(stringContainsLine(stringResult, vm2Name + " .*" + queue2Name + " .*100 .*false .*null .*100 .*"
        + " .*com.qcdunit.QueueCommandsDUnitTestListener"));
  }

  @Override
  public void tearDown2() throws Exception {
    for (String path : this.filesToBeDeleted) {
      try {
        final File fileToDelete = new File(path);
        FileUtil.delete(fileToDelete);
        if (path.endsWith(".jar")) {
          executeCommand("undeploy --jar=" + fileToDelete.getName());
        }
      } catch (IOException e) {
        getLogWriter().error("Unable to delete file", e);
      }
    }
    this.filesToBeDeleted.clear();
    super.tearDown2();
  }
  
  private void writeJarBytesToFile(File jarFile, byte[] jarBytes) throws IOException {
    final OutputStream outStream = new FileOutputStream(jarFile);
    outStream.write(jarBytes);
    outStream.close();
  }
}
