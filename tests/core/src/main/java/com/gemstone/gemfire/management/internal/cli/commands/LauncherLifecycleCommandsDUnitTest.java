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
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.Query;
import javax.management.QueryExp;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import com.gemstone.gemfire.distributed.AbstractLauncher.ServiceState;
import com.gemstone.gemfire.distributed.AbstractLauncher.Status;
import com.gemstone.gemfire.distributed.LocatorLauncher;
import com.gemstone.gemfire.distributed.LocatorLauncher.Builder;
import com.gemstone.gemfire.distributed.LocatorLauncher.Command;
import com.gemstone.gemfire.distributed.LocatorLauncher.LocatorState;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.lang.ObjectUtils;
import com.gemstone.gemfire.internal.lang.StringUtils;
import com.gemstone.gemfire.internal.process.ProcessType;
import com.gemstone.gemfire.internal.util.IOUtils;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;

import dunit.Host;

/**
 * The LauncherLifecycleCommandsDUnitTest class is a test suite of integration tests testing the functionality
 * and behavior of GemFire launcher lifecycle commands inside Gfsh.
 * </p>
 * @author John Blum
 * @see com.gemstone.gemfire.distributed.AbstractLauncher.Status
 * @see com.gemstone.gemfire.distributed.LocatorLauncher
 * @see com.gemstone.gemfire.distributed.LocatorLauncher.Builder
 * @see com.gemstone.gemfire.distributed.LocatorLauncher.Command
 * @see com.gemstone.gemfire.distributed.LocatorLauncher.LocatorState
 * @see com.gemstone.gemfire.internal.AvailablePortHelper
 * @see com.gemstone.gemfire.management.cli.Result
 * @see com.gemstone.gemfire.management.internal.cli.commands.CliCommandTestBase
 * @see com.gemstone.gemfire.management.internal.cli.commands.LauncherLifecycleCommands
 * @since 7.0
 */
public class LauncherLifecycleCommandsDUnitTest extends CliCommandTestBase {

  public LauncherLifecycleCommandsDUnitTest(final String testName) {
    super(testName);
  }

  protected static String getMemberId(final int jmxManagerPort, final String memberName) throws Exception {
    return getMemberId(InetAddress.getLocalHost().getHostName(), jmxManagerPort, memberName);
  }

  protected static String getMemberId(final String jmxManagerHost, final int jmxManagerPort, final String memberName)
    throws Exception
  {
    JMXConnector connector = null;

    try {
      connector = JMXConnectorFactory.connect(new JMXServiceURL(String.format(
        "service:jmx:rmi://%1$s/jndi/rmi://%1$s:%2$d/jmxrmi", jmxManagerHost, jmxManagerPort)));

      final MBeanServerConnection connection = connector.getMBeanServerConnection();

      final ObjectName objectNamePattern = ObjectName.getInstance("GemFire:type=Member,*");

      final QueryExp query = Query.eq(Query.attr("Name"), Query.value(memberName));

      final Set<ObjectName> objectNames = connection.queryNames(objectNamePattern, query);

      assertNotNull(objectNames);
      assertEquals(1, objectNames.size());

      //final ObjectName objectName = ObjectName.getInstance("GemFire:type=Member,Name=" + memberName);
      final ObjectName objectName = objectNames.iterator().next();

      //System.err.printf("ObjectName for Member with Name (%1$s) is %2$s%n", memberName, objectName);

      return ObjectUtils.toString(connection.getAttribute(objectName, "Id"));
    }
    finally {
      IOUtils.close(connector);
    }
  }

  @SuppressWarnings("unused")
  protected static void assertStatus(final LocatorState expectedStatus, final LocatorState actualStatus) {
    assertEquals(expectedStatus.getStatus(), actualStatus.getStatus());
    assertEquals(expectedStatus.getTimestamp(), actualStatus.getTimestamp());
    assertEquals(expectedStatus.getServiceLocation(), actualStatus.getServiceLocation());
    assertTrue(ObjectUtils.equalsIgnoreNull(expectedStatus.getPid(), actualStatus.getPid()));
    assertEquals(expectedStatus.getUptime(), actualStatus.getUptime());
    assertEquals(expectedStatus.getWorkingDirectory(), actualStatus.getWorkingDirectory());
    assertEquals(expectedStatus.getJvmArguments(), actualStatus.getJvmArguments());
    assertEquals(expectedStatus.getClasspath(), actualStatus.getClasspath());
    assertEquals(expectedStatus.getGemFireVersion(), actualStatus.getGemFireVersion());
    assertEquals(expectedStatus.getJavaVersion(), actualStatus.getJavaVersion());
  }

  // TODO replace method with more robust implementation of to compare service state and status...
  protected static String serviceStateStatusStringNormalized(final ServiceState state) {
    return serviceStateStatusStringNormalized(state.toString());
  }

  protected static String serviceStateStatusStringNormalized(final String locatorStatus) {
    assertNotNull(locatorStatus);
    assertTrue("locatorStatus is missing Uptime: " + locatorStatus, locatorStatus.contains("Uptime"));
    assertTrue("locatorStatus is missing Uptime: " + locatorStatus, locatorStatus.contains("JVM Arguments"));
    final StringBuilder buffer = new StringBuilder();
    buffer.append(locatorStatus.substring(0, locatorStatus.indexOf("Uptime")));
    buffer.append(locatorStatus.substring(locatorStatus.indexOf("JVM Arguments")));
    return buffer.toString().trim();
  }

  protected static Status stopLocator(final File workingDirectory) {
    return stopLocator(IOUtils.tryGetCanonicalPathElseGetAbsolutePath(workingDirectory));
  }

  protected static Status stopLocator(final String workingDirectory) {
    return (new Builder().setCommand(Command.STOP).setWorkingDirectory(workingDirectory).build().stop().getStatus());
  }

  protected static String toString(final Result result) {
    assert result != null : "The Result object from the command execution cannot be null!";

    final StringBuilder buffer = new StringBuilder(StringUtils.LINE_SEPARATOR);

    while (result.hasNextLine()) {
      buffer.append(result.nextLine());
      buffer.append(StringUtils.LINE_SEPARATOR);
    }

    return buffer.toString();
  }

  protected static void writePid(final File pidFile, final int pid) throws IOException {
    assertTrue("The PID file must actually exist!", pidFile != null && pidFile.isFile());
    final FileWriter writer = new FileWriter(pidFile, false);
    writer.write(String.valueOf(pid));
    writer.write("\n");
    writer.flush();
    IOUtils.close(writer);
  }

  public void testStartLocatorCapturesOutputOnError() throws IOException {
    final int locatorPort = AvailablePortHelper.getRandomAvailableTCPPort();

    final String pathname = (getClass().getSimpleName() + "_" + testName);
    final File workingDirectory = new File(pathname);

    workingDirectory.mkdir();

    assertTrue(workingDirectory.isDirectory());

    final File pidFile = new File(workingDirectory, ProcessType.LOCATOR.getPidFileName());

    pidFile.createNewFile();
    final int readPid = Host.getHost(0).getVM(3).getPid();
    writePid(pidFile, readPid);
    pidFile.deleteOnExit();

    assertTrue(pidFile.isFile());

    final CommandResult result = executeCommand(CliStrings.START_LOCATOR + " --name=" + pathname
      + " --dir=" + pathname + " --port=" + locatorPort + " --J=-Dgemfire.jmx-manager-http-port=0 --J=-Dgemfire.jmx-manager-port="
        + AvailablePortHelper.getRandomAvailableTCPPort());

    assertNotNull(result);
    assertEquals(Result.Status.ERROR, result.getStatus());

    final String resultString = toString(result);

    assertTrue(resultString, resultString.contains(
      "Exception in thread \"main\" java.lang.RuntimeException: A PID file already exists and a Locator may be running in "
        + IOUtils.tryGetCanonicalFileElseGetAbsoluteFile(workingDirectory)));
    assertTrue(resultString, resultString.contains(
      "Caused by: com.gemstone.gemfire.internal.process.FileAlreadyExistsException: Pid file already exists: "
        + IOUtils.tryGetCanonicalFileElseGetAbsoluteFile(pidFile)));
  }

  public void testStartLocatorWithRelativeDirectory() {
    final int locatorPort = AvailablePortHelper.getRandomAvailableTCPPort();
    final String pathname = (getClass().getSimpleName() + "_" + testName);
    final File workingDirectory = new File(pathname);

    workingDirectory.mkdir();

    assertTrue(workingDirectory.isDirectory());

    try {
      final CommandResult result = executeCommand(CliStrings.START_LOCATOR + " --name=" + pathname
        + " --dir=" + pathname + " --port=" + locatorPort + " --force=true" + " --J=-Dgemfire.jmx-manager-http-port=0 --J=-Dgemfire.jmx-manager-port="
        + AvailablePortHelper.getRandomAvailableTCPPort());

      assertNotNull(result);
      assertEquals(Result.Status.OK, result.getStatus());

      final String locatorOutput = toString(result);

      assertNotNull(locatorOutput);
      //System.err.println(locatorOutput);
      assertTrue("Locator output was: " + locatorOutput, locatorOutput.contains("Locator in "
        + IOUtils.tryGetCanonicalFileElseGetAbsoluteFile(workingDirectory)));
    }
    finally {
      assertEquals(Status.STOPPED, stopLocator(workingDirectory));
    }
  }

  public void testStatusLocatorUsingMemberNameIDWhenGfshIsNotConnected() {
    final CommandResult result = executeCommand(CliStrings.STATUS_LOCATOR + " --name=" + testName);

    assertEquals(Result.Status.ERROR, result.getStatus());
    assertEquals(CliStrings.format(CliStrings.STATUS_SERVICE__GFSH_NOT_CONNECTED_ERROR_MESSAGE, "Locator"),
      StringUtils.trim(toString(result)));
  }

  public void testStatusLocatorUsingMemberName() {
    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);

    final int jmxManagerPort = ports[0];
    final int locatorPort = ports[1];

    final String pathname = (getClass().getSimpleName() + "_" + testName);
    final File workingDirectory = new File(pathname);

    workingDirectory.mkdir();

    assertTrue(workingDirectory.isDirectory());

    try {
      CommandResult result = executeCommand(CliStrings.START_LOCATOR + " --name=" + pathname + " --dir=" + pathname
        + " --port=" + locatorPort + " --force=true --J=-Dgemfire.jmx-manager-http-port=0 --J=-Dgemfire.jmx-manager-port="
          + jmxManagerPort);

      assertNotNull(result);
      assertEquals(Result.Status.OK, result.getStatus());

      final LocatorLauncher locatorLauncher = new LocatorLauncher.Builder()
        .setCommand(LocatorLauncher.Command.STATUS)
        .setBindAddress(null)
        .setPort(locatorPort)
        .setWorkingDirectory(workingDirectory.getPath())
        .build();

      assertNotNull(locatorLauncher);

      final LocatorState expectedLocatorState = locatorLauncher.waitOnStatusResponse(60, 10, TimeUnit.SECONDS);

      assertNotNull(expectedLocatorState);
      assertEquals(Status.ONLINE, expectedLocatorState.getStatus());

      result = executeCommand(CliStrings.CONNECT + " --locator=localhost[" + locatorPort + "]");

      assertNotNull(result);
      assertEquals(Result.Status.OK, result.getStatus());

      result = executeCommand(CliStrings.STATUS_LOCATOR + " --name=invalidLocatorMemberName");

      assertNotNull(result);
      assertEquals(Result.Status.ERROR, result.getStatus());
      assertEquals(CliStrings.format(CliStrings.STATUS_LOCATOR__NO_LOCATOR_FOUND_FOR_MEMBER_ERROR_MESSAGE,
        "invalidLocatorMemberName"), StringUtils.trim(toString(result)));

      result = executeCommand(CliStrings.STATUS_LOCATOR + " --name=" + pathname);

      assertNotNull(result);
      assertEquals(Result.Status.OK, result.getStatus());
      assertEquals(serviceStateStatusStringNormalized(expectedLocatorState), serviceStateStatusStringNormalized(toString(result)));
    }
    finally {
      assertEquals(Status.STOPPED, stopLocator(workingDirectory));
    }
  }

  public void testStatusLocatorUsingMemberId() throws Exception {
    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);

    final int jmxManagerPort = ports[0];
    final int locatorPort = ports[1];

    final String pathname = (getClass().getSimpleName() + "_" + testName);
    final File workingDirectory = new File(pathname);

    workingDirectory.mkdir();

    assertTrue(workingDirectory.isDirectory());

    try {
      CommandResult result = executeCommand(CliStrings.START_LOCATOR + " --name=" + pathname + " --dir=" + pathname
        + " --port=" + locatorPort + " --force=true --J=-Dgemfire.jmx-manager-http-port=0 --J=-Dgemfire.jmx-manager-port="
          + jmxManagerPort);

      assertNotNull(result);
      assertEquals(Result.Status.OK, result.getStatus());

      final LocatorLauncher locatorLauncher = new LocatorLauncher.Builder()
        .setCommand(LocatorLauncher.Command.STATUS)
        .setBindAddress(null)
        .setPort(locatorPort)
        .setWorkingDirectory(workingDirectory.getPath())
        .build();

      assertNotNull(locatorLauncher);

      final LocatorState expectedLocatorState = locatorLauncher.waitOnStatusResponse(60, 10, TimeUnit.SECONDS);

      assertNotNull(expectedLocatorState);
      assertEquals(Status.ONLINE, expectedLocatorState.getStatus());

      result = executeCommand(CliStrings.CONNECT + " --locator=localhost[" + locatorPort + "]");

      assertNotNull(result);
      assertEquals(Result.Status.OK, result.getStatus());

      final String memberId = getMemberId(jmxManagerPort, pathname);

      result = executeCommand(CliStrings.STATUS_LOCATOR + " --name=" + memberId);

      assertNotNull(result);
      assertEquals(Result.Status.OK, result.getStatus());
      assertEquals(serviceStateStatusStringNormalized(expectedLocatorState), serviceStateStatusStringNormalized(toString(result)));
    }
    finally {
      assertEquals(Status.STOPPED, stopLocator(workingDirectory));
    }
  }

  public void testStopLocatorUsingMemberNameIDWhenGfshIsNotConnected() {
    final CommandResult result = executeCommand(CliStrings.STOP_LOCATOR + " --name=" + testName);

    assertNotNull(result);
    assertEquals(Result.Status.ERROR, result.getStatus());
    assertEquals(CliStrings.format(CliStrings.STOP_SERVICE__GFSH_NOT_CONNECTED_ERROR_MESSAGE, "Locator"),
      StringUtils.trim(toString(result)));
  }

  public void BUG46760WORKAROUNDtestStopLocatorUsingMemberName() {
    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);

    final int jmxManagerPort = ports[0];
    final int locatorPort = ports[1];

    final String pathname = (getClass().getSimpleName() + "_" + testName);
    final File workingDirectory = new File(pathname);

    workingDirectory.mkdir();

    assertTrue(workingDirectory.isDirectory());

    CommandResult result = executeCommand(CliStrings.START_LOCATOR + " --name=" + pathname + " --dir=" + pathname
      + " --port=" + locatorPort + " --force=true --J=-Dgemfire.jmx-manager-http-port=0 --J=-Dgemfire.jmx-manager-port="
        + jmxManagerPort);

    assertNotNull(result);
    assertEquals(Result.Status.OK, result.getStatus());

    final LocatorLauncher locatorLauncher = new LocatorLauncher.Builder()
      .setCommand(LocatorLauncher.Command.STOP)
      .setBindAddress(null)
      .setPort(locatorPort)
      .setWorkingDirectory(workingDirectory.getPath())
      .build();

    assertNotNull(locatorLauncher);

    LocatorState locatorStatus = locatorLauncher.waitOnStatusResponse(60, 10, TimeUnit.SECONDS);

    assertNotNull(locatorStatus);
    assertEquals(Status.ONLINE, locatorStatus.getStatus());

    result = executeCommand(CliStrings.CONNECT + " --locator=localhost[" + locatorPort + "]");

    assertNotNull(result);
    assertEquals(Result.Status.OK, result.getStatus());

    result = executeCommand(CliStrings.STOP_LOCATOR + " --name=invalidLocatorMemberName");

    assertNotNull(result);
    assertEquals(Result.Status.ERROR, result.getStatus());
    assertEquals(CliStrings.format(CliStrings.STOP_LOCATOR__NO_LOCATOR_FOUND_FOR_MEMBER_ERROR_MESSAGE,
      "invalidLocatorMemberName"), StringUtils.trim(toString(result)));

    locatorStatus = locatorLauncher.status();

    assertNotNull(locatorStatus);
    assertEquals(Status.ONLINE, locatorStatus.getStatus());

    result = executeCommand(CliStrings.STOP_LOCATOR + " --name=" + pathname);

    assertNotNull(result);
    assertEquals(Result.Status.OK, result.getStatus());
    // TODO figure out what output to assert and validate on now that 'stop locator' uses Gfsh's logger
    // and standard err/out...
    //assertEquals(CliStrings.format(CliStrings.STOP_LOCATOR__SHUTDOWN_MEMBER_MESSAGE, pathname),
    //  StringUtils.trim(toString(result)));

    final WaitCriterion waitCriteria = new WaitCriterion() {
      @Override public boolean done() {
        final LocatorState locatorStatus = locatorLauncher.status();
        return (locatorStatus != null && Status.NOT_RESPONDING.equals(locatorStatus.getStatus()));
      }
      @Override public String description() {
        return "wait for the Locator to stop";
      }
    };

    waitForCriterion(waitCriteria, 15 * 1000, 5000, true);

    locatorStatus = locatorLauncher.status();

    assertNotNull(locatorStatus);
    assertEquals(Status.NOT_RESPONDING, locatorStatus.getStatus());
  }

  public void BUG46760WORKAROUNDtestStopLocatorUsingMemberId() throws Exception {
    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);

    final int jmxManagerPort = ports[0];
    final int locatorPort = ports[1];

    final String pathname = (getClass().getSimpleName() + "_" + testName);
    final File workingDirectory = new File(pathname);

    workingDirectory.mkdir();

    assertTrue(workingDirectory.isDirectory());

    CommandResult result = executeCommand(CliStrings.START_LOCATOR + " --name=" + pathname + " --dir=" + pathname
      + " --port=" + locatorPort + " --force=true --J=-Dgemfire.jmx-manager-http-port=0 --J=-Dgemfire.jmx-manager-port="
        + jmxManagerPort);

    assertNotNull(result);
    assertEquals(Result.Status.OK, result.getStatus());

    final LocatorLauncher locatorLauncher = new LocatorLauncher.Builder()
      .setCommand(LocatorLauncher.Command.STOP)
      .setBindAddress(null)
      .setPort(locatorPort)
      .setWorkingDirectory(workingDirectory.getPath())
      .build();

    assertNotNull(locatorLauncher);

    LocatorState locatorStatus = locatorLauncher.waitOnStatusResponse(60, 10, TimeUnit.SECONDS);

    assertNotNull(locatorStatus);
    assertEquals(Status.ONLINE, locatorStatus.getStatus());

    result = executeCommand(CliStrings.CONNECT + " --locator=localhost[" + locatorPort + "]");

    assertNotNull(result);
    assertEquals(Result.Status.OK, result.getStatus());

    final String memberId = getMemberId(jmxManagerPort, pathname);

    result = executeCommand(CliStrings.STOP_LOCATOR + " --name=" + memberId);

    assertNotNull(result);
    assertEquals(Result.Status.OK, result.getStatus());
    // TODO figure out what output to assert and validate on now that 'stop locator' uses Gfsh's logger
    // and standard err/out...
    //assertEquals(CliStrings.format(CliStrings.STOP_LOCATOR__SHUTDOWN_MEMBER_MESSAGE, memberId),
    //  StringUtils.trim(toString(result)));

    final WaitCriterion waitCriteria = new WaitCriterion() {
      @Override public boolean done() {
        final LocatorState locatorStatus = locatorLauncher.status();
        return (locatorStatus != null && Status.NOT_RESPONDING.equals(locatorStatus.getStatus()));
      }
      @Override public String description() {
        return "wait for the Locator to stop";
      }
    };

    // TODO: Why do you expect it to be NOT_RESPONDING?
    waitForCriterion(waitCriteria, 15 * 1000, 5000, true);

    locatorStatus = locatorLauncher.status();

    assertNotNull(locatorStatus);
    assertEquals(Status.NOT_RESPONDING, locatorStatus.getStatus());
  }

}
