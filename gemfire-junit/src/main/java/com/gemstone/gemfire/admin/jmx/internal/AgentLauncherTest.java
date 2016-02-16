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
package com.gemstone.gemfire.admin.jmx.internal;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.gemstone.gemfire.admin.jmx.AgentConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import junit.framework.TestCase;
import quickstart.ProcessWrapper;

public class AgentLauncherTest extends TestCase {

  private static final String JTESTS = System.getProperty("JTESTS");

  public AgentLauncherTest(final String name) {
    super(name);
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    System.setProperty(AgentConfigImpl.AGENT_PROPSFILE_PROPERTY_NAME, "");
  }

  protected void assertEquals(final AgentLauncher.Status expected, final AgentLauncher.Status actual) {
    assertEquals(expected.baseName, actual.baseName);
    assertEquals(expected.state, actual.state);
    assertEquals(expected.pid, actual.pid);
  }

  protected void assertStatus(final AgentLauncher.Status actual,
                              final String expectedBasename,
                              final int expectedState,
                              final int expectedPid)
  {
    assertNotNull(actual);
    assertEquals(expectedBasename, actual.baseName);
    assertEquals(expectedState, actual.state);
    assertEquals(expectedPid, actual.pid);
  }

  protected static boolean deleteAgentWorkingDirectory(final File agentWorkingDirectory) {
    return (!agentWorkingDirectory.exists() || deleteFileRecursive(agentWorkingDirectory));
  }

  protected static boolean deleteFileRecursive(final File file) {
    boolean result = true;

    if (file.isDirectory()) {
      for (final File childFile : file.listFiles()) {
        result &= deleteFileRecursive(childFile);
      }
    }

    return (result && file.delete());
  }

  protected static int findAvailablePort(int attempts, final Integer... usedPorts) {
    final List usedPortsList = (usedPorts != null ? Arrays.asList(usedPorts) : Collections.emptyList());

    attempts = Math.max(attempts, 1);

    while (attempts-- > 0) {
      final int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
      if (!usedPortsList.contains(port)) {
        return port;
      }
    }

    throw new RuntimeException("Failed to find an available port after (" + attempts + ") attempts!");
  }

  protected static File getAgentWorkingDirectory(final String testCaseName) {
    return new File("AgentLauncherTest_" + testCaseName);
  }

  protected static void runAgent(final String[] args, final String processOutputPattern) throws Exception {
    final ProcessWrapper agentProcess = new ProcessWrapper(AgentLauncher.class, args);

    agentProcess.execute();

    if (processOutputPattern != null) {
      agentProcess.waitForOutputToMatch(processOutputPattern);
    }
    agentProcess.waitFor();
  }

  public void testGetStartOptions() throws Exception {
    final String[] commandLineArguments = {
      "start",
      "appendto-log-file=true",
      "log-level=warn",
      "mcast-port=10336",
      "-dir=" + System.getProperty("user.home"),
      "-J-Xms256M",
      "-J-Xmx1024M"
    };

    final AgentLauncher launcher = new AgentLauncher("Agent");

    final Map<String, Object> startOptions = launcher.getStartOptions(commandLineArguments);

    assertNotNull(startOptions);
    assertEquals("true", startOptions.get(AgentLauncher.APPENDTO_LOG_FILE));
    assertEquals(new File(System.getProperty("user.home")), startOptions.get(AgentLauncher.DIR));

    final Properties props = (Properties) startOptions.get(AgentLauncher.AGENT_PROPS);

    assertNotNull(props);
    assertEquals(2, props.size());
    assertEquals("warn", props.getProperty("log-level"));
    assertEquals("10336", props.getProperty("mcast-port"));

    final List<String> vmArgs = (List<String>) startOptions.get(AgentLauncher.VMARGS);

    assertNotNull(vmArgs);
    assertEquals(2, vmArgs.size());
    assertTrue(vmArgs.contains("-Xms256M"));
    assertTrue(vmArgs.contains("-Xmx1024M"));

    // now assert the System property 'gfAgentPropertyFile'
    assertEquals(new File(System.getProperty("user.home"), AgentConfig.DEFAULT_PROPERTY_FILE).getPath(),
      System.getProperty(AgentConfigImpl.AGENT_PROPSFILE_PROPERTY_NAME));
  }

  // test code coverage for Bug #44658 - Agent ignores 'property-file' command line option (regression in 6.6.2)
  public void testGetStartOptionsWithPropertyFileOption() throws Exception {
    final String[] commandLineArguments = {
      "start",
      "-dir=" + System.getProperty("user.dir"),
      "-J-Xms512M",
      "log-level=warn",
      "mcast-port=10448",
      "property-file=/path/to/custom/property/file.properties",
    };

    final AgentLauncher launcher = new AgentLauncher("Agent");

    final Map<String, Object> startOptions = launcher.getStartOptions(commandLineArguments);

    assertNotNull("The start options for the Agent launcher having command-line arguments should not have been null!",
      startOptions);
    assertFalse(startOptions.isEmpty());
    assertEquals(new File(System.getProperty("user.dir")), startOptions.get(AgentLauncher.DIR));

    final Properties props = (Properties) startOptions.get(AgentLauncher.AGENT_PROPS);

    assertNotNull(props);
    assertEquals(3, props.size());
    assertEquals("warn", props.getProperty("log-level"));
    assertEquals("10448", props.getProperty("mcast-port"));
    assertEquals("/path/to/custom/property/file.properties", props.getProperty(AgentConfigImpl.PROPERTY_FILE_NAME));

    final List<String> vmArgs = (List<String>) startOptions.get(AgentLauncher.VMARGS);

    assertNotNull(vmArgs);
    assertEquals(1, vmArgs.size());
    assertTrue(vmArgs.contains("-Xms512M"));

    // now assert the System property 'gfAgentPropertyFile'
    assertEquals("/path/to/custom/property/file.properties", System.getProperty(
      AgentConfigImpl.AGENT_PROPSFILE_PROPERTY_NAME));
  }

  public void testGetStopOptions() throws Exception {
    final String[] commandLineArguments = {
      "stop",
      "-dir="+System.getProperty("user.home")
    };

    final AgentLauncher launcher = new AgentLauncher("Agent");

    final Map<String, Object> stopOptions = launcher.getStopOptions(commandLineArguments);

    assertNotNull(stopOptions);
    assertEquals(new File(System.getProperty("user.home")), stopOptions.get(AgentLauncher.DIR));
  }

  public void testCreateStatus() throws Exception {
    final AgentLauncher.Status status = AgentLauncher.createStatus("agent", AgentLauncher.RUNNING, 12345);

    assertNotNull(status);
    assertEquals("agent", status.baseName);
    assertEquals(AgentLauncher.RUNNING, status.state);
    assertEquals(12345, status.pid);
    assertNull(status.msg);
    assertNull(status.exception);
  }

  public void testCreateStatusWithMessageAndException() throws Exception {
    final AgentLauncher.Status status = AgentLauncher.createStatus("agent", AgentLauncher.STARTING, 11235,
      "Test Message!", new Exception("Test Exception!"));

    assertNotNull(status);
    assertEquals("agent", status.baseName);
    assertEquals(AgentLauncher.STARTING, status.state);
    assertEquals(11235, status.pid);
    assertEquals("Test Message!", status.msg);
    assertEquals("Test Exception!", status.exception.getMessage());
  }

  public void testGetStatusWhenStatusFileDoesNotExists() throws Exception {
    final AgentLauncher launcher = new AgentLauncher("Agent");

    final AgentLauncher.Status status = launcher.getStatus();

    assertStatus(status, "Agent", AgentLauncher.SHUTDOWN, 0);
    assertEquals(LocalizedStrings.AgentLauncher_0_IS_NOT_RUNNING_IN_SPECIFIED_WORKING_DIRECTORY_1.toLocalizedString("Agent", null), status.msg);
    assertNull(status.exception);
  }

  public void testPause() throws Exception {
    final long t0 = System.currentTimeMillis();
    AgentLauncher.pause(100);
    final long t1 = System.currentTimeMillis();
    assertTrue("Failed to wait 1/10th of a second!", t1 - t0 >= 100);
  }

  public void testStartStatusAndStop() throws Exception {
    final File agentWorkingDirectory = getAgentWorkingDirectory("testStartStatusAndStop");
    final File agentStatusFile = new File(agentWorkingDirectory, ".agent.ser");

    assertTrue("Expected the Agent working directory (" + agentWorkingDirectory.getAbsolutePath()
      + ") to be successfully deleted!", deleteAgentWorkingDirectory(agentWorkingDirectory));

    assertTrue("Failed to create working directory (" + agentWorkingDirectory.getAbsolutePath() + ") for Agent!",
      agentWorkingDirectory.mkdir());

    assertFalse("Did not expect the Agent status file " + agentStatusFile.getAbsolutePath() + ") to exist!",
      agentStatusFile.exists());

    runAgent(new String[] { "start", "mcast-port=10336", "http-enabled=false", "rmi-enabled=false", "snmp-enabled=false",
      "-classpath=" + JTESTS, "-dir=" + agentWorkingDirectory.getAbsolutePath() },
      "Starting JMX Agent with pid: \\d+");

    assertTrue("Expected the Agent status file (" + agentStatusFile.getAbsolutePath() + ") to have been created!",
      agentStatusFile.exists());

    runAgent(new String[] { "status", "-dir=" + agentWorkingDirectory.getAbsolutePath() },
      "Agent pid: \\d+ status: running");

    runAgent(new String[] { "stop", "-dir=" + agentWorkingDirectory.getAbsolutePath() }, "The Agent has shut down.");

    assertFalse("Expected the Agent status file (" + agentStatusFile.getAbsolutePath() + ") to have been removed!",
      agentStatusFile.exists());
  }

  public void testWriteReadAndDeleteStatus() throws Exception {
    final File expectedStatusFile = new File(System.getProperty("user.dir"), ".agent.ser");
    final AgentLauncher launcher = new AgentLauncher("Agent");

    launcher.getStartOptions(new String[] { "-dir=" + System.getProperty("user.dir") });

    assertFalse(expectedStatusFile.exists());

    final AgentLauncher.Status expectedStatus = AgentLauncher.createStatus("agent", AgentLauncher.RUNNING, 13579);

    assertStatus(expectedStatus, "agent", AgentLauncher.RUNNING, 13579);

    launcher.writeStatus(expectedStatus);

    assertTrue(expectedStatusFile.exists());

    final AgentLauncher.Status actualStatus = launcher.readStatus();

    assertNotNull(actualStatus);
    assertEquals(expectedStatus, actualStatus);
    assertTrue(expectedStatusFile.exists());

    launcher.deleteStatus();

    assertFalse(expectedStatusFile.exists());
  }

}
