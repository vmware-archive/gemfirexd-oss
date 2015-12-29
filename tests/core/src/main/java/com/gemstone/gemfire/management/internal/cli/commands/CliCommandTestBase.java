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

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.management.ManagementService;
import com.gemstone.gemfire.management.internal.cli.CommandManager;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;
import com.gemstone.gemfire.management.internal.cli.util.CommandStringBuilder;

import dunit.Host;
import dunit.SerializableCallable;
import dunit.SerializableRunnable;
import management.cli.TestableGfsh;
import management.util.ManagementUtil;
import util.TestException;
import util.TestHelper;

/**
 * Base class for all the CLI/gfsh command dunit tests.
 * 
 * @author Tushar Khairnar
 * @author Abhishek Chaudhari
 * @author David Hoots
 * @author John Blum
 */
public class CliCommandTestBase extends CacheTestCase {

  private static final long serialVersionUID = 1L;

  protected static final String USE_HTTP_SYSTEM_PROPERTY = "useHTTP";

  private ManagementService managementService;

  private transient TestableGfsh shell;

  private boolean useHttpOnConnect = Boolean.getBoolean("useHTTP");

  private int httpPort;
  private int jmxPort;

  private String jmxHost;

  public CliCommandTestBase(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
  }

  public void tearDown2() throws Exception {
    destroyDefaultSetup();
    super.tearDown2();
  }

  /**
   * Create all of the components necessary for the default setup. The provided properties will be used when creating
   * the default cache. This will create GFSH in the controller VM (VM[4]) (no cache) and the manager in VM[0] (with
   * cache). When adding regions, functions, keys, whatever to your cache for tests, you'll need to use
   * Host.getHost(0).getVM(0).invoke(new SerializableRunnable() { public void run() { ... } } in order to have this
   * setup run in the same VM as the manager.
   * 
   * @param props
   *          Properties used when creating the cache for this default setup.
   * @return The default shell
   */
  @SuppressWarnings("serial")
  protected final TestableGfsh createDefaultSetup(final Properties props) {
    Object[] result = (Object[]) Host.getHost(0).getVM(0).invoke(new SerializableCallable() {
      public Object call() {
        final Object[] result = new Object[3];
        final Properties localProps = (props != null ? props : new Properties());

        try {
          jmxHost = InetAddress.getLocalHost().getHostName();
        }
        catch (UnknownHostException ignore) {
          jmxHost = "localhost";
        }

        if (!localProps.containsKey(DistributionConfig.NAME_NAME)) {
          localProps.setProperty(DistributionConfig.NAME_NAME, "Manager");
        }

        final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);

        jmxPort = ports[0];
        httpPort = ports[1];

        localProps.setProperty(DistributionConfig.JMX_MANAGER_NAME, "true");
        localProps.setProperty(DistributionConfig.JMX_MANAGER_START_NAME, "true");
        localProps.setProperty(DistributionConfig.JMX_MANAGER_BIND_ADDRESS_NAME, String.valueOf(jmxHost));
        localProps.setProperty(DistributionConfig.JMX_MANAGER_PORT_NAME, String.valueOf(jmxPort));

        if (Boolean.getBoolean(USE_HTTP_SYSTEM_PROPERTY)) {
          //httpPort = 8080;
          //System.setProperty(DistributionConfig.GEMFIRE_PREFIX + DistributionConfig.JMX_MANAGER_HTTP_PORT_NAME, String.valueOf(httpPort));
          localProps.setProperty(DistributionConfig.JMX_MANAGER_HTTP_PORT_NAME, String.valueOf(httpPort));
        }

        getSystem(localProps);
        verifyManagementServiceStarted(getCache());

        result[0] = jmxHost;
        result[1] = jmxPort;
        result[2] = httpPort;

        return result;
      }
    });

    this.jmxHost = (String) result[0];
    this.jmxPort = (Integer) result[1];
    this.httpPort = (Integer) result[2];

    //ThreadUtils.sleep(5 * 60 * 1000); // 5 minutes

    return defaultShellConnect();
  }

  /**
   * Destroy all of the components created for the default setup.
   */
  @SuppressWarnings("serial")
  protected final void destroyDefaultSetup() {
    if (this.shell != null) {
      // Note: TestableGfsh doesn't use Launcher & code that does System.exit()
      // for gfsh is in Launcher. This is just to ensure cleanup.
      executeCommand(shell, "exit");
//      if(shell.isConnectedAndReady()){
//        executeCommand(shell, "disconnect");
//      }
      this.shell.eof();
      this.shell = null;
    }

    disconnectAllFromDS();

    Host.getHost(0).getVM(0).invoke(new SerializableRunnable() {
      public void run() {
        verifyManagementServiceStopped();
        //if (Boolean.getBoolean(USE_HTTP_SYSTEM_PROPERTY)) {
        //  System.gc();
        //}
      }
    });

    if (Boolean.getBoolean(USE_HTTP_SYSTEM_PROPERTY)) {
      // TODO it is very expensive to bounce a Hydra managed VM, need to find a better way!
      // However, not bouncing the Hydra managed VM causes it to run out of memory due to all the restarts of the
      // embedded HTTP service in the Manager.  Tomcat, or something, has a memory leak.  Hydra did complain about
      // the web app leaving a Function Execution Service Thread running, but this Thread would be in GemFire, not
      // the web app.  However, because the web app delegates command executions to the Manager, Hydra has identified
      // the web app as the source of the memory leak with the Thread.
      Host.getHost(0).getVM(0).bounce();
    }
  }

  /**
   * Start the default management service using the provided Cache.
   * 
   * @param cache
   *          Cache to use when creating the management service
   */
  private void verifyManagementServiceStarted(Cache cache) {
    assert(cache != null);

    this.managementService = ManagementService.getExistingManagementService(cache);
    assertNotNull(this.managementService);
    assertTrue(this.managementService.isManager());
    assertTrue(ManagementUtil.checkIfCommandsAreLoadedOrNot());
  }

  /**
   * Stop the default management service.
   */
  private void verifyManagementServiceStopped() {
    if (this.managementService != null) {
      assertFalse(this.managementService.isManager());
      this.managementService = null;
    }
  }

  /**
   * Connect the default shell to the default JMX server.
   * 
   * @return The default shell.
   */
  private TestableGfsh defaultShellConnect() {
    TestableGfsh shell = getDefaultShell();
    shellConnect(this.jmxHost, this.jmxPort, this.httpPort, shell);
    return shell;
  }

  /**
   * Connect a shell to the JMX server at the given host and port
   * 
   * @param host
   *          Host of the JMX server
   * @param jmxPort
   *          Port of the JMX server
   * @param shell
   *          Shell to connect
   */
  protected void shellConnect(final String host, final int jmxPort, final int httpPort, TestableGfsh shell) {
    assert(host != null);
    assert(shell != null);

    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.CONNECT);
    String endpoint;

    if (useHttpOnConnect) {
      endpoint = "http://" + host + ":" + httpPort + "/gemfire/v1";
      command.addOption(CliStrings.CONNECT__USE_HTTP, Boolean.TRUE.toString());
      command.addOption(CliStrings.CONNECT__URL, endpoint);
    }
    else {
      endpoint = host + "[" + jmxPort + "]";
      command.addOption(CliStrings.CONNECT__JMX_MANAGER, endpoint);
    }

    CommandResult result = executeCommand(shell, command.toString());

    if (!shell.isConnectedAndReady()) {
      throw new TestException("Connect command failed to connect to manager " + endpoint + " result=" + commandResultToString(result));
    }

    info("Successfully connected to managing node using " + (useHttpOnConnect ? "HTTP" : "JMX"));
    assertEquals(true, shell.isConnectedAndReady());
  }

  /**
   * Get the default shell (will create one if it doesn't already exist).
   * 
   * @return The default shell
   */
  protected synchronized final TestableGfsh getDefaultShell() {
    if (this.shell == null) {
      this.shell = createShell();
    }

    return this.shell;
  }

  /**
   * Create a TestableGfsh object.
   * 
   * @return The created shell.
   */
  protected TestableGfsh createShell() {
    try {
      String shellId = getClass().getSimpleName()+"_"+getName();
      TestableGfsh shell = new TestableGfsh(shellId, true, new String[] {});
      shell.setTimeout(300);
      shell.start();
      info("Started testable shell: " + shell);
      return shell;
    } catch (ClassNotFoundException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (IOException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }

  /**
   * Execute a command using the default shell and clear the shell events before returning.
   * 
   * @param command
   *          Command to execute
   * @return The result of the command execution
   */
  protected CommandResult executeCommand(String command) {
    assert(command != null);

    return executeCommand(getDefaultShell(), command);
  }

  /**
   * Execute a command in the provided shell and clear the shell events before returning.
   * 
   * @param shell
   *          Shell in which to execute the command.
   * @param command
   *          Command to execute
   * @return The result of the command execution
   */
  protected CommandResult executeCommand(TestableGfsh shell, String command) {
    assert(shell != null);
    assert(command != null);

    CommandResult commandResult = executeCommandWithoutClear(shell, command);
    shell.clearEvents();
    return commandResult;
  }

  /**
   * Execute a command using the default shell. Useful for getting additional information from the shell after the
   * command has been executed (using getDefaultShell().???). Caller is responsible for calling
   * getDefaultShell().clearEvents() when done.
   * 
   * @param command
   *          Command to execute
   * @return The result of the command execution
   */
  @SuppressWarnings("unused")
  protected CommandResult executeCommandWithoutClear(String command) {
    assert(command != null);

    return executeCommandWithoutClear(getDefaultShell(), command);
  }

  /**
   * Execute a command in the provided shell. Useful for getting additional information from the shell after the command
   * has been executed (using getDefaultShell().???). Caller is responsible for calling getDefaultShell().clearEvents()
   * when done.
   * 
   * @param shell
   *          Shell in which to execute the command.
   * @param command
   *          Command to execute
   * @return The result of the command execution
   */
  protected CommandResult executeCommandWithoutClear(TestableGfsh shell, String command) {
    assert(shell != null);
    assert(command != null);

    try {
      info("Executing command " + command + " with command Mgr " + CommandManager.getInstance());
    } catch (ClassNotFoundException cnfex) {
      throw new TestException(TestHelper.getStackTrace(cnfex));
    } catch (IOException ioex) {
      throw new TestException(TestHelper.getStackTrace(ioex));
    }
    try {
      shell.addChars(command).addChars(";").newline();
    } catch (IOException ioex) {
      throw new TestException(TestHelper.getStackTrace(ioex));
    }
    shell.waitForOutput();

    return extractCommandResult(shell.getCommandOutput());
  }

  /**
   * Utility method for viewing the results of a command.
   * 
   * @param commandResult
   *          Results to dump
   * @param printStream
   *          Stream to dump the results to
   */
  protected void printResult(final CommandResult commandResult, PrintStream printStream) {
    assert(commandResult != null);
    assert(printStream != null);

    commandResult.resetToFirstLine();
    printStream.print(commandResultToString(commandResult));
  }

  protected String commandResultToString(final CommandResult commandResult) {
    assertNotNull(commandResult);

    commandResult.resetToFirstLine();

    StringBuilder buffer = new StringBuilder(commandResult.getHeader());

    while (commandResult.hasNextLine()) {
      buffer.append(commandResult.nextLine());
    }

    buffer.append(commandResult.getFooter());

    return buffer.toString();
  }

  /**
   * Utility method for finding the CommandResult object in the Map of CommandOutput objects.
   * 
   * @param commandOutput
   *          CommandOutput Map to search
   * @return The CommandResult object or null if not found.
   */
  protected CommandResult extractCommandResult(Map<String, Object> commandOutput) {
    assert(commandOutput != null);

    for (Object resultObject : commandOutput.values()) {
      if (resultObject instanceof CommandResult) {
        CommandResult result = (CommandResult) resultObject;
        result.resetToFirstLine();
        return result;
      }
    }
    return null;
  }

  /**
   * Utility method to determine how many times a string occurs in another string. Note that when looking for matches
   * substrings of other matches will be counted as a match. For example, looking for "AA" in the string "AAAA" will
   * result in a return value of 3.
   * 
   * @param stringToSearch
   *          String to search
   * @param stringToCount
   *          String to look for and count
   * @return The number of matches.
   */
  protected int countMatchesInString(final String stringToSearch, final String stringToCount) {
    assert(stringToSearch != null);
    assert(stringToCount != null);
    
    int length = stringToSearch.length();
    int count = 0;
    for (int i = 0; i < length; i++) {
      if (stringToSearch.substring(i).startsWith(stringToCount)) {
        count++;
      }
    }
    return count;
  }

  /**
   * Determines if a string contains a trimmed line that matches the pattern. So, any single line whose leading and
   * trailing spaces have been removed which contains a string that exactly matches the given pattern will be considered
   * a match.
   * 
   * @param stringToSearch
   *          String to search
   * @param stringPattern
   *          Pattern to search for
   * @return True if a match is found, false otherwise
   */
  protected boolean stringContainsLine(final String stringToSearch, final String stringPattern) {
    assert(stringToSearch != null);
    assert(stringPattern != null);
    
    Pattern pattern = Pattern.compile("^\\s*" + stringPattern + "\\s*$", Pattern.MULTILINE);
    Matcher matcher = pattern.matcher(stringToSearch);
    return matcher.find();
  }

  /**
   * Counts the number of distinct lines in a String.
   * 
   * @param stringToSearch
   *          String to search for lines.
   * @param countBlankLines
   *          Whether to count blank lines (true to count)
   * @return The number of lines found.
   */
  protected int countLinesInString(final String stringToSearch, final boolean countBlankLines) {
    assert(stringToSearch != null);
    
    int length = stringToSearch.length();
    int count = 0;
    char character = 0;
    boolean foundNonSpaceChar = false;

    for (int i = 0; i < length; i++) {
      character = stringToSearch.charAt(i);
      if (character == '\n' || character == '\r') {
        if (countBlankLines) {
          count++;
        } else {
          if (foundNonSpaceChar) {
            count++;
          }
        }
        foundNonSpaceChar = false;
      } else if (character != ' ' && character != '\t') {
        foundNonSpaceChar = true;
      }
    }

    // Even if the last line isn't terminated, it still counts as a line
    if (character != '\n' && character != '\r') {
      count++;
    }

    return count;
  }

  /** 
   * Get a specific line from the string (using \n or \r as a line separator).
   * 
   * @param stringToSearch String to get the line from
   * @param lineNumber Line number to get
   * @return The line
   */
  protected String getLineFromString(final String stringToSearch, final int lineNumber) {
    assert(stringToSearch != null);
    assert(lineNumber > 0);
    
    int length = stringToSearch.length();
    int count = 0;
    int startIndex = 0;
    char character;
    int endIndex = length;

    for (int i = 0; i < length; i++) {
      character = stringToSearch.charAt(i);
      if (character == '\n' || character == '\r') {
        if (lineNumber == 1) {
          endIndex = i;
          break;
        }
        if (++count == lineNumber-1) {
          startIndex = i+1;
        } else if (count >= lineNumber) {
          endIndex = i;
          break;
        }
      }
    }

    return stringToSearch.substring(startIndex, endIndex);
  }
  
  protected void info(String string) {
    getLogWriter().info(string);
  }

  protected void debug(String string) {
    getLogWriter().fine(string);
  }

  protected void error(String string) {
    getLogWriter().error(string);
  }

  protected void error(String string, Throwable e) {
    getLogWriter().error(string, e);
  }
}
