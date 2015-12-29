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
package util;

import hydra.DistributedSystemHelper;
import hydra.Log;
import hydra.blackboard.SharedCounters;
import hydra.blackboard.SharedMap;
import management.test.cli.CommandTest;

import com.gemstone.gemfire.distributed.DistributedLockService;

/** Class to invoke gfsh commands in a remote jvm that is not a part of the
 *  distributed system.
 *  
 * @author lynng
 *
 */
public class CliHelper {
  
  // lock objects to ensure only one command is run at a time
  protected static DistributedLockService lockService; // the distributed lock service for this jvm
  protected static final String LOCK_SERVICE_NAME = "CliLockService";
  protected static final String lockObj = "cliLockObject";
  
  // blackboard keys
  protected static final String COMMAND_KEY = "CliHelper_command";
  protected static final String HALT_KEY = "CliHelper_halt";
  protected static final String COMMAND_OUTPUT_TEXT_KEY = "CliHelper_commandOutputText";
  protected static final String PRESENTATION_STR_KEY = "CliHelper_presentationStr";
  
  // instructions to initialize a new cli session
  public static final String START_NEW_CLI = "StartNewCLI";
  public static final String CONNECT_CLI = "ConnectCLI";
  
  /** Execute the given command either in this member or a remote member depending on the value
   *  of CliHelperPrms.executeRemote.
   * 
   * @param command The command to execute.
   * @param haltOnCommandFailure If false, then allow the method to fail when the command fails.
   *                             if true, then expect the method to succeed when, for example, the test gave the
   *                             command bad arguments intentionally.
   * @return [0] The TestableShell.getOutputText() result of the command.
   *         [1] The presentation string (the output text of the command that is presented to the user).
   */
  public static synchronized String[] execCommand(String command, boolean haltOnCommandFailure) {
    if (CliHelperPrms.getExecuteRemote()) {
      return execCommandOnRemoteCli(command, haltOnCommandFailure);
    } else {
      return execCommandLocally(command, haltOnCommandFailure);
    }
  }
  
  /** Execute the given command in this member
   * 
   * @param command The command to execute.
   * @param haltOnCommandFailure If false, then allow the method to fail when the command fails.
   *                             if true, then expect the method to succeed when, for example, the test gave the
   *                             command bad arguments intentionally.
   * @return [0] The TestableShell.getOutputText() result of the command.
   *         [1] The presentation string (the output text of the command that is presented to the user).
   */
  public static synchronized String[] execCommandLocally(String command, boolean haltOnCommandFailure) {
    if (command.equals(START_NEW_CLI)) { // initialize a new cli instance
      Log.getLogWriter().info("Directive to start a new cli instance...");
      CommandTest.testInstance.shell = null;
      CommandTest.testInstance.initShell();
      return new String[] {"", ""};
    } else if (command.equals(CONNECT_CLI)) {
      Log.getLogWriter().info("Directive to connect cli...");
      return CommandTest.connectCLI();
    } else { // is a real cli command rather than a directive to this method
      return CommandTest.testInstance.execCommand(command, haltOnCommandFailure);
    }
  }
  
  /** Execute the given command in another member running gfsh. The other member
   *  must be checking the blackboard to know to execute the command. This method
   *  ensures that only one command will be run at a time even if it is called
   *  concurrently.
   * 
   * @param command The command to execute.
   * @param haltOnCommandFailure If false, then allow the method to fail when the command fails.
   *                             if true, then expect the method to succeed when, for example, the test gave the
   *                             command bad arguments intentionally.
   * @return [0] The TestableShell.getOutputText() result of the command.
   *         [1] The presentation string (the output text of the command that is presented to the user).
   */
  public static synchronized String[] execCommandOnRemoteCli(String command, boolean haltOnCommandFailure) {
    if (lockService == null) {
      Log.getLogWriter().info("Creating lock service " + LOCK_SERVICE_NAME);
      lockService = DistributedLockService.create(LOCK_SERVICE_NAME, DistributedSystemHelper.getDistributedSystem());
      Log.getLogWriter().info("Created lock service " + LOCK_SERVICE_NAME);
    }
    try {
      lockService.lock(lockObj, -1, -1); // make sure only one command runs at a time
      Log.getLogWriter().info("Requesting command " + command + " be executed on remote cli jvm");
      SharedMap sharedMap = CliBB.getBB().getSharedMap();
      SharedCounters sharedCounters = CliBB.getBB().getSharedCounters();
      sharedCounters.zero(CliBB.commandComplete);
      sharedMap.put(COMMAND_KEY, command);
      sharedMap.put(HALT_KEY, haltOnCommandFailure);
      sharedCounters.increment(CliBB.commandWaiting);
      TestHelper.waitForCounter(CliBB.getBB(), "commandComplete", CliBB.commandComplete, 1, true, -1, 250);
      String outputText = (String) sharedMap.get(COMMAND_OUTPUT_TEXT_KEY);
      String presentationStr = (String) sharedMap.get(PRESENTATION_STR_KEY);
      return new String[] {outputText, presentationStr};
    } finally {
      lockService.unlock(lockObj);
    }
  }
  
  /** Execute a command in this member running gfsh. The command is on the blackboard. This
   *  can be called repeatedly to poll for a command waiting to execute.
   * 
   * @return [0] The TestableShell.getOutputText() result of the command.
   *         [1] The presentation string (the output text of the command that is presented to the user).
   */
  public static synchronized void HydraTask_execCommandFromBB() {
    try {
      TestHelper.waitForCounter(CliBB.getBB(), "commandWaiting", CliBB.commandWaiting, 1, true, 30000, 500);
    } catch (TestException e) { // no command to execute
      return;
    }
    SharedCounters sharedCounters = CliBB.getBB().getSharedCounters();
    String command = (String)CliBB.getBB().getSharedMap().get(COMMAND_KEY);
    Boolean halt = (Boolean)CliBB.getBB().getSharedMap().get(HALT_KEY);
    Log.getLogWriter().info("Obtained command " + command + " with halt " + halt);
    String[] result = execCommandLocally(command, halt);
    SharedMap sharedMap = CliBB.getBB().getSharedMap();
    sharedMap.put(COMMAND_OUTPUT_TEXT_KEY, result[0]);
    sharedMap.put(PRESENTATION_STR_KEY, result[1]);
    sharedCounters.zero(CliBB.commandWaiting);
    sharedCounters.increment(CliBB.commandComplete);
  }

}
