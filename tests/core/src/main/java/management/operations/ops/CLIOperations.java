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
package management.operations.ops;

import static management.util.HydraUtil.logInfo;
import hydra.HydraVector;
import hydra.TestConfig;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import management.cli.CLIBlackboard;
import management.cli.GfshPrms;
import management.operations.ops.cli.GfshCommandDescriptorReader;
import management.operations.ops.cli.TestCommand;
import management.operations.ops.cli.TestCommandExecutor;
import management.operations.ops.cli.TestCommandInstance;
import management.test.cli.CLITest;
import management.util.HydraUtil;
import util.TestException;

/**
 * 
 * @author tushark
 * 
 */
public class CLIOperations {

  /*
   * public final static String FIRE_N_FORGET = "fireNforget";
   * public final static String FIRE_N_FORGET_IN_SEPARATE_PROCESS = "fireNforgetInSeparateProcess";
   */
  public final static String EXEC_COMMAND_VERIFY = "execCommandVerify";
  public final static String EXEC_SCRIPT_VERIFY = "execScriptVerify";
  public final static String EXEC_SCRIPT_VERIFY_IN_SEPARATE_PROCESS = "execScriptInSeparateProcessVerify";
  public final static String EXEC_SCENARIO = "scenario";

  /*
   * public final static int MODE_FIRE_N_FORGET = 0;
   * public final static int MODE_FIRE_N_FORGET_IN_SEPARATE_PROCESS = 1;
   */
  public final static int MODE_EXEC_COMMAND_VERIFY = 2;
  public final static int MODE_EXEC_SCRIPT_VERIFY = 5;
  public final static int MODE_EXEC_SCRIPT_VERIFY_IN_SEPARATE_PROCESS = 8;
  public final static int MODE_EXEC_SCENARIO = 11;

  public final static String opPrefix = "CLIOperations: ";
  
  private String selectedMode = null;
  private TestCommand selectedcommand = null;
  private TestCommandInstance selectedcommandMode = null;
  
  

  public TestCommandInstance getSelectedcommandMode() {
    return selectedcommandMode;
  }

  public String getSelectedMode() {
    return selectedMode;
  }

  public TestCommand getSelectedcommand() {
    return selectedcommand;
  }

  public static void HydraStartTask_ReadCommandSpecs() {

    HydraVector commandSpecList = TestConfig.tab().vecAt(GfshPrms.commandSpec);
    HydraVector cliList = TestConfig.tab().vecAt(GfshPrms.commandList);
    Iterator<String> iterator = commandSpecList.iterator();

    HashMap<String, TestCommand> commandMap = new HashMap<String, TestCommand>();
    while (iterator.hasNext()) {
      String commandSpec = iterator.next();
      commandSpec = commandSpec.replaceAll("'", "\"");
      logInfo("Command Descriptor : " + commandSpec);
      TestCommand testCommand = GfshCommandDescriptorReader.parseCommandDescriptor(commandSpec);
      commandMap.put(testCommand.command, testCommand);
    }

    logInfo("Command list is " + cliList);

    logInfo("Command map is " + commandMap);

    CLIBlackboard.getBB().saveMap(CLIBlackboard.COMMAND_MAP, commandMap);
  }
  
  public String selectMode(){
    String modeStr = TestConfig.tab().stringAt(GfshPrms.cliModes);
    return modeStr;
  }

  public void doCLIOperation() {

    String modeStr = selectMode();
    logInfo(opPrefix + " Selected mode " + modeStr);
    int mode = -1;

    if (EXEC_COMMAND_VERIFY.equals(modeStr))
      mode = MODE_EXEC_COMMAND_VERIFY;
    else if (EXEC_SCRIPT_VERIFY.equals(modeStr))
      mode = MODE_EXEC_SCRIPT_VERIFY;
    else if (EXEC_SCRIPT_VERIFY_IN_SEPARATE_PROCESS.equals(modeStr))
      mode = MODE_EXEC_SCRIPT_VERIFY_IN_SEPARATE_PROCESS;
    else if (EXEC_SCENARIO.equals(modeStr))
      mode = MODE_EXEC_SCENARIO;

    if (mode == -1)
      throw new TestException("Unknown mode selected " + modeStr);

    TestCommand testCommand = null;
    switch (mode) {
    case MODE_EXEC_COMMAND_VERIFY:
      if(selectedcommand==null)
        selectedcommand = selectCommand();
      executeAndVerifyCommand(mode, selectedcommand);
      break;
    case MODE_EXEC_SCRIPT_VERIFY:
      // List<TestCommand> script = generateScript();
      // executeScript(mode,script);
      throw new TestException(opPrefix + " MODE_EXEC_SCRIPT_VERIFY not implemented yet");
      // break;
    case MODE_EXEC_SCRIPT_VERIFY_IN_SEPARATE_PROCESS:
      throw new TestException(opPrefix + " MODE_EXEC_SCRIPT_VERIFY_IN_SEPARATE_PROCESS not implemented yet");
      // break;
    case MODE_EXEC_SCENARIO:
      throw new TestException(opPrefix + " MODE_EXEC_SCENARIO not implemented yet");
      // executeScenario();
      // break;
    default:
      throw new TestException("Unknown CLI mode of operation: " + mode);
    }
  }

  private void executeAndVerifyCommand(int mode, TestCommand testCommand) {
    TestCommandInstance instance = selectedcommandMode; //testCommand.getRandomCommandInstnance();
    TestCommandExecutor executor = testCommand.getExecutor();
    logInfo(opPrefix + " Executing command {<" + instance.toString() + ">}");
    StringBuilder sb = (StringBuilder) CLITest.commands.get();
    String command = instance.toString();
    command = command.replaceAll(HydraUtil.NEW_LINE,(HydraUtil.NEW_LINE+HydraUtil.TAB+HydraUtil.TAB ) );
    //sb.append(command).append(HydraUtil.NEW_LINE).append(HydraUtil.NEW_LINE);
    Object result = executor.executeAndVerify(instance);
    logInfo(opPrefix + " Successfully completed executing command {<" + instance.toString() + ">}");
  }

  public TestCommand selectCommand() {    
    String command = TestConfig.tab().stringAt(GfshPrms.cliCommands);
    logInfo(opPrefix + " Selected Command " + command);
    HashMap<String, TestCommand> commandMap = (HashMap<String, TestCommand>) CLIBlackboard.getBB().getMap(
        CLIBlackboard.COMMAND_MAP);
    if (commandMap.containsKey(command)) {
      TestCommand testCommand = commandMap.get(command);
      selectedcommandMode = testCommand.getRandomCommandInstnance();
      return testCommand;
    } else {
      throw new TestException("Command descriptor for " + command
          + " not found. Please check your hydra configurations.");
    }
  }

  private void executeScenario() {
    // TODO
  }

  private List<TestCommand> generateScript() {
    // TODO

    return null;
  }

  private void executeScript(int mode, List<TestCommand> script) {
    // TODO
  }

}