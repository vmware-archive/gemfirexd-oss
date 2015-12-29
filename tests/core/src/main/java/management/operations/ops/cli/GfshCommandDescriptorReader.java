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
package management.operations.ops.cli;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import management.operations.ops.cli.TestCommand.CommandMode;
import management.operations.ops.cli.TestCommand.CommandOption;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import util.TestException;
import util.TestHelper;

/**
 * TODO
 * 
 * 1. Make it options more configurable add probability in command
 * descriptor so that options get tested with good probability instead of random
 * chances : DONE
 * 
 * 2. Add a way to specify inter-dependency on other options.
 * This will override probability. Also support illustrated usage : DONE
 * 
 * 3. Support -J type arguments
 * 
 * 4. Support attr=value typed properties
 * 
 * 5. Support Gfsh shell parameters for ex. when region name is not specified, it is taken
 * from Gfsh context (set using use region command)
 * 
 * 6. Support specifying values as a part of descriptor for options so as to control
 * command generation in more complete fashion
 * 
 * @author tushark
 */

public class GfshCommandDescriptorReader {

  public static final String TOKEN_EXECUTOR = "executor";
  public static final String TOKEN_COMMAND = "command";
  public static final String TOKEN_ARGUMENTS = "arguments";
  public static final String TOKEN_MANDATORY_OPTIONS = "mandatoryOptions";
  public static final String TOKEN_OPTIONS = "options";
  public static final String TOKEN_MODES = "modes";
  public static final String TOKEN_PROBABILITY = "probability";
  public static final String TOKEN_ALSO_INCLUDE = "alsoInclude";

  public static TestCommand parseCommandDescriptor(String commandDesc) {
    JSONObject json = null;

    TestCommand testCommand = null;
    try {
      json = new JSONObject(commandDesc);
      String command = json.getString(TOKEN_COMMAND);
      String executorName = null;
      try {
        executorName = json.getString(TOKEN_EXECUTOR);
      } catch (JSONException e) {
        throw new TestException("No executor for command " + command);
      }

      JSONArray arguments = json.getJSONArray(TOKEN_ARGUMENTS);
      JSONArray mandatoryOptions = json.getJSONArray(TOKEN_MANDATORY_OPTIONS);
      JSONArray options = json.getJSONArray(TOKEN_OPTIONS);
      JSONArray modes = json.getJSONArray(TOKEN_MODES);

      testCommand = new TestCommand();
      testCommand.command = command;
      testCommand.executorName = executorName;
      testCommand.arguments = readCommandArguments(arguments);
      testCommand.mandatoryOptions = readMandatoryOptions(mandatoryOptions);
      testCommand.options = readOptions(options);
      testCommand.modes = readModes(modes);
      return testCommand;

    } catch (JSONException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }

  private static Map<String, CommandMode> readModes(JSONArray jsonModes) throws JSONException {
    Map<String, CommandMode> modes = new HashMap<String, CommandMode>();
    for (int i = 0; i < jsonModes.length(); i++) {
      JSONObject jsonMode = (JSONObject) jsonModes.get(i);
      CommandMode mode = new CommandMode();
      mode.name = jsonMode.getString("name");
      mode.mandatoryOptions = readMandatoryOptions(jsonMode.getJSONArray(TOKEN_MANDATORY_OPTIONS));
      mode.options = readOptions(jsonMode.getJSONArray(TOKEN_OPTIONS));
      try{
        JSONArray jsonArguments = jsonMode.getJSONArray(TOKEN_ARGUMENTS);
        mode.arguments = readCommandArguments(jsonArguments);
      }catch(JSONException e){
        // ignore ("no arugments");
      }
      if (jsonMode.has(TOKEN_PROBABILITY))
        mode.probability = jsonMode.getInt(TOKEN_PROBABILITY);
      modes.put(mode.name, mode);
    }
    return modes;
  }

  private static List<CommandOption> readOptions(JSONArray jsonOptions) throws JSONException {
    List<CommandOption> options = new ArrayList<CommandOption>();
    for (int i = 0; i < jsonOptions.length(); i++) {
      JSONObject jsonOption = (JSONObject) jsonOptions.get(i);
      options.add(readOption(jsonOption));
    }
    return options;
  }

  private static List<String> readMandatoryOptions(JSONArray jSonmandatoryOptions) throws JSONException {
    List<String> mandatoryOptions = new ArrayList<String>();
    for (int i = 0; i < jSonmandatoryOptions.length(); i++) {
      String jsonArgument = (String) jSonmandatoryOptions.get(i);
      mandatoryOptions.add(jsonArgument);
    }
    return mandatoryOptions;
  }

  private static List<String> readCommandArguments(JSONArray jsonArguments) throws JSONException {
    List<String> arguments = new ArrayList<String>();
    for (int i = 0; i < jsonArguments.length(); i++) {
      String jsonArgument = (String) jsonArguments.get(i);
      arguments.add(jsonArgument);
    }
    return arguments;
  }

  private static CommandOption readOption(JSONObject jsonOption) throws JSONException {
    CommandOption option = new CommandOption();
    option.name = jsonOption.getString("name");
    if (jsonOption.has(TOKEN_PROBABILITY))
      option.probability = jsonOption.getInt(TOKEN_PROBABILITY);
    if (jsonOption.has(TOKEN_ALSO_INCLUDE)) {
      option.linkedSet = new ArrayList<String>();
      JSONArray jsonArray = jsonOption.getJSONArray(TOKEN_ALSO_INCLUDE);
      for (int i = 0; i < jsonArray.length(); i++) {
        String str = (String) jsonArray.get(i);
        option.linkedSet.add(str);
      }
    }
    return option;
  }

  public static void main(String[] args) throws Exception {

    File textFile = new File("/tushark1/code-checkout/checkout/mnm/mm_dev_Jan12/tests/management/test/gfshCommand.txt");
    BufferedReader reader = new BufferedReader(new FileReader(textFile));
    char bytes[] = new char[(int) textFile.length()];
    reader.read(bytes, 0, bytes.length);
    String commandString = new String(bytes);
    System.out.println("Input String : " + commandString);
    TestCommand command = parseCommandDescriptor(commandString);

    TestCommandInstance cmdInstance = command.getRandomCommandInstnance();

    System.out.println("Generate Commands 1 " + cmdInstance.toString());

    cmdInstance = command.getRandomCommandInstnance();
    System.out.println("Generate Commands 2 " + cmdInstance.toString());

    cmdInstance = command.getRandomCommandInstnance();
    System.out.println("Generate Commands 3 " + cmdInstance.toString());

    cmdInstance = command.getRandomCommandInstnance();
    System.out.println("Generate Commands 4 " + cmdInstance.toString());

    cmdInstance = command.getRandomCommandInstnance();
    System.out.println("Generate Commands 5 " + cmdInstance.toString());

    cmdInstance = command.getRandomCommandInstnance();
    System.out.println("Generate Commands 6 " + cmdInstance.toString());
  }

}
