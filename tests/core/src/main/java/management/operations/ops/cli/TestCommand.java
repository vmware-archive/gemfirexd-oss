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

import static management.util.HydraUtil.logInfo;
import static management.util.HydraUtil.logFine;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import management.operations.ops.CLIOperations;
import management.util.HydraUtil;
import util.TestException;

public class TestCommand implements Serializable {

  public String command;
  public String executorName;
  public List<String> arguments;
  public List<String> mandatoryOptions;
  public List<CommandOption> options;
  public Map<String, CommandMode> modes;
  static Random random = new Random();

  public static class CommandMode implements Serializable {
    public String name;
    public List<String> arguments;
    public List<String> mandatoryOptions;
    public List<CommandOption> options;
    public int probability = 50;
  }

  public static class CommandOption implements Serializable {
    public String name;
    public int probability = 50;
    public List<String> linkedSet;
    
    public String toString(){
      return " CO [ name : " + name + " p : " + probability + " linkedoptions : " + (linkedSet==null? null : linkedSet.toString());      
    }
  }

  /**
   * This method returns an instance of command with randomly selected mode of
   * operation and randomly selected options. See
   * gfshCommandDescriptorGrammar.txt to effectively use it for the type of
   * command to generate.
   * 
   * @return TestCommandInstance
   */
  public TestCommandInstance getRandomCommandInstnance() {
    TestCommandInstance instance = new TestCommandInstance();
    instance.testCommand = this;

    int numMode = this.modes.size();
    int randomMode = random.nextInt(numMode);
    CommandMode mode = null;
    Set<String> keys = modes.keySet();
    Iterator<String> keyIterator = keys.iterator();
    for (int i = 0;; i++) {
      if (keyIterator.hasNext()) {
        String key = keyIterator.next();
        if (i == randomMode) {
          mode = modes.get(key);
          logInfo(CLIOperations.opPrefix + "For command " + this.command + " mode selected is " + key);
          break;
        }
      } else
        break;
    }

    if (mode == null)
      throw new TestException("Could not get command mode modes " + modes.size() + " rseed " + numMode);

    List<String> arguments = new ArrayList<String>();
    arguments.addAll(this.arguments); //arguments are ordered add first default args then mode args
    if (mode.arguments != null)
      arguments.addAll(mode.arguments);
    

    List<String> mandatoryOptions = new ArrayList<String>();
    mandatoryOptions.addAll(mode.mandatoryOptions);
    mandatoryOptions.addAll(this.mandatoryOptions);

    List<CommandOption> options = new ArrayList<CommandOption>();
    options.addAll(addOptions(mode.options));
    options.addAll(addOptions(this.options));
    
    TestCommandExecutor executor = getExecutor();
    logFine("Creating a instance iwth options " + HydraUtil.ObjectToString(options));
    logFine("Creating a instance iwth moptions " + HydraUtil.ObjectToString(mandatoryOptions));
    logFine("Creating a instance iwth arguments " + HydraUtil.ObjectToString(arguments));
    instance.setMode(mode.name);
    /*if(mode.arguments!=null){
      for(String arg : mode.arguments)
        instance.addArgument(arg);
    }*/
    executor.fillMandotoryOptionValues(instance, mandatoryOptions);
    executor.fillOptionValues(instance, options);
    executor.fillArguments(instance, arguments);
    return instance;
  }

  private Collection<? extends CommandOption> addOptions(List<CommandOption> options2) {
    List<CommandOption> options = new ArrayList<CommandOption>();
    Set<String> linkedOptions = new HashSet<String>();
    for (CommandOption op : options2) {
      if (!linkedOptions.contains(op.name)) {// if already added due to linked
                                             // to previously added option
        int r = random.nextInt(101);
        if (r <= op.probability)
          options.add(op);
        if (op.linkedSet != null && op.linkedSet.size() > 0) {
          for (String s : op.linkedSet) {
            linkedOptions.add(s);
          }
        }
      }
    }
    for (String s : linkedOptions) {
      for (CommandOption op : options2) {
        if (op.name.equals(s))
          options.add(op);
      }
    }
    return options;
  }

  private TestCommandExecutor executor = null;
  
  public TestCommandExecutor getExecutor() {
    String klass = this.executorName;
    HydraUtil.logFine("Executor " + executor + " command "  + this);
    if(executor==null){
      this.executor = (TestCommandExecutor) HydraUtil.getInstanceOfClass(klass);
    }
    HydraUtil.logFine("Executor " + executor + " command "  + this);
    return executor;
  }

}