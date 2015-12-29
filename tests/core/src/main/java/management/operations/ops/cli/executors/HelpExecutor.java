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
package management.operations.ops.cli.executors;

import java.util.List;

import management.cli.TestableGfsh;
import management.operations.ops.cli.TestCommand.CommandOption;
import management.operations.ops.cli.TestCommandExecutor;
import management.operations.ops.cli.TestCommand;
import management.operations.ops.cli.TestCommandInstance;

public class HelpExecutor implements TestCommandExecutor {

  @Override
  public Object executeAndVerify(TestCommandInstance instance) {
    // TODO Auto-generated method stub
    return null;
  }

  //TODO ADd validations
  /*
  @Override
  public Object verifyCommand() {
    // TODO Auto-generated method stub
    return null;
  }*/

  @Override
  public void fillOptionValues(TestCommandInstance instance,
      List<CommandOption> options) {
    for (CommandOption op : options) {
      instance.addOption(op.name, "<dummy value>");
    }

  }

  @Override
  public void fillMandotoryOptionValues(TestCommandInstance instance,
      List<String> mandotoryCommandOptions) {
    for (String op : mandotoryCommandOptions) {
      instance.addMandatoryOption(op, "<dummy value>");
    }
  }
  
  @Override
  public void setUpGemfire() {
    // TODO Auto-generated method stub
    
  }
  
  @Override
  public void fillArguments(TestCommandInstance instance, List<String> args) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public Object execute(TestCommandInstance instance) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Object verifyGemfire(TestableGfsh gfsh, Object object) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Object verifyJMX(TestableGfsh gfsh, Object object) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Object verifyCommand(TestableGfsh gfsh, Object object) {
    // TODO Auto-generated method stub
    return null;
  }

}
