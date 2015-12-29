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

import java.io.Serializable;
import java.util.List;

import management.cli.TestableGfsh;
import management.operations.ops.cli.TestCommand.CommandOption;

public interface TestCommandExecutor extends Serializable {

  public void fillOptionValues(TestCommandInstance instance, List<CommandOption> options);
  
  public void fillArguments(TestCommandInstance instance, List<String> args);

  public void fillMandotoryOptionValues(TestCommandInstance instance, List<String> mandotoryCommandOptions);

  public Object execute(TestCommandInstance instance);

  public Object executeAndVerify(TestCommandInstance instance);

  /**
   * Mandate to implement validation using gemfire APIs
   * 
   * 
   */
  
  public void setUpGemfire();
  
  public Object verifyGemfire(TestableGfsh gfsh, Object object);

  /**
   * Mandate to implement validation using JMX APIs
   * 
   * 
   */
  public Object verifyJMX(TestableGfsh gfsh, Object object);

  /**
   * Mandate to implement validation using other commands
   * 
   * 
   */
  public Object verifyCommand(TestableGfsh gfsh, Object object);

}
