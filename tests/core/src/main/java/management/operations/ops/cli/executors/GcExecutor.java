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

import management.cli.CommandOutputValidator;
import management.cli.CommandOutputValidator.CommandOutputValidatorResult;
import management.cli.TestableGfsh;
import management.operations.ops.cli.TestCommand.CommandOption;
import management.operations.ops.cli.TestCommandInstance;
import util.TestException;

import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;

public class GcExecutor extends AbstractTestCommandExecutor {

  
  /*
  Steps
    1. List down all command modes  :Done
    2. Check command generated with dummy executeMethod : Done
    3. Run mini-regression to testout all modes execute correctly : Done
    4. Add command output validators : Done
    5. Add bad-inputs for mode : ***Remaining***
    6. Add bad-combinations for mode : ***Remaining***
 */
  
  
  @Override
  public Object verifyCommand(TestableGfsh gfsh, Object object) {
    Object[] outputs = (Object[])object;
    CommandOutputValidator validator = new CommandOutputValidator(gfsh, outputs);
    validator.addUnExpectedErrorString(CliStrings.GC__MSG__CANNOT_EXECUTE)
      .addUnExpectedErrorString(CliStrings.GC__MSG__MEMBER_NOT_FOUND)
      .addUnExpectedErrorString("No member found for executing function");
    CommandOutputValidatorResult result = validator.validate();
    if(!result.result){
      throw new TestException(result.getExceptionMessage());
    }
    return result;
  }

 

  @Override
  protected void fillMandatoryOption(TestCommandInstance instance, String name) {
    if(CliStrings.GC__MEMBER.equals(name)){
      instance.addOption(name, getMemberId());
    }
    else if(CliStrings.GC__GROUP.equals(name)){
      instance.addOption(name, getGroup());
    }
  }
  
  /*
  @Override
  public Object executeAndVerify(TestCommandInstance instance) {
    HydraUtil.logInfo("Executed command : " + instance.toString());
    return null;
  }*/
  
  @Override
  protected void fillArgument(TestCommandInstance instance, String name) {
    //NOOP    
  }
  
  @Override
  protected void fillOption(TestCommandInstance instance, CommandOption op) {
   //No non-mandatory options 
  }


}
