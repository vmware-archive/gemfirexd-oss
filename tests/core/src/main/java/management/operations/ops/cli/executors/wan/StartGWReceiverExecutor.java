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
package management.operations.ops.cli.executors.wan;

import management.cli.CommandOutputValidator;
import management.cli.CommandOutputValidator.CommandOutputValidatorResult;
import management.cli.TestableGfsh;
import management.operations.ops.cli.TestCommand.CommandOption;
import management.operations.ops.cli.TestCommandInstance;
import management.operations.ops.cli.executors.AbstractTestCommandExecutor;
import management.util.HydraUtil;
import util.TestException;

import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;

public class StartGWReceiverExecutor extends AbstractTestCommandExecutor {

  
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
  protected void fillMandatoryOption(TestCommandInstance instance, String name) {
    if(CliStrings.START_GATEWAYRECEIVER__GROUP.equals(name)){
      instance.addOption(name, getGroupsNoManaging());
    }else if(CliStrings.START_GATEWAYRECEIVER__MEMBER.equals(name)){
      instance.addOption(name, getMemberIdInDS());
    }
  }  
  
  @Override
  protected void fillArgument(TestCommandInstance instance, String name) {
    //NOOP    
  }
  
  @Override
  protected void fillOption(TestCommandInstance instance, CommandOption op) {
   //No non-mandatory options 
  }


}
