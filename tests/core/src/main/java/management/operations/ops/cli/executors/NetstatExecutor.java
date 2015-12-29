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

import java.util.Collection;
import java.util.Map;

import util.TestException;

import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;

import management.cli.CommandOutputValidator;
import management.cli.TestableGfsh;
import management.cli.CommandOutputValidator.CommandOutputValidatorResult;
import management.operations.ops.cli.TestCommand.CommandOption;
import management.operations.ops.cli.TestCommandInstance;
import management.test.cli.CLITest;
import management.util.HydraUtil;

public class NetstatExecutor extends AbstractTestCommandExecutor {
  
  
  /*
  Steps
    1. List down all command modes  :Done
    2. Check command generated with dummy executeMethod : Done
    3. Run mini-regression to testout all modes execute correctly : Done
    4. Add command output validators : Done
    5. Add bad-inputs for mode : ***Remaining***
    6. Add bad-combinations for mode : ***Remaining***
 */

  //TODO : Add validations : contains ls-of output. output contains all hosts etc.
  /*@Override
  public Object verifyCommand() {
    // TODO Auto-generated method stub
    return null;
  }*/
  
  /*
  @Override
  public Object executeAndVerify(TestCommandInstance instance) {
    HydraUtil.logInfo("Executed command : " + instance.toString());
    return null;
  }*/
  
  @Override
  public Object executeAndVerify(TestCommandInstance instance) {
    TestableGfsh gfsh = CLITest.getTestableShell();
    if(!gfsh.isConnectedAndReady()){
      CLITest.connectGfshToManagerNode();
    }    
    Object object[] = TestableGfsh.execAndLogCommand(gfsh, instance.toString(), CLITest.getGfshOutputFile(), true);    
    Map map = (Map) object[0];
    CommandResult result =null;
    Collection values = map.values();
    for(Object r : values){
      if(r instanceof CommandResult){
        result = (CommandResult) r;      
        if(!result.getStatus().equals(Result.Status.OK))
          throw new TestException("Command return status is *NOT* OK. Command execution has failed");
        else
          HydraUtil.logInfo("Completed exeuction of <" + instance + "> successfully");         
      }
    }    
    verifyGemfire(gfsh,object);verifyJMX(gfsh,object);verifyCommand(gfsh,object);    
    return result;   
  }
  
  @Override
  public Object verifyCommand(TestableGfsh gfsh, Object object) {
    Object[] outputs = (Object[])object;
    CommandOutputValidator validator = new CommandOutputValidator(gfsh, outputs);
    validator.addUnExpectedErrorString(CliStrings.NETSTAT__MSG__ERROR_OCCURRED_WHILE_EXECUTING_NETSTAT_ON_0)
    .addUnExpectedErrorString(CliStrings.NETSTAT__MSG__COULD_NOT_FIND_MEMBERS_0)
    .addUnExpectedErrorString(CliStrings.NETSTAT__MSG__ONLY_ONE_OF_MEMBER_OR_GROUP_SHOULD_BE_SPECIFIED);
    
    /* Fix for Bug 46102 : Result object does not contain message NETSTAT__MSG__SAVED_OUTPUT_IN_0  as 
       string field as byte array and FileData as byte array 
       The message is added on the client side after consuming file data on the output.
    if(fileName!=null){
      validator.addExpectedString(CliStrings.NETSTAT__MSG__SAVED_OUTPUT_IN_0);
    }*/
    
    CommandOutputValidatorResult result = validator.validate();
    if(!result.result){
      //throw new TestException(result.getExceptionMessage());
      addFailure(" Command Failed validation : " + result.getExceptionMessage());
      //CLITest.currentCommand.append(" Command Failed validation : " + result.getExceptionMessage());
      //CLITest.hasCommandFailed = true;
    }
    if(fileName!=null && !checkFile(fileName)){
      //throw new TestException("Netstat command failed to create file named "  +fileName);
      addFailure(" Command Failed validation : " + "Netstat command failed to create file named "  +fileName);
      //CLITest.currentCommand.append(" Command Failed validation : " + "Netstat command failed to create file named "  +fileName);
      //CLITest.hasCommandFailed = true;
    }
    fileName = null;
    return result;
  }


  @Override
  protected void fillOption(TestCommandInstance instance, CommandOption op) {
    //NOOP
  }

  private String fileName = null;
  @Override
  protected void fillMandatoryOption(TestCommandInstance instance, String name) {
    /*if (CliStrings.NETSTAT__MEMBER.equals(name)) {
      instance.addOption(name, getMemberId());
    } else*/ if (CliStrings.NETSTAT__GROUP.equals(name)) {
      instance.addOption(name, getGroup());
    } else if (CliStrings.NETSTAT__FILE.equals(name)) {
      fileName = getFile(instance.getMode(),FILE_TYPE_TEXT);
      instance.addOption(name,fileName);
    } else if (CliStrings.NETSTAT__WITHLSOF.equals(name)) {
      instance.addOption(name, "true");
    }else if(name.equals(CliStrings.NETSTAT__MEMBER)){
      instance.addOption(name,getMemberId());
    }
  }  

  @Override
  protected void fillArgument(TestCommandInstance instance, String name) {
    
  }

}
