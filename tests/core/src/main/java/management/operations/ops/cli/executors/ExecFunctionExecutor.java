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
import java.util.Set;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MalformedObjectNameException;
import javax.management.ReflectionException;

import management.cli.CLIBlackboard;
import management.cli.TestableGfsh;
import management.operations.ops.cli.TestCommand.CommandOption;
import management.operations.ops.cli.TestCommandInstance;
import management.util.HydraUtil;
import management.util.ManagementUtil;
import util.TestException;

import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;

public class ExecFunctionExecutor extends AbstractTestCommandExecutor {
  
  /*
  Steps
    1. List down all command modes  :Done
    2. Check command generated with dummy executeMethod : Done
    3. Run mini-regression to testout all modes execute correctly : Done
    4. Add command output validators : DONE
    5. Add bad-inputs for mode : ***Remaining***
    6. Add bad-combinations for mode : ***Remaining***
 */
  
  
  @Override
  public Object verifyCommand(TestableGfsh gfsh, Object object) {
    //Validation only for group and member not for with-region mode
    if(this.member!=null || this.group!=null){
      List<String> memberList =CLIBlackboard.getBB().getList(CLIBlackboard.FUNCTION_EXEC_LIST);
      HydraUtil.logInfo("Member Execution List " + memberList);
      if(this.group!=null){
        try {
          Set<String> members = ManagementUtil.getMembersForGroup(group);
          
          for(String m : members){
            boolean flag = false;
            for(String minList : memberList){
              if(minList.equals(m))
                flag =true;
            }
            if(!flag)
              addFailure("Member " + m + " is not recorded in Blackboard for functionExecution. List " + memberList);
          }
        } catch (MalformedObjectNameException e) {
          throw new TestException("Error while getting member for group", e);
        } catch (InstanceNotFoundException e) {
          throw new TestException("Error while getting member for group", e);
        } catch (AttributeNotFoundException e) {
          throw new TestException("Error while getting member for group", e);
        } catch (NullPointerException e) {
          throw new TestException("Error while getting member for group", e);
        } catch (MBeanException e) {
          throw new TestException("Error while getting member for group", e);
        } catch (ReflectionException e) {
          throw new TestException("Error while getting member for group", e);
        }
      }else{
        String member = memberList.get(0);
        if(!member.equals(this.member)){
          addFailure("Blackboard list indicated different members " + memberList + " expected : " + this.member);
        }
      }
      this.member=null;
      this.group=null;
    }
    return null;
  }

  @Override
  protected void fillOption(TestCommandInstance instance, CommandOption op) {
    
  }

  public Object executeAndVerify(TestCommandInstance instance) {
    CLIBlackboard.getBB().clearList(CLIBlackboard.FUNCTION_EXEC_LIST);
    return super.executeAndVerify(instance);
  }
  
  private String member=null;
  private String group=null;

  @Override
  protected void fillMandatoryOption(TestCommandInstance instance, String name) {
    if(CliStrings.EXECUTE_FUNCTION__ID.equals(name)){
      //instance.addOption(name, getFunctionId());
      instance.addOption(name, instance.getMode());
      return;
    }
    
    if(CliStrings.EXECUTE_FUNCTION__ONGROUP.equals(name)){
      this.group = getGroup();
      instance.addOption(name, group);
      return;
    }
    
    if(CliStrings.EXECUTE_FUNCTION__ONMEMBER.equals(name)){
      this.member = getMemberId();
      instance.addOption(name, member);
      return;
    }
    
    if(CliStrings.EXECUTE_FUNCTION__ONREGION.equals(name)){
      instance.addOption(name, getRegion());
      return;
    }

    
    if(CliStrings.EXECUTE_FUNCTION__ARGUMENTS.equals(name)){
      instance.addOption(name, instance.getMode());
      return;
    } 
    
    if(CliStrings.EXECUTE_FUNCTION__FILTER.equals(name)){
      instance.addOption(name, "KEY*"); 
      return;
    }
    
    if(CliStrings.EXECUTE_FUNCTION__RESULTCOLLECTOR.equals(name)){
      //instance.addOption(name, FunctionOperations.CustomResultCollector.class.getCanonicalName());
      instance.addOption(name, "management.operations.ops.FunctionOperations$CustomResultCollector");
      //Bug 46167
      return;
    } 
  }
  
  @Override
  protected void fillArgument(TestCommandInstance instance, String name) {
    //NOOP    
  }


}
