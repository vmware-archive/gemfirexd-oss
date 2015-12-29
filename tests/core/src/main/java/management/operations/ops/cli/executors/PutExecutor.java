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

import management.operations.ops.cli.TestCommand.CommandOption;
import management.operations.ops.cli.TestCommandInstance;
import management.test.cli.CLITest;
import management.util.HydraUtil;
import util.TestException;

import com.gemstone.gemfire.management.internal.cli.dto.Key1;
import com.gemstone.gemfire.management.internal.cli.dto.Value2;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;

public class PutExecutor extends AbstractTestCommandExecutor {
  
  
  
  /*
  Steps
    1. List down all command modes  :Done
    2. Check command generated with dummy executeMethod : Done
    3. Run mini-regression to testout all modes execute correctly : Done
    4. Add command output validators : ***Remaining***
    5. Add bad-inputs for mode : ***Remaining***
    6. Add bad-combinations for mode : ***Remaining***
 */
  
  
  private static final String keyTemplate = "('id':'?','name':'name?')";
  private static final String valueTemplate = "('stateName':'State?','population':?1,'capitalCity':'capital?','areaInSqKm':?2)";
  
  //TODO :ADD command validations
  /*
  @Override
  public Object verifyCommand() {
    // TODO Auto-generated method stub
    return null;
  }*/

  protected void fillOption(TestCommandInstance instance, CommandOption op) {   
    if(CliStrings.PUT__PUTIFABSENT.equals(op.name)){
      instance.addOption(op.name, "true");
      return;
    }    
    throw new TestException("Unknown option " + op.name);    
  }

  protected void fillMandatoryOption(TestCommandInstance instance,String name) {
    int keyIndex = HydraUtil.getnextRandomInt(1000);
    String keyPrefix = "KEY_" + keyIndex;
    if(instance.getMode().contains("json")){
      if(CliStrings.PUT__KEY.equals(name)){
        String keyString = keyPrefix ;        
        String keyJson = keyTemplate.replaceAll("\\?", keyString);
        instance.addOption(name, keyJson);
        return;
      }else if(CliStrings.PUT__REGIONNAME.equals(name)){
        CLITest.useRegion(instance,name, CLITest.getJSONRegion());
        return;
      }else if(CliStrings.PUT__VALUE.equals(name)){
        String keyString = keyPrefix ;  
        String population = ""+keyIndex*100;
        String area = ""+keyIndex*(100.4365);        
        String valueJson = valueTemplate.replaceAll("\\?1", population);
        valueJson = valueJson.replaceAll("\\?2", area);
        valueJson = valueJson.replaceAll("\\?", keyString);
        instance.addOption(name,valueJson);
        return;
      }else if(CliStrings.GET__KEYCLASS.equals(name)){
        instance.addOption(name, Key1.class.getCanonicalName());
        return;
      }else if(CliStrings.GET__VALUEKLASS.equals(name)){
        instance.addOption(name, Value2.class.getCanonicalName());
        return;
      }      
    }    
    else{
      if(CliStrings.PUT__KEY.equals(name)){
        instance.addOption(name, keyPrefix);
        return;
      }else if(CliStrings.PUT__REGIONNAME.equals(name)){
        CLITest.useRegion(instance,name, CLITest.getStringRegion());
        return;
      }else if(CliStrings.PUT__VALUE.equals(name)){
        instance.addOption(name, "VALUE_"+keyIndex);
        return;
      } else if(CliStrings.GET__KEYCLASS.equals(name)){
        throw new TestException("No key-class for this mode test setup check again");  
      }else if(CliStrings.GET__VALUEKLASS.equals(name)){
        throw new TestException("No value-class for this mode test setup check again");  
      } 
    }    
    throw new TestException("Unknown option " + name);
  }  
  
  @Override
  protected void fillArgument(TestCommandInstance instance, String name) {
    //NOOP    
  }


}
