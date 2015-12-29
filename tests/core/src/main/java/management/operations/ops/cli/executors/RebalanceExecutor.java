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

import hydra.CacheHelper;
import hydra.TestConfig;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import management.cli.TestableGfsh;
import management.operations.events.impl.RegionEvents;
import management.operations.ops.EntryOperations;
import management.operations.ops.cli.TestCommand.CommandOption;
import management.operations.ops.cli.TestCommandExecutor;
import management.operations.ops.cli.TestCommandInstance;
import management.test.cli.CLITest;
import management.util.HydraUtil;
import util.TestException;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;

public class RebalanceExecutor extends AbstractTestCommandExecutor {
  
  
  /*
  Steps
    1. List down all command modes  :Done
    2. Check command generated with dummy executeMethod : Done
    3. Run mini-regression to testout all modes execute correctly : Done
    4. Add command output validators : ***Remaining***
    5. Add bad-inputs for mode : ***Remaining***
    6. Add bad-combinations for mode : ***Remaining***
 */
  

  //TODO Add validations that command has indeed done moving of buckets
  /*
  @Override
  public Object verifyCommand() {
    // TODO Auto-generated method stub
    return null;
  }*/

    protected void fillOption(TestCommandInstance instance, CommandOption op) {
    
    if(CliStrings.REBALANCE__SIMULATE.equals(op.name)){
      instance.addOption(op.name, "true");
      return;
    }
    
    if(CliStrings.REBALANCE__EXCLUDEREGION.equals(op.name)){
      instance.addOption(op.name, getExistingRegionList());
      return;
    }
    
    if(CliStrings.REBALANCE__INCLUDEREGION.equals(op.name)){
      instance.addOption(op.name, getExistingRegionList());
      return;
    }
    
    if(CliStrings.REBALANCE__TIMEOUT.equals(op.name)){
      instance.addOption(op.name, 5+TestConfig.tab().getRandGen().nextInt(15));//minimum 5 seconds
      return;
    }
    
    //instance.addOption(op.name, "<dummy-value>");
  }

  protected void fillMandatoryOption(TestCommandInstance instance,String name) {
    if(CliStrings.REBALANCE__SIMULATE.equals(name)){
      instance.addOption(name, "true");
      return;
    }
    
    if(CliStrings.REBALANCE__EXCLUDEREGION.equals(name)){
      instance.addOption(name, getExistingRegionList());
      return;
    }
    
    if(CliStrings.REBALANCE__INCLUDEREGION.equals(name)){
      instance.addOption(name, getExistingRegionList());
      return;
    }      
    
  }

  private Object getExistingRegionList() {
    StringBuilder sb = new StringBuilder();
    List<String> selectedList = new ArrayList<String>();
    List<String> regionList = RegionEvents.getAllRegions();
    for(int i=0;i<regionList.size();i++){
      int ijk = TestConfig.tab().getRandGen().nextInt(1000);
      if(ijk%2==0){
        selectedList.add(regionList.get(i));        
      }
    }
    if(selectedList.size()==0)
      selectedList.add(regionList.get(0));
    for(int i=0;i<selectedList.size();i++){
      sb.append(selectedList.get(i));
      if(i!=selectedList.size()-1)sb.append(",");
    }
    return sb.toString();
  }  
  
  /*
  @Override
  public void setUpGemfire() {
    HydraUtil.logInfo("Setting up data for rebalance command ");
    performEntryOperationsOnOneRegionOnly();
    HydraUtil.logInfo("Setting up data for rebalance command Completed");
  }*/
  
  private void performEntryOperationsOnOneRegionOnly(){
    List<String> regionList = RegionEvents.getAllRegions();
    HydraUtil.logInfo("Obtained region list " + regionList);
    int numRegionsToModify = TestConfig.tab().getRandGen().nextInt(regionList.size()-1);
    HydraUtil.logInfo("Performing entry operations on "  +numRegionsToModify + " regions");
    
    for(int i=0;i<numRegionsToModify;i++){
      int regionNum = TestConfig.tab().getRandGen().nextInt(regionList.size()-1);
      String regionPath = regionList.get(regionNum);
      HydraUtil.logInfo("Performing entry operations on "  +regionPath);
      Region region = CacheHelper.getCache().getRegion(regionPath);
      EntryOperations eOps = CLITest.getEntryOperations(regionPath);
      //Make configurable
      for(int j=0;j<1000;j++){
        eOps.doEntryOperation();      
      }
    }
  }
  
  @Override
  protected void fillArgument(TestCommandInstance instance, String name) {
    //NOOP    
  }


}
