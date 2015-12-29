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

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import management.cli.TestableGfsh;
import management.operations.OperationsBlackboard;
import management.operations.events.impl.RegionEvents;
import management.operations.ops.cli.TestCommand.CommandOption;
import management.operations.ops.cli.TestCommandInstance;
import management.test.cli.CLITest;
import management.test.federation.FederationBlackboard;
import management.util.HydraUtil;
import management.util.ManagementUtil;
import util.TestException;

import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.CacheLoaderException;
import com.gemstone.gemfire.cache.CacheWriter;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.ExpirationAction;
import com.gemstone.gemfire.cache.LoaderHelper;
import com.gemstone.gemfire.cache.RegionEvent;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.cli.dto.Key1;
import com.gemstone.gemfire.management.internal.cli.dto.Value1;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;

public class DestroyRegionExecutor extends AbstractTestCommandExecutor {

  /*
  Steps
    1. List down all command modes  :Done
    2. Check command generated with dummy executeMethod : Done
    3. Run mini-regression to testout all modes execute correctly : Done
    4. Add command output validators : Done
    5. Add bad-inputs for mode : ***Remaining***
    6. Add bad-combinations for mode : ***Remaining***
 */
  
  /*
  @Override
  public Object verifyCommand() {
    // TODO Auto-generated method stub
    return null;
  }*/

  /*
  @Override
  public void setUpGemfire() {
    //Make sure here  that at least one function is registered so that it can be executed or destroyed    
  }*/

  private String regionPath = null;
  
  
  //Invalid Shortcuts
  //Valid shortcuts
  /*
    PARTITION
    PARTITION_REDUNDANT
    PARTITION_PERSISTENT
    PARTITION_REDUNDANT_PERSISTENT
    PARTITION_OVERFLOW
    PARTITION_REDUNDANT_OVERFLOW
    PARTITION_PERSISTENT_OVERFLOW
    PARTITION_REDUNDANT_PERSISTENT_OVERFLOW
    PARTITION_HEAP_LRU
    PARTITION_REDUNDANT_HEAP_LRU
    REPLICATE
    REPLICATE_PERSISTENT
    REPLICATE_OVERFLOW
    REPLICATE_PERSISTENT_OVERFLOW
    REPLICATE_HEAP_LRU
    LOCAL
    LOCAL_PERSISTENT
    LOCAL_HEAP_LRU
    LOCAL_OVERFLOW
    LOCAL_PERSISTENT_OVERFLOW
    PARTITION_PROXY
    PARTITION_PROXY_REDUNDANT
    REPLICATE_PROXY
  */
  @Override
  protected void fillArgument(TestCommandInstance instance, String name) {
    if(CliStrings.DESTROY_REGION__REGION.equals(name)){
      this.regionPath = getCurrentRegion();
      //instance.addArgument(regionPath);
      instance.addOption(name, regionPath);
    }
  }


  public Object executeAndVerify(TestCommandInstance instance) {    
    TestableGfsh gfsh = CLITest.getTestableShell();
    if(!gfsh.isConnectedAndReady()){
      CLITest.connectGfshToManagerNode();
    }    
    Object object[] = TestableGfsh.execAndLogCommand(gfsh, instance.toString(), CLITest.getGfshOutputFile(), false);
    Map map = (Map) object[0];
    CommandResult result =null;
    Collection values = map.values();
    for(Object r : values){
      if(r instanceof CommandResult){
        result = (CommandResult) r;      
        if(!result.getStatus().equals(Result.Status.OK)){
          //throw new TestException("Command return status is *NOT* OK. Command execution has failed");
          CLITest.currentCommand.append(" Command return status is *NOT* OK. Command execution has failed");
          CLITest.hasCommandFailed = true;
        }
        else
          HydraUtil.logInfo("Completed exeuction of <" + instance + "> successfully");         
      }
    }    
    verifyGemfire(gfsh,object);verifyJMX(gfsh,object);verifyCommand(gfsh,object);    
    
    RegionEvents e = new RegionEvents();
    e.regionDestroyed(regionPath, null);
    e.exportToBlackBoard(OperationsBlackboard.getBB());
    this.regionPath = null;    

    return object;
  }

  @Override
  protected void fillOption(TestCommandInstance instance, CommandOption op) {
    //NOOP
  }
  
  @Override
  public Object verifyJMX(TestableGfsh gfsh, Object object) {
    HydraUtil.logInfo("Sleeping for replication");
    HydraUtil.sleepForReplicationJMX();
    HydraUtil.logInfo("Checking mbeans for region " + regionPath + " Executor " + this);
    try {
      boolean jmxMBeansFound = checkMBean(regionPath, null);
      if(jmxMBeansFound){
        // TODO throw new TestException("Cant find mbeans for regionPath " + regionPath + " group=" + group);
        CLITest.currentCommand.append(" Cant find mbeans for regionPath " + regionPath + " group=" + null);
        CLITest.hasCommandFailed = true;
      }
      return jmxMBeansFound;
    } catch (MalformedObjectNameException e) {
      throw new TestException("Error checking regionMBeans", e);
    } catch (InstanceNotFoundException e) {
      throw new TestException("Error checking regionMBeans", e);
    } catch (AttributeNotFoundException e) {
      throw new TestException("Error checking regionMBeans", e);
    } catch (MalformedURLException e) {
      throw new TestException("Error checking regionMBeans", e);
    } catch (NullPointerException e) {
      throw new TestException("Error checking regionMBeans", e);
    } catch (MBeanException e) {
      throw new TestException("Error checking regionMBeans", e);
    } catch (ReflectionException e) {
      throw new TestException("Error checking regionMBeans", e);
    } catch (IOException e) {
      throw new TestException("Error checking regionMBeans", e);
    }    
  }  
  
  private static String regionPattern = "GemFire:service=Region,name=?1,type=Member,member=?2";
  private static String distrRgionPattern = "GemFire:service=Region,name=?1,type=Distributed";
  
  public static boolean checkMBean(String regionName,String group) throws MalformedURLException, IOException, 
    MalformedObjectNameException, NullPointerException, InstanceNotFoundException, 
    AttributeNotFoundException, MBeanException, ReflectionException{
    boolean regionMBeansExist = true;    
    Collection<String> urls = FederationBlackboard.getBB().getManagingNodes();    
    ObjectName distributedRegionMBean = new ObjectName(distrRgionPattern.replace("?1", regionName)); 
    String regionMBean1 = regionPattern.replace("?1", regionName);    
    HydraUtil.logInfo("Checking for JMX MBean for region : " + regionName);    
    for(String url : urls){      
      MBeanServerConnection connection = ManagementUtil.connectToUrl(url);
      HydraUtil.logInfo("Checking DistrRegionMBean on url " + url);
      boolean distrFound = ManagementUtil.checkIfMBeanExists(connection, distributedRegionMBean);
      if(!distrFound){
        HydraUtil.logInfo("DistributedRegionMBean " + distributedRegionMBean + " is not found on url " + url);
        regionMBeansExist = false;
      }
      Set<String> members = ManagementUtil.getMembersForGroup(connection,group);
      HydraUtil.logInfo("Member for group(" + group + ") : " + HydraUtil.ObjectToString(members));
      for(String member : members){
        ObjectName regionMBean = new ObjectName(regionMBean1.replace("?2", member));
        if(!ManagementUtil.checkIfMBeanExists(connection, regionMBean)){
          HydraUtil.logInfo("RegionMBean " + regionMBean + " is not found on url " + url);
          return false;
        }else{
          HydraUtil.logInfo("RegionMBean " + regionMBean + " is FOUND on url " + url);
        }
      }
    }
    return regionMBeansExist;
  }
  

  @Override
  protected void fillMandatoryOption(TestCommandInstance instance, String name) {
    //NOOP 
    if(CliStrings.DESTROY_REGION__REGION.equals(name)){
      this.regionPath = getCurrentRegionFilterColocated();
      instance.addOption(name, regionPath);
    }
  }
  
  /*
  @Override
  public Object executeAndVerify(TestCommandInstance instance) {
    HydraUtil.logInfo("Executed command : " + instance.toString());
    //TODO : After successful execution export regipn create event to blackboard
    return null;
  }*/

}
