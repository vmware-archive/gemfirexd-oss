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
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionEvent;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.cli.commands.CreateAlterDestroyRegionCommands;
import com.gemstone.gemfire.management.internal.cli.dto.Key1;
import com.gemstone.gemfire.management.internal.cli.dto.Value1;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;
import com.gemstone.gemfire.management.internal.cli.result.ResultData;
import com.gemstone.gemfire.management.internal.cli.result.TabularResultData;

public class CreateRegionExecutor extends AbstractTestCommandExecutor {

  private static final String LISTENER_CLASS = "management.operations.ops.cli.executors.CreateRegionExecutor$CRETestCacheListener";
  private static final String LOADER_CLASS = "management.operations.ops.cli.executors.CreateRegionExecutor$CRETestCacheLoader";
  private static final String WRITER_CLASS = "management.operations.ops.cli.executors.CreateRegionExecutor$CRETestCacheWriter";
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
  private String group= null;
  private boolean skipIfExists= true;
  
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
    //addArgument(instance, name); 
  }
  
  private void addArgument(TestCommandInstance instance, String name){
    
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
    boolean testValidationSucceeded = true;
    for(Object r : values){
      if(r instanceof CommandResult){
        result = (CommandResult) r;      
        if(!result.getStatus().equals(Result.Status.OK)){
          //throw new TestException("Command return status is *NOT* OK. Command execution has failed");
//          CLITest.currentCommand.append(" Command return status is *NOT* OK. Command execution has failed");
//          CLITest.hasCommandFailed = true;
          addFailure(" Command return status is *NOT* OK. Command execution has failed");
          testValidationSucceeded = false;
        }
        else
          HydraUtil.logInfo("Completed exeuction of <" + instance + "> successfully");         
      }
    }    
    verifyGemfire(gfsh,object);
    testValidationSucceeded = (Boolean) verifyJMX(gfsh,object);
    verifyCommand(gfsh,object);    
    if(testValidationSucceeded){
      RegionEvents e = new RegionEvents();
      e.regionAdded(regionPath);
      e.exportToBlackBoard(OperationsBlackboard.getBB());
    }
    this.regionPath = null;
    this.group = null;
    return object;
  }

  private String getRegionShortCutForMode(String mode) {
    RegionShortcut r[] = RegionShortcut.values();
    if(mode.contains("override-disk")){
      r = filterOnlyDiskShortCuts(r);
    }else if (mode.contains("expiration")){
      r = filterProxyShortCuts(r);
    }else if (mode.contains("override-pr")){
      r = filterPRShortCuts(r);
    }
    r = removeHDFSShortCuts(r);
    RegionShortcut selected = HydraUtil.getRandomElement(r);
    return selected.name();
  }

  protected String getAlreadyExistingRegionForMode(String mode) {
    List<String> regionList = RegionEvents.getAllRegions();
    if(mode.contains("override-disk")){
      regionList = filterOnlyDiskRegions(regionList);
    }else if (mode.contains("skip-if-exists")){
      regionList = filterOnlyHydraTemplateRegions(regionList);
    }else if (mode.contains("override-pr")){
     if(mode.contains("colocated-with")){
       return Region.SEPARATOR + COLOCATED_ROOT_REGION;
     }else{
       regionList = filterOnlyPartitionedRegions(regionList);
     }
    }    
    return HydraUtil.getRandomElement(regionList);
  }

  

  private List<String> filterOnlyDiskRegions(List<String> regionList) {
    List<String> list = new ArrayList<String>();
    for(String rs : regionList){
      if(rs.contains("override-disk") || rs.contains("Persistent")){
        list.add(rs);
      }
    }    
    HydraUtil.logInfo("Filtered only disk regions : " + list);
    return list;
  }
  
  private List<String> filterOnlyHydraTemplateRegions(List<String> regionList) {
    List<String> list = new ArrayList<String>();
    for(String rs : regionList){
      if(rs.contains("Test")){//All region created using hydra templates start with Test
        list.add(rs);
      }
    }    
    HydraUtil.logInfo("Filtered only disk regions : " + list);
    return list;
  }
  
  private List<String> filterOnlyPartitionedRegions(List<String> regionList) {
    List<String> list = new ArrayList<String>();
    for(String rs : regionList){
      if(rs.contains("Partition") || rs.contains("override-pr")){
        list.add(rs);
      }
    }
    return list;
  }
  
  private RegionShortcut[] removeHDFSShortCuts(RegionShortcut[] r) {
    List<RegionShortcut> list = new ArrayList<RegionShortcut>();
    for(RegionShortcut rs : r){
      if(!rs.name().contains("_HDFS_")){
        list.add(rs);
      }
    }
    RegionShortcut[] array = new RegionShortcut[list.size()];
    HydraUtil.logInfo("Removed HDFS shortcuts : " + list);
    return list.toArray(array);
  }

  private RegionShortcut[] filterOnlyDiskShortCuts(RegionShortcut[] r) {    
    List<RegionShortcut> list = new ArrayList<RegionShortcut>();
    for(RegionShortcut rs : r){
      if((rs.name().contains("PERSISTENT") || rs.name().contains("OVERFLOW"))){
        list.add(rs);
      }
    }
    RegionShortcut[] array = new RegionShortcut[list.size()];
    HydraUtil.logInfo("Filtered only disk shortcuts : " + list);
    return list.toArray(array);
  }
  
  private RegionShortcut[] filterPRShortCuts(RegionShortcut[] r) {
    List<RegionShortcut> list = new ArrayList<RegionShortcut>();
    for(RegionShortcut rs : r){
      if((rs.name().contains("PARTITION") && !rs.name().contains("PROXY"))){
        list.add(rs);
      }
    }
    RegionShortcut[] array = new RegionShortcut[list.size()];
    HydraUtil.logInfo("Filtered only PR shortcuts : " + list);
    return list.toArray(array);
  }
  
  private RegionShortcut[] filterProxyShortCuts(RegionShortcut[] r) {    
    List<RegionShortcut> list = new ArrayList<RegionShortcut>();
    for(RegionShortcut rs : r){
      if(!rs.name().contains("PROXY")){
        list.add(rs);
      }
    }
    RegionShortcut[] array = new RegionShortcut[list.size()];
    HydraUtil.logInfo("Filtered PROXY shortcuts : " + list);
    return list.toArray(array);
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
      Object[] outputs = (Object[])object;
      Map<String, Object> commandOutput = (Map<String, Object>) outputs[0];
      //find out failed member
      List<String> memmberIds = getFailedMemberIdsFromOutput(commandOutput);
      boolean jmxMBeansCreated = checkMBean(regionPath, group,memmberIds);
      if(!jmxMBeansCreated){
        // TODO throw new TestException("Cant find mbeans for regionPath " + regionPath + " group=" + group);
//        CLITest.currentCommand.append(" Cant find mbeans for regionPath " + regionPath + " group=" + group);
//        CLITest.hasCommandFailed = true;
        addFailure("Cant find mbeans for regionPath " + regionPath + " group=" + group);
      }
      return jmxMBeansCreated;
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
  
  private List<String> getFailedMemberIdsFromOutput(Map<String, Object> commandOutput) {
    List<String> memberIds = new ArrayList<String>();
    for(Object obj : commandOutput.values()){
      CommandResult result = (CommandResult)obj;
      if(result.getType().equals(ResultData.TYPE_TABULAR)){
        TabularResultData table = (TabularResultData)result.getResultData();
        List<String> statuses = table.retrieveAllValues("Status");
        List<String> members = table.retrieveAllValues("Member");
        for(int i=0;i<statuses.size();i++){
          String status = statuses.get(i);
          String member = members.get(i);
          if(status.contains("ERROR")){
            memberIds.add(member);
            HydraUtil.logFine("Command failed on member " + member);
          }else{            
            HydraUtil.logFine("Command Succeeded on member " + member + " for validation");
          }
        }
      }
       
    }
    return memberIds;
  }

  private static String regionPattern = "GemFire:service=Region,name=?1,type=Member,member=?2";
  private static String distrRgionPattern = "GemFire:service=Region,name=?1,type=Distributed";  
  
  
  public static boolean checkMBean(String regionName,String group, List<String> memmberIds) throws MalformedURLException, IOException, 
    MalformedObjectNameException, NullPointerException, InstanceNotFoundException, 
    AttributeNotFoundException, MBeanException, ReflectionException{
    boolean regionMBeansExist = true;    
    Collection<String> urls = FederationBlackboard.getBB().getManagingNodes();
    
    if(regionName.contains("-")){
      regionName = "\"" + regionName + "\"";
    }
    
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
      
      if(memmberIds!=null && memmberIds.size()>0){
        for(String m : memmberIds){
          if(members.contains(m)){
            HydraUtil.logFine("Removing member as command could not created region on member " + m);
            members.remove(m);
          }
        }
      }
            
      for(String member : members){      
        ObjectName regionMBean = new ObjectName(regionMBean1.replace("?2", member));
        if(!ManagementUtil.checkIfMBeanExists(connection, regionMBean)){
          HydraUtil.logInfo("RegionMBean " + regionMBean + " is not found on url " + url);
          return false;
        }else{
          HydraUtil.logInfo("RegionMBean " + regionMBean + " is FOUND on url " + url);
        }
      }
      HydraUtil.logInfo("Checked for memebers : " + members + " Result : " + regionMBeansExist);
    }
    return regionMBeansExist;
  }
  

  @Override
  protected void fillMandatoryOption(TestCommandInstance instance, String name) {
    
    HydraUtil.logFine("fillMandatoryOption : mode(" +  instance.getMode() +") # " + name );
    
    if(CliStrings.CREATE_REGION__GROUP.equals(name)){
      this.group = getGroup();
      instance.addOption(name,group);
    }else if(CliStrings.CREATE_REGION__SKIPIFEXISTS.equals(name)){
      instance.addOption(name, "true");
      skipIfExists = true;
      //TODO : Add test case for false also
      //instance.addOption(name, "" + HydraUtil.getRandomBoolean());
    }else if(CliStrings.CREATE_REGION__USEATTRIBUTESFROM.equals(name)){
      String regionName =  getAlreadyExistingRegionForMode(instance.getMode());
      instance.addOption(name,regionName);
    }else if(CliStrings.CREATE_REGION__REGIONSHORTCUT.equals(name)){
      String shortcut =getRegionShortCutForMode(instance.getMode());      
      instance.addOption(name, shortcut);
      //TODO Fix for default store bug due to nature of Hydra. Hydra does not 
      //allow to set directories for default diskStore. Remote this later.
      if(shortcut.contains("PERSISTENT") || shortcut.contains("OVERFLOW")){
        instance.addOption(CliStrings.CREATE_REGION__DISKSTORE, getDiskStore());
      }      
    }else if(CliStrings.CREATE_REGION__REGION.equals(name)){
      
      String mode = instance.getMode();
      if(mode.contains(CliStrings.CREATE_REGION__SKIPIFEXISTS)){
        regionPath = getAlreadyExistingRegion();
        instance.addOption(name, regionPath);
      }
      //if skip-if-exists then add existing regionName
      else {
        regionPath = getNewRegion(mode);
        instance.addOption(name, regionPath);
      }
      HydraUtil.logFine("Region Name for new command " + regionPath + " Executor " + this);
    }
    
    //TODO : Add command verification by using describe region command    
    
    //TODO : Add colocated-with option
    
    String mode = instance.getMode();
    
    if(mode.contains("override-disk")){
      if(CliStrings.CREATE_REGION__DISKSTORE.equals(name)){
        instance.addOption(name, getDiskStore());
      }else if(CliStrings.CREATE_REGION__DISKSYNCHRONOUS.equals(name)){
        instance.addOption(name, ""+HydraUtil.getRandomBoolean());
      }else if(CliStrings.CREATE_REGION__STATISTICSENABLED.equals(name)){
        instance.addOption(name, "true");
      }      
    }else if(mode.contains("override-key-value-constraint")){
      if(CliStrings.CREATE_REGION__KEYCONSTRAINT.equals(name)){
       instance.addOption(name, Key1.class.getCanonicalName()); 
      }else if(CliStrings.CREATE_REGION__VALUECONSTRAINT.equals(name)){
        instance.addOption(name, Value1.class.getCanonicalName()); 
      }      
    }else if(mode.contains("override-expiration")){
      if(CliStrings.CREATE_REGION__ENTRYEXPIRATIONIDLETIME.equals(name)){
        instance.addOption(name, HydraUtil.getnextRandomInt(60));
      }else if(CliStrings.CREATE_REGION__ENTRYEXPIRATIONIDLETIMEACTION.equals(name)){
        instance.addOption(name, getExpiryAction());
      }else if(CliStrings.CREATE_REGION__ENTRYEXPIRATIONTIMETOLIVE.equals(name)){
        instance.addOption(name, HydraUtil.getnextRandomInt(60));
      }else if(CliStrings.CREATE_REGION__ENTRYEXPIRATIONTTLACTION.equals(name)){
        instance.addOption(name, getExpiryAction());
      }else if(CliStrings.CREATE_REGION__STATISTICSENABLED.equals(name)){
        instance.addOption(name, "true");
      }      
    }else if(mode.contains("region-override-region-expiration")){      
      if(CliStrings.CREATE_REGION__REGIONEXPIRATIONIDLETIME.equals(name)){
        instance.addOption(name, HydraUtil.getnextRandomInt(60));
      }else if(CliStrings.CREATE_REGION__REGIONEXPIRATIONIDLETIMEACTION.equals(name)){
        instance.addOption(name, getExpiryAction());
      }else if(CliStrings.CREATE_REGION__REGIONEXPIRATIONTTL.equals(name)){
        instance.addOption(name, HydraUtil.getnextRandomInt(60));
      }else if(CliStrings.CREATE_REGION__REGIONEXPIRATIONTTLACTION.equals(name)){
        instance.addOption(name, getExpiryAction());
      }else if(CliStrings.CREATE_REGION__STATISTICSENABLED.equals(name)){
        instance.addOption(name, "true");
      }      
    }else if(mode.contains("override-conflation")){
      if(CliStrings.CREATE_REGION__ENABLEASYNCCONFLATION.equals(name)){
        instance.addOption(name, "true");
      }else if(CliStrings.CREATE_REGION__ENABLESUBSCRIPTIONCONFLATION.equals(name)){
        instance.addOption(name, "true");
      }
    }else if(mode.contains("override-listeners")){
      
      if(CliStrings.CREATE_REGION__CACHELISTENER.equals(name)){
        instance.addOption(name, LISTENER_CLASS);
      }else if(CliStrings.CREATE_REGION__CACHELOADER.equals(name)){
        instance.addOption(name, LOADER_CLASS);
      }else if(CliStrings.CREATE_REGION__CACHEWRITER.equals(name)){
        instance.addOption(name, WRITER_CLASS);
      }   
    }else if(mode.contains("override-concurrency-enabled")){
      if(CliStrings.CREATE_REGION__CONCURRENCYCHECKSENABLED.equals(name)){
        instance.addOption(name, "true");
      }      
    }else if(mode.contains("override-concurrency-level")){
      if(CliStrings.CREATE_REGION__CONCURRENCYLEVEL.equals(name)){
        instance.addOption(name, "" + HydraUtil.getnextNonZeroRandomInt(100));
      }      
    }else if(mode.contains("override-cloning-enabled")){
      if(CliStrings.CREATE_REGION__CLONINGENABLED.equals(name)){
        instance.addOption(name, "true");
      }
    }else if(mode.contains("override-pr")){
      if(CliStrings.CREATE_REGION__LOCALMAXMEMORY.equals(name)){
        instance.addOption(name, HydraUtil.getnextRandomInt(1024));
      }else if(CliStrings.CREATE_REGION__RECOVERYDELAY.equals(name)){
        instance.addOption(name, HydraUtil.getnextRandomInt(5000));
      }else if(CliStrings.CREATE_REGION__REDUNDANTCOPIES.equals(name)){
        if(mode.contains("from-other")){//using existing region as template so set same number of buckets
          instance.addOption(name,1);
        }else{
          int copies = HydraUtil.getnextRandomInt(4);
          if(copies==0)
            copies=1;
          instance.addOption(name,copies);
        }
      }else if(CliStrings.CREATE_REGION__STARTUPRECOVERYDDELAY.equals(name)){
        instance.addOption(name, HydraUtil.getnextRandomInt(5000));
      }else if(CliStrings.CREATE_REGION__TOTALMAXMEMORY.equals(name)){
        int numMemebers = FederationBlackboard.getBB().getMemberNames().size();
        instance.addOption(name, (HydraUtil.getnextRandomInt(1024)*numMemebers));
      }else if(CliStrings.CREATE_REGION__TOTALNUMBUCKETS.equals(name)){
        if(mode.contains("from-other")){//using existing region as template so set same number of buckets
          instance.addOption(name, 20);
        }else
          instance.addOption(name, HydraUtil.getnextRandomInt(1024));
      }
    }else if(mode.contains("pr-colocated-with")){
      if(CliStrings.CREATE_REGION__COLOCATEDWITH.equals(name)){
        instance.addOption(name, getPrColocatedRootRegion());
      }
    }
  }
    

  public static class CRETestCacheLoader implements com.gemstone.gemfire.cache.CacheLoader{

    @Override
    public void close() {
      HydraUtil.logInfo(this.getClass().getName() + " : CLOSED");
    }

    @Override
    public Object load(LoaderHelper helper) throws CacheLoaderException {
      HydraUtil.logInfo(this.getClass().getName() + " : load for Key " + helper.getKey());
      return null;
    }    
  }
  
  public static class CRETestCacheWriter implements CacheWriter{

    @Override
    public void close() {
      HydraUtil.logInfo(this.getClass().getName() + " : CLOSED");      
    }

    @Override
    public void beforeUpdate(EntryEvent event) throws CacheWriterException {
      HydraUtil.logInfo(this.getClass().getName() + " : beforeUpdate : " + event.getKey());      
    }

    @Override
    public void beforeCreate(EntryEvent event) throws CacheWriterException {
      HydraUtil.logInfo(this.getClass().getName() + " : beforeCreate : " + event.getKey());      
    }

    @Override
    public void beforeDestroy(EntryEvent event) throws CacheWriterException {
      HydraUtil.logInfo(this.getClass().getName() + " : beforeDestroy : " + event.getKey());      
    }

    @Override
    public void beforeRegionDestroy(RegionEvent event) throws CacheWriterException {
      HydraUtil.logInfo(this.getClass().getName() + " : beforeRegionDestroy : " + event.getRegion().getFullPath());     
    }

    @Override
    public void beforeRegionClear(RegionEvent event) throws CacheWriterException {
      HydraUtil.logInfo(this.getClass().getName() + " : beforeRegionClear : " + event.getRegion().getFullPath());      
    }    
  }
  
  public static class CRETestCacheListener implements CacheListener{

    @Override
    public void close() {
      HydraUtil.logInfo(this.getClass().getName() + " : CLOSED");      
    }

    @Override
    public void afterCreate(EntryEvent event) {
      HydraUtil.logInfo(this.getClass().getName() + " : afterCreate : " + event.getKey());        
    }

    @Override
    public void afterUpdate(EntryEvent event) {
      HydraUtil.logInfo(this.getClass().getName() + " : afterUpdate : " + event.getKey());        
    }

    @Override
    public void afterInvalidate(EntryEvent event) {
      HydraUtil.logInfo(this.getClass().getName() + " : afterInvalidate : " + event.getKey());        
    }

    @Override
    public void afterDestroy(EntryEvent event) {
      HydraUtil.logInfo(this.getClass().getName() + " : afterDestroy : " + event.getKey());        
    }

    @Override
    public void afterRegionInvalidate(RegionEvent event) {
      HydraUtil.logInfo(this.getClass().getName() + " : afterRegionInvalidate : " + event.getRegion().getFullPath());        
    }

    @Override
    public void afterRegionDestroy(RegionEvent event) {
      HydraUtil.logInfo(this.getClass().getName() + " : afterRegionDestroy : " + event.getRegion().getFullPath());        
    }

    @Override
    public void afterRegionClear(RegionEvent event) {
      HydraUtil.logInfo(this.getClass().getName() + " : afterRegionClear : " + event.getRegion().getFullPath());        
    }

    @Override
    public void afterRegionCreate(RegionEvent event) {
      HydraUtil.logInfo(this.getClass().getName() + " : afterRegionCreate : " + event.getRegion().getFullPath());        
    }

    @Override
    public void afterRegionLive(RegionEvent event) {
      HydraUtil.logInfo(this.getClass().getName() + " : afterRegionLive : " + event.getRegion().getFullPath());       
    }   
  }

  private static final String[] expirationActions = { 
    ExpirationAction.INVALIDATE.toString(), 
    ExpirationAction.LOCAL_INVALIDATE.toString(), 
    ExpirationAction.DESTROY.toString(), 
    ExpirationAction.LOCAL_DESTROY.toString()
  };
  
  private Object getExpiryAction() {    
    return HydraUtil.getRandomElement(expirationActions);
  }

  /*
  @Override
  public Object executeAndVerify(TestCommandInstance instance) {
    HydraUtil.logInfo("Executed command : " + instance.toString());
    //TODO : After successful execution export regipn create event to blackboard
    return null;
  }*/

}
