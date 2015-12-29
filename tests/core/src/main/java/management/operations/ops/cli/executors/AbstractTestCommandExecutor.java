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

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import management.cli.CLIBlackboard;
import management.cli.TestableGfsh;
import management.operations.events.impl.FunctionEvents;
import management.operations.events.impl.RegionEvents;
import management.operations.ops.cli.TestCommand.CommandOption;
import management.operations.ops.cli.TestCommandExecutor;
import management.operations.ops.cli.TestCommandInstance;
import management.test.cli.CLITest;
import management.test.federation.FederationBlackboard;
import management.util.HydraUtil;
import management.util.ManagementUtil;
import util.TestException;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;

public abstract class AbstractTestCommandExecutor implements TestCommandExecutor{
  
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  public static final String FILE_TYPE_TEXT = ".txt";

  @Override
  public void fillOptionValues(TestCommandInstance instance,
      List<CommandOption> options) {
    for (CommandOption op : options) {
      fillOption(instance,op);
    }

  }
  
  @Override
  public void fillMandotoryOptionValues(TestCommandInstance instance,
      List<String> mandotoryCommandOptions) {
    for (String op : mandotoryCommandOptions) {
      fillMandatoryOption(instance,op);
    }
  }
  
  @Override
  public void fillArguments(TestCommandInstance instance, List<String> args){
    for (String op : args) {
      fillArgument(instance,op);
    }
  }
  
  @Override
  public Object verifyGemfire(TestableGfsh gfsh, Object object) {
    //NOOP
    return null;
  }

  @Override
  public Object verifyJMX(TestableGfsh gfsh, Object object) {
    //NOOP
    return null;
  }
  
  @Override
  public Object verifyCommand(TestableGfsh gfsh, Object object) {
    //NOOP
    /*
     * CommandOutputValidator validator = new CommandOutputValidator(gfsh, outputs);
     * CommandOutputValidatorResult result = validator.validate();
        if(!result.result){
          throw new TestException(result.getExceptionMessage());
        }
        return result;
     */
    return null;
  }
  
  @Override
  public Object execute(TestCommandInstance instance) {
    // NOOP : Is never called .Fire N Forget is not implemented
    return null;
  } 
  
  @Override
  public void setUpGemfire() {
    //NOOP
  }
  
  
  protected abstract void fillArgument(TestCommandInstance instance, String name);
  protected abstract void fillOption(TestCommandInstance instance, CommandOption op);
  protected abstract void fillMandatoryOption(TestCommandInstance instance,String name);

  @Override
  public Object executeAndVerify(TestCommandInstance instance) {
    TestableGfsh gfsh = CLITest.getTestableShell();
    if(!gfsh.isConnectedAndReady()){
      CLITest.connectGfshToManagerNode();
    }    
    //Object object[] = TestableGfsh.execAndLogCommand(gfsh, instance.toString(), CLITest.getGfshOutputFile(), false);    
    Object object[] = TestableGfsh.execAndLogCommand(gfsh, instance.toString(), CLITest.getGfshOutputFile(), false);
    Map map = (Map) object[0];
    CommandResult result =null;
    Collection values = map.values();
    for(Object r : values){
      if(r instanceof CommandResult){
        result = (CommandResult) r;      
        if(!result.getStatus().equals(Result.Status.OK)){
          //throw new TestException("Command return status is *NOT* OK. Command execution has failed");
          addFailure(" Command return status is *NOT* OK. Command execution has failed");
        }
        else
          HydraUtil.logInfo("Completed exeuction of <" + instance + "> successfully");         
      }
    }    
    verifyGemfire(gfsh,object);verifyJMX(gfsh,object);verifyCommand(gfsh,object);    
    return result;   
  }
  
  protected String getMemberId() {    
    Collection<String> collection = FederationBlackboard.getBB().getMemberNames().values();
    return HydraUtil.getRandomElement(collection);       
  }
  
  
  
  protected String getMemberIds() {    
    Collection<String> collection = FederationBlackboard.getBB().getMemberNames().values();
    int members = HydraUtil.getnextRandomInt(collection.size());
    List<String> finalist = new ArrayList<String>();
    for(int i=0;i<members;i++){
      String t = (String) HydraUtil.getRandomElement(collection);
      if(!finalist.contains(t))
        finalist.add(t);
    }
    
    if(finalist.size()==0)
      finalist.add((String) HydraUtil.getRandomElement(collection));
      
    StringBuilder sb = new StringBuilder();
    for(int i=0;i<finalist.size();i++){
      sb.append(finalist.get(i));
      if(i<finalist.size()-1)sb.append(",");
    }
    return sb.toString();    
  }

  protected String getGroup() {
    String array[] = { "managing", "managed", "managed2", "locator_managing", "managed1"};
    return HydraUtil.getRandomElement(array);    
  }
  
  protected String getGroups() {
    String array[] = { "managing", "managed", "managed2", "locator_managing", "managed1"};    
    int groups = HydraUtil.getnextRandomInt(array.length);
    List<String> finalist = new ArrayList<String>();
    for(int i=0;i<groups;i++){
      String a = HydraUtil.getRandomElement(array);
      if(!finalist.contains(a))
        finalist.add(a);
    }
    
    if(finalist.size()==0)
      finalist.add(HydraUtil.getRandomElement(array));
    
    StringBuilder sb = new StringBuilder();
    for(int i=0;i<finalist.size();i++){
      sb.append(finalist.get(i));
      if(i<finalist.size()-1)sb.append(",");
    }
    return sb.toString();
  }
  
  
  
  protected String getFile(String prefix, String type) {
    long fileNum = CLIBlackboard.getBB().getSharedCounters().incrementAndRead(CLIBlackboard.FILE_COUNTER);
    return prefix +"_" + fileNum + type;
  }
  
  protected String getDirectory(String prefix) {
    long dirNum = CLIBlackboard.getBB().getSharedCounters().incrementAndRead(CLIBlackboard.DIRECTORY_COUNTER);
    return prefix +"_" + dirNum;
  }
  
  protected String getNewRegion(String prefix) {
    long regionNum = CLIBlackboard.getBB().getSharedCounters().incrementAndRead(CLIBlackboard.REGION_COUNTER);        
    return Region.SEPARATOR + prefix +"_" + regionNum;
  }
  
  public final static String COLOCATED_ROOT_REGION = "TestPartition_1";
  public final static String COLOCATED_ROOT_DISK_REGION = "TestPersistentPartitionDisk1_2";
  public final static String COLOCATED_REGION_PATTERN = "PartitionColocated";
  
  
  protected Object getPrColocatedRootRegion() {
    return COLOCATED_ROOT_REGION;
  }
  
  /**
   * Return a random region from region list exported to Blackboard using RegionEvents
   * @return
   */
  protected String getAlreadyExistingRegion() {
    List<String> regionList = RegionEvents.getAllRegions();
    return HydraUtil.getRandomElement(regionList);
  }
  
  /**
   * Return a random region from region list exported to Blackboard using RegionEvents
   * This will remove all region destroyed and exported to Blackboard using RegionEvents
   * @return
   */
  protected String getCurrentRegion() {
    List<String> regionList = RegionEvents.getCurrentRegions();
    return HydraUtil.getRandomElement(regionList);
  }
  
  /**
   * Same as getCurrentRegion but filters out co-located regions
   * Regions mentioned here are according to template file : createRegionDefinitions.inc
   * @return
   */
  protected String getCurrentRegionFilterColocated() {
    List<String> regionList = RegionEvents.getCurrentRegions();
    List<String> newRegionList = new ArrayList<String>();
    for(String str : regionList){
      boolean isColocatedChain =(str.contains(COLOCATED_ROOT_REGION) 
            || str.contains(COLOCATED_REGION_PATTERN)
            || str.contains(COLOCATED_ROOT_DISK_REGION)); 
      if(!isColocatedChain){
        newRegionList.add(str);
      }else{
        HydraUtil.logInfo("Filterting region " + str + " as it falls inside Colocated Chain");
      }
    }
    return HydraUtil.getRandomElement(newRegionList);
  }

  
  String[] diskStores = { "disk1", "disk2", "disk3", "disk4" };
  protected Object getDiskStore() {    
    return HydraUtil.getRandomElement(diskStores);
  }
  
 
  protected boolean checkDirectory(String directory2) {
    File file = new File(".",directory2);
    return file.exists() && file.isDirectory() && file.list()!=null && file.list().length!=0;
  }
  
  protected boolean checkFile(String fileName) {
    File file = new File(".",fileName);   
    return file.exists() && file.isFile() && file.length()!=0;
  }
  
  protected void addFailure(String message){
    CLITest.currentCommand.append(" ").append(message);
    CLITest.hasCommandFailed = true;
  }
  
  //WAN Related helpers
  protected String getSenderId() {
    String wanDsName = CLIBlackboard.GW_SENDER_ID_LIST + "_" + ManagementUtil.getWanSiteName();
    List<String> list = (List<String>)CLIBlackboard.getBB().getList(wanDsName);
    String senderId = HydraUtil.getRandomElement(list);
    HydraUtil.logInfo("Registered Sender ID# : " + senderId);
    return senderId;
  }
  
  protected String getRunningSenderId() {
    String wanDsName = CLIBlackboard.GW_SENDER_RUNNING_LIST + "_" + ManagementUtil.getWanSiteName();    
    List<String> list = (List<String>)CLIBlackboard.getBB().getList(wanDsName);
    HydraUtil.logInfo("Running sender List " + list);
    String senderId =null;
    if(list.size()>0){
      senderId = HydraUtil.getRandomElement(list);
      list.remove(senderId);
      CLIBlackboard.getBB().saveList(wanDsName, list);  
    }else{    
      senderId = getSenderId();
    }
    HydraUtil.logInfo("Running Sender ID# : " + senderId);
    return senderId;
  }
  
  protected String getPausedSenderId() {
    String wanDsName = CLIBlackboard.GW_SENDER_PAUSED_LIST + "_" + ManagementUtil.getWanSiteName();
    List<String> list = (List<String>)CLIBlackboard.getBB().getList(wanDsName);
    String senderId =null;
    if(list.size()>0){
      senderId = HydraUtil.getRandomElement(list);
      list.remove(senderId);
      CLIBlackboard.getBB().saveList(wanDsName, list);
    }else{
      //Get one from registered list. Command probably fail 
      senderId = getSenderId();
    }
    HydraUtil.logInfo("Paused Sender ID# : " + senderId);
    return senderId;
  }
  
  protected String getStoppedSenderId() {
    String wanDsName = CLIBlackboard.GW_SENDER_STOPPED_LIST + "_" + ManagementUtil.getWanSiteName();
    List<String> list = (List<String>)CLIBlackboard.getBB().getList(wanDsName);
    String senderId = null;
    if(list.size()>0){
      senderId = HydraUtil.getRandomElement(list);
      list.remove(senderId);
      CLIBlackboard.getBB().saveList(wanDsName, list);   
    }else{
      //Get one from registered list. Command probably fail 
      senderId = getSenderId();
    }
    HydraUtil.logInfo("Stopped Sender ID# : " + senderId);
    return senderId;
  }
  
  protected void addToRunningSenderIdList(String senderId){
    String wanDsName = CLIBlackboard.GW_SENDER_RUNNING_LIST + "_" + ManagementUtil.getWanSiteName();
    CLIBlackboard.getBB().addToList(wanDsName, senderId);
  }
  
  protected void addToPausedSenderIdList(String senderId){
    String wanDsName = CLIBlackboard.GW_SENDER_PAUSED_LIST + "_" + ManagementUtil.getWanSiteName();
    CLIBlackboard.getBB().addToList(wanDsName, senderId);
  }

  protected void addToStoppedSenderIdList(String senderId){
    String wanDsName = CLIBlackboard.GW_SENDER_STOPPED_LIST + "_" + ManagementUtil.getWanSiteName();
    CLIBlackboard.getBB().addToList(wanDsName, senderId);
  }
  
 
  
  /**
   * This method filter managing node and then filters memberId for members only present
   * in Gfsh's DS.
   * 
   * DS is WAN Site Name derived from Hydra Client Name
   * 
   * @return
   */
  
  protected String getMemberIdInDS() {    
    Collection<String> collection = FederationBlackboard.getBB().getMemberNames().values();
    Collection<String> col = ManagementUtil.filter(collection, "managing");
    List<String> list = new ArrayList<String>();
    for(String o : col)
      list.add(o.toString());
    col = ManagementUtil.filterForThisDS(list);
    return HydraUtil.getRandomElement(col);       
  }
  
  protected String getGroupsNoManaging() {
    String array[] = { "managed", "managed2", "managed1"};    
    int groups = HydraUtil.getnextRandomInt(array.length);
    List<String> finalist = new ArrayList<String>();
    for(int i=0;i<groups;i++){
      String a = HydraUtil.getRandomElement(array);
      if(!finalist.contains(a))
        finalist.add(a);
    }
    
    if(finalist.size()==0)
      finalist.add(HydraUtil.getRandomElement(array));
    
    StringBuilder sb = new StringBuilder();
    for(int i=0;i<finalist.size();i++){
      sb.append(finalist.get(i));
      if(i<finalist.size()-1)sb.append(",");
    }
    return sb.toString();
  }
  
  protected Object getRegion() {
    List<String> regionList = RegionEvents.getAllRegions();
    return HydraUtil.getRandomElement(regionList);
  }
  
  protected Object getFunctionId() {
    return HydraUtil.getRandomElement(FunctionEvents.getAllRegisteredFunction());
  }
  
}
