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
package management.test.cli;

import static management.util.HydraUtil.logInfo;
import hydra.BridgeHelper;
import hydra.BridgePrms;
import hydra.CacheHelper;
import hydra.CachePrms;
import hydra.ClientPrms;
import hydra.ConfigPrms;
import hydra.DistributedSystemHelper;
import hydra.GatewayReceiverHelper;
import hydra.GatewaySenderHelper;
import hydra.GatewaySenderPrms;
import hydra.GemFireDescription;
import hydra.HydraThreadLocal;
import hydra.HydraVector;
import hydra.JMXManagerBlackboard;
import hydra.JMXManagerHelper;
import hydra.Log;
import hydra.MasterController;
import hydra.RegionHelper;
import hydra.RemoteTestModule;
import hydra.StopSchedulingOrder;
import hydra.TestConfig;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import management.cli.CLIBlackboard;
import management.cli.GfshPrms;
import management.cli.TestableGfsh;
import management.operations.OperationsBlackboard;
import management.operations.RegionKeyValueConfig;
import management.operations.SimpleRegionKeyValueConfig;
import management.operations.events.impl.AbstractEvents;
import management.operations.events.impl.EntryEvents;
import management.operations.events.impl.FunctionEvents;
import management.operations.events.impl.RegionEvents;
import management.operations.ops.CLIOperations;
import management.operations.ops.EntryOperations;
import management.operations.ops.FunctionOperations;
import management.operations.ops.RegionOperations;
import management.operations.ops.cli.TestCommand;
import management.operations.ops.cli.TestCommandExecutor;
import management.operations.ops.cli.TestCommandInstance;
import management.test.federation.FederationBlackboard;
import management.util.HydraUtil;
import management.util.ManagementUtil;
import util.TestException;
import util.TestHelper;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.wan.GatewayReceiver;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.management.ManagementService;
import com.gemstone.gemfire.management.internal.cli.dto.Key1;
import com.gemstone.gemfire.management.internal.cli.dto.Value2;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.shell.Gfsh;

public class CLITest {
  
  private static CLITest cliTestInstance;
  private DistributedSystem ds = null;
  private GemFireCacheImpl cache = null;
  private ManagementService service;
  private CacheServer server = null;
  private HydraThreadLocal regionHT = new HydraThreadLocal();
  private HydraThreadLocal entryHT = new HydraThreadLocal();
  private HydraThreadLocal cliHT = new HydraThreadLocal();
  private HydraThreadLocal functionHT = new HydraThreadLocal();
  public static final String REGION_NAME = "OperationsRegion";  
  //private static RegionKeyValueConfig regionkeyValueConfig = new
  // SimpleRegionKeyValueConfig("KEY", 100000);
  private static HydraThreadLocal regionKeyValueConfigHL = new HydraThreadLocal();
  public static HydraThreadLocal commands = new HydraThreadLocal();
  public static HydraThreadLocal tGfshInstances = new HydraThreadLocal();
  public static HydraThreadLocal outputFileHL = new HydraThreadLocal();
  
  private final static String dataRegionName= "PartitionBridge";
  private final static String jsonRegionName= "PartitionJSONBridge";
  private final static String childRegionNames= "ReplicatedNoAckBridge";
  private Region dataStringRegion = null;
  private Region dataJsonRegion = null;
  private Region[] regionHierarchyRegions = null;
  private static List<String> regionList = null;
  
  //TODO  : Temp stuff just to get all error caught
  public static List<String> failedCommands = new ArrayList<String>();
  public static StringBuilder currentCommand=null;
  public static boolean hasCommandFailed;
  
  
  public synchronized static void HydraInitTask_startLocator() {
    if (cliTestInstance == null) {
      cliTestInstance = new CLITest();      
    }
    cliTestInstance.startLocator();
  } 
  
  public synchronized static void HydraInitTask_initialize() {
    if (cliTestInstance == null) {
      cliTestInstance = new CLITest();      
    }
    cliTestInstance.initializeCLI();
  }  

  public static void HydraInitTask_becomeManager() {
    cliTestInstance.becomeManager();
  }
  
  
  public static synchronized void HydraInitTask_createRegionsOnEdge(){
    cliTestInstance.createRegionsOnEdge();
  }
  
  public static synchronized void HydraInitTask_createRegionsOnBridge(){
    cliTestInstance.createRegionsOnBridge();
  }
  
  public static synchronized void HydraInitTask_createDataRegions(){
    cliTestInstance.createDataRegions();
  }
  
  public static synchronized void HydraInitTask_registerFunctions(){
    cliTestInstance.registerFunctions();
  }
  
  public static void HydraInitTask_startWANSendersAndReceivers() {
    cliTestInstance.startWANSendersAndReceivers();
  }
  
  public static void HydraCloseTask_printEvents(){
    cliTestInstance.printEvents();
  }
  
  public static void HydraCloseTask_validateEvents(){
   //cliTestInstance.validateEvents();
  }
  
  public static void HydraCloseTask_printFailedCommands(){
    StringBuilder sb = new StringBuilder();
    for(String s : failedCommands){
      sb.append(s).append("\n");
    }
    HydraUtil.logInfo("Failed Commands " + failedCommands.size());
    if(failedCommands.size()>0){
      HydraUtil.logErrorAndRaiseException("Failed Commands " + sb.toString());      
    }
   }
   
 
  
  public static void HydraCloseTask_PrintRegionOps() {
    logInfo("Printing all events ");
    logInfo("Printing all commands ");    
    StringBuilder sb = (StringBuilder) commands.get();
    logInfo(sb.toString());
    //OperationsBlackboard.getBB().printEvents();
  }
  
  public static void HydraTask_cliOps() {
    CLIBlackboard.getBB().printSharedCounters();
    long numCommandsCompleted  = CLIBlackboard.getBB().getSharedCounters().incrementAndRead(CLIBlackboard.gfshExecutionNumberCompleted);
    if(numCommandsCompleted==GfshPrms.getNumCommandToExecute()){
      throw new StopSchedulingOrder("CLITest : Finished with " + numCommandsCompleted + " commands as configured. Test is exiting now");      
    }
    long gfshCount = logGfshExecutionNumber();
    logInfo("CLITest : Cli operation task : " + gfshCount);
//    CLIOperations cliOps = getCLIOperations();
    currentCommand = new StringBuilder();
    hasCommandFailed = false;
    TestCommand selecteCommand = selectCommand();
    TestCommandInstance selectedMOde = selecteCommand.getRandomCommandInstnance(); 
    HydraUtil.logInfo("Selecting command for iteration #" + gfshCount + " : " + selecteCommand.command);    
//    cliOps.doCLIOperation();
    executeAndVerifyCommand(selecteCommand, selectedMOde);
    
    if(hasCommandFailed){
      failedCommands.add("Command # " + gfshCount + " " + currentCommand.toString());
    }
    logInfo("CLITest : Cli operation task #" + gfshCount + " completed successfully");
  }
  
  public static TestCommand selectCommand() {    
    String command = TestConfig.tab().stringAt(GfshPrms.cliCommands);
    logInfo("CLITest :  Selected Command " + command);
    HashMap<String, TestCommand> commandMap = (HashMap<String, TestCommand>) CLIBlackboard.getBB().getMap(
        CLIBlackboard.COMMAND_MAP);
    if (commandMap.containsKey(command)) {
      TestCommand testCommand = commandMap.get(command);      
      return testCommand;
    } else {
      throw new TestException("Command descriptor for " + command
          + " not found. Please check your hydra configurations.");
    }
  }
  
  private static void executeAndVerifyCommand(TestCommand testCommand, TestCommandInstance instance) {
    TestCommandExecutor executor = testCommand.getExecutor();
    logInfo("CLITest : Executing command {<" + instance.toString() + ">}");
    StringBuilder sb = (StringBuilder) CLITest.commands.get();
    String command = instance.toString();
    command = command.replaceAll(HydraUtil.NEW_LINE,(HydraUtil.NEW_LINE+HydraUtil.TAB+HydraUtil.TAB ) );
    currentCommand.append("Command <").append(command).append("> ");
    //sb.append(command).append(HydraUtil.NEW_LINE).append(HydraUtil.NEW_LINE);
    Object result = executor.executeAndVerify(instance);
    logInfo("CLITest : Successfully completed executing command {<" + instance.toString() + ">}");
  }
  
  /**
   * cliOps and gemfireOps and synchronized so that gfsh only gets executed after gemfire operations are completed
   * This way test can generate any test senario if needed
   * 
   
  public static void HydraTask_cliOps() {
    CLIBlackboard.getBB().printSharedCounters();
    long gemfireTasksCompleted  = CLIBlackboard.getBB().getSharedCounters().read(CLIBlackboard.gemfireExecutionNumberCompleted);
    long gfshCounter  = CLIBlackboard.getBB().getSharedCounters().read(CLIBlackboard.gfshExecutionNumber);
    long numCommandsCompleted  = CLIBlackboard.getBB().getSharedCounters().read(CLIBlackboard.gfshExecutionNumberCompleted);
    
    if(numCommandsCompleted==GfshPrms.getNumCommandToExecute()){
      throw new StopSchedulingOrder("CLITest : Finished with " + numCommandsCompleted + " commands as configured. Test is exiting now");
    }
    CLIOperations cliOps = getCLIOperations();
    TestCommand selecteCommand = null;
    TestCommandInstance selectedMOde = null;
    if(gemfireTasksCompleted==(gfshCounter+1)){    
      long gfshCount = logGfshExecutionNumber();
      logInfo("CLITest : Cli operation task : " + gfshCount);      
      //call to executor for test scenario creation    
      /*if(GfshPrms.waitForGemfireTaskToComplete()){
        logInfo("Waiting for gemfire taks number " + (gfshCount) + " to complete");
        TestHelper.waitForCounter(CLIBlackboard.getBB(), "TASKS_COMPLETE_COUNT", CLIBlackboard.gemfireExecutionNumberCompleted, (gfshCount), true, 10*1000);
      } * /      
      cliOps.doCLIOperation();
      
      if(GfshPrms.disconnectAfterEachTask() && getTestableShell().isConnectedAndReady()){
          TestableGfsh.execAndLogCommand(getTestableShell(),"disconnect", getGfshOutputFile());
      }
      HydraUtil.logInfo("Selecting command for next iterations ");
      selecteCommand = cliOps.selectCommand();
      selectedMOde = cliOps.getSelectedcommandMode();
      CLIBlackboard.getBB().addCommandsForNextExecution(selecteCommand, selectedMOde);
      CLIBlackboard.getBB().getSharedCounters().incrementAndRead(CLIBlackboard.gfshExecutionNumberCompleted);
    }else if(gemfireTasksCompleted==(gfshCounter)){
      HydraUtil.logInfo("CLITest : Waiting for gemfire task #" + (gfshCounter+1) +  " to complete");
    }else if(gemfireTasksCompleted<(gfshCounter)){
      throw new TestException("CLITest : Wrong sheduling order gemfireCounter" + gemfireTasksCompleted + " expected number " + (gfshCounter+1));
    }
  }*/
  
  /*
  public static void HydraTask_gemfireOps() {
    CLIBlackboard.getBB().printSharedCounters();
    long gfshTaskCompleted  = CLIBlackboard.getBB().getSharedCounters().read(CLIBlackboard.gfshExecutionNumberCompleted);
    long gemfireCounter  = CLIBlackboard.getBB().getSharedCounters().read(CLIBlackboard.gemfireExecutionNumber);
    if(gemfireCounter==0 || gemfireCounter==(gfshTaskCompleted)){      
      long gemfireTaskCount = logGemfireExecutionNumber();
      logInfo("CLITest : Gemfire operation task : " + gemfireTaskCount);    
      /*if(GfshPrms.waitForGemfireTaskToComplete()){
        logInfo("Waiting for gfsh taks number  " + (gemfireTaskCount-1) + " to complete");
        TestHelper.waitForCounter(CLIBlackboard.getBB(), "gfshExecutionNumberCompleted", CLIBlackboard.gfshExecutionNumberCompleted, (gemfireTaskCount-1), true, 10*1000);
      }* /
      
      Object[] commands = CLIBlackboard.getBB().getCommandsForNextExecution();
      if(commands[0]!=null){// if its null nothing to do
        TestCommand command = (TestCommand) commands[0];
        //TestCommandInstance instance = (TestCommandInstance) commands[1];
        TestCommandExecutor executor = command.getExecutor();
        executor.setUpGemfire();
      }else{
        logInfo("CLITest : TestCommand is null so assuming no work assigned for setting up gemfire resources for commands");
      }
      CLIBlackboard.getBB().getSharedCounters().incrementAndRead(CLIBlackboard.gemfireExecutionNumberCompleted);    
    }else if(gemfireCounter<(gfshTaskCompleted)){
      logInfo("CLITest : Skpping this task so that Gfsh can complete taks number : " + gemfireCounter);
    }else if(gemfireCounter>(gfshTaskCompleted)){
      logInfo("CLITest : Skpping this task so that Gfsh can complete taks number : " + gemfireCounter);
      //throw new TestException("Wrong sheduling order gemfireCounter" + gemfireCounter + " expected number " + (gfshTaskCompleted-1));
    }
  }*/
  
  public static void HydraTask_shellCommands(){    
      cliTestInstance.doShellCommandTests();        
  }
  

  public static ManagementService getManagementService(){
    return cliTestInstance.service;
  }
  
  public static HydraThreadLocal getRegionHT() {
    return cliTestInstance.regionHT;
  } 
  
  private static HydraThreadLocal getEntryOpHT() {
    return cliTestInstance.entryHT;
  }
  
  private static HydraThreadLocal getCLIOpHT() {
    return cliTestInstance.cliHT;
  }
  
  private static HydraThreadLocal getFuncHT() {
    return cliTestInstance.functionHT;
  }
  
  public static String getStringRegionName(){
    return cliTestInstance.dataStringRegion.getName();    
  }
  
  public static String getJSONRegionName(){
    return cliTestInstance.dataJsonRegion.getName();    
  }
  
  private void startLocator() {
    DistributedSystemHelper.createLocator();
    DistributedSystemHelper.startLocatorAndDS();
    waitForLocatorDiscovery();    
  }
  
  protected void waitForLocatorDiscovery(){
    logInfo("Waititng for locator discovery.");   
    boolean isDiscovered;    
    do{
      isDiscovered = true;
      List<Locator> locators = Locator.getLocators();
      Map gfLocMap = ((InternalLocator)locators.get(0)).getAllLocatorsInfo();
      List dsList = new ArrayList(); //non discovered ds
      for (GemFireDescription gfd : TestConfig.getInstance().getGemFireDescriptions().values()) {
        Integer ds = gfd.getDistributedSystemId();
        if(!ds.equals(-1) && !dsList.contains(ds) && !gfLocMap.containsKey(ds)){
          dsList.add(ds);
          isDiscovered = false;
        }
      }
      if(!isDiscovered){
        logInfo("Waiting for locator discovery to complete. Locators not discoverd so far from ds " + dsList 
            + ". Distribution locator map from gemfire system is " +  (new  ConcurrentHashMap(gfLocMap)).toString());
        MasterController.sleepForMs(5 * 1000); //wait for 5 seconds
      }       
    }while(!isDiscovered);
    logInfo("Locator discovery completed.");
  }
  
  public static RegionKeyValueConfig getRegionKeyValueConfig() {
    RegionKeyValueConfig regionkeyValueConfig = (RegionKeyValueConfig) regionKeyValueConfigHL
        .get();
    if (regionkeyValueConfig == null) {
      regionkeyValueConfig = new SimpleRegionKeyValueConfig("KEY"
          + RemoteTestModule.getCurrentThread().getThreadId(), 100000);
      regionKeyValueConfigHL.set(regionkeyValueConfig);
    }
    return regionkeyValueConfig;
  }
  
  public static synchronized RegionOperations getRegionOperations(){
    RegionOperations regionOps = (RegionOperations) getRegionHT().get();
    if(regionOps==null){
      regionOps = new RegionOperations(cliTestInstance.cache, new RegionEvents());
      getRegionHT().set(regionOps);
    }
    return regionOps;
  }
  
  public static synchronized FunctionOperations getFunctionOperations(){
    FunctionOperations funcOps = (FunctionOperations) getFuncHT().get();
    if(funcOps==null){
      funcOps = new FunctionOperations(new FunctionEvents());
      getFuncHT().set(funcOps);
    }
    return funcOps;
  }
  
  public static synchronized CLIOperations getCLIOperations(){
    CLIOperations cliOps = (CLIOperations) getCLIOpHT().get();    
    if(cliOps==null){
      cliOps = new CLIOperations();
      cliOps.selectCommand();
      cliOps.getSelectedcommandMode();
      getCLIOpHT().set(cliOps);
    }
    return cliOps;
  }
  
  public static PrintWriter getGfshOutputFile(){
    return (PrintWriter)outputFileHL.get();
  }
  
  
  public static synchronized TestableGfsh getTestableShell(){
    TestableGfsh shell = (TestableGfsh) tGfshInstances.get();    
    if(shell==null){
      Gfsh.SUPPORT_MUTLIPLESHELL  = true;
      String argss[] = {};
      try {
        String tName = Thread.currentThread().getName();
        shell =  new TestableGfsh(tName, true, argss);
        shell.start();
        PrintWriter commandOutputFile =  null;        
        final String fileName = "commandOutput_" + RemoteTestModule.getMyPid() + "_" + tName;
        try {
          commandOutputFile = new PrintWriter(new FileOutputStream(new File(fileName)));
        } catch (FileNotFoundException e) {
          throw new TestException(TestHelper.getStackTrace(e));
        }
        outputFileHL.set(commandOutputFile);
      } catch (ClassNotFoundException e) {        
        throw new TestException(TestHelper.getStackTrace(e));
      } catch (IOException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }      
      tGfshInstances.set(shell);
    }
    return shell;
  } 
  
  
  
  public static synchronized EntryOperations getEntryOperations(String region){
    Map<String,EntryOperations> entryOpsMap = (Map<String,EntryOperations>) getEntryOpHT().get();
    if(entryOpsMap==null){
      entryOpsMap = new HashMap<String,EntryOperations>();
      getEntryOpHT().set(entryOpsMap);
    }

    EntryOperations entryOps = entryOpsMap.get(region);      
    if(entryOps==null){
      Region regionObject = cliTestInstance.cache.getRegion(region);
      if(regionObject!=null){
          entryOps = new EntryOperations(regionObject,new EntryEvents());
          entryOps.setKeyValueConfig(getRegionKeyValueConfig());
          entryOpsMap.put(region, entryOps);  
      }else 
        throw new TestException("Region does not exist " + region);          
    }    
    return entryOps;
  }
  
  
  protected static long  logGfshExecutionNumber() {
    long exeNum = CLIBlackboard.getBB().getSharedCounters().incrementAndRead(CLIBlackboard.gfshExecutionNumber);
    Log.getLogWriter().info("Beginning Gfsh task with execution number " + exeNum);
    return exeNum;
  }
  
  protected static long  logGemfireExecutionNumber() {
    long exeNum = CLIBlackboard.getBB().getSharedCounters().incrementAndRead(CLIBlackboard.gemfireExecutionNumber);
    Log.getLogWriter().info("Beginning Gemfire task with execution number " + exeNum);
    return exeNum;
  }
  
  private void doShellCommandTests() {
   
    ShellCommandTest.testConnectDisconnect();
    
    ShellCommandTest.testHelp();
    
    ShellCommandTest.testHint();
    
    ShellCommandTest.testSetVariableAndEcho();
    
    ShellCommandTest.testEncryptPassword();
    
  }
  
  
  private void initializeCLI() {
    //JMXBlackboard.getBB().initBlackBoard();
    CLIBlackboard.getBB().initBlackBoard();
    String clientName = RemoteTestModule.getMyClientName();
    
    if(!clientName.contains("gfsh")){
      createCache();
      //String memberName = CacheHelper.getCache().getDistributedSystem().getDistributedMember().getName();
      String memberName = ManagementUtil.getMemberID();
      FederationBlackboard.getBB().addMemberName("vmId" + RemoteTestModule.getMyVmid(), memberName);
    }
    else{
     // getTestableShell(); //initialize the Gfsh    
    }
    
    //only managed are members
    if(!clientName.contains("edge") && !clientName.contains("managing") && !clientName.contains("gfsh")){
       ManagementUtil.saveMemberMbeanInBlackboard();         
    }
    
    if (!clientName.contains("edge") && !clientName.contains("gfsh") && service == null) {
      service = ManagementService.getManagementService(cache);
      if(!ManagementUtil.checkLocalMemberMBean())
        throw new TestException("Could not find MemberMbean in platform mbean server");
    }
    
    
    
  }
  
  public String selectManagingNode() {
    Collection<String> urls = FederationBlackboard.getBB().getManagingNodes();
    List<String> list = new ArrayList<String>();
    list.addAll(urls);
    return HydraUtil.getRandomElement(list);
  }
  
  
  private void printEvents() {
    HydraVector printEventsList = TestConfig.tab().vecAt(GfshPrms.printEventsList);
    boolean exportRegionEvents = false;
    boolean exportDlocknEvents = false;
    boolean exportFuncnEvents = false;
    for(int i=0;i<printEventsList.size();i++){
      String eventName = (String)printEventsList.get(i);
      if(eventName.contains("REGION"))
        exportRegionEvents = true;
      if(eventName.contains("DLOCK"))
        exportDlocknEvents = true;
      if(eventName.contains("FUNCT"))
        exportFuncnEvents = true;
      
      //TODO : Add other events when required
    }
    
    if(exportRegionEvents){      
      RegionEvents regionEvents = (RegionEvents) getRegionOperations().getOperationRecorder();
      logInfo("Exporting region events");
      regionEvents.exportToBlackBoard(OperationsBlackboard.getBB());
    }
    
    if(exportFuncnEvents){      
      FunctionEvents regionEvents = (FunctionEvents) getFunctionOperations().getOperationRecorder();
      logInfo("Exporting region events");
      regionEvents.exportToBlackBoard(OperationsBlackboard.getBB());
    }
    
    /*if(exportDlocknEvents){
      DLockEvents dlockEvents = (DLockEvents) getDLockOperations().getOperationRecorder();
      logInfo("Exporting dlock events");
      dlockEvents.exportToBlackBoard(OperationsBlackboard.getBB());
    }*/
    
    HydraVector clientNames = TestConfig.tab().vecAt(ClientPrms.names);
    Iterator iterator = clientNames.iterator(); 
    int count =0;
    while(iterator.hasNext()){
      String name = (String) iterator.next();
      if(name.contains("gfsh"))
        count++;
    }
    
    CLIBlackboard.getBB().getSharedCounters().increment(CLIBlackboard.VM_TASKS_COMPLETE_COUNT);
    logInfo("Waiting for even VM to export events to blackboard and updates its counters");
    TestHelper.waitForCounter(CLIBlackboard.getBB(), "TASKS_COMPLETE_COUNT", CLIBlackboard.VM_TASKS_COMPLETE_COUNT, count, true, 10*1000);
    
    
    for(int i=0;i<printEventsList.size();i++){
      String eventName = (String)printEventsList.get(i);    
      Map<String,AbstractEvents> emap = AbstractEvents.importFromBlackBoard(eventName, OperationsBlackboard.getBB());
      if(emap!=null){
        for(Map.Entry<String,AbstractEvents> e : emap.entrySet()){
          String clientName = e.getKey();
          AbstractEvents ae = e.getValue();
          logInfo("Events from client " + clientName);
          ae.printEvents();
        }
      }else
        logInfo("No events found of type " + eventName);
    }    
  }
  
  
  private void createRegionsOnBridge(){
    RegionOperations regionOps = getRegionOperations();
    HydraVector regionList = TestConfig.tab().vecAt(GfshPrms.regionListToStartWith);
    logInfo("Creating regions on gemfire bridges : ");
    int count = 1;
    for(int i=0;i<regionList.size();i++){
      String name = (String)regionList.get(i);
      if(name.contains("Bridge")){
        //String regionName = regionOps.createRegion(name);
        int index = name.indexOf("Bridge");
        String rName = "Test" + name.substring(0, index) + "_"+ count++;
        logInfo("Creating region named " + rName + " with template " + name);
        regionOps.createRegion(rName, name);
        logInfo("Created region named " + rName + " with template " + name);
      }
    }
    RegionEvents regionEvents = (RegionEvents) regionOps.getOperationRecorder();
    regionEvents.exportToBlackBoard(OperationsBlackboard.getBB());
    //regionEvents.clearEvents();
  }
  
  private void createDataRegions(){
    RegionOperations regionOps = getRegionOperations();    
    logInfo("Creating data-regions on gemfire bridges : ");
    String dataRegionCreated = regionOps.createRegion(dataRegionName);
    dataStringRegion = RegionHelper.getRegion(dataRegionCreated);
    if(dataStringRegion==null)
      throw new TestException("Error in creating data region, its null");
    
    for(int i=0;i<1000;i++){
      dataStringRegion.put("KEY_"+i,"VALUE_"+i);
    }
    
    dataRegionCreated = regionOps.createRegion(jsonRegionName);
    dataJsonRegion = RegionHelper.getRegion(dataRegionCreated);
    if(dataJsonRegion==null)
      throw new TestException("Error in creating data region, its null");
    
    int numTopRegions = TestConfig.tab().intAt(GfshPrms.regionHierarchyWidth);
    int numChildRegions = TestConfig.tab().intAt(GfshPrms.regionHierarchyWidth);
    int regionCount = 0;
    int totalRegions = numTopRegions + numTopRegions*numChildRegions;
    regionHierarchyRegions = new Region[totalRegions];
    for(int i=0;i<numTopRegions;i++){
       String genName = regionOps.createRegion(childRegionNames);
       Region topRegion = RegionHelper.getRegion(genName);
       regionHierarchyRegions[regionCount++] = topRegion;
       Region parentRegion = topRegion;
       for(int j=0;j<numChildRegions;j++){
         logInfo("Creating subregion named " + (parentRegion.getName() +"_" + j));
         Region childRegion = parentRegion.createSubregion(parentRegion.getName() +"_" + j, parentRegion.getAttributes());
         logInfo("Created subregion named " + childRegion.getFullPath());
         regionHierarchyRegions[regionCount++] = childRegion;
         getRegionOperations().getOperationRecorder().regionAdded(childRegion);//manually generate events
         parentRegion = childRegion;         
       }
    }
    
    for(int i=0;i<1000;i++){
      Key1 key = new Key1();
      String keyId = "KEY_" + i;  
      key.setId(keyId);
      key.setName(keyId);
      Value2 value = new Value2();
      value.setStateName(key.toString());
      value.setAreaInSqKm(0);
      value.setCapitalCity(key.toString());
      value.setPopulation(0);
      dataJsonRegion.put(key,value);
    }
    
    for(int i=0;i<1000;i++){
      Region region = HydraUtil.getRandomElement(regionHierarchyRegions);
      region.put("KEY_"+i,"VALUE_"+i);
    }
    
    RegionEvents regionEvents = (RegionEvents) regionOps.getOperationRecorder();
    regionEvents.exportToBlackBoard(OperationsBlackboard.getBB());
    
    
    
    /*regionKeyValueConfigHL.set(value)
    EntryOperations eOps = getEntryOperations(dataRegion.getName());
    for(int i=0;i>1000;i++){
      eOps.add();
    }*/
    
    
  }
 
  
  private void createRegionsOnEdge() {
    logInfo("Creating region on gemfire clients");
    RegionOperations regionOps = getRegionOperations();
    HydraVector regionList = TestConfig.tab().vecAt(GfshPrms.regionListToStartWith);
    logInfo("Creating regions on gemfire edges : ");
    int count = 1;
    for(int i=0;i<regionList.size();i++){
      String name = (String)regionList.get(i);
      if(name.contains("Edge")){
        int index = name.indexOf("Edge");        
        String rName = "Test" + name.substring(0, index) + "_"+ count++;        
        logInfo("Creating region named " + rName + " with template " + name);
        regionOps.createRegion(rName, name);
        logInfo("Created region named " + rName + " with template " + name);
      }
    }
  }
  
  
  private void starBridgeServer() {
    if(server==null){
      String bridgeConfig = TestConfig.tab().stringAt(BridgePrms.names);
      server = BridgeHelper.startBridgeServer(bridgeConfig);      
      logInfo("Started cacheServer on " + RemoteTestModule.getMyClientName());
      logInfo("Has server started " + server.isRunning());
    }
  }  
  
  private void registerFunctions() {
    getFunctionOperations().registerFunctions();
    FunctionEvents funcEvents = (FunctionEvents) getFunctionOperations().getOperationRecorder();
    funcEvents.exportToBlackBoard(OperationsBlackboard.getBB());
  }
  
  private void createCache() {
    if (cache == null) {
      String clientName = RemoteTestModule.getMyClientName();      
      String cacheConfig = TestConfig.tab().stringAt(CachePrms.names);
      cache = (GemFireCacheImpl) CacheHelper.createCache(cacheConfig);
      ds = cache.getDistributedSystem();
    }
  }
  
  private void becomeManager() {
    ManagementUtil.saveMemberManagerInBlackboard();
    if(!service.isManager())
      service.startManager();
    
    HydraUtil.sleepForReplicationJMX();
     
    try {
      ManagementUtil.checkIfThisMemberIsCompliantManager(ManagementFactory.getPlatformMBeanServer());
    } catch (IOException e) {
      throw new TestException("Error connecting manager", e);
    } catch (TestException e) {
      throw e; 
    }
    
    /*-
    String connector = TestConfig.tab().stringAt(FederationPrms.rmiConnectorType);
    if (connector == null | "custom".equals(connector)) {
      ManagementUtil.startRmiConnector();
    } */
    
    logInfo("JMX Manager Blackboard ");
    JMXManagerBlackboard.getInstance().print();    
    
    logInfo("JMX Manager Endpoints  " + HydraUtil.ObjectToString(JMXManagerHelper.getEndpoints()));
    
  }
  
  private void startWANSendersAndReceivers() {   
    String receiverConfig = ConfigPrms.getGatewayReceiverConfig();
    HydraVector names = TestConfig.tab().vecAt(GatewaySenderPrms.names);
    Set<GatewayReceiver> rs = GatewayReceiverHelper.createAndStartGatewayReceivers(receiverConfig);
    for(GatewayReceiver r : rs){
      logInfo("Started GatewayReceiver " + r);
    }
    
    String wanDsSenderName = CLIBlackboard.GW_SENDER_ID_LIST + "_" + ManagementUtil.getWanSiteName();
    String wanDsRunningSenderName = CLIBlackboard.GW_SENDER_RUNNING_LIST + "_" + ManagementUtil.getWanSiteName();
    String wanDsStoppedSenderName = CLIBlackboard.GW_SENDER_STOPPED_LIST + "_" + ManagementUtil.getWanSiteName();
    String wanDsPausedSenderName = CLIBlackboard.GW_SENDER_PAUSED_LIST + "_" + ManagementUtil.getWanSiteName();
   
    ///INIT ALL MAPS
    
    if (!CLIBlackboard.getBB().getSharedMap().containsKey(wanDsSenderName))
      CLIBlackboard.getBB().getSharedMap().put(wanDsSenderName, new ArrayList());
    
    if (!CLIBlackboard.getBB().getSharedMap().containsKey(wanDsRunningSenderName))
      CLIBlackboard.getBB().getSharedMap().put(wanDsRunningSenderName, new ArrayList());
    
    if (!CLIBlackboard.getBB().getSharedMap().containsKey(wanDsStoppedSenderName))
      CLIBlackboard.getBB().getSharedMap().put(wanDsStoppedSenderName, new ArrayList());
    
    if (!CLIBlackboard.getBB().getSharedMap().containsKey(wanDsPausedSenderName))
      CLIBlackboard.getBB().getSharedMap().put(wanDsPausedSenderName, new ArrayList());
    
    //33% running, 33% paused, 33% stopped 
    int stopped=(int) (names.size()*(0.33)); 
    int paused = (int) (names.size()*(0.33));
    
    for(Object i : names){
      logInfo("Starting GatewaySender " + i);
      Set<GatewaySender> senders = GatewaySenderHelper.createAndStartGatewaySenders((String)i);
      for(GatewaySender sender : senders){                
        if(HydraUtil.getRandomBoolean()){
          HydraUtil.logInfo("Adding senderId to running list" + sender.getId());
          CLIBlackboard.getBB().addToList(wanDsSenderName, sender.getId());
          CLIBlackboard.getBB().addToList(wanDsRunningSenderName, sender.getId());
        }else{
          if(stopped>0){            
            sender.stop();
            HydraUtil.logInfo("Adding senderId to stopped list" + sender.getId());
            CLIBlackboard.getBB().addToList(wanDsStoppedSenderName, sender.getId());
            stopped--;
          }
          else if(paused>0){
            sender.pause();
            HydraUtil.logInfo("Adding senderId tpo paused list" + sender.getId());
            CLIBlackboard.getBB().addToList(wanDsPausedSenderName, sender.getId());
            paused--;
          }
        }
      }
    }
  }

  
  public static void connectGfshToManagerNode() {
    String url = FederationBlackboard.getBB().getManagingNode();
    Pattern MY_PATTERN = Pattern.compile("service:jmx:rmi:///jndi/rmi://(.*?):(.*?)/jmxrmi");
    Matcher m = MY_PATTERN.matcher(url);   
    String endpoint= null;
    while (m.find()) {
      String s = m.group(1);
      String s2 = m.group(2);
      endpoint = s +"[" + s2 +"]";
    }

    TestableGfsh shell = getTestableShell();
    Log.getLogWriter().info("Connecting shell " + shell + " to managing node hosted at url " + url );
    TestableGfsh.execAndLogCommand(shell, "connect --" + CliStrings.CONNECT__JMX_MANAGER + "=" + endpoint, getGfshOutputFile(),false);
    boolean connected = shell.isConnectedAndReady();
    if (!connected) {
      throw new TestException("connect command failed to connect to manager " + endpoint);
    }
    Log.getLogWriter().info("Successfully connected to managing node ");    
  }
  
  public synchronized static String getChildRegion() {
    if(regionList==null){
      regionList = RegionEvents.getAllRegions();
    }
    
    List<String> childregions = new ArrayList<String>();
    
    for(int i=0;i<regionList.size();i++){
      if(regionList.get(i).contains("Child"))
        childregions.add(regionList.get(i));
    }
    return HydraUtil.getRandomElement(childregions); 
  }

  public synchronized static String getStringRegion() {    
    if(regionList==null){
      regionList = RegionEvents.getAllRegions();
    }
    
    List<String> childregions = new ArrayList<String>();
    
    for(int i=0;i<regionList.size();i++){
      if(regionList.get(i).contains("String"))
        childregions.add(regionList.get(i));
    }
    return HydraUtil.getRandomElement(childregions);       
  }

  public synchronized static String getJSONRegion() {    
    if(regionList==null){
      regionList = RegionEvents.getAllRegions();
    }
    
    List<String> childregions = new ArrayList<String>();
    
    for(int i=0;i<regionList.size();i++){
      if(regionList.get(i).contains("JSON"))
        childregions.add(regionList.get(i));
    }
    return HydraUtil.getRandomElement(childregions);    
  }

  public static void useRegion(TestCommandInstance instnace, String name, String jsonRegion) {    
    instnace.addOption(name, jsonRegion);
    /*
    String regionSelected = getTestableShell().getEnvProperty(CliConstants.ENV_APP_CONTEXT_PATH);
    boolean useRegion = HydraUtil.getRandomBoolean();
    boolean isRegionSelected = !(regionSelected==null || regionSelected.isEmpty() || regionSelected.equals("/")); 
    if(isRegionSelected){
      /*if(useRegion)//
        instnace.addOption(name, jsonRegion);* /
      logInfo("Using context region " + regionSelected);
      
    }else{
      TestableGfsh gfsh = CLITest.getTestableShell();
      if(!gfsh.isConnectedAndReady()){
        CLITest.connectGfshToManagerNode();
      }
      logInfo("Setting context region " + jsonRegion);
      TestableGfsh.execCommand(gfsh, "use region " + jsonRegion, getGfshOutputFile());
      logInfo("Not adding option selected regionPath =" + jsonRegion);
      /*if(useRegion)
        instnace.addOption(name, jsonRegion);* /
    }*/
  }  
 
}
