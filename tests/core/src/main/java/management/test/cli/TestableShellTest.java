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

import hydra.CacheHelper;
import hydra.CachePrms;
import hydra.Log;
import hydra.TestConfig;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import management.cli.TestableGfsh;
import management.jmx.JMXBlackboard;
import management.test.federation.FederationBlackboard;
import management.test.federation.FederationPrms;
import management.util.HydraUtil;
import management.util.ManagementUtil;
import util.TestException;

import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.management.ManagementService;
import com.gemstone.gemfire.management.internal.cli.CommandManager;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.shell.Gfsh;

public class TestableShellTest {
  
  private static TestableShellTest testInstance;
  private DistributedSystem ds = null;
  private GemFireCacheImpl cache = null;
  private ManagementService service;
  private Map<String,TestableGfsh> shellMap=null;
  //private String[] commands = { "help", "list member", "list region", "hint", "history" };
  private String[] commands = { "help", "history"};
  
  public synchronized static void HydraInitTask_initialize() {
    if (testInstance == null) {
      testInstance = new TestableShellTest();
      testInstance.initialize();
    }
  }
  
  public static void HydraInitTask_becomeManager() {
    testInstance.becomeManager();
  }  
  
  public static void HydraInitTask_createShell() {
    testInstance.createShell();
  }
  
  public static void HydraTask_shellOperations() {
    testInstance.shellOps();
  }
  
  public static void HydraCloseTask_closeShell() {
    testInstance.closeShell();
  }
  
  public void closeShell(){
    TestableGfsh shell = shellMap.get(Thread.currentThread().getName());
    if(shell!=null)
      shell.eof();    
  }
  
  private static void execCommand(String id, TestableGfsh shell, String command) throws IOException, ClassNotFoundException {
    log(id + " : Executing command " + command + " with command Mgr " + CommandManager.getInstance());
    shell.addChars(command).addChars(";").newline();
    shell.waitForOutput();
    if(shell.hasError()){
      log("Command failed with error " + shell.getError());
      throw new TestException("Command failed with error " + shell.getError());
    }else{
      Map completorOutput = shell.getCompletorOutput();
      Map commandOutput = shell.getCommandOutput();
      log(id + " : Command completorOutput for " + command + "--->" + completorOutput);
      log(id + " : Command outputText for " + command + "--->" + shell.getOutputText());
      log(id + " : Command output for " + command + "--->" + commandOutput);
    }
    shell.clearEvents();
  }
  
  private static void log(String string) {
    Log.getLogWriter().info(string);    
  }
  
  public void createShell(){
    String argss[] = {};
    try {
      Gfsh.SUPPORT_MUTLIPLESHELL  = true;
      String shellId = Thread.currentThread().getName();
      TestableGfsh shell =  new TestableGfsh(shellId, true, argss);
      shell.start();
      shellMap.put(Thread.currentThread().getName(), shell);
    } catch (ClassNotFoundException e) {
      HydraUtil.logErrorAndRaiseException("Error starting shell", e);
    } catch (IOException e) {
      HydraUtil.logErrorAndRaiseException("Error starting shell", e);
    }    
  }
  
 

  private void createCache() {
    if (cache == null) {
      String cacheConfig = TestConfig.tab().stringAt(CachePrms.names);
      cache = (GemFireCacheImpl) CacheHelper.createCache(cacheConfig);
    }
  }
  
  private void becomeManager() {
    ManagementUtil.saveMemberManagerInBlackboard();
    service.startManager();
    HydraUtil.sleepForReplicationJMX();
    
    /*
      Check all managed nodes member mbean
      Check manager Mbean
      Check distributed Mbeans
    
    try {
      ManagementUtil.checkIfThisMemberIsCompliantManager(ManagementFactory.getPlatformMBeanServer());
    } catch (IOException e) {
      throw new TestException("Error connecting manager", e);
    } catch (TestException e) {
      throw e; 
    } */
    
    if(!ManagementUtil.checkIfCommandsAreLoadedOrNot())
    	throw new TestException("Member failed to load any commands");
    
    String connector = TestConfig.tab().stringAt(FederationPrms.rmiConnectorType);
    if (connector == null | "custom".equals(connector)) {
      ManagementUtil.startRmiConnector();
    }
    /*try {
      registerMemberMbean();
    } catch (MalformedObjectNameException e) {
      throw new TestException("Error registering memberMBean", e);
    } catch (NullPointerException e) {
      throw new TestException("Error registering memberMBean", e);
    }*/
  }
  
  private void initialize() {
    //Init JMX Blackboard
    JMXBlackboard.getBB().initBlackBoard();
    
    //Read Prms
    createCache();
    ManagementUtil.saveMemberMbeanInBlackboard();
    if (service == null) {
      service = ManagementService.getManagementService(cache);
      if(!ManagementUtil.checkLocalMemberMBean())
        throw new TestException("Could not find MemberMbean in platform mbean server");
    }
    
    shellMap = new HashMap<String,TestableGfsh>();
    
    if(!ManagementUtil.checkIfCommandsAreLoadedOrNot())
    	throw new TestException("Member failed to load any commands");
  }
  
  private void shellOps() {
    // TODO Auto-generated method stub
    String url = FederationBlackboard.getBB().getManagingNode();
    Pattern MY_PATTERN = Pattern.compile("service:jmx:rmi:///jndi/rmi://(.*?):(.*?)/jmxrmi");
    Matcher m = MY_PATTERN.matcher(url);
    //int port=0;
    String endpoint= null;
    while (m.find()) {
        String s = m.group(1);        
        String s2 = m.group(2);
        endpoint = s +"[" + s2 +"]";
    }
    
    TestableGfsh shell = null;
    String shellId = Thread.currentThread().getName();
    log("Connecting shell to managing node hosted at url " + url );
    log("ShellMAp " + shellMap);
    try {
      shell = shellMap.get(Thread.currentThread().getName());
      //shell = (TestableGfsh) Gfsh.getCurrentInstance();
      execCommand(shellId,shell,"connect --" + CliStrings.CONNECT__JMX_MANAGER + "=" + endpoint);
    } catch (IOException e) {
      HydraUtil.logErrorAndRaiseException("Error connecting shell to managing node", e);
    } catch (ClassNotFoundException e) {
      HydraUtil.logErrorAndRaiseException("Error connecting shell to managing node", e);
    }
    boolean connected = shell.isConnectedAndReady();
    if(!connected)
    	throw new TestException("connect command failed to connect to manager " + endpoint);
    log("Successfully connected to managing node ");
    
    try {
      String command = HydraUtil.getRandomElement(commands);
      execCommand(shellId,shell,command);
      command = "disconnect";
      execCommand(shellId,shell,command);
      connected = shell.isConnectedAndReady();
      if(connected)
      	throw new TestException("connect command failed to dis-connect from manager " + endpoint);      
    } catch (IOException e) {
      HydraUtil.logErrorAndRaiseException("Error connecting shell to managing node", e);
    } catch (ClassNotFoundException e) {
      HydraUtil.logErrorAndRaiseException("Error connecting shell to managing node", e);
    }       
  }
  
  
}
