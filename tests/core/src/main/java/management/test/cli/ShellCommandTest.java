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

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import management.cli.TestableGfsh;
import management.util.HydraUtil;
import util.TestException;

import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.cli.CommandManager;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;

public class ShellCommandTest {  
  
  public static void testConnectDisconnect(){
    String cmd[] =  {  "describe connection","disconnect"};
    TestableGfsh gfsh = CLITest.getTestableShell();
    if(gfsh.isConnectedAndReady()){
      TestableGfsh.execAndLogCommand(gfsh,cmd[1], CLITest.getGfshOutputFile(),true);
    }
    for(int i=0;i<10;i++){
      CLITest.connectGfshToManagerNode();
      if(!gfsh.isConnectedAndReady())
        throw new TestException("Connect command failed");      
      Object object[] = TestableGfsh.execAndLogCommand(gfsh,cmd[0], CLITest.getGfshOutputFile(),true);
      logOutput(cmd[0],object);
      
      object = TestableGfsh.execAndLogCommand(gfsh,cmd[1], CLITest.getGfshOutputFile(),true);
      logOutput(cmd[0],object);    
    }    
  }
  
  private static void logOutput(String command, Object object[]){
    Map map = (Map) object[0];
    CommandResult result =null;
    Collection values = map.values();
    for(Object r : values){
      if(r instanceof CommandResult){
        result = (CommandResult) r;      
        if(!result.getStatus().equals(Result.Status.OK))
          throw new TestException("Command return status is OK. Command execution has failed");
        else
          HydraUtil.logInfo("Completed exeuction of <" + command + "> successfully");         
      }
    }    
  }
  
  public static void testHelp(){
    Set<String> commands = CommandManager.getExisting().getCommands().keySet();
    TestableGfsh gfsh = CLITest.getTestableShell();
    CLITest.connectGfshToManagerNode();
    if(!gfsh.isConnectedAndReady())
      throw new TestException("Connect command failed");
    
    for(String cmd : commands){
      Object object[] = TestableGfsh.execAndLogCommand(gfsh,"help " + cmd, CLITest.getGfshOutputFile(),true);
      logOutput("help " + cmd,object);      
    }
    
  }
  
  public static void testHint(){
    TestableGfsh gfsh = CLITest.getTestableShell();
    
    if(gfsh.isConnectedAndReady()){
      TestableGfsh.execAndLogCommand(gfsh,"disconnect", CLITest.getGfshOutputFile(),true);
    }
    
    if(gfsh.isConnectedAndReady())
      throw new TestException("Connect command failed");
    
    String topics[] = { "Data",
       "Disk Store",
       "Function Execution",
       "GemFire",
       "Help",
       "JMX",
       "Lifecycle",
       "Locator",
       "Manager",            
       "Region",
       "Server",
       "Statistics",
       "gfsh"   
   };
   
   for(String cmd : topics){
     Object object[] = TestableGfsh.execAndLogCommand(gfsh,"hint " + cmd, CLITest.getGfshOutputFile(),true);
     logOutput("help " + cmd,object);
   }
   
   }
  
  public static void testEncryptPassword(){
    TestableGfsh gfsh = CLITest.getTestableShell();
    
    if(gfsh.isConnectedAndReady()){
      TestableGfsh.execAndLogCommand(gfsh,"disconnect", CLITest.getGfshOutputFile(),true);
    }
    
    String cmd = "encrypt password " + System.nanoTime(); 
    Object object[] = TestableGfsh.execAndLogCommand(gfsh,cmd, CLITest.getGfshOutputFile(),true);
    logOutput(cmd,object);
  }
  
  public static void testSetVariableAndEcho(){
    
    TestableGfsh gfsh = CLITest.getTestableShell();
    
    if(gfsh.isConnectedAndReady()){
      TestableGfsh.execAndLogCommand(gfsh,"disconnect", CLITest.getGfshOutputFile(),true);
    }
    
    for(int i=0;i<10;i++){
      gfsh.setEnvProperty("TESTSYS"+i, "SYS_VALUE"+i);
      String command = "echo \"Hello World! This is $TESTSYS"+i+"\"";
      Object object[] = TestableGfsh.execAndLogCommand(gfsh,command, CLITest.getGfshOutputFile(),true);
    }
    
    for(int i=0;i<10;i++){
      //gfsh.setEnvProperty("TESTSYS"+i, "SYS_VALUE"+i);
      String command = "set variable MYVAR"+i +"="+"VARVAL"+i;
      Object object[] = TestableGfsh.execAndLogCommand(gfsh,command, CLITest.getGfshOutputFile(),true);
      command = "echo \"Hello World! This is $TESTSYS"+i+"\"";
      object = TestableGfsh.execAndLogCommand(gfsh,command, CLITest.getGfshOutputFile(),true);
    }   
    
    Object object[] = TestableGfsh.execAndLogCommand(gfsh,"echo $*", CLITest.getGfshOutputFile(),true);
    
    CLITest.connectGfshToManagerNode();
    String command = "set variable ARBITRARYVAR=qwertyuiop";
    object = TestableGfsh.execAndLogCommand(gfsh,command, CLITest.getGfshOutputFile(),true);
    TestableGfsh.execAndLogCommand(gfsh,"disconnect", CLITest.getGfshOutputFile(),true);
    TestableGfsh.execAndLogCommand(gfsh,"echo $*", CLITest.getGfshOutputFile(),true);
    String property = gfsh.getEnvProperty("ARBITRARYVAR");
    if(property!=null){
      throw new TestException("Bug 45579 : stale session information");
    }    
    
  }
  
  

}
