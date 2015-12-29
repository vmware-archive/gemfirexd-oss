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
package management.cli;

import hydra.Log;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import management.util.HydraUtil;

import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.cli.CommandManager;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;
import com.gemstone.gemfire.management.internal.cli.shell.Gfsh;


/*-
 * 
 * Sample class for demonstrating how to use TestableShell
 * 
 */
public class TestCLI {

  private static void execCommand(String id, TestableGfsh shell, String command) throws IOException, ClassNotFoundException {
	  
    Util.log(id + " : Executing command " + command);
    shell.addChars(command).addChars(";").newline();
    shell.waitForOutput();
    if(shell.hasError()){
      Util.log("Command failed with error " + shell.getError());
      System.exit(0);
    }else{
      Map completorOutput = shell.getCompletorOutput();
      Map commandOutput = shell.getCommandOutput();
      Util.debug(id + " : Command completorOutput for " + command + "--->" + completorOutput);
      Util.debug(id + " : Command outputText for " + command + "--->" + shell.getOutputText());
      Util.log(id + " : Command output for " + command + "--->" + commandOutput);
      Collection values = commandOutput.values();
      for(Object r : values){
    	  if(r instanceof CommandResult){
    		  CommandResult result = (CommandResult) r;
    		  Util.log(id + " : CommandResult  " + HydraUtil.ObjectToString(result));    		  
    	  }else if(r instanceof Result){
    		  Result result = (Result)r;
    		  Util.log(id + " : result status " + result.getStatus());
    		  Util.log(id + " result in toString format " + result.toString());
    	  }
      }
      
    }
    shell.clearEvents();
  }
  
  private static void execCommandWithTestConnect(String id, TestableGfsh shell, String command) throws IOException, ClassNotFoundException{
	  execCommand(id,shell, "connect");
      boolean connected = shell.isConnectedAndReady();
      if(connected){
    	  execCommand(id,shell, command);
	      execCommand(id,shell, "disconnect");
	      connected = shell.isConnectedAndReady();
	      if(connected){
	    	  Util.error("Connect command failed to dis-connect to manager");
	    	  System.exit(0);
	      }
      }else{
    	  Util.error("Connect command failed to connect to manager");
    	  System.exit(0);
      }
  }
  
  public static class ShellThread implements Runnable{
	  
	private TestableGfsh gfsh=null;  
	private String commands[] =null;
	private int numIterations = 10;
	private String id=null;
	private CountDownLatch latch=null;
	
	public void setLatch(CountDownLatch latch) {
		this.latch = latch;
	}

	public void setNumIterations(int numIterations) {
		this.numIterations = numIterations;
	}

	public void setCommands(String[] commands) {
		this.commands = commands;
	}
	
	public void setTimeout(long t){
		gfsh.setTimeout(t);
	}

	public ShellThread(String id) throws ClassNotFoundException, IOException{
		this.id = id;
		String argss[] = {};
		gfsh= new TestableGfsh(id, true, argss);
		gfsh.start();
	}

	@Override
	public void run() {		
		for (int i = 0; i < numIterations; i++) {
	      //if(i%100==0)
	    	  Util.error("Executing command number " + i);
	      String command = HydraUtil.getRandomElement(commands);
	      try {
			execCommandWithTestConnect(id,gfsh, command);
			latch.countDown();
			} catch (IOException e) {
				Util.error(e);
				System.exit(0);
			} catch (ClassNotFoundException e) {
				Util.error(e);
				System.exit(0);
			}
	    }
	    Util.log("Terminating shell - " + id);
	    gfsh.eof();
	    Util.log("Terminated shell - " + id);
	}	  
  }

  public static void main(String[] args) throws IOException, ClassNotFoundException {
	  
	Log.createLogWriter( "test", args[0] );
    Gfsh.SUPPORT_MUTLIPLESHELL = true;
    TestableGfsh.setVirtualTerminal(false);
    Set set = CommandManager.getInstance().getCommands().keySet();
    Util.log("Total command loaded " + set.size());
    Util.log("Commands : " + HydraUtil.ObjectToString(set));

    // check-auto complete
    //execCommand("Shell1", shell, "\t");
    //execCommand("Shell2",shell2, "\t");
    
    /*- 
     * Check timeouts with sleep command
    shell.setTimeout(15);
    execCommand("shell1", shell, "sleep");
    
    shell.setTimeout(5);
    Util.log("Expecting sleep timeout error");
    execCommand("shell1", shell, "sleep");
    Util.log("Completed sleep test");*/

    //execCommand("Shell1", shell, "connect");    
    //execCommand("Shell2",shell2, "connect");
    
    int NUM_COMMANDS = Integer.parseInt(args[1]);
    int NUM_THREAD = Integer.parseInt(args[2]);
    
    Util.log("Starting with logLevel " +args[0] + " Threds " + NUM_THREAD + " iterations " + NUM_COMMANDS);
    
    CountDownLatch latch = new CountDownLatch(NUM_COMMANDS*NUM_THREAD);
    String commands[] = { "help", "history", "list member", "list region"};
    List<Thread> threads = new ArrayList<Thread>();
    for(int i=0;i<NUM_THREAD;i++){
    	ShellThread thread1 = new ShellThread("ShellThread"+i);
    	thread1.setCommands(commands);
        thread1.setLatch(latch);
        thread1.setNumIterations(NUM_COMMANDS);
        thread1.setTimeout(5);
        Thread t1 = new Thread(thread1);
        t1.setName("ShellThread-"+i);
        threads.add(t1);
    }
    
    for(Thread t : threads)
    	t.start();
    try {
		latch.await();
	} catch (InterruptedException e) {
		e.printStackTrace();
		Util.error(e);
	}    
    /*-
    shell2.setTimeout(5);
    for (int i = 0; i < NUM_COMMANDS; i++) {
      if(i%10==0)
    	  Util.error("Executing command number " + i);
      String command = HydraUtil.getRandomElement(commands);
      execCommandWithTestConnect("Shell2",shell2, command);
      execCommandWithTestConnect("Shell1",shell, command);      
    }*/    
  }

}
