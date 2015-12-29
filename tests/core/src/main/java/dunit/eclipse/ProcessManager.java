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
package dunit.eclipse;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.rmi.NotBoundException;
import java.util.Arrays;

import com.gemstone.gemfire.internal.FileUtil;
import com.gemstone.gemfire.internal.sequencelog.EntryLogger;
import com.gemstone.gemfire.internal.sequencelog.SequenceLoggerImpl;

/**
 * @author dsmith
 *
 */
public class ProcessManager {
  
  private int namingPort;
  private Process[] processes;
  private volatile boolean shuttingDown;

  public ProcessManager(int numVMs, int namingPort) {
    this.namingPort = namingPort;
    processes = new Process[numVMs]; 
  }
  
  public void launchVMs() throws IOException, NotBoundException {
    //launch other vms
    int debugPort = Integer.getInteger("dunit.debug.port", 0);
    for (int i = 0; i < processes.length; i++) {
      String[] cmd = buildJavaCommand(i, namingPort, debugPort);
      System.out.println("Executing " + Arrays.asList(cmd));
      File workingDir = new File(DUnitLauncher.DUNIT_DIR, "vm" + i);
      FileUtil.delete(workingDir);
      workingDir.mkdirs();
      //TODO - delete directory contents, preferably with commons io FileUtils
      processes[i] = Runtime.getRuntime().exec(cmd, null, workingDir);
      linkStreams(i, processes[i].getErrorStream(), System.err);
      linkStreams(i, processes[i].getInputStream(), System.out);
      if (debugPort != 0) {
        debugPort++;
      }
    }
  }
  
  public void killVMs() {
    shuttingDown = true;
    for(int i = 0; i < processes.length; i++) {
      if(processes[i] != null) {
        //TODO - stop it gracefully? Why bother
        processes[i].destroy();
      }
    }
  }
  
  public Process[] getProcesses() {
    return processes;
  }
  
  private void linkStreams(final int vmNum, final InputStream in, final PrintStream out) {
    Thread ioTransport = new Thread() {
      public void run() {
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        try {
          String line = reader.readLine();
          while(line != null) {
            out.print("[vm-" + vmNum + "]");
            out.println(line);
            line = reader.readLine();
          }
        } catch(Exception e) {
          if(!shuttingDown) {
            out.println("Error transporting IO from child process");
            e.printStackTrace(out);
          }
        }
      }
    };

    ioTransport.setDaemon(true);
    ioTransport.start();
  }

  private String[] buildJavaCommand(int vmNum, int namingPort, int debugPort) {
    String suspendVM = System.getProperty("dunit.debug.suspend.vm");
    String suspendDebug = "n";
    if (suspendVM != null) {
      if ((vmNum + "").equals(suspendVM)) {
        suspendDebug = "y";
      }
    }
    String cmd = System.getProperty( "java.home" ) + File.separator + "bin" + File.separator + "java";
    String classPath = System.getProperty("java.class.path");
//    String tmpDir = System.getProperty("java.io.tmpdir");
    return new String[] { cmd, "-classpath", classPath,
        "-D" + DUnitLauncher.RMI_PORT_PARAM + "=" + namingPort,
        "-D" + DUnitLauncher.VM_NUM_PARAM + "=" + vmNum,
        "-D" + SequenceLoggerImpl.ENABLED_TYPES_PROPERTY + "=all",
        "-D" + EntryLogger.TRACK_VALUES_PROPERTY + "=false",
        "-DDistributionManager.VERBOSE=true",
//        "-Dgemfire.verbose-lru=true",
        "-D" + DUnitLauncher.WORKSPACE_DIR_PARAM + "=" + new File(".").getAbsolutePath(),
        "-Djava.library.path=" + System.getProperty("java.library.path"),
        "-agentlib:jdwp=transport=dt_socket,server=y,suspend=" + suspendDebug + ",address=" + debugPort,
        "-ea",
        "-Djava.awt.headless=true",

        "dunit.eclipse.ChildVM" };
  }

}
