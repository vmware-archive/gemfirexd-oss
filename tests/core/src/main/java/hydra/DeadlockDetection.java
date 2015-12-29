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
package hydra;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.distributed.internal.deadlock.DeadlockDetector;
import com.gemstone.gemfire.distributed.internal.deadlock.Dependency;
import com.gemstone.gemfire.distributed.internal.deadlock.DependencyGraph;
import com.gemstone.gemfire.distributed.internal.deadlock.DependencyMonitorManager;
import com.gemstone.gemfire.distributed.internal.deadlock.ThreadReference;

/**
 * @author dsmith
 *
 */
public class DeadlockDetection {
  private static final File DEADLOCK_FILE = new File("deadlock.txt");
  private static final File DEPENDENCY_FILE = new File("thread_dependency_graph.ser");
  private static final File DEPENDENCY_FILE_TEXT = new File("thread_dependency_graph.text");

  public static void detectDeadlocks() {
    detectDeadlocks(null);
    
  }

  public static synchronized void detectDeadlocks(ClientRecord failedClient) {
    try {
      if(RemoteTestModule.log != null) {
        RemoteTestModule.log.info("Searching for deadlocks. Hung thread " + failedClient);
      } else if (MasterController.log != null) {
        MasterController.log.info("Searching for deadlocks. Hung thread " + failedClient);
      }
      Map clientVms = ClientMgr.getClientVms();
      if(clientVms == null) {
        //maybe we're running the deadlock detection in a child VM.
        clientVms = RemoteTestModule.Master.getClientVms();
      }
      Collection<ClientVmRecord> vmRecords = clientVms.values();
      ThreadReference failedThread = getFailedThread(failedClient); 
      DeadlockDetector detector = new DeadlockDetector();
      for(ClientVmRecord record : vmRecords) {
        ClientRecord client = record.getRepresentativeClient();
        if(client == null)
          continue;
        MethExecutorResult results = client.getTestModule()
        .executeMethodOnClass(DeadlockDetection.class.getName(),
        "collectDependencies");
        if(results.exceptionOccurred()) {
          throw results.getException();
        }
        Set<Dependency> dependencies = (Set<Dependency>) results.getResult();
        detector.addDependencies(dependencies);
      }
      
      writeDependencyFile(detector.getDependencyGraph());

      LinkedList<Dependency> deadlock = null;
      if(failedThread != null) {
        DependencyGraph graph = detector.findDependencyGraph(failedThread);
        writeDependencies(failedThread, graph);
        deadlock = graph.findCycle();
      }
      
      if(deadlock == null) {
        deadlock = detector.findDeadlock();
      }
      
      if(deadlock != null) {
        writeDeadlock(deadlock);
      }
    } catch (Throwable t) {
      if(RemoteTestModule.log != null) {
        RemoteTestModule.log.error("Error collecting dependencies for deadlock detection", t);
      } else if (MasterController.log != null) {
        MasterController.log.error("Error collecting dependencies for deadlock detection", t);
      }
    }
    
  }
  
  private static ThreadReference getFailedThread(ClientRecord failedClient) throws Throwable {
    if(failedClient == null) {
      return null;
    }
    
    MethExecutorResult result = failedClient.getTestModule().executeMethodOnClass(
        DeadlockDetection.class.getCanonicalName(), "findHydraThread",
        new Object[] { failedClient.getTid() });
    
    if(result.exceptionOccurred()) {
      throw result.getException();
    } else {
      return (ThreadReference) result.getResult();
    }
  }
  


  private static synchronized void writeDeadlock(LinkedList<Dependency> deadlock) {
    StringBuilder out = new StringBuilder();
    out.append("\n==================================================================");
    out.append("\nDeadlock detected!!!");
    out.append("\n==================================================================\n");
    out.append(DeadlockDetector.prettyFormat(deadlock));
    String text = out.toString();
    ResultLogger.writeErrorFile(text);
    FileUtil.appendToFile(DEADLOCK_FILE.getName(), text);
  }

  private static synchronized void writeDependencyFile(DependencyGraph dependencyGraph) throws IOException {
    if ( ! DEPENDENCY_FILE.exists() ) {
      FileOutputStream fos = new FileOutputStream(DEPENDENCY_FILE);
      ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(fos));
      try {
      oos.writeObject(dependencyGraph);
      } finally {
        oos.close();
      }
      
      PrintStream ps = new PrintStream(DEPENDENCY_FILE_TEXT);
      try {
        ps.print(DeadlockDetector.prettyFormat(dependencyGraph));
      } finally {
        ps.close();
      }
    } 
  }
  
  private static void writeDependencies(ThreadReference failedThread,
      DependencyGraph graph) {
    if(!DEADLOCK_FILE.exists()) {
      StringBuilder out = new StringBuilder();
      out.append("\n==================================================================");
      out.append("\nDependencies of hung hydra thread " + failedThread);
      out.append("\n==================================================================\n");
      out.append(DeadlockDetector.prettyFormat(graph));
      String text = out.toString();
      ResultLogger.writeErrorFile(text);
    }
  }
  
  //===================================================================
  //These methods are invoked in hydra client VMs
  //===================================================================
  
  public static ThreadReference findHydraThread(int hydraThreadId) {
    String locality = getLocality();
    Thread[] allThreads = DependencyMonitorManager.getAllThreads();
    for(Thread thread: allThreads) {
      if(thread instanceof HydraThread) {
        if(((HydraThread) thread).getRemoteMod().getThreadId() == hydraThreadId) {
          return DeadlockDetector.getThreadReference(locality, thread);
        }
      }
    }
    
    return null;
  }

  public static Set<Dependency> collectDependencies() {
    String locality = getLocality();
    return DeadlockDetector.collectAllDependencies(locality);
  }
  
  private static String getLocality() {
    String locality = MasterController.getNameFor(RemoteTestModule.getMyVmid(),
        -1, RemoteTestModule.getMyClientName(), RemoteTestModule.getMyHost(),
        RemoteTestModule.getMyPid());
    return locality;
  }
  
  //===================================================================
  //This method is for analyzing the dumped DEPENDENCY_FILE
  //===================================================================
  
  public static void main(String [] args) throws FileNotFoundException, IOException, ClassNotFoundException {
    if(args.length != 1) {
      System.err.println("Usage: java hydra.DeadlockDetection dependency_file.ser");
      System.err.println("Dumps the list of dependencies in human readable format");
    }
    File file = new File(args[0]);
    
    ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(new FileInputStream(file)));
    
    DependencyGraph graph = (DependencyGraph) ois.readObject();
    
    System.out.println(DeadlockDetector.prettyFormat(graph));
  }

}
