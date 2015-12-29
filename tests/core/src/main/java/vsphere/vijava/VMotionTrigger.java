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
package vsphere.vijava;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.rmi.RemoteException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.StringTokenizer;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.vmware.vim25.HostVMotionCompatibility;
import com.vmware.vim25.TaskInfo;
import com.vmware.vim25.VirtualMachineMovePriority;
import com.vmware.vim25.VirtualMachinePowerState;
import com.vmware.vim25.mo.ComputeResource;
import com.vmware.vim25.mo.Folder;
import com.vmware.vim25.mo.HostSystem;
import com.vmware.vim25.mo.InventoryNavigator;
import com.vmware.vim25.mo.ServiceInstance;
import com.vmware.vim25.mo.Task;
import com.vmware.vim25.mo.VirtualMachine;

/**
 * <pre>
 * <b>How to run:</b>
 * 1. Compile the code. You can use compile-tests target.
 * 2. cd to build-dir/tests/classes
 * 3. Set classpath to include current directory, {checkout-dir}/tests/lib/vijava5b20110825.jar and {checkout-dir}/tests/lib/dom4j-1.6.1.jar
 * 4. Create input file with valid data. Format is specified below.
 * 5. Run "java vsphere.vijava.VMotionTrigger {input-file} [{log_file}] [CONFIG|WARNING|INFO|DEBUG]"
 * 
 * 
 * <b>Format of the input file required by this application:</b> *  
 * A line starting with # is treated as a comment line. Blank lines are not allowed.
 * 
 * # First non-comment line:
 * N                               # Number of VMs participating in vMotion.
 * 
 * # Second non-comment line:
 * vmname1  hostname1  hostname2   # vmname1 will be moved between hostname1 and hostname2.
 * 
 * # (N+1)th non-comment line:
 * vmnameN  hostnameX  hostnameY  hostnameZ
 * 
 * # Next non-comment line:
 * number                          # Interval between two consecutive vMotions. Use "default" for default value of 360 seconds.
 * 
 * # Next non-comment line:
 * number                          # Concurrency of vMotions, 1 <= concurrency <= max(3, N).  Use "default" for default value of 1, meaning only one vMotion will be active at any time.
 * 
 * # Next non-comment line:
 * number                          # Duration in minutes the script would continue running for. Once this time is reached, the script would self-terminate. Default is 60.
 * 
 * </pre>
 */
public class VMotionTrigger {

  private static VMotionTrigger               singleton             = null;

  private ServiceInstance                     si                    = null;

  private ScheduledExecutorService            stpe                  = null;

  private static int                          vMotionInterval       = 360;

  private static int                          vMotionConcurrency    = 1;

  private static int                          scriptDurationMinutes = 60;

  /**
   * Index of the VM in {@link #vmList} which was last picked up by a thread in
   * {@link #stpe}.
   */
  private static int                          lastVMIndex           = -1;

  /**
   * Index of the host a vm was last vMotion'ed to.
   */
  private static int[]                        lastHostIndex;

  /**
   * Keeps track of which VMs are currently being vMotion'ed.
   */
  private static Boolean[]                    vmsInVMotion;

  /**
   * Given list of VMs and their respective hosts participating in vMotion.
   * Element at zeroth index of each internal array list contains the vm name.
   * Subsequent elements contain host names.
   */
  private static ArrayList<ArrayList<String>> vmList             = new ArrayList<ArrayList<String>>();

  private static DateFormat                   df                 = new SimpleDateFormat(
                                                                     "yyyy.MM.dd hh:mm:ss z ");
  private static PrintStream logStream                           = null;

  static final int                            CONFIG             = 0;

  static final int                            WARNING            = 1;

  static final int                            INFO               = 2;

  static final int                            DEBUG              = 3;

  private static int                          activeLogLevel     = 2;

  public static synchronized VMotionTrigger getVMotionTrigger()
      throws RemoteException, MalformedURLException {
    if (singleton != null) {
      return singleton;
    } else {
      return singleton = new VMotionTrigger();
    }
  }

  private VMotionTrigger() throws RemoteException, MalformedURLException {
    String url = "https://w2-gf-vc01.gemstone.com/sdk";
    assert System.console() != null;

    String username = System.console().readLine("%s", "username: ");
    if (username == null || username.trim().equals("")) {
      writeToConsole("User cannot be empty.");
      System.exit(1);
    }

    String password = String.copyValueOf(System.console().readPassword("%s", "password: ")).trim();
    if (password == null || password.trim().equals("")) {
      writeToConsole("Invalid password.");
      System.exit(1);
    }

    si = new ServiceInstance(new URL(url), username.trim(), password.trim(), true);
  }

  public static void main(String[] args) {
    VMotionTrigger trigger = null;
    try {
      if (args == null || args.length < 1 || args.length > 3) {
        writeToConsole("\tUsage: java vsphere.vijava.VMotionTrigger <input_file> [<log_file>] [CONFIG|WARNING|INFO|DEBUG]\n");
        System.exit(1);
      }
      trigger = getVMotionTrigger();
      readInputData(args);
      trigger.createVMotionScheduler();
      trigger.scheduleTasks();

      waitForProgramTermination();

    } catch (RemoteException re) {
      log(WARNING, "Failed to get going. " + re);
    } catch (MalformedURLException mue) {
      log(WARNING, "Failed to get going. " + mue);
    } catch (Throwable e) {
      log(WARNING, "Failed to get going. ", e);
    } finally {
      writeToConsole("Waiting for scheduled vMotion tasks to complete...\n");
      if (trigger != null) {
        trigger.closeTrigger();
      }
      if (logStream != null) {
        PrintStream tmp = logStream;
        logStream = null;
        tmp.close();
      }
      writeToConsole("vMotion Trigger terminated. Thank you.\n");
    }
  }

  private static void waitForProgramTermination() {
    Thread readLine = new Thread(new Runnable() {
      public void run() {
        writeToConsole("\nAt any point during the program execution, press enter to terminate this trigger and return to command prompt.\n");
        System.console().readLine();
      }
    });

    writeToConsole("This trigger will terminate after " + scriptDurationMinutes
        + " minutes.");
    readLine.start();

    boolean exit = false;
    long startTime = System.currentTimeMillis();
    while (!exit) {
      try {
        Thread.sleep(5000);
      } catch (InterruptedException ie) {
        break;
      }
      if (((System.currentTimeMillis() - startTime) >= (scriptDurationMinutes * 60 * 1000))
          || !readLine.isAlive()) {
        exit = true;
      }
    }
    readLine.interrupt();
    try {
      readLine.join(10 * 1000);
    } catch (InterruptedException ie) {
    }
  }

  private static void writeToConsole(String s) {
    System.console().writer().write(s);
    System.console().writer().flush();
    System.console().flush();
  }

  private static void setLogLevel(String[] args) {
    if (args.length == 3) {
      String logLevel = args[2];
      if (logLevel.equalsIgnoreCase("CONFIG")) {
        activeLogLevel = CONFIG;
      } else if (logLevel.equalsIgnoreCase("WARNING")) {
        activeLogLevel = WARNING;
      } else if (logLevel.equalsIgnoreCase("INFO")) {
        activeLogLevel = INFO;
      } else if (logLevel.equalsIgnoreCase("DEBUG")) {
        activeLogLevel = DEBUG;
      } else {
        log("Invalid value for log level. Setting log level to INFO.");
        activeLogLevel = INFO;
      }
    }
  }

  private static void readInputData(String[] args) throws IOException {
    int i = 0, numOfVMs = 0;
    String line = null, badInput = null;
    BufferedReader br = new BufferedReader(new FileReader(new File(args[0])));
    try {
      if (args.length > 1) {
        logStream = new PrintStream(new File(args[1]));
      }
    } catch (FileNotFoundException fnfe) {
      log(WARNING, "Log file could not be created. Using default output stream for logging.");
    }
    setLogLevel(args);
    try {
      while ((line = br.readLine()) != null) {
        if (line.startsWith("#")) {
          continue;
        } else {
          StringTokenizer st = new StringTokenizer(line);
          if (!st.hasMoreTokens()) {
            badInput = "A line either has to be a comment or a data line. No empty lines allowed.";
            break;
          }
          if (i == 0) { // number of vms
            numOfVMs = Integer.parseInt(st.nextToken());
            if (numOfVMs < 1) {
              badInput = "Number of VMs to be moved cannot be less than one.";
              break;
            }
          } else if ((i > 0) && (i <= numOfVMs)) { // vm details
            String vm = st.nextToken();
            for (ArrayList<String> vm1 : vmList) {
              if (vm1.get(0).equalsIgnoreCase(vm)) {
                badInput = "VM " + vm1.get(0) + " is repeated.";
                break;
              }
            }
            if (badInput != null) {
              break;
            }
            ArrayList<String> array = new ArrayList<String>();
            while (st.hasMoreTokens()) {
              String host = st.nextToken();
              for (String host1 : array) {
                if (host1.equalsIgnoreCase(host)) {
                  badInput = "Host " + host + " is repeated for VM " + vm;
                  break;
                }
              }
              if (badInput != null) {
                break;
              }
              array.add(host);
            }
            if (badInput != null) {
              break;
            }
            if (array.size() < 2) {
              badInput = "VM " + vm
                  + " needs atleast two hosts to participate in vMotion.";
              break;
            }
            array.add(0, vm);
            vmList.add(array);
          } else if (i == (numOfVMs + 1)) { // interval
            try {
              String token = st.nextToken();
              if (!token.equalsIgnoreCase("default")) {
                vMotionInterval = Integer.parseInt(token);
              }
            } catch (NumberFormatException nfe) {
              throw new NumberFormatException(
                  "Expected a positive integer for vMotion interval. "
                      + nfe.getMessage());
            }
            if (vMotionInterval < 0) {
              badInput = "Interval between two consecutive vMotions cannot be negative.";
              break;
            } else if (vMotionInterval < 180) {
              log(WARNING, "Interval between two consecutive vMotions is preferred to be more than 180 seconds. It is "
                  + vMotionInterval + " seconds.");
            }
          } else if (i == (numOfVMs + 2)) { // concurrency level
            try {
              String token = st.nextToken();
              if (!token.equalsIgnoreCase("default")) {
                vMotionConcurrency = Integer.parseInt(token);
              }
            } catch (NumberFormatException nfe) {
              throw new NumberFormatException(
                  "Expected a positive integer for vMotion concurrency. "
                      + nfe.getMessage());
            }
            if (vMotionConcurrency < 1
                || vMotionConcurrency > Math.min(numOfVMs, 3)) {
              badInput = "Concurrency in vmotion cannot be less than one AND greater than minimum of three and the number of VMs available for vmotion. It is "
                  + vMotionConcurrency + ".";
              break;
            }
          } else if (i == (numOfVMs + 3)) { // duration in minutes for script to run
            try {
              String token = st.nextToken();
              if (!token.equalsIgnoreCase("default")) {
                scriptDurationMinutes = Integer.parseInt(token);
              }
            } catch (NumberFormatException nfe) {
              throw new NumberFormatException(
                  "Expected a positive integer for duration in minutes of the script. "
                      + nfe.getMessage());
            }
            if (scriptDurationMinutes < 0) {
              badInput = "Duration of the script cannot be negative.";
              break;
            }
          } else {
            // ignore
            break;
          }
          i++;
        }
      }
      if (vmList.isEmpty()) {
        badInput = "Details about VMs placement not specified.";
      }
      if (badInput != null) {
        throw new IllegalArgumentException(badInput);
      }
      printInputData();
    } finally {
      br.close();
    }
  }

  private static void printInputData() {
    StringBuffer sb = new StringBuffer();
    sb.append("Number of VMs for vmotion: " + vmList.size());
    for (ArrayList<String> vm : vmList) {
      sb.append("\n  " + vm.get(0) + ":");
      for (int i = 1; i < vm.size(); i++) {
        sb.append(" " + vm.get(i));
      }
    }
    sb.append("\nInterval between two consecutive vMotions: " + vMotionInterval + " seconds.");
    sb.append("\nvMotion concurrency: " + vMotionConcurrency);
    sb.append("\nvMotion duration: " + scriptDurationMinutes + " minutes.");
    log(DEBUG, sb.toString());
  }

  private void createVMotionScheduler() {
    stpe = new ScheduledThreadPoolExecutor(3, new ThreadFactory() {
          AtomicInteger threadNum = new AtomicInteger();

          public Thread newThread(final Runnable r) {
            Thread result = new Thread(r, "vMotion_Task_"
                + threadNum.incrementAndGet());
            //result.setDaemon(true);
            log(DEBUG, "returning new thread...");
            return result;
          }
        });
        
    vmsInVMotion = new Boolean[vmList.size()];
    for (int i = 0; i < vmsInVMotion.length; i++) {
      vmsInVMotion[i] = Boolean.FALSE;
    }
    lastHostIndex = new int[vmList.size()];
  }

  private void scheduleTasks() {
    for (int i = 0; i < vMotionConcurrency; i++) {
      log(DEBUG, "scheduling a task");
      stpe.scheduleWithFixedDelay(new VMotionTask(), 5, vMotionInterval, TimeUnit.SECONDS);
    }
  }

  private void closeTrigger() {
    if (stpe != null) {
      log(CONFIG, "Shutting down the vmotion trigger...");
      stpe.shutdown();
      try {
        stpe.awaitTermination(420, TimeUnit.SECONDS);
      } catch (InterruptedException ie) {
      }
    }
    if (si != null) {
      si.getServerConnection().logout();
    }
  }

  class VMotionTask implements Runnable {

    public void run() {
      int vmToUse = -1;
      int attempts = 0;
      try {
        synchronized (vmList) {
          while (attempts < vmList.size()) {
            lastVMIndex = (lastVMIndex == (vmList.size() - 1)) ? 0
                : ++lastVMIndex;
            vmToUse = lastVMIndex;
            if (!vmsInVMotion[vmToUse]) {
              vmsInVMotion[vmToUse] = Boolean.TRUE;
              lastHostIndex[vmToUse] = (lastHostIndex[vmToUse] == (vmList
                  .get(vmToUse).size() - 1)) ? 1
                  : ++lastHostIndex[vmToUse];
              break;
            }
            ++attempts;
          }
        }
        if (attempts == vmList.size()) {
          vmToUse = -1; // avoid logic in finally block
          return; // No vm available for vMotion.
        }

        String targetVMName = vmList.get(vmToUse).get(0);
        String newHostName = vmList.get(vmToUse).get(lastHostIndex[vmToUse]);

        Folder rootFolder = si.getRootFolder();
        InventoryNavigator in = new InventoryNavigator(rootFolder);
        HostSystem newHost = (HostSystem)in.searchManagedEntity("HostSystem",
            newHostName);
        if (newHost == null) {
          log(WARNING, "Could not resolve host " + newHostName + ", vMotion to this host cannot be performed.");
          return;
        }

        //dummyVMotion(si, rootFolder, newHost, targetVMName, newHostName);
        migrateVM(si, rootFolder, newHost, targetVMName, newHostName);

      } catch (Exception e) {
        log(WARNING, "Found ", e);
      } finally {
        if (vmToUse != -1) {
          synchronized (vmList) {
            vmsInVMotion[vmToUse] = Boolean.FALSE;
          }
        }
      }
    }

  }

  private static void dummyVMotion(ServiceInstance si, Folder rootFolder,
      HostSystem newHost, String targetVMName, String newHostName) {
    log("Selected host [vm] for vMotion: " + newHostName + " [" + targetVMName
        + "]");
    try {Thread.sleep(120000);} catch(InterruptedException ir) {}
    log("vMotion of " + targetVMName + " to " + newHostName
        + " completed");
  }

  private static boolean migrateVM(ServiceInstance si, Folder rootFolder,
      HostSystem newHost, String targetVMName, String newHostName)
      throws Exception {

    log("Selected host [vm] for vMotion: " + newHostName + " [" + targetVMName
        + "]");
    VirtualMachine vm = (VirtualMachine)new InventoryNavigator(rootFolder)
        .searchManagedEntity("VirtualMachine", targetVMName);
    if (vm == null) {
      log(WARNING, "Could not resolve VM " + targetVMName + ", vMotion of this VM cannot be performed.");
      return false;
    }

    ComputeResource cr = (ComputeResource)newHost.getParent();

    String[] checks = new String[] { "cpu", "software" };
    HostVMotionCompatibility[] vmcs = si.queryVMotionCompatibility(vm,
        new HostSystem[] { newHost }, checks);

    String[] comps = vmcs[0].getCompatibility();
    if (checks.length != comps.length) {
      log(WARNING, "CPU/software NOT compatible, vMotion failed.");
      return false;
    }

    long start = System.currentTimeMillis();
    Task task = vm.migrateVM_Task(cr.getResourcePool(), newHost,
        VirtualMachineMovePriority.highPriority,
        VirtualMachinePowerState.poweredOn);
    if (task.waitForMe() == Task.SUCCESS) {
      long end = System.currentTimeMillis();
      log("vMotion of " + targetVMName + " to " + newHostName
          + " completed in " + (end - start) + "ms. Task result: "
          + task.getTaskInfo().getResult());
      return true;
    } else {
      TaskInfo info = task.getTaskInfo();
      log(WARNING, "vMotion of " + targetVMName + " to " + newHostName
          + " failed. Error details: " + info.getError().getFault());
      return false;
    }
  }

  public static void log(String s) {
    log(INFO, s, null);
  }

  public static void log(int level, String s) {
    log(level, s, null);
  }

  public static void log(int level, String s, Throwable t) {
    if (level > activeLogLevel) {
      return;
    }
    String currentTime = df.format(new Date());
    String levelStr = "";
    switch (level) {
      case 0:
        levelStr = "Config ";
        break;
      case 1:
        levelStr = "Warning ";
        break;
      case 2:
        levelStr = "Info ";
        break;
      case 3:
        levelStr = "Debug ";
        break;
      default:
        break;
    }
    if (logStream != null) {
      logStream.println("\n[" + levelStr + currentTime
          + Thread.currentThread().getName() + "] " + s);
      if (t != null) t.printStackTrace(logStream);
      logStream.flush();
    } else {
      System.out.println("\n[" + levelStr + currentTime
        + Thread.currentThread().getName() + "] " + s);
      if (t != null) t.printStackTrace();
    }
    
  }
}
