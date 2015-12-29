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

package util;

import com.gemstone.gemfire.LogWriter;
import hydra.*;
import java.util.*;
import perffmwk.*;

/**
 *
 *  Provides methods to do non-GemFire work until cpu active is
 *  within a specified range.  
 *  @author jfarris
 *  @since 5.0
 *
 */

public class CpuLoad {


  // number of work threads required to get to requested cpu load
  protected static HydraThreadLocal numWorkThreads  = new HydraThreadLocal();  

  // The singleton instance of CpuLoad in this VM 
  protected static CpuLoad cpuLoad;

  protected static final long workThreadSleepTime = CpuLoadPrms.getSleepMsNonGemFire();

  /**
   *  Hydra task to initialize number of worker threads required to reach requested
   *  CPU load.
   *
   *  @author jfarris
   *  @since 5.0
   */
  public static void initNonGemFireWorkTask() {
    if (CpuLoadPrms.doWorkTask()) {
      if (cpuLoad == null) {
         cpuLoad  = new CpuLoad();
         cpuLoad.initNonGemFireWork(); 
      }
    }
  }

  /**
   *  Determines number of worker threads required to reach requested
   *  CPU load.
   *
   *  @author jfarris
   *  @since 5.0
   */
  protected void initNonGemFireWork() {

    // duration of work task
    double  meanCpuLoad = 0;
    int     minReqdCpuLoad = CpuLoadPrms.getCpuLoadMin();
    int     maxReqdCpuLoad = CpuLoadPrms.getCpuLoadMax();
    //final   long workThreadSleepTime = CpuLoadPrms.getSleepMsNonGemFire();
    Vector  workThreads = new Vector();
    boolean metReqdLoad = false;

    long startTime = System.currentTimeMillis();
    long elapsedTime = 0;

    while( ! metReqdLoad ) {
      if ( meanCpuLoad < minReqdCpuLoad ) {
	Thread workThread = getWorkerThread();
        workThreads.add( workThread );
        log().fine("#### Starting work thread: " + workThread.getName());
        ((Thread)workThreads.lastElement()).start();
      
        // get mean cpu active for 10 sec interval
        long t1 = System.currentTimeMillis();
        MasterController.sleepForMs( 10000 );
        long t2 =  System.currentTimeMillis();
        meanCpuLoad = getMeanCpuActive( t1, t2);
        log().info( "Mean CPU Active: " + meanCpuLoad  );
        elapsedTime = System.currentTimeMillis() - startTime;
      } else if ( meanCpuLoad > maxReqdCpuLoad ) {
	throw new TestException("Test exceeded non-GemFire cpu load target: " + maxReqdCpuLoad +
                                   ", mean cpu active: " + meanCpuLoad);
      } else {
        metReqdLoad = true;
        break;
      }
    }
    numWorkThreads.set(new Integer(workThreads.size()));      

    log().info("### Met required CPU Load: " + metReqdLoad + ", mean cpu active: " + meanCpuLoad);
    //log().info("### Number of worker threads (sleeping for 60 sec): " + (workThreads.size()));
    //  MasterController.sleepForMs(60000);



    for (int i = 0; i < workThreads.size(); i++) {
      ((Thread)workThreads.get(i)).interrupt();
    }
  }  

  /**
   *  Hydra task to start worker threads required to reach requested
   *  CPU load.
   *
   *  @author jfarris
   *  @since 5.0
   */
  public static void doNonGemFireWorkTask() {
    if (CpuLoadPrms.doWorkTask()) {
      cpuLoad.doNonGemFireWork(); 
    }
  }

  /**
   *  Start number of worker threads specified by numWorkThreads
   *  CPU load and continue running until end of test.
   *
   *  @author jfarris
   *  @since 5.0
   */

  public void doNonGemFireWork() {

    // duration of work task
    long    workSec = CpuLoadPrms.getWorkSec();
    long    workMs = workSec * TestHelper.SEC_MILLI_FACTOR;

    Vector  workThreads = new Vector();

    long startTime = System.currentTimeMillis();
    long elapsedTime = 0;

    // start worker threads
    Integer numThreadsToStart = (Integer)(numWorkThreads.get()); 
    log().info("Number of work threads to start for NonGemFireWork: " + numThreadsToStart);
    for (int i = 0; i < numThreadsToStart.intValue(); i++) {
      Thread workThread = getWorkerThread();
      workThreads.add( workThread );
      ((Thread)workThreads.lastElement()).start();
      log().info("start worker thread: " + i);
    }

    log().fine("duration of work task will be: " + workSec);
    
    elapsedTime = System.currentTimeMillis() - startTime;
    log().fine("elaspsed time after starting worker threads:  " + elapsedTime);
    long timeUntilEnd = workMs - elapsedTime;
    if (timeUntilEnd > 0) {    
      // let work threads run until test is complete
      log().info("Non-GemFire work running until end of test in: " + timeUntilEnd + " ms");
      MasterController.sleepForMs( (int) timeUntilEnd );
    }
    elapsedTime = System.currentTimeMillis() - startTime;
    log().fine("#### Stopping CPU work task");
    log().fine("Elapsed time: " + elapsedTime);
    for (int i = 0; i < workThreads.size(); i++) {

      ((Thread)workThreads.get(i)).interrupt();
    }

  }

  /**
   *  Gets thread for non-GemFire work:  indexOf on random string.
   *  String length and sleep time after operation are
   *  configurable.
   *
   *  @author jfarris
   *  @since 5.0
   */

  private Thread getWorkerThread() {

    Thread workThread = new Thread( new Runnable() {
      public void run () {
        try {
          RandomValues randVals = new RandomValues();
          while (true) {
            String str = randVals.getRandom_String();
            int i = str.indexOf("str2");
            //int int1 = randVals.getRandom_int();
            //int int2 = randVals.getRandom_int();
            //int int3 = int1 * int2;
            Thread.sleep( workThreadSleepTime );
	  }
        } catch (InterruptedException e) {
	  log().fine("Non-GemFire work thread interrupted");
        }
      }
    });
    return workThread;
    
  }


  /**
   *  Returns the average CPU used on the local host during the specified time interval.
   *  
   *  @author Lise Storc
   *  @since 5.0
   *  @param startTime  start of time interval for CPU active stats
   *  @param endTime  end of time interval for CPU active  stats 

   */

  public static double getMeanCpuActive( long startTime, long endTime ) {
    String clientName = System.getProperty(ClientPrms.CLIENT_NAME_PROPERTY);
    ClientDescription cd = TestConfig.getInstance().getClientDescription(clientName);
    //String archive = cd.getGemFireDescription().getName() + "_" + RemoteTestModule.getMyPid();
    String archive = cd.getGemFireDescription().getName() + "*";

   String osName = System.getProperty("os.name");
   String specType;
    if (osName.equals("SunOS")) {
      specType = "SolarisSystemStats ";
    } else if (osName.startsWith("Windows")) {
      specType = "WindowsSystemStats ";
    } else if (osName.startsWith("Linux")) {
      specType = "LinuxSystemStats ";
    } else {
      throw new TestException("Unable to generate average CPU active:  Unsupported OS in \"" + osName + "\". Supported OSs are: SunOS(sparc Solaris), Linux(x86) and Windows.");
    }

    TrimSpec trim = new TrimSpec( "runtime_MeanCpuActive" );
    trim.start( startTime );
    trim.end( endTime );
    log().fine( "HEY: trim is " + trim );

    String spec = archive + " " // search the archive for this vm only
	//      + "LinuxSystemStats "
	        + specType
                + "* " // match all instances of the stat (there is only one)
                + "cpuActive " 
                + StatSpecTokens.FILTER_TYPE + "=" + StatSpecTokens.FILTER_NONE + " "
                + StatSpecTokens.COMBINE_TYPE + "=" + StatSpecTokens.RAW + " "
                + StatSpecTokens.OP_TYPES + "=" + StatSpecTokens.MEAN;
    log().fine( "HEY: spec is " + spec );
    List psvs = PerfStatMgr.getInstance().readStatistics( spec, trim );
    log().fine( "HEY: psvs are " + psvs );
    PerfStatValue psv = (PerfStatValue) psvs.get(0);
    double mean = psv.getMean();
    log().fine( "HEY: mean cpu active is " + mean + " for trim " + trim );
    return mean;
  }

  /**
   *  Gets the log writer.
   */
  protected static LogWriter log() {
    return Log.getLogWriter();
  }
        
}
