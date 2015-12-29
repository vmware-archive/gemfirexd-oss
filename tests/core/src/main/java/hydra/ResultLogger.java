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

import hydra.log.LogPrms;
import java.io.*;
import java.util.*;
import perffmwk.*;
import util.*;

/**
 *  This class acts as a handy place for logging results to files.
 */
public class ResultLogger {

  //////////////////////////////////////////////////////////////////////////////
  //    TASK RESULTS
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  Used by clients to log the task result in the client log.  See
   *  {@link RemoteTestModule#executeTask(int,int,int)}.
   */
  protected static void logTaskResult( TestTask task, TestTaskResult result ) {
    String header = "Task result: " + task.toShortString() + ": ";
    if ( result.getErrorStatus() ) {
      Log.getLogWriter().severe( header + result.getErrorString() );
    } else {
      Log.getLogWriter().info( header + result.getResult() );
    }
  }

  /**
   *  Used by master to log the task result reported by a client in the master
   *  log.  See {@link BaseTaskScheduler#reportResult}.  Also writes the error,
   *  if any, to the error file.
   */
  protected static void logTaskResult( ClientRecord client,
                                       TestTask task, TestTaskResult result ) {
    String header = "Result for " + client + ": " + task.toShortString() + ": ";
    if ( result.getErrorStatus() ) {
      Log.getLogWriter().severe( header + result.getErrorString() );

      String err = "CLIENT " + client + "\n"
                 + task.toShortString() + "\n"
                 + result.getErrorString();
      writeErrorFile( err );

    } else {
      Log.getLogWriter().info( header + result.getResult() );
    }
  }

  /**
   *  Used by master to log a client-reported hang in the master log.  See
   *  {@link BaseTaskScheduler#reportResult}.  Also writes the hang to the
   *  hang file.
   */
  protected static void logHangResult( ClientRecord client,
                                       TestTask task, TestTaskResult result ) {
    String header = "Result for " + client + ": " + task.toShortString() + ": ";
    if ( result.getErrorStatus() ) {
      Log.getLogWriter().severe( header + result.getErrorString() );

      String err = "CLIENT " + client + "\n"
                 + task.toShortString() + "\n"
                 + result.getErrorString();
      writeErrorFile( err );
      writeHangFile();
      DeadlockDetection.detectDeadlocks(client);

    } else {
      throw new HydraInternalException("Attempt to log a hang with no error");
    }
  }

  /**
   *  Used by master to log a client hang in the master log.  Also writes the
   *  hang to the hang file.  See {@link
   *  BaseTaskScheduler#maxResultWaitSec(ClientRecord,long)}.
   */
  protected static void logHangResult( ClientRecord client,
                                       TestTask task, String msg ) {
    String header = "Result for " + client + ": " + task.toShortString() + ": ";
    Log.getLogWriter().severe( header + "HANG " + msg );

    String err = "CLIENT " + client + "\n"
               + task.toShortString() + "\n"
               + "HANG " + msg + "\n";
    writeErrorFile( err );
    writeHangFile();
    DeadlockDetection.detectDeadlocks(client);
  }

  //////////////////////////////////////////////////////////////////////////////
  //    ERROR AND HANG FILES
  //////////////////////////////////////////////////////////////////////////////

  private static final String ERROR_FILE = "errors.txt";
  private static final Object ERROR_FILE_LOCK = new Object();
  private static final String HANG_FILE = "hang.txt";
  private static final Object HANG_FILE_LOCK = new Object();
  private static final String divider =
         "----------------------------------------" +
         "----------------------------------------\n";

  /**
   *  Writes the message to the error file.
   */
  static void writeErrorFile( String msg ) {
    synchronized( ERROR_FILE_LOCK ) {
      FileUtil.appendToFile( ERROR_FILE, msg + divider );
    }
  }

  /**
   *  Writes the message to the hang file.
   */
  private static void writeHangFile() {
    synchronized( HANG_FILE_LOCK ) {
      if ( ! FileUtil.exists( HANG_FILE ) ) {
        FileUtil.appendToFile( HANG_FILE,
                "possible hang in test...processes left running..." +
                "see " + ERROR_FILE + " for more info\n" );
      }
    }
  }

  /**
   *  Used by master to report errors in resource startup and unexpected
   *  exceptions encountered by the test harness.
   */
  protected static void reportErr( String msg, Throwable t ) {
    String err = "ERROR " + msg + "\n";
    if ( t != null ) {
      err += "\n" + TestHelper.getStackTrace(t);
    }
    Log.getLogWriter().severe( err );
    err = "THREAD " + Thread.currentThread().getName() + "\n" + err;
    writeErrorFile( err );
  }

  /**
   *  Used by master to report hangs in resource startup.
   */
  protected static void reportHang( String msg, Throwable t ) {
    String err = "HANG ";
    if ( msg == null && t != null ) {
      err += t.getMessage() + "\n" + TestHelper.getStackTrace(t);
    } else if ( msg != null && t == null ) {
      err += msg + "\n";
    } else if ( msg != null && t != null ) {
      err += msg + "\n" + TestHelper.getStackTrace(t);
    } else {
      err += "no information available\n";
    }
    Log.getLogWriter().severe( err );
    err = "THREAD " + Thread.currentThread().getName() + "\n" + err;
    writeErrorFile( err );
    writeHangFile();
    DeadlockDetection.detectDeadlocks();
  }

  /**
   * Used by master to report errors in the WindowTester/GFMon VM result file.
   */
  protected static void reportErr(int pid, String msg) {
    String err = "CLIENT WindowTester/GFMon VM pid=" + pid + "\n"
               + "ERROR " + msg;
    writeErrorFile(err);
  }

  /**
   * Used by master to report hangs in the WindowTester/GFMon VM result file.
   */
  protected static void reportHang(int pid, String msg) {
    String err = "CLIENT WindowTester/GFMon VM pid=" + pid + "\n"
               + "HANG " + msg;
    writeErrorFile(err);
    writeHangFile();
    DeadlockDetection.detectDeadlocks();
  }

  //////////////////////////////////////////////////////////////////////////////
  //    FINAL OUTCOME PROCESSING SUPPORT METHODS
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  Used by master to see if the test had errors (includes hangs).
   */
  protected static boolean hasErrorFile() {
    synchronized( ERROR_FILE_LOCK ) {
      return FileUtil.exists( ERROR_FILE );
    }
  }

  /**
   *  Used by master and clients to see if the test had a hang.
   */
  protected static boolean hasHangFile() {
    synchronized( HANG_FILE_LOCK ) {
      return FileUtil.exists( HANG_FILE );
    }
  }

  /**
   *  Used by master to report unexpected exceptions in the test harness.
   *  @param m The method in which the exception occurred.
   *  @param t The exception.
   */
  protected static void reportAsErr( String m, Throwable t ) {
    String err = m + " -- unexpected exception";
    reportErr( err, t );
  }

  /**
   *  Used by master to report unexpected problems treated as hangs by the
   *  test harness.
   */
  protected static void reportAsHang( String msg, Throwable t ) {
    reportHang( msg + " -- treating as hang", t );
  }

  //////////////////////////////////////////////////////////////////////////////
  //    FINAL OUTCOME PROCESSING
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  Used by master to log the final test outcome.
   *  @param msg The test signoff message.
   *  @param passed Whether the test passed.
   */
  protected static void logFinalOutcome( String msg, boolean passed ) {
    Log.getLogWriter().severe( msg );
    if (!passed) {
      try {
        FileUtil.createNewFile(FAILED_FILE);
      } catch (IOException e) {
        String s = "Unable to create new file " + FAILED_FILE;
        throw new HydraRuntimeException(s, e);
      }
    }
  }
  private static final String FAILED_FILE = "failed.txt";

  //////////////////////////////////////////////////////////////////////////////
  //    PERFORMANCE REPOR
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  Generates the performance report and writes it out.
   */
  protected static void generatePerformanceReportFile() {
    ClientDescription cd = new ClientDescription();
    cd.setName( "perfreporter" );
    MasterDescription md = TestConfig.getInstance().getMasterDescription();
    cd.setVmDescription( md.getVmDescription() );
    String heap = "-Xmx" + TestConfig.tab().stringAt( PerfReportPrms.heapSize );

    String brief = "-DBrief=" +
      TestConfig.tab().booleanAt(PerfReportPrms.generateBriefReport, false);

    cd.getVmDescription().setExtraVMArgs( heap + " " + brief );

    int pid = Java.java( cd, "perffmwk.PerfReporter" );
    Log.getLogWriter().info( "Done generating performance report...see "
           + md.getVmDescription().getHostDescription().getUserDir()
           + File.separator + "perfreport.txt for the result."
           + " The performance report log is bgexec*_" + pid + ".log." );
    int maxWaitSec = TestConfig.tab().intAt( PerfReportPrms.maxReportWaitSec );
    HostDescription hd = cd.getVmDescription().getHostDescription();
    if ( ProcessMgr.waitForDeath( hd.getHostName(), pid, maxWaitSec ) ) {
      Nuker.getInstance().removePID(hd, pid );
    } else {
      Log.getLogWriter().warning( "Waited more than " + maxWaitSec + " seconds for performance report to complete, try increasing " );
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  //    MERGE LOGS
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  Uses {@link com.gemstone.gemfire.internal.MergeLogFiles} to merge test
   *  log files into a single file, "mergedLogs.txt".  The files
   *  are assumed to end in <code>.log</code> and reside in the master user
   *  directory and system logs (either local or remote).
   *
   *  @param passed
   *         Did the test pass?
   */
  public static void mergeLogFiles( boolean passed ) {
    String fn = "mergedLogs.txt";
    String value =
      TestConfig.tab().stringAt( LogPrms.mergeLogFiles, "false" );

    if ( value.equalsIgnoreCase("true") ||
         (value.equalsIgnoreCase("onFailure") && !passed) ) {
      try {
        String masterUserDir = System.getProperty( "user.dir" );
        List logFiles = TestFileUtil.getMergeLogFiles(masterUserDir);
        int pid = Java.javaMergeLogFiles( fn, logFiles );
        Log.getLogWriter().info( "Done merging log files...see " + fn
           + " for the result."
           + " The merge process log is bgexec*_" + pid + ".log." );
      } 
      catch (VirtualMachineError e) {
        // Don't try to handle this; let thread group catch it.
        throw e;
      }
      catch( Throwable t ) {
        t.printStackTrace();
      }
    }
  }
}
