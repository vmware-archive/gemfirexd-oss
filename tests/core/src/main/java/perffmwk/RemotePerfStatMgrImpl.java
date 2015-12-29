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

package perffmwk;

import hydra.*;

import java.io.*;
import java.rmi.*;
import java.rmi.server.*;
import java.text.DecimalFormat;
import java.util.*;

import util.*;

/**
 * This class implements a remote object for aggregating information
 * about performance.  It should not be accessed directly by test code.
 * Use {@link PerfStatMgr#getInstance()} instead.
 *
 * @author Lise Storc
 * @author David Whitlock
 *
 */
public class RemotePerfStatMgrImpl extends UnicastRemoteObject
implements RemotePerfStatMgr {

  /** The name under which the instance of
   * RemotePerfStatMgrImpl is bound in the RMI registry */
  public static final String RMI_NAME = "RemotePerfStatMgr";

  /** Used for formatting numbers */
  private static final DecimalFormat format = new DecimalFormat("###.###");

  /** Used to generated the next available underlying statistic */
  private static int nextPerfStatsIndex = -1;

  private static final String COMMENT_DIVIDER =
    "\n//==============================================================================";

  //////////////////////////////////////////////////////////////////////////////
  ////    Instance Fields                                                   ////
  //////////////////////////////////////////////////////////////////////////////

  /** Holds the statistics configuration for the test */
  private StatConfig statconfig;

  //////////////////////////////////////////////////////////////////////////////
  ////    Constructors                                                      ////
  //////////////////////////////////////////////////////////////////////////////

  public RemotePerfStatMgrImpl() 
  throws RemoteException {
    super(); // Do RMI magic
    this.statconfig = new StatConfig( System.getProperty( "user.dir" ) );
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    Remote Instance Methods                                           ////
  //////////////////////////////////////////////////////////////////////////////

  /**
   * @see RemotePerfStatMgr#startTrim(String)
   */
  public void startTrim( String trimspecName ) 
  throws RemoteException {
    String name = trimspecName;
    if ( name == null ) {
      name = TrimSpec.DEFAULT_TRIM_SPEC_NAME;
    }
    TrimSpec trimspec = this.statconfig.getTrimSpec( name );
    trimspec.start();
  }

  /**
   * @see RemotePerfStatMgr#endTrim(String)
   */
  public void endTrim( String trimspecName ) 
  throws RemoteException {
    String name = trimspecName;
    if ( name == null ) {
      name = TrimSpec.DEFAULT_TRIM_SPEC_NAME;
    }
    TrimSpec trimspec = this.statconfig.getTrimSpec( name );
    trimspec.end();
  }

  /**
   * @see RemotePerfStatMgr#reportTrimInterval(TrimInterval)
   */
  public synchronized void reportTrimInterval(TrimInterval interval)
  throws RemoteException {
    String name = interval.getName();
    TrimSpec trimspec = this.statconfig.getTrimSpec(name);
    trimspec.start(interval.getStart());
    trimspec.end(interval.getEnd());
  }

  /**
   * @see RemotePerfStatMgr#reportExtendedTrimInterval(TrimInterval)
   */
  public synchronized void reportExtendedTrimInterval(TrimInterval interval)
  throws RemoteException {
    String name = interval.getName();
    TrimSpec trimspec = this.statconfig.getTrimSpec(name);
    trimspec.start(interval.getStart());
    trimspec.endExtended(interval.getEnd());
  }

  /**
   * @see RemotePerfStatMgr#reportTrimIntervals(Map )
   */
  public synchronized void reportTrimIntervals( Map intervals )
  throws RemoteException {
    for ( Iterator i = intervals.keySet().iterator(); i.hasNext(); ) {
      String name = (String) i.next();
      TrimSpec trimspec = this.statconfig.getTrimSpec( name );
      TrimInterval interval = (TrimInterval) intervals.get( name );
      
      long startTime = interval.getStart();
      if ( startTime != -1 ) {
        trimspec.start( startTime );
      }
      long endTime = interval.getEnd();
      if ( endTime != -1 ) {
        trimspec.end( endTime );
      }
    }
  }

  /**
   * @see RemotePerfStatMgr#reportExtendedTrimIntervals(Map )
   */
  public synchronized void reportExtendedTrimIntervals( Map intervals )
  throws RemoteException {
    for ( Iterator i = intervals.keySet().iterator(); i.hasNext(); ) {
      String name = (String) i.next();
      TrimSpec trimspec = this.statconfig.getTrimSpec( name );
      TrimInterval interval = (TrimInterval) intervals.get( name );
      
      long startTime = interval.getStart();
      if ( startTime != -1 ) {
        trimspec.startExtended( startTime );
      }
      long endTime = interval.getEnd();
      if ( endTime != -1 ) {
        trimspec.endExtended( endTime );
      }
    }
  }

  /**
   * @see RemotePerfStatMgr#getTrimSpec(String)
   */
  public TrimSpec getTrimSpec( String trimspecName ) 
  throws RemoteException {
    String name = trimspecName;
    if ( name == null ) {
      name = TrimSpec.DEFAULT_TRIM_SPEC_NAME;
    }
    return this.statconfig.getTrimSpec( name );
  }

  /**
   * @see PerfStatMgr#registerStatistics(PerformanceStatistics)
   */
  public void registerStatistics( List statspecs )
  throws RemoteException {
    // @todo lises figure out how to avoid registering duplicates at runtime
    //             in an efficient manner (cache keys in remote vms?)
    for ( Iterator i = statspecs.iterator(); i.hasNext(); ) {
      RuntimeStatSpec statspec = (RuntimeStatSpec) i.next();
      if ( this.statconfig.getStatSpecsWithId( statspec.getId() ) == null ) {
        Log.getLogWriter().info( "Autogenerating name for " + statspec + " with name " + statspec.getName() );
        statspec.autogenerateName();
        this.statconfig.addStatSpec( statspec );
        if ( Log.getLogWriter().fineEnabled() ) {
          Log.getLogWriter().fine( "Registered statspec\n" + statspec );
        }
      } else {
        if ( Log.getLogWriter().finerEnabled() ) {
          Log.getLogWriter().finer( "Already registered statspec\n" + statspec );
        }
      }
    }
  }

  /**
   *  Generates the trim specifications and writes them out.
   */
  public void generateTrimSpecificationsFile() {
    String trimspecs = generateTrimSpecifications();
    if ( trimspecs != null ) {
      FileUtil.writeToFile( PerfReporter.TRIM_SPEC_FILE_NAME, trimspecs );
    }
  }

  /**
   *  Generates the statistic specifications and writes them out.
   */
  public void generateStatisticsSpecificationsFile() {
    String histFile = null;
    if (TestConfig.tab().booleanAt(HistogramStatsPrms.enable, false)) {
      histFile = (String)TestConfig.tab()
                        .get(HistogramStatsPrms.statisticsSpecification);
    }
    Object obj = TestConfig.tab().get(
        PerfReportPrms.useAutoGeneratedStatisticsSpecification);
    
    boolean useAutoGeneratedStasSpec = (obj == null? false: Boolean.valueOf((String)obj));
    
    String statFile = null;
    if (useAutoGeneratedStasSpec) {
      statFile = System.getProperty("user.dir") + "/" + "autoGenerate.spec";
    }
    else {
      statFile = (String)TestConfig.tab().get(
          PerfReportPrms.statisticsSpecification);
    }
    String stats = generateStatisticsSpecifications(statFile, histFile);
    this.statconfig.resetStatSpecs(); // clear the runtime
    if ( stats != null ) { // write the spec and parse it back in
      FileUtil.writeToFile( PerfReporter.STAT_SPEC_FILE_NAME, stats );
      try {
	StatSpecParser.parseFile( PerfReporter.STAT_SPEC_FILE_NAME, this.statconfig );
      } catch( FileNotFoundException e ) {
	throw new HydraRuntimeException( "Should not happen" );
      }
    }
  }

  /**
   * Returns the trim specifications as a string.
   */
  private String generateTrimSpecifications() {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw, true);
    pw.println("// Trim Specifications (Autogenerated)");
    pw.println("// " + TestConfig.getInstance().getTestName());
    pw.println("// " + (new Date()).toString());
    pw.println(COMMENT_DIVIDER);
    printTrimSpecs(pw);
    pw.flush();

    return sw.toString();
  }

  /**
   * Returns the statistics specifications file as a string.
   */
  private String generateStatisticsSpecifications(String statFile,
                                                  String histFile ) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw, true);
    pw.println("// Statistics Specifications (Autogenerated)");
    pw.println("// " + TestConfig.getInstance().getTestName());
    pw.println("// " + (new Date()).toString());
    pw.println(COMMENT_DIVIDER);
    if (statFile == null) {
      printUserDefinedStatisticsSpecs(pw);
      pw.println(StatSpecTokens.INCLUDE
                + " $JTESTS/perffmwk/statistics.spec\n;");
    } else {
      pw.println(StatSpecTokens.INCLUDE + " " + statFile + "\n;");
    }
    if (histFile != null) {
      pw.println(StatSpecTokens.INCLUDE + " " + histFile + "\n;");
    }
    pw.flush();
    return sw.toString();
  }

  /**
   *  Adds registered trim specs to the stream.
   */
  private void printTrimSpecs( PrintWriter pw ) {
    for ( Iterator i = this.statconfig.getTrimSpecs().values().iterator(); i.hasNext(); ) {
      TrimSpec trimspec = (TrimSpec) i.next();
      pw.println( trimspec.toSpecString() );
    }
  }

  /**
   *  Adds registered user-defined statistics and their values to the stream.
   */
  private void printUserDefinedStatisticsSpecs( PrintWriter pw ) {
    for ( Iterator i = this.statconfig.getStatSpecs().values().iterator(); i.hasNext(); ) {
      StatSpec statspec = (StatSpec) i.next();
      pw.println( statspec.toSpecString() );
    }
  }
}
