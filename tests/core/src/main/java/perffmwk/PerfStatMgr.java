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

import com.gemstone.gemfire.*;
import com.gemstone.gemfire.internal.*;

import hydra.*;

import java.io.*;
import java.rmi.*;
import java.util.*;

/**
 * A <code>PerfStatMgr</code> provides runtime support for the internal
 * management, trim interval settings, and queries on statistics, whether
 * system or application-defined.
 * <p>
 * The "real" manager is {@link perffmwk.RemotePerfStatMgr} RMI remote object
 * that lives in the hydra master VM.  Instances of this class provide a
 * friendly wrapper around the remote object.  For instance, if a
 * <code>java.rmi.RemoteException</code> is thrown while performing some operation, it
 * is wrapped in a <code>HydraRuntimeException</code>.
 */
public final class PerfStatMgr {

  /** The singleton instance of this class */
  private static PerfStatMgr instance;

  /** Number of times to retry reading archives before giving up. */
  protected static int RETRY_LIMIT = 500;

  /** The remote object that we delegate to */
  private RemotePerfStatMgr delegate;

  //////////////////////  Static Methods  //////////////////////

  /**
   * Returns an instance of <code>PerfStatMgr</code>.
   *
   * @throws HydraRuntimeException
   *         Something goes wrong while looking up remote object
   */
  public static PerfStatMgr getInstance() {
    if (instance == null) {
      synchronized( PerfStatMgr.class ) {
	if ( instance == null ) {
	  // Look up the remote object in the RMI registry that runs on
	  // the master's host
	  try {
	    RemotePerfStatMgr rps = (RemotePerfStatMgr)
              RmiRegistryHelper.lookupInMaster(RemotePerfStatMgrImpl.RMI_NAME);
	    instance = new PerfStatMgr(rps);
	  } catch (Exception ex) {
	    String s = "While looking up " + RemotePerfStatMgrImpl.RMI_NAME;
	    throw new HydraRuntimeException(s, ex);
	  }
	}
      }
    }
    return instance;
  }

  ///////////////////////  Constructors  ///////////////////////

  /**
   * Creates a new <code>PerfStatMgr</code> that delegates to
   * the given remote object.
   */
  private PerfStatMgr(RemotePerfStatMgr delegate) {
    this.delegate = delegate;
  }

  /////////////////  Delegated Instance Methods  /////////////////

  /**
   * Marks the starting point for the default trim spec to use to
   * trim associated statistics in performance reports.
   */
  public void startTrim() {
    startTrim( null );
  }

  /**
   * Marks the starting point for the specified trim spec to use to
   * trim associated statistics in performance reports.
   *
   * @param trimspecName
   *        The name of the trim spec for which to record a start time.
   */
  public void startTrim( String trimspecName ) {
    try {
      delegate.startTrim( trimspecName );

    } catch (RemoteException ex) {
      String s = "While invoking startTrim()";
      throw new HydraRuntimeException(s, ex);
    }
  }

  /**
   * Marks the ending point for the default trim spec to use to
   * trim associated statistics in performance reports.
   */
  public void endTrim() {
    endTrim( null );
  }

  /**
   * Marks the ending point for the specified trim spec to use to
   * trim associated statistics in performance reports.
   *
   * @param trimspecName
   *        The name of the trim spec for which to record an end time.
   */
  public void endTrim( String trimspecName ) {
    try {
      delegate.endTrim( trimspecName );

    } catch (RemoteException ex) {
      String s = "While invoking endTrim()";
      throw new HydraRuntimeException(s, ex);
    }
  }

  /**
   * Reports the specified trim interval.
   */
  public void reportTrimInterval(TrimInterval interval) {
    try {
      delegate.reportTrimInterval(interval);
    } catch (RemoteException ex) {
      String s = "While invoking reportTrimInterval: " + interval;
      throw new HydraRuntimeException(s, ex);
    }
  }

  /**
   * Reports the specified extended trim interval.
   */
  public void reportExtendedTrimInterval(TrimInterval interval) {
    try {
      delegate.reportExtendedTrimInterval(interval);
    } catch (RemoteException ex) {
      String s = "While invoking reportExtendedTrimInterval: " + interval;
      throw new HydraRuntimeException(s, ex);
    }
  }

  /**
   *  Reports the specified trim intervals.
   */
  public void reportTrimIntervals( Map intervals ) {
    try {
      delegate.reportTrimIntervals( intervals );

    } catch (RemoteException ex) {
      String s = "While invoking reportTrimIntervals()";
      throw new HydraRuntimeException(s, ex);
    }
  }

  /**
   *  Reports the specified extended trim intervals.
   */
  public void reportExtendedTrimIntervals( Map intervals ) {
    try {
      delegate.reportExtendedTrimIntervals( intervals );

    } catch (RemoteException ex) {
      String s = "While invoking reportExtendedTrimIntervals()";
      throw new HydraRuntimeException(s, ex);
    }
  }

  /**
   * For internal use only.
   * <p>
   * Registers a new statistics instance with the hydra master so that master
   * is aware of its existence.
   *
   * @param statInst
   *        The statistic instance to register.
   */
  protected void registerStatistics( PerformanceStatistics statInst ) {
    StatisticDescriptor[] statDescs = statInst.statistics().getType().getStatistics();
    List statspecs = new ArrayList();
    for ( int i = 0; i < statDescs.length; i++ ) {
      StatisticDescriptor statDesc = statDescs[i];
      statspecs.add( new RuntimeStatSpec( statInst, statDesc ) );
    }
    try {
      delegate.registerStatistics(statspecs);

    } catch (RemoteException ex) {
      String s = "While invoking registerStatistics()";
      throw new HydraRuntimeException(s, ex);
    }
  }

  /**
   * Reads statistics from archive files based on the specification string and
   * trimspec.
   *
   * @return a List of PerfStatValue instances (null if there were no matches)
   * @throws StatConfigException if the specification string is malformed.
   */
  public List readStatistics( String specification, TrimSpec trimspec ) {
    RuntimeStatSpec statspec = StatSpecParser.parseRuntimeStatSpec( specification );
    return readStatistics( statspec, trimspec );
  }

  /**
   * Reads statistics from archive files based on the specification string.
   * The specification must contain only the <code>instanceId</code> and optional
   * <code>statSpecParams</code> as indicated in the specification grammar
   * <A href="statspec_grammar.txt">statspec_grammar.txt</A>.  Returns all
   * statistics that match the specification.
   * <p><b>Example</b>
   * <blockquote><pre>
   * // uses perffmwk.samples.SampleThreadStatistics
   * String spec = "* " // search all archives
   *             + "perffmwk.samples.SampleThreadStatistics "
   *             + "* " // match all instances
   *             + SampleThreadStatistics.OPS + " "
   *             + StatSpecTokens.FILTER_TYPE + "=" + StatSpecTokens.FILTER_NONE + " "
   *             + StatSpecTokens.COMBINE_TYPE + "=" + StatSpecTokens.COMBINE_ACROSS_FILES + " "
   *             + StatSpecTokens.OP_TYPES + "=" + StatSpecTokens.MAX;
   * List psvs = PerfStatMgr.getInstance().readStatistics( spec );
   * </pre></blockquote>
   *
   * @return a List of PerfStatValue instances (null if there were no matches)
   * @throws StatConfigException if the specification string is malformed.
   */
  public List readStatistics( String specification ) {
    RuntimeStatSpec statspec = StatSpecParser.parseRuntimeStatSpec( specification );
    return readStatistics( statspec );
  }

  /**
   * Reads statistics from archive files based on the specification string.
   * The specification must contain only the <code>instanceId</code> and optional
   * <code>statSpecParams</code> as indicated in the specification grammar
   * <A href="statspec_grammar.txt">statspec_grammar.txt</A>.  Returns all
   * statistics that match the specification.
   * <p><b>Example</b>
   * <blockquote><pre>
   * // uses perffmwk.samples.SampleThreadStatistics
   * String spec = "* " // search all archives
   *             + perffmwk.samples.SampleThreadStatistics "
   *             + "* " // match all instances
   *             + SampleThreadStatistics.OPS + " "
   *             + StatSpecTokens.FILTER_TYPE + "=" + StatSpecTokens.FILTER_NONE + " "
   *             + StatSpecTokens.COMBINE_TYPE + "=" + StatSpecTokens.COMBINE_ACROSS_FILES + " "
   *             + StatSpecTokens.OP_TYPES + "=" + StatSpecTokens.MAX;
   * List psvs = PerfStatMgr.getInstance().readStatistics( spec );
   * </pre></blockquote>
   *  @param exactMatch Determines if the stat type should be an exact match (to avoid
   *         confusion between the product's CachePerfStats and the framework's 
   *         cacheperf.CachePerfStats.
   *
   * @return a List of PerfStatValue instances (null if there were no matches)
   * @throws StatConfigException if the specification string is malformed.
   */
  public List readStatistics( String specification, boolean exactMatch ) {
    RuntimeStatSpec statspec = StatSpecParser.parseRuntimeStatSpec( specification );
    statspec.setExactMatch(exactMatch);
    return readStatistics( statspec );
  }

  /**
   * Reads the named statistic for the statistics instance from archive files.
   * Returns all statistics that match the default specification for the instance.
   * <p><b>Example</b>
   * <blockquote><pre>
   * // uses perffmwk.samples.SampleThreadStatistics
   * SampleThreadStatistics statInst = SampleThreadStatistics.getInstance();
   * List psvs = PerfStatMgr.getInstance().readStatistics( statInst, SampleThreadStatistics.OPS );
   * </pre></blockquote>
   *
   * @return a List of PerfStatValue instances (null if there were no matches)
   */
  public List readStatistics( PerformanceStatistics statInst, String statName ) {
    StatisticDescriptor sd = statInst.getStatisticDescriptor( statName );
    RuntimeStatSpec statspec = new RuntimeStatSpec( statInst, sd );
    return readStatistics( statspec );
  }

  /**
   * Reads statistics from archive files based on the specification.
   * Returns all statistics that match the specification.
   *
   * @return a List of PerfStatValue instances (null if there were no matches)
   */
  public List readStatistics( RuntimeStatSpec statspec ) {
    TrimSpec trimspec = getTrimSpec( statspec.getTrimSpecName() );
    return readStatistics( statspec, trimspec );
  }

  /**
   * Reads statistics from archive files based on the specification and
   * trimspec.  Returns all statistics that match the specification.
   * <p><b>Example</b>
   * <blockquote><pre>
   * // uses perffmwk.samples.SampleThreadStatistics
   * SampleThreadStatistics statInst = SampleThreadStatistics.getInstance();
   * StatisticDescriptor sd = statInst.getStatisticDescriptor( SampleThreadStatistics.OPS );
   * RuntimeStatSpec statSpec = new RuntimeStatSpec( statInst, sd );
   * // change default spec a bit
   * statSpec.setFilter( StatSpecTokens.FILTER_NONE );
   * statSpec.setCombineType( StatSpecTokens.COMBINE_ACROSS_ARCHIVES );
   * statSpec.setStddev( false );
   * List psvs = PerfStatMgr.getInstance().readStatistics( statSpec );
   * </pre></blockquote>
   *
   * @return a List of PerfStatValue instances (null if there were no matches)
   */
  public List readStatistics( RuntimeStatSpec statspec, TrimSpec trimspec ) {
    long start = -1;
    if ( Log.getLogWriter().fineEnabled() ) {
      start = System.currentTimeMillis();
      Log.getLogWriter().fine( "StatSpec:\n" + statspec );
    }
    if ( Log.getLogWriter().fineEnabled() ) {
      Log.getLogWriter().fine( "TrimSpec: " + trimspec );
    }
    // work around Bug 30288, problem reading stats that are being written
    for (int i = 1; i <= RETRY_LIMIT; i++) {
      try {
        StatArchiveReader reader = getStatArchiveReader( statspec );
        List results = readStatistics( statspec, trimspec, reader );
        if ( Log.getLogWriter().fineEnabled() ) {
          long elapsed = System.currentTimeMillis() - start;
          Log.getLogWriter().fine( "Read statistics in " + elapsed + " ms" );
        }
        return results;
      } catch (ArrayIndexOutOfBoundsException e) {
        MasterController.sleepForMs(1);
        if (i == RETRY_LIMIT) {
          String s = "Either the workaround for Bug 30288 failed, or else all statistics instances being combined do not have values for the full duration of the given trim interval.";
          throw new HydraRuntimeException(s, e);
        }
      }
    }
    throw new HydraInternalException("Should not happen");
  }

  /**
   * For internal use only.
   * <p>
   * Fetches, trims, and converts the matching statistics using the reader.
   */
  private List readStatistics( StatSpec statspec, TrimSpec trimspec,
                               StatArchiveReader reader ) {
    List psvs = new ArrayList();
    StatArchiveReader.StatValue[] values = reader.matchSpec( statspec );
    if ( Log.getLogWriter().finerEnabled() ) {
      Log.getLogWriter().finer( "Values: " + values );
    }
    for ( int i = 0; i < values.length; i++ ) {
      StatArchiveReader.StatValue sv = values[i];
      if ( Log.getLogWriter().finerEnabled() ) {
        Log.getLogWriter().finer( "Value: " + sv.toString() );
      }
      PerfStatValue psv = PerfStatReader.getPerfStatValue( statspec, trimspec, sv );
      psvs.add( psv );
    }
    if ( Log.getLogWriter().finerEnabled() ) {
      Log.getLogWriter().finer( "PerfStatValues: " + psvs );
    }
    return psvs.size() == 0 ? null : psvs;
  }

  /**
   * For internal use only.
   * <p>
   * Fetches the trim spec with the specified name from the master.
   */
  private TrimSpec getTrimSpec( String trimspecName ) {
    try {
      return delegate.getTrimSpec( trimspecName );

    } catch (RemoteException ex) {
      String s = "While invoking getTrimSpec()";
      throw new HydraRuntimeException(s, ex);
    }
  }

  /**
   * For internal use only.
   * <p>
   * Fetches a statistic reader for the stat spec on matching archives.
   */
  private StatArchiveReader getStatArchiveReader( RuntimeStatSpec statspec ) {
    try {
      List archives = statspec.getMatchingArchives();
      if ( Log.getLogWriter().fineEnabled() ) {
        Log.getLogWriter().fine( "Archives: " + archives );
      }
      return new StatArchiveReader(
          (File[])archives.toArray( new File[ archives.size() ] ),
          new RuntimeStatSpec[] { statspec }, true);
    } catch( IOException e ) {
      throw new StatConfigException( "Unable to read archive", e );
    }
  }
}
