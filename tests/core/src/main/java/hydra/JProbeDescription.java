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

import java.io.Serializable;
import java.util.*;

/**
 *
 *  Encodes information about a jprobe configuration.
 *
 */
public class JProbeDescription extends AbstractDescription
implements Serializable {

  /** The logical name for the jprobe configuration */
  private String name;

  /** The function carried out */
  private String function;

  /** The type of measurement to take */
  private String measurement;

  /** The set of filters to use*/
  private Vector filters;

  /** The set of triggers to use*/
  private Vector triggers;

  /** Whether to use deep traces */
  private boolean useDeepTraces;

  /** Whether to start recording from the start */
  private boolean recordFromStart;

  /** Whether to take a final snapshot */
  private boolean finalSnapshot;

  /** Whether the user will monitor */
  private boolean monitor;

  /** Whether to keep garbage collected objects in heap snapshots */
  private boolean garbageKeep;

  /** Whether to track object allocation */
  private boolean trackObjects;

  /** The port on which to monitor the run */
  private int port;

  //////////////////////////////////////////////////////////////////////////////
  ////    CONSTRUCTORS                                                      ////
  //////////////////////////////////////////////////////////////////////////////

  public JProbeDescription() {
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    ACCESSORS                                                         ////
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  Returns the logical name for the jprobe description.
   */
  public String getName() {
    return this.name;
  }
  private void setName( String name ) {
    this.name = name;
  }
  /**
   *  Returns the function.
   */
  public String getFunction() {
    return this.function;
  }
  private void setFunction( String function ) {
    this.function = function;
  }
  /**
   *  Returns the measurement type.
   */
  public String getMeasurement() {
    return this.measurement;
  }
  private void setMeasurement( String measurement ) {
    this.measurement = measurement;
  }
  /**
   *  Returns the set of filters.
   */
  public Vector getFilters() {
    return this.filters;
  }
  private void setFilters( Vector filters ) {
    this.filters = filters;
  }
  private String getFilterStr() {
    Vector filters = getFilters();
    String filterStr = null;
    for ( int i = 0; i < filters.size(); i++ ) {
      String filter = (String) filters.elementAt( i );
      if ( i == 0 ) filterStr = "";
      filterStr += filter;
      if ( i < filters.size() - 1 ) filterStr += ",";
    }
    return filterStr;
  }
  /**
   *  Returns the set of triggers.
   */
  public Vector getTriggers() {
    return this.triggers;
  }
  private void setTriggers( Vector triggers ) {
    this.triggers = triggers;
  }
  private String getTriggerStr() {
    Vector triggers = getTriggers();
    String triggerStr = null;
    for ( int i = 0; i < triggers.size(); i++ ) {
      String trigger = (String) triggers.elementAt( i );
      if ( i == 0 ) triggerStr = "";
      triggerStr += trigger;
      if ( i < triggers.size() - 1 ) triggerStr += ",";
    }
    return triggerStr;
  }
  /**
   * Returns whether to use deep traces.
   */
  public boolean useDeepTraces() {
    return this.useDeepTraces;
  }
  private void useDeepTraces(boolean useDeepTraces) {
    this.useDeepTraces = useDeepTraces;
  }
  /**
   *  Returns whether to record from the start.
   */
  public boolean getRecordFromStart() {
    return this.recordFromStart;
  }
  private void setRecordFromStart( boolean recordFromStart ) {
    this.recordFromStart = recordFromStart;
  }
  /**
   *  Returns whether to take a final snapshot.
   */
  public boolean getFinalSnapshot() {
    return this.finalSnapshot;
  }
  private void setFinalSnapshot( boolean finalSnapshot ) {
    this.finalSnapshot = finalSnapshot;
  }
  /**
   *  Returns whether to track object allocation
   */
  public boolean getTrackObjects() {
    return this.trackObjects;
  }
  private void setTrackObjects( boolean trackObjects ) {
    this.trackObjects = trackObjects;
  }
  /**
   *  Returns whether the user will monitor.
   */
  public boolean getMonitor() {
    return this.monitor;
  }
  private void setMonitor( boolean monitor ) {
    this.monitor = monitor;
  }
  /**
   *  Returns whether to keep garbage.
   */
  public boolean getGarbageKeep() {
    return this.garbageKeep;
  }
  private void setGarbageKeep( boolean garbageKeep ) {
    this.garbageKeep = garbageKeep;
  }
  /**
   *  Returns the port for this run.
   */
  public int getPort() {
    return this.port;
  }
  private void setPort( int port ) {
    this.port = port;
  }
  /**
   * Returns jprobe command line, based on the values in this description,
   * the provided jprobe home, and the vm type.
   */
  public String getCommandLine( String jprobeHome, String vmType ) {

    String cmd = getExecutable( jprobeHome ) + " ";
    Vector prms = getCommandLineParameters( vmType );
    for ( int i = 0; i < prms.size(); i++ ) {
      cmd += prms.elementAt( i ) + " ";
    }
    return cmd;
  }
  /**
   *  Returns jprobe executable, based on the provided jprobe home.
   */
  public String getExecutable( String jprobeHome ) {

    if ( jprobeHome == null )
      throw new HydraConfigException( "Unable to build executable for JProbe description " +
                                       getName() + " because JPROBE is not set" );

    return jprobeHome + "/bin/jplauncher";
  }
  /**
   * Returns jprobe command line parameters, based on the values in this
   * description and the vm type.
   */
  public Vector getCommandLineParameters(String vmType) {
    Vector prms = new Vector();

    if ( vmType != null ) {
      prms.add( "-" + vmType );
    }
    prms.add( "-jp_java=" + System.getProperty("java.home") + "/bin/java");
    prms.add( "-jp_function=" + getFunction() );
    prms.add( "-jp_snapshot_dir=" + System.getProperty("user.dir"));
    if ( getFunction().equals( "performance" ) ) {
      prms.add( "-jp_measurement=" + getMeasurement() );
      //prms.add( "-jp_record_from_start=" + getRecordFromStart() );
      //prms.add( "-jp_final_snapshot=" + getFinalSnapshot() );
      //prms.add( "-jp_track_objects=" + getTrackObjects() );
    }

    //if ( getFunction().equals( "performance" ) ) {
      //prms.add( "-jp_garbage_keep=" + getGarbageKeep() );
    //}
    if (getMonitor()) {
      prms.add( "-jp_socket=:" + getPort() );
    } else {
      //prms.add("-jp_cm=false");
    }

    String filter = getFilterStr();
    if ( filter != null ) {
      if (!getFunction().equals("memory"))
        prms.add( "-jp_collect_data=" + filter );
    }

    String trigger = getTriggerStr();
    if ( trigger != null )
      prms.add( "-jp_trigger=" + trigger );

    return prms;
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    PRINTING                                                          ////
  //////////////////////////////////////////////////////////////////////////////

  public SortedMap toSortedMap() {
    SortedMap map = new TreeMap();
    String header = this.getClass().getName() + "." + this.getName() + ".";
    map.put( header + "port", String.valueOf( this.getPort() ) );
    map.put( header + "function", this.getFunction() );
    map.put( header + "measurement", this.getMeasurement() );
    map.put( header + "filters", this.getFilters() );
    map.put( header + "triggers", this.getTriggers() );
    map.put( header + "useDeepTraces", String.valueOf(this.useDeepTraces()));
    map.put( header + "garbageKeep", String.valueOf( this.getGarbageKeep() ) );
    map.put( header + "recordFromStart", String.valueOf( this.getRecordFromStart() ) );
    map.put( header + "finalSnapshot", String.valueOf( this.getFinalSnapshot() ) );
    map.put( header + "trackObjects", String.valueOf( this.getTrackObjects() ) );
    map.put( header + "monitor", String.valueOf( this.getMonitor() ) );
    return map;
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    CONFIGURATION                                                     ////
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  Creates jprobe configuration descriptions from the jprobe parameters
   *  in the test configuration.
   */
  protected static void configure( TestConfig config ) {

    ConfigHashtable tab = config.getParameters();

    // create a description for each jprobe configuration name
    Vector names = tab.vecAt( JProbePrms.names, new HydraVector() );

    for ( int i = 0; i < names.size(); i++ ) {

      JProbeDescription jpd = new JProbeDescription();

      String name = (String) names.elementAt(i);
      jpd.setName( name );

      String function = tab.stringAtWild( JProbePrms.function, i, "performance" );
      jpd.setFunction( function );

      String measurement = tab.stringAtWild( JProbePrms.measurement, i, "cpu" );
      jpd.setMeasurement( measurement );

      Vector filters = tab.vecAtWild( JProbePrms.filters, i, new HydraVector() );
      jpd.setFilters( filters );

      Vector triggers = tab.vecAtWild( JProbePrms.triggers, i, new HydraVector() );
      jpd.setTriggers( triggers );

      boolean useDeepTraces = tab.booleanAtWild(JProbePrms.useDeepTraces, i, Boolean.TRUE);
      jpd.useDeepTraces(useDeepTraces);

      boolean garbageKeep = tab.booleanAtWild( JProbePrms.garbageKeep, i, Boolean.FALSE );
      jpd.setGarbageKeep( garbageKeep );

      boolean recordFromStart = tab.booleanAtWild( JProbePrms.recordFromStart, i, Boolean.TRUE );
      jpd.setRecordFromStart( recordFromStart );

      boolean finalSnapshot = tab.booleanAtWild( JProbePrms.finalSnapshot, i, Boolean.TRUE );
      jpd.setFinalSnapshot( finalSnapshot );

      boolean trackObjects = tab.booleanAtWild( JProbePrms.trackObjects, i, Boolean.TRUE );
      jpd.setTrackObjects( trackObjects );

      boolean monitor = tab.booleanAtWild( JProbePrms.monitor, i, Boolean.FALSE );
      jpd.setMonitor( monitor );

      // TBD: delay until needed on remote vms to properly choose unused port
      int port = PortHelper.getRandomPort();
      jpd.setPort( port );

      config.addJProbeDescription( jpd );
    }
  }
}
