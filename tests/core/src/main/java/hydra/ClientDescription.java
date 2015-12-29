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
 *  Encodes information needed to describe and start a client process.
 *
 */
public class ClientDescription extends AbstractDescription
implements Serializable {

  /** The logical name for client vms with this description */
  private String name;

  /** The vm description used to create client vms */
  private VmDescription vmd;

  /** The gemfire system the client vms can connect to (if any ) */
  private GemFireDescription gfd;

  /** The jprobe configuration the client vms use (if any ) */
  private JProbeDescription jpd;

  /** The JDK version configuration for client vms to use */
  private JDKVersionDescription jvd;

  /** The version configuration for client vms to use */
  private VersionDescription vd;

  /** The number of client vms to start */ 
  private int vmQuantity;

  /** Number of independent client threads per client vm */ 
  private int vmThreads;

  //////////////////////////////////////////////////////////////////////////////
  ////    CONSTRUCTORS                                                      ////
  //////////////////////////////////////////////////////////////////////////////

  public ClientDescription() {
  }

  // this method is only used for STARTTASKs and ENDTASKs
  protected ClientDescription copy() {
    ClientDescription cd = new ClientDescription();
    cd.setName( this.getName() );
    cd.setVmDescription( this.getVmDescription() );
    cd.setGemFireDescription( this.getGemFireDescription() );
    if (TestConfig.tab().booleanAt(Prms.versionStartAndEndTasks, false)) {
      cd.setJDKVersionDescription(this.getJDKVersionDescription());
      cd.setVersionDescription(this.getVersionDescription());
    }
    cd.setJProbeDescription( this.getJProbeDescription() );
    cd.setVmQuantity( this.getVmQuantity() );
    cd.setVmThreads( this.getVmThreads() );
    return cd;
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    ACCESSORS                                                         ////
  //////////////////////////////////////////////////////////////////////////////

  public String getName() {
    return this.name;
  }
  public void setName( String name ) {
    this.name = name;
  }
  public VmDescription getVmDescription() {
    return this.vmd;
  }
  public void setVmDescription( VmDescription vmd ) {
    this.vmd = vmd;
  }
  public GemFireDescription getGemFireDescription() {
    return this.gfd;
  }
  public void setGemFireDescription( GemFireDescription gfd ) {
    this.gfd = gfd;
  }
  public JDKVersionDescription getJDKVersionDescription() {
    return this.jvd;
  }
  public void setJDKVersionDescription( JDKVersionDescription jvd ) {
    this.jvd = jvd;
  }
  public VersionDescription getVersionDescription() {
    return this.vd;
  }
  public void setVersionDescription( VersionDescription vd ) {
    this.vd = vd;
  }
  public JProbeDescription getJProbeDescription() {
    return this.jpd;
  }
  private void setJProbeDescription( JProbeDescription jpd ) {
    this.jpd = jpd;
  }
  public int getVmQuantity() {
    return this.vmQuantity;
  }
  protected void setVmQuantity( int n ) {
    this.vmQuantity = n;
  }
  public int getVmThreads() {
    return this.vmThreads;
  }
  protected void setVmThreads( int n ) {
    this.vmThreads = n;
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    PRINTING                                                          ////
  //////////////////////////////////////////////////////////////////////////////

  public SortedMap toSortedMap() {
    SortedMap map = new TreeMap();
    String header = this.getClass().getName() + "." + this.getName() + ".";
    map.put( header + "vmName", this.getVmDescription().getName() );
    if ( this.getGemFireDescription() != null )
      map.put( header + "gemfireName", this.getGemFireDescription().getName() );
    if (this.getJDKVersionDescription() != null) {
      map.put(header + "jdkVersionName", this.getJDKVersionDescription().getName());
    }
    if (this.getVersionDescription() != null) {
      map.put(header + "versionName", this.getVersionDescription().getName());
    }
    if ( this.getJProbeDescription() != null )
      map.put( header + "jprobeName", this.getJProbeDescription().getName() );
    map.put( header + "vmQuantity", String.valueOf( this.getVmQuantity() ) );
    map.put( header + "vmThreads", String.valueOf( this.getVmThreads() ) );
    return map;
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    CONFIGURATION                                                     ////
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  Creates client descriptions from the client parameters in the test
   *  configuration.
   */
  protected static void configure( TestConfig config ) {

    ConfigHashtable tab = config.getParameters();

    // create a description for each client name
    Vector names = tab.vecAt( ClientPrms.names, new HydraVector() );

    for ( int i = 0; i < names.size(); i++ ) {

      ClientDescription cd = new ClientDescription();

      String name = (String) names.elementAt(i);
      cd.setName( name );

      String vmName = tab.stringAtWild( ClientPrms.vmNames, i, null );
      if ( vmName == null )
        throw new HydraConfigException( "Missing " + BasePrms.nameForKey( ClientPrms.vmNames ) );
      VmDescription vmd = config.getVmDescription( vmName );
      if ( vmd == null )
        throw new HydraConfigException( "Undefined value in " + BasePrms.nameForKey( ClientPrms.vmNames ) + ": " + vmName );
      cd.setVmDescription( vmd );

      String gemfireName = tab.stringAtWild( ClientPrms.gemfireNames, i, BasePrms.NONE );
      if ( ! gemfireName.equals( BasePrms.NONE ) ) {
        GemFireDescription gfd = config.getGemFireDescription( gemfireName );
        if ( gfd == null )
          throw new HydraConfigException( "Undefined value in " + BasePrms.nameForKey( ClientPrms.gemfireNames ) + ": " + gemfireName );
        cd.setGemFireDescription( gfd );
      }

      String jdkVersionName = tab.stringAtWild(ClientPrms.jdkVersionNames, i, BasePrms.NONE);
      if (!jdkVersionName.equals(BasePrms.NONE)) {
        JDKVersionDescription jvd = config.getJDKVersionDescription(jdkVersionName);
        cd.setJDKVersionDescription(jvd);
      }

      String versionName = tab.stringAtWild(ClientPrms.versionNames, i, BasePrms.NONE);
      if (!versionName.equals(BasePrms.NONE)) {
        VersionDescription vd = config.getVersionDescription(versionName);
        cd.setVersionDescription(vd);
      }

      String jprobeName = tab.stringAtWild( ClientPrms.jprobeNames, i, BasePrms.NONE );
      if ( ! jprobeName.equals( BasePrms.NONE ) ) {
        JProbeDescription jpd = config.getJProbeDescription( jprobeName );
        if ( jpd == null )
          throw new HydraConfigException( "Undefined value in " + BasePrms.nameForKey( ClientPrms.jprobeNames ) + ": " + jprobeName );
        cd.setJProbeDescription( jpd );
      }

      int vmQuantity = tab.intAtWild( ClientPrms.vmQuantities, i, 1 );
      cd.setVmQuantity( vmQuantity );
      if ( vmQuantity < 1 ) {
        throw new HydraConfigException( "Illegal value for " + BasePrms.nameForKey( ClientPrms.vmQuantities ) + ": " + vmQuantity );
      }

      int vmThreads = tab.intAtWild( ClientPrms.vmThreads, i, 1 );
      if ( vmThreads < 1 ) {
        throw new HydraConfigException( "Illegal value for " + BasePrms.nameForKey( ClientPrms.vmThreads ) + ": " + vmThreads );
      }
      cd.setVmThreads( vmThreads );

      config.addClientDescription( cd );
    }
  }
}
