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
import java.net.InetAddress;
import java.util.*;

/**
 *
 *  Encodes information about the hydra master controller.
 *
 */
public class MasterDescription extends AbstractDescription
implements Serializable {

  /** The logical name for the master controller */
  private String name;

  /** Master vm description */
  private VmDescription vmd;

  /** Distribution ports for gossip servers */
  private HashMap ports = new HashMap();

  /** Distribution locators for gossip servers */
  private HashMap locators = new HashMap();

  /** Time server host */
  private String timeServerHost;

  /** Time server port */
  private int timeServerPort;

  //////////////////////////////////////////////////////////////////////////////
  ////    CONSTRUCTORS                                                      ////
  //////////////////////////////////////////////////////////////////////////////

  public MasterDescription() {
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    ACCESSORS                                                         ////
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  Returns the rmi name for the master proxy object.
   */
  public String getName() {
    return this.name;
  }
  private void setName( String name ) {
    this.name = name;
  }
  /**
   *  Returns the vm description for the master controller.
   */
  public VmDescription getVmDescription() {
    return this.vmd;
  }
  private void setVmDescription( VmDescription vmd ) {
    this.vmd = vmd;
  }
  /**
   *  Returns the port for the gossip server for the
   *  specified distributed system.
   */
  protected int getLocatorPort( String dsName ) {
    return ((Integer)this.ports.get( dsName )).intValue();
  }
  /**
   *  Returns the locator string for the gossip server for the
   *  default distributed system.
   */
  public String getLocator() {
    return getLocator( GemFirePrms.DEFAULT_DISTRIBUTED_SYSTEM_NAME );
  }
  /**
   *  Returns the locator string for the gossip server for the
   *  specified distributed system.
   */
  public String getLocator( String dsName ) {
    return (String) this.locators.get( dsName );
  }
  /**
   *  Sets the host for the time server.
   */
  protected void setTimeServerHost(String host) {
    this.timeServerHost = host;
  }
  /**
   *  Returns the host for the time server.
   */
  protected String getTimeServerHost() {
    return this.timeServerHost;
  }
  /**
   *  Sets the port for the time server.
   */
  protected void setTimeServerPort(int port) {
    this.timeServerPort = port;
  }
  /**
   *  Returns the port for the time server.
   */
  protected int getTimeServerPort() {
    return this.timeServerPort;
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    PRINTING                                                          ////
  //////////////////////////////////////////////////////////////////////////////

  public SortedMap toSortedMap() {
    SortedMap map = new TreeMap();
    String header = this.getClass().getName() + "." + this.getName() + ".";
    map.putAll( this.getVmDescription().getHostDescription().toSortedMap() );
    map.putAll( this.getVmDescription().toSortedMap() );
    map.put( header + "locators", this.locators );
    return map;
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    CONFIGURATION                                                     ////
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  Creates the one and only master description.
   */
  public static void configure( TestConfig config ) {

    ConfigHashtable tab = config.getParameters();

    MasterDescription md = new MasterDescription();

    // name
    md.setName( "master" );

    // host description
    HostDescription hd = new HostDescription();
    hd.setName( "masterHost" );
    hd.setHostName( HostHelper.getLocalHost() );
    hd.setCanonicalHostName( HostHelper.getCanonicalHostName() );
    hd.setOSType( HostHelper.getLocalHostOS() );
    hd.setGemFireHome( System.getProperty( "gemfire.home" ) );
    hd.setTestDir( System.getProperty( "JTESTS" ) );
    hd.setExtraTestDir(HostDescription.getOptionalProperty("EXTRA_JTESTS"));
    hd.setUserDir(System.getProperty("user.dir"));
    hd.setJavaHome( System.getProperty( "java.home" ) );
    hd.setJavaVendor(HostPrms.getJavaVendor());
    hd.setJProbeHome( "unknown" ); // @todo lises set this to master's system property

    // vm description
    VmDescription vmd = new VmDescription();
    vmd.setName( "masterVM" );
    vmd.setHostDescription( hd );
    vmd.setClassPath( System.getProperty( "java.class.path" ) );
    vmd.setLibPath( System.getProperty( "java.library.path" ) );
    vmd.setExtraVMArgs( "unknown" );
    md.setVmDescription( vmd );

    // set description in test config
    config.setMasterDescription( md );
  }
  protected void postprocess() {
    if (TestConfig.tab().booleanAt(Prms.manageLocatorAgents)) {
      Map gfds = TestConfig.getInstance().getGemFireDescriptions();
      if (gfds.size() != 0) {
        StringBuffer sb = new StringBuffer(); 
        InetAddress addr = HostHelper.getIPAddress();
        sb.append(addr.getHostName());
        sb.append("@");
        sb.append(HostHelper.getHostAddress(addr));
        for (Iterator i = gfds.values().iterator(); i.hasNext();) {
          GemFireDescription gfd = (GemFireDescription)i.next();
          String ds = gfd.getDistributedSystem();
          if (!this.locators.containsKey(ds)) {
            if (gfd.getUseLocator().booleanValue()) {
              int port = PortHelper.getRandomPort();
              this.ports.put(ds, new Integer(port));
              this.locators.put( ds, sb.toString() + "[" + port + "]" );
            } else {
              this.locators.put(ds, "");
            }
          }
        }
      }
    }
  }
}
