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

import hydra.HostHelper.OSType;
import java.io.*;
import java.util.*;

/**
 *
 *  Encodes information about a host.
 *
 */
public class HostDescription extends AbstractDescription
implements Serializable {

  /** The logical name for hosts with this description */
  private String name;

  /** Name of the host for this description */
  private String hostName;

  /** The canonical name of the host */ 
  private String canonicalHostName;

  /** Type of O/S on the host */
  private OSType osType;

  /** File separator for the host O/S */
  private char fileSep;

  /** Path separator for the host O/S */
  private char pathSep;

  private Integer bootstrapPort;

  /** The gemfire home directory for vms started from this description */
  private String gemfireHome;

  /** The user directory for this host */
  private String userDir;

  /** The resource directory for this host */
  private String resourceDir;

  /** The test directory for vms started from this description */
  private String testDir;

  /** An auxiliary test directory for vms started from this description
      This is used by Gmefire Addons that import hydra, such as GfMon */
  private String extraTestDir;

  /** The java home directory for vms started from this description */
  private String javaHome;

  /** The vendor for the jdk in the java home directory. */
  private String javaVendor;

  /** The jprobe home directory for vms started from this description */
  private String jprobeHome;

  /** The ant home for the ant scripts to run */
  private String antHome; 

  //////////////////////////////////////////////////////////////////////////////
  ////    CONSTRUCTORS                                                      ////
  //////////////////////////////////////////////////////////////////////////////

  public HostDescription() {
  }
  protected HostDescription copy() {
    HostDescription hd = new HostDescription();
    hd.setName(this.getName());
    hd.setHostName(this.getHostName());
    hd.setCanonicalHostName(this.getCanonicalHostName());
    hd.setOSType(this.getOSType());
    hd.setBootstrapPort(this.getBootstrapPort());
    hd.setGemFireHome(this.getGemFireHome());
    hd.setUserDir(this.getUserDir());
    hd.setResourceDir(this.getResourceDir());
    hd.setTestDir(this.getTestDir());
    hd.setExtraTestDir(this.getExtraTestDir());
    hd.setJavaHome(this.getJavaHome());
    hd.setJavaVendor(this.getJavaVendor());
    hd.setJProbeHome(this.getJProbeHome());
    hd.setAntHome(this.getAntHome());
    return hd;
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    ACCESSORS                                                         ////
  //////////////////////////////////////////////////////////////////////////////

  public String getName() {
    return this.name;
  }
  protected void setName( String name ) {
      this.name = name;
  }
  public String getHostName() {
    return this.hostName;
  }
  protected void setHostName( String name ) {
    this.hostName = name;
  }

  /**
   * Sets the canonical name of the host for this
   * <code>HostDescription</code>.
   */
  protected void setCanonicalHostName(String name) {
    this.canonicalHostName = name;
  }

  /**
   * Returns the canonical name of the host for this
   * <code>HostDescription</code>. 
   */
  public String getCanonicalHostName() {
    return this.canonicalHostName;
  }

  public OSType getOSType() {
    return this.osType;
  }
  protected void setOSType(OSType os) {
    this.osType = os;
    this.fileSep = EnvHelper.getFileSep(os);
    this.pathSep = EnvHelper.getPathSep(os);
  }

  public char getFileSep() {
    return this.fileSep;
  }

  public char getPathSep() {
    return this.pathSep;
  }

  public Integer getBootstrapPort() {
    return this.bootstrapPort;
  }

  protected void setBootstrapPort(Integer i) {
    this.bootstrapPort = i;
  }
  
  public String getGemFireHome() {
    return this.gemfireHome;
  }
  protected void setGemFireHome( String dir ) {
    this.gemfireHome = dir;
  }
  public String getUserDir() {
    return this.userDir;
  }
  protected void setUserDir( String dir ) {
    this.userDir = dir;
  }
  public String getResourceDir() {
    return this.resourceDir;
  }
  protected void setResourceDir( String dir ) {
    this.resourceDir = dir;
  }

  /**
   * @return the name of the directory in which test classes are located (<code>$JTESTS</code>).
   */
  public String getTestDir() {
    return this.testDir;
  }

  protected void setTestDir( String dir ) {
    this.testDir = dir;
  }

  /**
   * @since 5.2
   * @returns null or the name of the directory in which extra test classes are located (<code>$EXTRA_JTESTS</code>).
   */
  public String getExtraTestDir() {
    return this.extraTestDir;
  }

  protected void setExtraTestDir( String dir ) {
    this.extraTestDir = dir;
  }

  public String getJavaHome() {
    return this.javaHome;
  }
  protected void setJavaHome( String dir ) {
    this.javaHome = dir;
  }
  public String getJavaVendor() {
    return this.javaVendor;
  }
  protected void setJavaVendor( String vendor ) {
    this.javaVendor = vendor;
  }
  public String getJProbeHome() {
    return this.jprobeHome;
  }
  protected void setJProbeHome( String dir ) {
    this.jprobeHome = dir;
  }

  public String getAntHome() {
    return this.antHome;
  }
  
  protected void setAntHome( String dir) {
    this.antHome = dir;
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    PRINTING                                                          ////
  //////////////////////////////////////////////////////////////////////////////

  public SortedMap toSortedMap() {
    SortedMap map = new TreeMap();
    String header = this.getClass().getName() + "." + this.getName() + ".";
    map.put( header + "hostName", this.getHostName() );
    map.put( header + "canonicalHostName", this.getCanonicalHostName() );
    map.put( header + "osType", this.getOSType() );
    map.put( header + "bootstrapPort", this.getBootstrapPort() );
    if ( this.getGemFireHome() != null )
      map.put( header + "gemfireHome", this.getGemFireHome() );
    map.put( header + "userDir", this.getUserDir() );
    map.put( header + "resourceDir", this.getResourceDir() );
    map.put( header + "testDir", this.getTestDir() );
    if ( this.getExtraTestDir() != null )
      map.put( header + "extraTestDir", this.getExtraTestDir() );
    map.put( header + "javaHome", this.getJavaHome() );
    map.put( header + "javaVendor", this.getJavaVendor() );
    if ( this.getJProbeHome() != null )
      map.put( header + "jprobeHome", this.getJProbeHome() );
    if ( this.getAntHome() != null )
        map.put( header + "antHome", this.getAntHome() ); 
    return map;
  }

  public String toString() {
    return toSortedMap().toString();
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    CONFIGURATION                                                     ////
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  Creates host descriptions from the host parameters in the test
   *  configuration.
   */
  protected static void configure( TestConfig config ) {

    ConfigHashtable tab = config.getParameters();
    SortedMap resourceDirBaseMap = HostPrms.getResourceDirBaseMap();
    SortedMap<String,Vector<String>> hostNameMap = HostPrms.getHostNameMap();
    SortedMap hostNameMapIndex = new TreeMap();
    String masterDir = TestConfig.getInstance().getMasterDescription()
                                 .getVmDescription().getHostDescription()
                                 .getUserDir();

    // create a description for each host name
    Vector names = tab.vecAt( HostPrms.names, new HydraVector() );

    for ( int i = 0; i < names.size(); i++ ) {

      HostDescription hd = new HostDescription();

      // name
      String name = (String) names.elementAt(i);
      hd.setName(name);

      // hostName (optional)
      {
        Long key = HostPrms.hostNames;
        String defaultHostName =
               config.getMasterDescription().getVmDescription()
                     .getHostDescription().getHostName();
        String hostName = null;
        if (hostNameMap == null) {
          hostName = tab.stringAtWild(key, i, defaultHostName);
        } else {
          for (String namePrefix : hostNameMap.keySet()) {
            if (name.startsWith(namePrefix)) {
              Vector<String> physicalHosts = hostNameMap.get(namePrefix);
              Integer nextIndex = (Integer)hostNameMapIndex.get(namePrefix);
              if (nextIndex == null) { // first time to use this name prefix
                hostName = (String)physicalHosts.get(0);
                hostNameMapIndex.put(namePrefix, Integer.valueOf(1));
              } else {
                int index = nextIndex.intValue();
                hostName = physicalHosts.get(index%physicalHosts.size());
                hostNameMapIndex.put(namePrefix, Integer.valueOf(index + 1));
              }
              break;
            }
          }
          if (hostName == null) {
            String s = BasePrms.nameForKey(key) + " has no list "
                     + "of physical host names for logical host name " + name;
            throw new HydraConfigException(s);
          }
        }
        hd.setHostName(EnvHelper.convertHostName(hostName));
        hd.setCanonicalHostName(HostHelper.getCanonicalHostName(hostName));
      }
      // osType
      {
        Long key = HostPrms.osTypes;
        String str = tab.getString(key, tab.getWild(key, i, null));
        if (str == null) {
          str = getRequiredProperty("os.name", key);
        }
        hd.setOSType(OSType.toOSType(str, key));
      }
      // bootstrapPort
      {
        Long key = HostPrms.bootstrapPort;
        String str = tab.getString(key, tab.getWild(key, i, null));
        if (str != null && !str.equalsIgnoreCase(BasePrms.NONE)) {
          hd.setBootstrapPort(getIntegerFor(str, key));
        }
      }
      // gemfireHome
      {
        Long key = HostPrms.gemfireHomes;
        String str = tab.getString(key, tab.getWild(key, i, null));
        if (str == null) {
          str = getRequiredProperty("gemfire.home", key);
        }
        hd.setGemFireHome(str);
      }
      // testDir
      {
        Long key = HostPrms.testDirs;
        String str = tab.getString(key, tab.getWild(key, i, null));
        if (str == null) {
          str = getRequiredProperty("JTESTS", key);
        }
        hd.setTestDir(str);
      }
      // extraTestDir
      {
        Long key = HostPrms.extraTestDirs;
        String str = tab.getString(key, tab.getWild(key, i, null));
        if (str == null) {
          str = getOptionalProperty("EXTRA_JTESTS");
        }
        hd.setExtraTestDir(str);
      }
      // userDir
      {
        Long key = HostPrms.userDirs;
        File fn = new File(masterDir);
        String str = tab.getString(key, tab.getWild(key, i, null));
        if (str == null) {
          str = fn.getParent();
        }
        str = str + hd.getFileSep() + fn.getName();
        str = EnvHelper.expandEnvVars(str, hd);
        hd.setUserDir(str);
      }
      // resourceDir
      {
        if (resourceDirBaseMap == null) {
          Long key = HostPrms.resourceDirBases;
          File fn = new File(masterDir);
          String str = tab.getString(key, tab.getWild(key, i, null));
          if (str == null) {
            str = hd.getUserDir();
          } else {
            str = str + hd.getFileSep() + fn.getName();
          }
          str = EnvHelper.expandEnvVars(str, hd);
          hd.setResourceDir(str);
        } else {
          String str = (String)resourceDirBaseMap.get(hd.getHostName());
          if (str == null) {
            String s = hd.getHostName() + " is missing from "
                     + tab.stringAt(HostPrms.resourceDirBaseMapFileName);
            throw new HydraConfigException(s);
          }
          str = str + hd.getFileSep() + (new File(masterDir)).getName();
          str = EnvHelper.expandEnvVars(str, hd);
          hd.setResourceDir(str);
        }
      }
      // javaHome
      {
        Long key = HostPrms.javaHomes;
        String str = tab.getString(key, tab.getWild(key, i, null));
        if (str == null) {
          str = getRequiredProperty("java.home", key);
        }
        hd.setJavaHome(str);
      }
      // javaVendor
      {
        String str = HostPrms.getJavaVendor(i);
        hd.setJavaVendor(str);
      }
      // jprobeHome
      {
        Long key = HostPrms.jprobeHomes;
        String str = tab.getString(key, tab.getWild(key, i, null));
        if (str == null) {
          str = getOptionalProperty("JPROBE");
        }
        hd.setJProbeHome(str);
      }
      // antHome
      {
        Long key = HostPrms.antHomes;
        String str = tab.getString(key, tab.getWild(key, i, null));
        if (str == null) {
          str = getOptionalProperty("ANT_HOME");
        }
        hd.setAntHome(str);
      }
      
      // add description to test config
      config.addHostDescription( hd );
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    MISCELLANEOUS                                                     ////
  //////////////////////////////////////////////////////////////////////////////

  private static String getRequiredProperty(String propertyName, Long key) {
    String propertyVal = System.getProperty(propertyName);
    if (propertyVal == null) {
      String s = "No value found for either " + BasePrms.nameForKey(key)
               + " or the " + propertyName + " system property";
      throw new HydraRuntimeException(s);
    }
    return propertyVal;
  }

  protected static String getOptionalProperty(String propertyName) {
    String property = System.getProperty(propertyName);
    return (property == null || property.length() == 0 ) ? null : property;
  }

  /**
   * Verifies that the given host description is equivalent to this one.  That
   * is, it is identical except for logical name.
   *
   * @throws HydraConfigException if the host descriptions are not equivalent.
   */
  protected void checkEquivalent(HostDescription hd) {
    if (hd.getHostName() != null && !hd.getHostName().equals(this.getHostName()) ||
        hd.getCanonicalHostName() != null && !hd.getCanonicalHostName().equals(this.getCanonicalHostName()) ||
        hd.getOSType() != null && hd.getOSType() != this.getOSType() ||
        hd.getGemFireHome() != null && !hd.getGemFireHome().equals(this.getGemFireHome()) ||
        hd.getUserDir() != null && !hd.getUserDir().equals(this.getUserDir()) ||
        //hd.getResourceDir() != null && !hd.getResourceDir().equals(this.getResourceDir()) ||
        hd.getTestDir() != null && !hd.getTestDir().equals(this.getTestDir()) ||
        hd.getExtraTestDir() != null && !hd.getExtraTestDir().equals(this.getExtraTestDir()) ||
        hd.getJavaHome() != null && !hd.getJavaHome().equals(this.getJavaHome()) ||
        hd.getJavaVendor() != null && !hd.getJavaVendor().equals(this.getJavaVendor())) {
      String s = "Attempt to configure multiple logical hosts on same physical host with different characteristics";
      throw new HydraConfigException(s);
    }
  }
}
