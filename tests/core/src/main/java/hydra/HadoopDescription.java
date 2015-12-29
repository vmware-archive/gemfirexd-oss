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

import batterytest.BatteryTest;
import hydra.HadoopPrms.NodeType;
import java.io.*;
import java.util.*;

/**
 * Encodes information about a hadoop cluster and the hosts it runs on.
 */
public class HadoopDescription extends AbstractDescription
implements Serializable {

  /** The logical name for Hadoop clusters with this description */
  private String name;

  private Boolean addHDFSConfigurationToClassPath;
  private String baseHDFSDirName;

  private List<String> dataNodeHosts;
  private List<String> dataNodeDataDrives;
  private List<String> dataNodeLogDrives;
  private List<DataNodeDescription> dnds = new ArrayList();

  private String extraClassPath;
  private String hadoopDist;

  private List<String> nameNodeHosts;
  private List<String> nameNodeDataDrives;
  private List<String> nameNodeLogDrives;
  private List<NameNodeDescription> nnds = new ArrayList();
  private String nameNodeURL;

  private List<String> nodeManagerDataDrives;
  private List<String> nodeManagerLogDrives;
  private List<NodeManagerDescription> nmds = new ArrayList();

  private Integer replication;

  private String resourceManagerHost;
  private String resourceManagerDataDrives;
  private String resourceManagerLogDrive;
  private ResourceManagerDescription rmd;
  private String resourceManagerURL;
  private String resourceTrackerAddress;
  private String schedulerAddress;
  private String securityAuthentication;
  private boolean useExistingCluster;

  public HadoopDescription() {
  }

  public String getName() {
    return this.name;
  }
  protected void setName(String str) {
    this.name = str;
  }

  public Boolean addHDFSConfigurationToClassPath() {
    return this.addHDFSConfigurationToClassPath;
  }
  protected void setAddHDFSConfigurationToClassPath(Boolean b) {
    this.addHDFSConfigurationToClassPath= b;
  }

  public String getBaseHDFSDirName() {
    return this.baseHDFSDirName;
  }
  protected void setBaseHDFSDirName(String str) {
    this.baseHDFSDirName = str;
  }

  public List<String> getDataNodeHosts() {
    return this.dataNodeHosts;
  }
  protected void setDataNodeHosts(List<String> s) {
    this.dataNodeHosts = s;
  }

  public List<String> getDataNodeDataDrives() {
    return this.dataNodeDataDrives;
  }
  protected void setDataNodeDataDrives(List<String> s) {
    this.dataNodeDataDrives = s;
  }

  public List<String> getDataNodeLogDrives() {
    return this.dataNodeLogDrives;
  }
  protected void setDataNodeLogDrives(List<String> s) {
    this.dataNodeLogDrives = s;
  }

  public List<DataNodeDescription> getDataNodeDescriptions() {
    return this.dnds;
  }
  protected void addDataNodeDescription(DataNodeDescription dnd) {
    this.dnds.add(dnd);
  }

  public String getHadoopDist() {
    return this.hadoopDist;
  }
  protected void setHadoopDist(String str) {
    this.hadoopDist = str;
  }

  public List<String> getNameNodeHosts() {
    return this.nameNodeHosts;
  }
  protected void setNameNodeHosts(List<String> s) {
    this.nameNodeHosts = s;
  }

  public List<String> getNameNodeDataDrives() {
    return this.nameNodeDataDrives;
  }
  protected void setNameNodeDataDrives(List<String> s) {
    this.nameNodeDataDrives = s;
  }

  public List<String> getNameNodeLogDrives() {
    return this.nameNodeLogDrives;
  }
  protected void setNameNodeLogDrives(List<String> s) {
    this.nameNodeLogDrives = s;
  }

  public List<NameNodeDescription> getNameNodeDescriptions() {
    return this.nnds;
  }
  protected void addNameNodeDescription(NameNodeDescription nnd) {
    this.nnds.add(nnd);
  }

  public String getNameNodeURL() {
    return this.nameNodeURL;
  }
  protected void setNameNodeURL(String str) {
    this.nameNodeURL = str;
  }

  public List<String> getNodeManagerDataDrives() {
    return this.nodeManagerDataDrives;
  }
  protected void setNodeManagerDataDrives(List<String> s) {
    this.nodeManagerDataDrives = s;
  }

  public List<String> getNodeManagerLogDrives() {
    return this.nodeManagerLogDrives;
  }
  protected void setNodeManagerLogDrives(List<String> s) {
    this.nodeManagerLogDrives = s;
  }

  public List<NodeManagerDescription> getNodeManagerDescriptions() {
    return this.nmds;
  }
  protected void addNodeManagerDescription(NodeManagerDescription nmd) {
    this.nmds.add(nmd);
  }

  public Integer getReplication() {
    return this.replication;
  }
  protected void setReplication(Integer i) {
    this.replication = i;
  }

  public String getResourceManagerHost() {
    return this.resourceManagerHost;
  }
  protected void setResourceManagerHost(String s) {
    this.resourceManagerHost = s;
  }

  public String getResourceManagerDataDrives() {
    return this.resourceManagerDataDrives;
  }
  protected void setResourceManagerDataDrives(String s) {
    this.resourceManagerDataDrives = s;
  }

  public String getResourceManagerLogDrive() {
    return this.resourceManagerLogDrive;
  }
  protected void setResourceManagerLogDrive(String s) {
    this.resourceManagerLogDrive = s;
  }

  public ResourceManagerDescription getResourceManagerDescription() {
    return this.rmd;
  }
  protected void setResourceManagerDescription(ResourceManagerDescription d) {
    this.rmd = d;
  }

  public String getResourceManagerURL() {
    return this.resourceManagerURL;
  }
  protected void setResourceManagerURL(String str) {
    this.resourceManagerURL = str;
  }

  public String getResourceTrackerAddress() {
    return this.resourceTrackerAddress;
  }
  protected void setResourceTrackerAddress(String str) {
    this.resourceTrackerAddress = str;
  }

  public String getSchedulerAddress() {
    return this.schedulerAddress;
  }
  protected void setSchedulerAddress(String str) {
    this.schedulerAddress = str;
  }

  public String getSecurityAuthentication() {
    return this.securityAuthentication;
  }
  public void setSecurityAuthentication(String str) {
    this.securityAuthentication = str;
  }

  public boolean isSecure() {
    return this.getSecurityAuthentication().equals(HadoopPrms.KERBEROS)
    || this.getSecurityAuthentication().equals(HadoopPrms.KERBEROS_KINIT);
  }

  public boolean useExistingCluster() {
    return this.useExistingCluster;
  }
  public void setUseExistingCluster(boolean b) {
    this.useExistingCluster = b;
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    PRINTING                                                          ////
  //////////////////////////////////////////////////////////////////////////////

  public SortedMap toSortedMap() {
    SortedMap map = new TreeMap();
    String header = this.getClass().getName() + "." + this.getName() + ".";
    map.put(header + "addHDFSConfigurationToClassPath", this.addHDFSConfigurationToClassPath());
    map.put(header + "baseHDFSDirName", this.getBaseHDFSDirName());
    map.put(header + "dataNodeHosts", this.getDataNodeHosts());
    map.put(header + "dataNodeDataDrives", this.getDataNodeDataDrives());
    map.put(header + "dataNodeLogDrives", this.getDataNodeLogDrives());
    for (DataNodeDescription dnd : this.getDataNodeDescriptions()) {
      map.putAll(dnd.toSortedMap());
    }
    map.put(header + "hadoopDist", this.getHadoopDist());
    map.put(header + "nameNodeHosts", this.getNameNodeHosts());
    map.put(header + "nameNodeDataDrives", this.getNameNodeDataDrives());
    map.put(header + "nameNodeLogDrives", this.getNameNodeLogDrives());
    for (NameNodeDescription nnd : this.getNameNodeDescriptions()) {
      map.putAll(nnd.toSortedMap());
    }
    map.put(header + "nameNodeURL", this.getNameNodeURL());

    map.put(header + "nodeManagerDataDrives", this.getNodeManagerDataDrives());
    map.put(header + "nodeManagerLogDrives", this.getNodeManagerLogDrives());
    for (NodeManagerDescription nmd : this.getNodeManagerDescriptions()) {
      map.putAll(nmd.toSortedMap());
    }
    map.put(header + "replication", this.getReplication());
    map.put(header + "resourceManagerHost", this.getResourceManagerHost());
    map.put(header + "resourceManagerDataDrives",
                      this.getResourceManagerDataDrives());
    map.put(header + "resourceManagerLogDrive",
                      this.getResourceManagerLogDrive());
    if (this.getResourceManagerDescription() != null) {
      map.putAll(this.getResourceManagerDescription().toSortedMap());
    }
    map.put(header + "resourceManagerURL", this.getResourceManagerURL());
    map.put(header + "resourceTrackerAddress", this.getResourceTrackerAddress());
    map.put(header + "schedulerAddress", this.getSchedulerAddress());
    map.put(header + "securityAuthentication", this.getSecurityAuthentication());
    map.put(header + "useExistingCluster", this.useExistingCluster());
    return map;
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    CONFIGURATION                                                     ////
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Creates hadoop descriptions from the hadoop parameters in the test
   * configuration.
   */
  protected static void configure(TestConfig config) {
    ConfigHashtable tab = config.getParameters();
    HostDescription mhd = config.getMasterDescription().getVmDescription()
                                .getHostDescription();

    // create a description for each hadoop cluster name
    Vector names = tab.vecAt(HadoopPrms.names, new HydraVector());

    String defaultHadoopDist = System.getProperty("HADOOP_DIST",
                                      HadoopPrms.DEFAULT_HADOOP_DIST);

    for (int i = 0; i < names.size(); i++) {

      HadoopDescription hdd = new HadoopDescription();

      // name
      String name = (String)names.elementAt(i);
      hdd.setName(name);

      // addHDFSConfigurationToClassPath
      {
        Long key = HadoopPrms.addHDFSConfigurationToClassPath;
        Boolean b = tab.getBoolean(key, tab.getWild(key, i, null));
        if (b == null) {
          b = Boolean.FALSE;
        }
        hdd.setAddHDFSConfigurationToClassPath(b);
      }
      // baseHDFSDirName
      {
        Long key = HadoopPrms.baseHDFSDirName;
        String str = tab.stringAtWild(key, i,
                         HadoopPrms.DEFAULT_BASE_HDFS_DIR_NAME);
        if (str.length() == 0) {
          String s = BasePrms.nameForKey(key) + " is empty";
          throw new HydraConfigException(s);
        }
        str = EnvHelper.expandEnvVars(str, mhd);
        hdd.setBaseHDFSDirName(str);
      }
      // dataNodeHosts (generates Hadoop HostDescriptions if needed)
      {
        Long key = HadoopPrms.dataNodeHosts;
        Vector strs = tab.vecAtWild(key, i, null);
        if (strs == null) {
          strs = new HydraVector(mhd.getHostName());
        }
        List<String> hosts = new ArrayList();
        for (int n = 0; n < strs.size(); n++) {
          String str = (String)strs.get(n);
          String host = EnvHelper.convertHostName(str);
          if (!hosts.contains(host)) {
            hosts.add(host);
            generateHostDescription(config, host, mhd);
          }
        }
        hdd.setDataNodeHosts(hosts);
      }
      // dataNodeDataDrives
      {
        Long key = HadoopPrms.dataNodeDataDrives;
        Vector strs = tab.vecAtWild(key, i, null);
        if (strs != null) {
          List<String> drives = new ArrayList();
          drives.addAll(strs);
          hdd.setDataNodeDataDrives(adjustSize(drives,
                                    hdd.getDataNodeHosts().size()));
        }
      }
      // dataNodeLogDrives
      {
        Long key = HadoopPrms.dataNodeLogDrives;
        Vector strs = tab.vecAtWild(key, i, null);
        if (strs != null) {
          List<String> drives = new ArrayList();
          drives.addAll(strs);
          for (String drive : drives) {
            if (drive.contains(":")) {
              String s = BasePrms.nameForKey(key)
                       + " contains an illegal multi-drive entry: " + drive;
              throw new HydraConfigException(s);
            }
          }
          hdd.setDataNodeLogDrives(adjustSize(drives,
                                   hdd.getDataNodeHosts().size()));
        }
      }
      // hadoopDist
      {
        Long key = HadoopPrms.hadoopDist;
        String str = tab.stringAtWild(key, i, defaultHadoopDist);
        str = EnvHelper.expandEnvVars(str, mhd);
        hdd.setHadoopDist(str);
      }
      // nameNodeHosts (generates Hadoop HostDescriptions if needed)
      {
        Long key = HadoopPrms.nameNodeHosts;
        Vector strs = tab.vecAtWild(key, i, null);
        if (strs == null) {
          strs = new HydraVector(mhd.getHostName());
        }
        if (strs.size() > 1) {
          String s = BasePrms.nameForKey(key)
                   + " does not support multiple NameNodes";
          throw new UnsupportedOperationException(s);
        }
        List<String> hosts = new ArrayList();
        for (int n = 0; n < strs.size(); n++) {
          String str = (String)strs.get(n);
          String host = EnvHelper.convertHostName(str);
          if (!hosts.contains(host)) {
            hosts.add(host);
            generateHostDescription(config, host, mhd);
          }
        }
        hdd.setNameNodeHosts(hosts);
      }
      // nameNodeDataDrives
      {
        Long key = HadoopPrms.nameNodeDataDrives;
        Vector strs = tab.vecAtWild(key, i, null);
        if (strs != null) {
          List<String> drives = new ArrayList();
          drives.addAll(strs);
          hdd.setNameNodeDataDrives(adjustSize(drives,
                                    hdd.getNameNodeHosts().size()));
        }
      }
      // nameNodeLogDrives
      {
        Long key = HadoopPrms.nameNodeLogDrives;
        Vector strs = tab.vecAtWild(key, i, null);
        if (strs != null) {
          List<String> drives = new ArrayList();
          drives.addAll(strs);
          for (String drive : drives) {
            if (drive.contains(":")) {
              String s = BasePrms.nameForKey(key)
                       + " contains an illegal multi-drive entry: " + drive;
              throw new HydraConfigException(s);
            }
          }
          hdd.setNameNodeLogDrives(adjustSize(drives,
                                              hdd.getNameNodeHosts().size()));
        }
      }
      // nameNodeURL and useExistingCluster
      {
        Long key = HadoopPrms.nameNodeURL;
        String str = tab.stringAtWild(key, i, null);
        if (str == null) {
          String host = hdd.getNameNodeHosts().iterator().next();
          str = "hdfs://" + HostHelper.getCanonicalHostName(host)
              + ":" + PortHelper.getRandomPort(); // TBD: use hostagent
          hdd.setUseExistingCluster(false);
        } else {
          hdd.setUseExistingCluster(true);
        }
        hdd.setNameNodeURL(str);
      }
      // nodeManagerDataDrives
      {
        Long key = HadoopPrms.nodeManagerDataDrives;
        Vector strs = tab.vecAtWild(key, i, null);
        if (strs != null) {
          List<String> drives = new ArrayList();
          drives.addAll(strs);
          hdd.setNodeManagerDataDrives(adjustSize(drives,
                                       hdd.getDataNodeHosts().size()));
        }
      }
      // nodeManagerLogDrives
      {
        Long key = HadoopPrms.nodeManagerLogDrives;
        Vector strs = tab.vecAtWild(key, i, null);
        if (strs != null) {
          List<String> drives = new ArrayList();
          drives.addAll(strs);
          for (String drive : drives) {
            if (drive.contains(":")) {
              String s = BasePrms.nameForKey(key)
                       + " contains an illegal multi-drive entry: " + drive;
              throw new HydraConfigException(s);
            }
          }
          hdd.setNodeManagerLogDrives(adjustSize(drives,
                                      hdd.getDataNodeHosts().size()));
        }
      }
      // replication
      {
        Long key = HadoopPrms.replication;
        Integer val = tab.getInteger(key, tab.getWild(key, i, null));
        if (val == null) {
          val = HadoopPrms.DEFAULT_REPLICATION;
        }
        hdd.setReplication(val);
      }
      // resourceManagerHost (generates a Hadoop HostDescription if needed)
      {
        Long key = HadoopPrms.resourceManagerHost;
        String str = tab.stringAtWild(key, i, null);
        if (str == null) {
          str = mhd.getHostName();
        }
        String host = EnvHelper.convertHostName(str);
        generateHostDescription(config, host, mhd);
        hdd.setResourceManagerHost(host);
      }
      // resourceManagerDataDrives
      {
        Long key = HadoopPrms.resourceManagerDataDrives;
        String str = tab.stringAtWild(key, i, null);
        if (str != null) {
          hdd.setResourceManagerDataDrives(str);
        }
      }
      // resourceManagerLogDrive
      {
        Long key = HadoopPrms.resourceManagerLogDrive;
        String str = tab.stringAtWild(key, i, null);
        if (str != null) {
          if (str.contains(":")) {
            String s = BasePrms.nameForKey(key)
                     + " contains an illegal multi-drive entry: " + str;
            throw new HydraConfigException(s);
          }
          hdd.setResourceManagerLogDrive(str);
        }
      }
      // resourceManagerURL
      {
        Long key = HadoopPrms.resourceManagerURL;
        String str = tab.stringAtWild(key, i, null);
        if (str == null) {
          ResourceManagerDescription rmd = hdd.getResourceManagerDescription();
          String host = hdd.getResourceManagerHost();
          str = "hdfs://" + HostHelper.getCanonicalHostName(host)
              + ":" + PortHelper.getRandomPort(); // TBD: use hostagent
        } else {
          String s = "Connecting to an existing resource manager is not supported";
          throw new UnsupportedOperationException(s);
        }
        hdd.setResourceManagerURL(str);
      }
      // resourceTrackerAddress
      {
        Long key = HadoopPrms.resourceTrackerAddress;
        String str = tab.stringAtWild(key, i, null);
        if (str == null) {
          str = hdd.getResourceManagerHost()
              + ":" + PortHelper.getRandomPort(); // TBD: use hostagent
        } else {
          String s = "Connecting to an existing resource tracker is not supported";
          throw new UnsupportedOperationException(s);
        }
        hdd.setResourceTrackerAddress(str);
      }
      // schedulerAddress
      {
        Long key = HadoopPrms.schedulerAddress;
        String str = tab.stringAtWild(key, i, null);
        if (str == null) {
          str = hdd.getResourceManagerHost()
              + ":" + PortHelper.getRandomPort(); // TBD: use hostagent
        } else {
          String s = "Connecting to an existing scheduler is not supported";
          throw new UnsupportedOperationException(s);
        }
        hdd.setSchedulerAddress(str);
      }
      // securityAuthentication
      {
        Long key = HadoopPrms.securityAuthentication;
        String str = tab.getString(key, tab.getWild(key, i, null));
        hdd.setSecurityAuthentication(getSecurityAuthentication(str, key));
      }

      // add description to test config
      config.addHadoopDescription(hdd);
    }

    // postprocessing ----------------------------------------------------------

    // generate a HostDescription for each unique host based on the master
    {
      Set<String> clusterHosts = new HashSet();
      for (HadoopDescription hdd : config.getHadoopDescriptions().values()) {
        clusterHosts.addAll(hdd.getNameNodeHosts());
        clusterHosts.addAll(hdd.getDataNodeHosts());
        if (hdd.getResourceManagerHost() != null) {
          clusterHosts.add(hdd.getResourceManagerHost());
        }
      }
      for (String host : clusterHosts) {
        generateHostDescription(config, host, mhd);
      }
    }

    // generate NodeDescriptions for each node/manager in each cluster
    for (HadoopDescription hdd : config.getHadoopDescriptions().values()) {
      for (int i = 0; i < hdd.getNameNodeHosts().size(); i++) {
        NameNodeDescription nnd = new NameNodeDescription();
        nnd.configure(config, hdd, i);
        hdd.addNameNodeDescription(nnd);
      }
      for (int i = 0; i < hdd.getDataNodeHosts().size(); i++) {
        DataNodeDescription dnd = new DataNodeDescription();
        dnd.configure(config, hdd, i);
        hdd.addDataNodeDescription(dnd);
      }
      for (int i = 0; i < hdd.getDataNodeHosts().size(); i++) {
        NodeManagerDescription nmd = new NodeManagerDescription();
        nmd.configure(config, hdd, i);
        hdd.addNodeManagerDescription(nmd);
      }
      ResourceManagerDescription rmd = new ResourceManagerDescription();
      rmd.configure(config, hdd);
      hdd.setResourceManagerDescription(rmd);
    }
  }

  /**
   * This method returns the extra classpath needed to support all clusters.
   */
  protected static String getExtraClassPath(TestConfig config) {
    String cp = "";
    for (HadoopDescription hdd : config.getHadoopDescriptions().values()) {
      if (hdd.addHDFSConfigurationToClassPath()) {
        if (hdd.useExistingCluster()) {
          String s = "HDFS configuration is unavailable for existing cluster: "
                   + hdd.getName() + " at " + hdd.getNameNodeURL();
          throw new UnsupportedOperationException(s);

        } else if (hdd.getNameNodeDescriptions().size() == 0) {
          String s = "No NameNode configuration found for Hadoop cluster: "
                   + hdd.getName();
          throw new UnsupportedOperationException(s);

        } else {
          NameNodeDescription nnd = hdd.getNameNodeDescriptions().get(0);
          if (cp.length() > 0) {
            cp += nnd.getHostDescription().getPathSep();
          }
          cp += nnd.getConfDir() + nnd.getHostDescription().getFileSep();
        }
      }
    }
    return (cp.length() > 0) ? cp : null;
  }

//------------------------------------------------------------------------------
// Security authentication configuration support
//------------------------------------------------------------------------------

  /**
   * Returns the security authentication for the given string.
   */
  private static String getSecurityAuthentication(String str, Long key) {
    if (str == null) {
      return HadoopPrms.SIMPLE;

    } else if (str.equalsIgnoreCase(HadoopPrms.KERBEROS)) {
      return HadoopPrms.KERBEROS;

    } else if (str.equalsIgnoreCase(HadoopPrms.KERBEROS_KINIT)
           ||  str.equalsIgnoreCase("kerberos_kinit")) {
      return HadoopPrms.KERBEROS_KINIT;

    } else if (str.equalsIgnoreCase(HadoopPrms.SIMPLE)) {
      return HadoopPrms.SIMPLE;

    } else {
      String s = BasePrms.nameForKey(key) + " has illegal value: " + str;
      throw new HydraConfigException(s);
    }
  }

//------------------------------------------------------------------------------
// HadoopDescription support
//------------------------------------------------------------------------------

  /**
   * Truncates or pads the list as needed to match the given size.
   */
  private static List<String> adjustSize(List<String> list, int size) {
    if (list.size() > size) { // truncate
      list = list.subList(0, size);
    } else if (list.size() < size) { // pad
      String last = list.get(list.size() - 1);
      while (list.size() < size) {
        list.add(last);
      }
    }
    return list;
  }

//------------------------------------------------------------------------------
// HostDescription support
//------------------------------------------------------------------------------

  /**
   * Generates a hadoop host description based on the hydra master.
   */
  private static void generateHostDescription(TestConfig config, String host,
                                              HostDescription mhd) {
    String logicalHost = HadoopPrms.LOGICAL_HOST_PREFIX + host;
    HostDescription hd = config.getHadoopHostDescription(logicalHost);
    if (hd == null) {
      hd = mhd.copy();
      hd.setName(logicalHost);
      hd.setHostName(host);
      hd.setCanonicalHostName(HostHelper.getCanonicalHostName(host));
      config.addHadoopHostDescription(hd);
    }
  }

//------------------------------------------------------------------------------
// NodeDescription support
//------------------------------------------------------------------------------

  /**
   * Represents a data node configuration for a given cluster and host.
   */
  public static class DataNodeDescription
  extends NodeDescription implements Serializable {

    public DataNodeDescription() {
    }

    public void configure(TestConfig config, HadoopDescription hdd, int i) {
      String host = hdd.getDataNodeHosts().get(i);
      String dataDrives = hdd.getDataNodeDataDrives() == null
                        ? null : hdd.getDataNodeDataDrives().get(i);
      String logDrive = hdd.getDataNodeLogDrives() == null
                      ? null : hdd.getDataNodeLogDrives().get(i);
      configure(NodeType.DataNode, config, hdd, host, dataDrives, logDrive);
    }
  }

  /**
   * Represents a name node configuration for a given cluster and host.
   */
  public static class NameNodeDescription
  extends NodeDescription implements Serializable {

    public NameNodeDescription() {
    }

    public void configure(TestConfig config, HadoopDescription hdd, int i) {
      String host = hdd.getNameNodeHosts().get(i);
      String dataDrives = hdd.getNameNodeDataDrives() == null
                        ? null : hdd.getNameNodeDataDrives().get(i);
      String logDrive = hdd.getNameNodeLogDrives() == null
                      ? null : hdd.getNameNodeLogDrives().get(i);
      configure(NodeType.NameNode, config, hdd, host, dataDrives, logDrive);
    }
  }

  /**
   * Represents a node manager configuration for a given cluster and host.
   */
  public static class NodeManagerDescription
  extends NodeDescription implements Serializable {

    public NodeManagerDescription() {
    }

    public void configure(TestConfig config, HadoopDescription hdd, int i) {
      String host = hdd.getDataNodeHosts().get(i);
      String dataDrives = hdd.getNodeManagerDataDrives() == null
                        ? null : hdd.getNodeManagerDataDrives().get(i);
      String logDrive = hdd.getNodeManagerLogDrives() == null
                      ? null : hdd.getNodeManagerLogDrives().get(i);
      configure(NodeType.NodeManager, config, hdd, host, dataDrives, logDrive);
    }
  }

  /**
   * Represents a resource manager configuration for a given cluster.
   */
  public static class ResourceManagerDescription
  extends NodeDescription implements Serializable {

    public ResourceManagerDescription() {
    }

    public void configure(TestConfig config, HadoopDescription hdd) {
      String host = hdd.getResourceManagerHost();
      String dataDrives = hdd.getResourceManagerDataDrives();
      String logDrive = hdd.getResourceManagerLogDrive();
      configure(NodeType.ResourceManager, config, hdd, host,
                                          dataDrives, logDrive);
    }
  }

  /**
   * Represents a node configuration for a given cluster and host.
   */
  public static abstract class NodeDescription implements Serializable {
    private String name;
    private String clusterName;
    private String confDir;
    private List<String> dataDirs;
    private HostDescription hd;
    private String hostName;
    private String logDir;
    private NodeType nodeType;
    private String pidDir;

    public String getName() {
      return this.name;
    }

    public String getClusterName() {
      return this.clusterName;
    }

    public String getConfDir() {
      return this.confDir;
    }

    public List<String> getDataDirs() {
      return this.dataDirs;
    }

    public String getDataDirsAsString() {
      StringBuffer sb = new StringBuffer();
      for (String dir : this.dataDirs) {
        sb.append(dir).append(",");
      }
      String s = sb.toString();
      return s.substring(0,s.length()-1);
    }

    public HostDescription getHostDescription() {
      return this.hd;
    }

    public String getHostName() {
      return this.hostName;
    }

    public String getLogDir() {
      return this.logDir;
    }

    public NodeType getNodeType() {
      return this.nodeType;
    }

    public String getPIDDir() {
      return this.pidDir;
    }

    public SortedMap toSortedMap() {
      SortedMap map = new TreeMap();
      String header = this.getClass().getName() + "." + this.getName() + ".";
      map.put(header + "clusterName", this.getClusterName());
      map.put(header + "confDir", this.getConfDir());
      map.put(header + "dataDirs", this.getDataDirs());
      map.put(header + "hostName", this.getHostName());
      map.put(header + "hostDescription", this.getHostDescription().getName());
      map.put(header + "logDir", this.getLogDir());
      map.put(header + "nodeType", this.getNodeType());
      map.put(header + "pidDir", this.getPIDDir());
      return map;
    }

    public String toString() {
      return this.name;
    }

    public void configure(NodeType type, TestConfig config,
                          HadoopDescription hdd, String host,
                          String dataDrives, String logDrive) {
      this.clusterName = hdd.getName();
      this.nodeType = type;
      this.name = this.clusterName + "_" + this.nodeType + "_" + host;
      this.hostName = host;
      this.hd = config.getHadoopHostDescriptionForPhysicalHost(host);

      this.confDir = hd.getUserDir()
                   + "/" + this.clusterName + "_" + this.nodeType
                   + "_" + this.hostName + "_conf";

      this.pidDir = hd.getUserDir()
                  + "/" + this.clusterName + "_" + this.nodeType
                  + "_" + this.hostName + "_pids";

      if (logDrive == null) {
        String locality = HostHelper.isLocalHost(this.hostName)
                        ? "" : "_nonlocal";
        this.logDir = hd.getUserDir()
                    + "/" + this.clusterName + "_" + this.nodeType
                    + "_" + this.hostName + locality + "_logs";
      } else {
        this.logDir = "/export/" + this.hostName + logDrive
                    + "/users/" + System.getProperty("user.name")
                    + "/" + hdd.getBaseHDFSDirName()
                    + "/" + FileUtil.filenameFor(hd.getUserDir())
                    + "/" + this.clusterName + "_" + this.nodeType
                    + "_" + this.hostName + logDrive + "_logs";
      }

      this.dataDirs = new ArrayList();
      if (dataDrives == null) {
        String locality = HostHelper.isLocalHost(this.hostName)
                        ? "" : "_nonlocal";
        String dataDir = hd.getUserDir()
                       + "/" + this.clusterName + "_" + this.nodeType
                       + "_" + this.hostName + locality + "_data";
        this.dataDirs.add(dataDir);
      } else {
        for (String drive : dataDrives.split(":")) {
          String dataDir = "/export/" + this.hostName + drive
                         + "/users/" + System.getProperty("user.name")
                         + "/" + hdd.getBaseHDFSDirName()
                         + "/" + FileUtil.filenameFor(hd.getUserDir())
                         + "/" + this.clusterName + "_" + this.nodeType
                         + "_" + this.hostName + drive + "_data";
          this.dataDirs.add(dataDir);
        }
      }

      // save the conf and log dirs and optionally the data dirs
      recordDir(this.confDir, true);
      recordDir(this.pidDir, true);
      recordDir(this.logDir, true);
      boolean moveHadoopData = Boolean.getBoolean(BatteryTest.MOVE_HADOOP_DATA);
      for (String dataDir : this.dataDirs) {
        recordDir(dataDir, moveHadoopData);
      }
    }

    /**
     * Extracts the file system (host and drive) from the directory.
     */
    private String getFileSystem(String dir) {
      String fs = dir;
      if (fs.startsWith("/export/")) {
        fs = fs.substring(8, dir.length() - 1);
      }
      return fs.substring(0, fs.indexOf("/"));
    }

    /**
     * Creates and records the directory.
     */
    private void recordDir(String dir, boolean moveAfterTest) {
      File f = new File(dir);
      if (f.exists()) {
        String s = "Already created directory " + dir;
        throw new HydraInternalException(s);
      }
      FileUtil.mkdir(f);
      Nuker.getInstance().recordHDFSDir(this.hd, this.nodeType, dir,
                                        moveAfterTest);
    }
  }
}
