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

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.admin.DistributedSystemConfig;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import java.io.File;
import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.*;

/**
 * Encodes information needed to describe and create an admin distributed
 * system.
 */
public class AdminDescription extends AbstractDescription
implements Serializable {

  private static final long serialVersionUID = 1L;

  /** The logical name of this admin description */
  private String name;

  /** Remaining parameters, in alphabetical order */
  private Integer ackSevereAlertThreshold;
  private Integer ackWaitThreshold;
  private Boolean disableTcp;
  private String  distributedSystem;
  private Boolean emailNotificationEnabled;
  private String  emailNotificationFrom;
  private String  emailNotificationHost;
  private String  emailNotificationToList;
  private Boolean enableNetworkPartitionDetection;
  private Boolean disableAutoReconnect;
  private GemFireDescription gemfireDescription; // from distributedSystem
  private String  locators;
  private Integer logDiskSpaceLimit;
  private String  logFile;
  private Integer logFileSizeLimit;
  private String  logLevel;
  private String  mcastAddress;
  private Integer mcastPort;
  private Integer memberTimeout;
  private Integer refreshInterval;
  private String  remoteCommand;
  private String  sslName;
  private SSLDescription sslDescription; // from sslName

//------------------------------------------------------------------------------
// Constructors
//------------------------------------------------------------------------------

  public AdminDescription() {
  }

//------------------------------------------------------------------------------
// Accessors, in aphabetical order after name
//------------------------------------------------------------------------------

  /**
   * Returns the logical name of this admin description.
   */
  public String getName() {
    return this.name;
  }

  /**
   * Sets the logical name of this admin description.
   */
  private void setName(String str) {
    this.name = str;
  }

  /**
   * Returns the ack severe alert threshold.
   */
  private Integer getAckSevereAlertThreshold() {
    return this.ackSevereAlertThreshold;
  }

  /**
   * Sets the ack severe alert threshold.
   */
  private void setAckSevereAlertThreshold(Integer i) {
    this.ackSevereAlertThreshold = i;
  }

  /**
   * Returns the ack wait threshold.
   */
  private Integer getAckWaitThreshold() {
    return this.ackWaitThreshold;
  }

  /**
   * Sets the ack wait threshold.
   */
  private void setAckWaitThreshold(Integer i) {
    this.ackWaitThreshold = i;
  }

  /**
   * Returns the disable tcp inherited from {@link #distributedSystem}.
   */
  public Boolean getDisableTcp() {
    return this.disableTcp;
  }

  /**
   * Sets the disable tcp inherited from {@link #distributedSystem}.
   */
  private void setDisableTcp(Boolean b) {
    this.disableTcp = b;
  }

  /**
   * Returns the name of the distributed system.
   */
  protected String getDistributedSystem() {
    return this.distributedSystem;
  }

  /**
   * Sets the name of the distributed system.
   */
  private void setDistributedSystem(String str) {
    this.distributedSystem = str;
  }

  /**
   * Returns the email notification enabled.
   */
  public Boolean getEmailNotificationEnabled() {
    return this.emailNotificationEnabled;
  }

  /**
   * Sets the email notification enabled.
   */
  private void setEmailNotificationEnabled(Boolean b) {
    this.emailNotificationEnabled = b;
  }

  /**
   * Returns the email notification from.
   */
  public String getEmailNotificationFrom() {
    return this.emailNotificationFrom;
  }

  /**
   * Sets the email notification from.
   */
  private void setEmailNotificationFrom(String emailID) {
    this.emailNotificationFrom = emailID;
  }

  /**
   * Returns the email notification host.
   */
  public String getEmailNotificationHost() {
    return this.emailNotificationHost;
  }

  /**
   * Sets the email notification host.
   */
  private void setEmailNotificationHost(String hostName) {
    this.emailNotificationHost = hostName;
  }

  /**
   * Returns the email notification to list.
   */
  public String getEmailNotificationToList() {
    return this.emailNotificationToList;
  }

  /**
   * Sets the email notification to list.
   */
  private void setEmailNotificationToList(String emailIDs) {
    this.emailNotificationToList = emailIDs;
  }

  /**
   * Returns the enable network partition detection.
   */
  public Boolean getEnableNetworkPartitionDetection() {
    return this.enableNetworkPartitionDetection;
  }

  /**
   * Sets the enable network partition detection.
   */
  private void setEnableNetworkPartitionDetection(Boolean b) {
    this.enableNetworkPartitionDetection = b;
  }

  public Boolean getDisableAutoReconnect() {
    return disableAutoReconnect;
  }

  private void setDisableAutoReconnect(Boolean disableAutoReconnect) {
    this.disableAutoReconnect = disableAutoReconnect;
  }

  /**
   * Returns the gemfire description.
   */
  private GemFireDescription getGemFireDescription() {
    return this.gemfireDescription;
  }

  /**
   * Sets the gemfire description.
   */
  private void setGemFireDescription(GemFireDescription gfd) {
    this.gemfireDescription = gfd;
  }

  /*
   * Returns the locators for {@link #distributedSystem} discovered at runtime
   * and caches them.
   */
  private String getLocators() {
    if (this.locators == null) {
      String locs = null;
      if (getGemFireDescription().getUseLocator().booleanValue()) {
        if (this.distributedSystem.equals(GemFirePrms.LONER)) {
          locs = DistributedSystemConfig.DEFAULT_LOCATORS;
        } else if (TestConfig.tab().booleanAt(Prms.manageLocatorAgents)) {
          locs = TestConfig.getInstance().getMasterDescription()
                           .getLocator(this.distributedSystem);
        } else { // client-managed
          List endpoints =
            DistributedSystemHelper.getEndpoints(this.distributedSystem);
          locs = DistributedSystemHelper.endpointsToString(endpoints);
        }
      } else {
        locs =  DistributedSystemConfig.DEFAULT_LOCATORS;
      }
      this.locators = locs;
    }
    return this.locators;
  }

  /**
   * Returns the log disk space limit.
   */
  private Integer getLogDiskSpaceLimit() {
    return this.logDiskSpaceLimit;
  }

  /**
   * Sets the log disk space limit.
   */
  private void setLogDiskSpaceLimit(Integer i) {
    this.logDiskSpaceLimit = i;
  }

  /*
   * For internal hydra use only.  Hydra client-side method only.
   * <p>
   * Returns the log file name.  Autogenerates the directory name at runtime
   * using the same path as the master.  The directory is created if needed.
   *
   * @throws HydraRuntimeException if the directory cannot be created.
   */
  private synchronized String getLogFile() {
    if (this.logFile == null) {
      HostDescription hd = TestConfig.getInstance().getMasterDescription()
                                     .getVmDescription().getHostDescription();
      String dirname = hd.getUserDir() + File.separator
                     + "vm_" + RemoteTestModule.getMyVmid()
                     + "_" + RemoteTestModule.getMyClientName()
                     + "_" + HostHelper.getLocalHost()
                     + "_" + RemoteTestModule.getMyPid()
                     + "_admin";
      File dir = new File(dirname);
      String fullname = dir.getAbsolutePath();
      try {
        FileUtil.mkdir(dir);
        try {
          RemoteTestModule.Master.recordDir(hd,
                    this.getGemFireDescription().getName(), fullname);
        } catch (RemoteException e) {
          String s = "Unable to access master to record directory: " + dir;
          throw new HydraRuntimeException(s, e);
        }
      }
      catch (VirtualMachineError e) {
        SystemFailure.initiateFailure(e);
        throw e;
      }
      catch (Error e) {
        String s = "Unable to create directory: " + dir;
        throw new HydraRuntimeException(s);
      }
      this.logFile = dirname + File.separator + "system.log";
    }
    return this.logFile;
  }

  /**
   * Returns the log file size limit.
   */
  private Integer getLogFileSizeLimit() {
    return this.logFileSizeLimit;
  }

  /**
   * Sets the log file size limit.
   */
  private void setLogFileSizeLimit(Integer i) {
    this.logFileSizeLimit = i;
  }

  /**
   * Returns the log level for the system log.
   */
  private String getLogLevel() {
    return this.logLevel;
  }

  /**
   * Sets the log level for the system log.
   */
  private void setLogLevel(String str) {
    this.logLevel = str;
  }

  /*
   * Returns the mcast address inherited from the {@link #distributedSystem}.
   */
  private String getMcastAddress() {
    return this.mcastAddress;
  }

  /*
   * Sets the mcast address inherited from the {@link #distributedSystem}.
   */
  private void setMcastAddress(String str) {
    this.mcastAddress = str;
  }

  /*
   * Returns the mcast port inherited from the {@link #distributedSystem}.
   * Requires the gemfire description to be postprocessed first.
   */
  private Integer getMcastPort() {
    if (this.mcastPort == null) {
      Map gfds = TestConfig.getInstance().getGemFireDescriptions();
      for (Iterator i = gfds.values().iterator(); i.hasNext();) {
        GemFireDescription gfd = (GemFireDescription)i.next();
        if (gfd.getDistributedSystem().equals(this.distributedSystem)) {
          this.mcastPort = gfd.getMcastPort();
          break;
        }
      }
    }
    return this.mcastPort;
  }

  /**
   * Returns the member timeout.
   */
  private Integer getMemberTimeout() {
    return this.memberTimeout;
  }

  /**
   * Sets the member timeout.
   */
  private void setMemberTimeout(Integer i) {
    this.memberTimeout = i;
  }

  /**
   * Returns the refresh interval.
   */
  private Integer getRefreshInterval() {
    return this.refreshInterval;
  }

  /**
   * Sets the refresh interval.
   */
  private void setRefreshInterval(Integer i) {
    this.refreshInterval = i;
  }

  /**
   * Returns the remote command.
   */
  private String getRemoteCommand() {
    return this.remoteCommand;
  }

  /**
   * Sets the remote command.
   */
  private void setRemoteCommand(String str) {
    this.remoteCommand = str;
  }

  /**
   * Returns the SSL name.
   */
  public String getSSLName() {
    return this.sslName;
  }

  /**
   * Sets the SSL name.
   */
  private void setSSLName(String str) {
    this.sslName = str;
  }

  /**
   * Returns the SSL description.
   */
  private SSLDescription getSSLDescription() {
    return this.sslDescription;
  }

  /**
   * Sets the SSL description.
   */
  private void setSSLDescription(SSLDescription sd) {
    this.sslDescription = sd;
  }

//------------------------------------------------------------------------------
// Admin configuration
//------------------------------------------------------------------------------

  /**
   * Configures the admin distributed system using this admin description.
   * <p>
   * If SSL is configured and enabled, sets SSL-related properties on the VM
   * as a side-effect.
   */
  public void configure(DistributedSystemConfig d) {
    d.setAckSevereAlertThreshold(this.getAckSevereAlertThreshold().intValue());
    d.setAckWaitThreshold(this.getAckWaitThreshold().intValue());
    if (TestConfig.tab().booleanAt(Prms.useIPv6)) {
      String ipv6 = HostHelper.getHostAddress(HostHelper.getIPv6Address());
      if (ipv6 == null) {
        String s = "IPv6 address is not available for host "
             + this.getGemFireDescription().getHostDescription().getHostName();
        throw new HydraRuntimeException(s);
      }
      d.setBindAddress(ipv6);
      d.setServerBindAddress(ipv6);
    }
    d.setDisableTcp(this.getDisableTcp().booleanValue());
    // @todo uncomment these when Bug 39009 is fixed
    //d.setEmailNotificationEnabled(this.getEmailNotificationEnabled().booleanValue());
    //d.setEmailNotificationFrom(this.getEmailNotificationFrom());
    //d.setEmailNotificationHost(this.getEmailNotificationHost());
    //d.setEmailNotificationToList(this.getEmailNotificationToList());
    d.setEnableNetworkPartitionDetection(this.getEnableNetworkPartitionDetection().booleanValue());
    Boolean b = getDisableAutoReconnect();
    if (b != null) {
      d.setDisableAutoReconnect(b.booleanValue());
    }
    d.setLocators(this.getLocators());
    d.setLogDiskSpaceLimit(this.getLogDiskSpaceLimit().intValue());
    d.setLogFile(this.getLogFile());
    d.setLogFileSizeLimit(this.getLogFileSizeLimit().intValue());
    d.setLogLevel(this.getLogLevel());
    d.setMcastAddress(this.getMcastAddress());
    d.setMcastPort(this.getMcastPort().intValue());
    d.setMemberTimeout(this.getMemberTimeout().intValue());
    d.setRefreshInterval(this.getRefreshInterval().intValue());
    d.setRemoteCommand(this.getRemoteCommand());
    SSLDescription sd = this.getSSLDescription();
    if (sd != null && sd.getSSLEnabled().booleanValue()) {
      d.setSSLCiphers(sd.getSSLCiphers());
      d.setSSLEnabled(sd.getSSLEnabled().booleanValue());
      d.setSSLProtocols(sd.getSSLProtocols());
      d.setSSLAuthenticationRequired(sd.getSSLRequireAuthentication()
                                       .booleanValue());
      // set the VM-level properties related to SSL as a side-effect
      String s = DistributionConfig.GEMFIRE_PREFIX;
      System.setProperty(s + DistributedSystemConfig.SSL_CIPHERS_NAME,
                             sd.getSSLCiphers());
      System.setProperty(s + DistributedSystemConfig.SSL_ENABLED_NAME,
                             sd.getSSLEnabled().toString());
      System.setProperty(s + DistributedSystemConfig.SSL_PROTOCOLS_NAME,
                             sd.getSSLProtocols());
      System.setProperty(s + DistributedSystemConfig.SSL_REQUIRE_AUTHENTICATION_NAME,
                             sd.getSSLRequireAuthentication().toString());
    }
  }

  /**
   * Returns the admin distributed system as a string.  For use only by {@link
   * DistributedSystemHelper#adminToString(AdminDistributedSystem)}.
   */
  protected static synchronized String adminToString(
                                       DistributedSystemConfig d) {
    StringBuffer buf = new StringBuffer();
    buf.append("\n  ackSevereAlertThreshold: " + d.getAckSevereAlertThreshold());
    buf.append("\n  ackWaitThreshold: " + d.getAckWaitThreshold());
    buf.append("\n  bindAddress: " + d.getBindAddress());
    buf.append("\n  disableTcp: " + d.getDisableTcp());
    // @todo uncomment these when Bug 39009 is fixed
    //buf.append("\n  setEmailNotificationEnabled: " + d.getEmailNotificationEnabled());
    //buf.append("\n  setEmailNotificationFrom: " + d.getEmailNotificationFrom());
    //buf.append("\n  setEmailNotificationHost: " + d.getEmailNotificationHost());
    //buf.append("\n  setEmailNotificationToList: " + d.getEmailNotificationToList());
    buf.append("\n  enableNetworkPartitionDetection: " + d.getEnableNetworkPartitionDetection());
    buf.append("\n  locators: " + d.getLocators());
    buf.append("\n  logDiskSpaceLimit: " + d.getLogDiskSpaceLimit());
    buf.append("\n  logFile: " + d.getLogFile());
    buf.append("\n  logFileSizeLimit: " + d.getLogFileSizeLimit());
    buf.append("\n  logLevel: " + d.getLogLevel());
    buf.append("\n  mcastAddress: " + d.getMcastAddress());
    buf.append("\n  mcastPort: " + d.getMcastPort());
    buf.append("\n  memberTimeout: " + d.getMemberTimeout());
    buf.append("\n  refreshInterval: " + d.getRefreshInterval());
    buf.append("\n  remoteCommand: " + d.getRemoteCommand());
    buf.append("\n  serverBindAddress: " + d.getServerBindAddress());
    buf.append("\n  sslCiphers: " + d.getSSLCiphers());
    buf.append("\n  sslEnabled: " + d.isSSLEnabled());
    buf.append("\n  sslProtocols: " + d.getSSLProtocols());
    buf.append("\n  sslRequireAuthentication: " + d.isSSLAuthenticationRequired());
    buf.append("\n  systemId: " + d.getSystemId());
    buf.append("\n  systemName: " + d.getSystemName());
    return buf.toString();
  }

//------------------------------------------------------------------------------
// Printing
//------------------------------------------------------------------------------

  public SortedMap toSortedMap() {
    SortedMap map = new TreeMap();
    String header = this.getClass().getName() + "." + this.getName() + ".";
    map.put(header + "ackSevereAlertThreshold", this.getAckSevereAlertThreshold());
    map.put(header + "ackWaitThreshold", this.getAckWaitThreshold());
    map.put(header + "disableTcp", this.getDisableTcp());
    map.put(header + "emailNotificationEnabled", this.getEmailNotificationEnabled());
    map.put(header + "emailNotificationFrom", this.getEmailNotificationFrom());
    map.put(header + "emailNotificationHost", this.getEmailNotificationHost());
    map.put(header + "emailNotificationToList", this.getEmailNotificationToList());
    map.put(header + "enableNetworkPartitionDetection", this.getEnableNetworkPartitionDetection());
    map.put(header + "disableAutoReconnect", this.getDisableAutoReconnect());
    map.put(header + "locators", "autogenerated");
    map.put(header + "logDiskSpaceLimit", this.getLogDiskSpaceLimit());
    map.put(header + "logFile", "autogenerated: same path as test directory");
    map.put(header + "logFileSizeLimit", this.getLogFileSizeLimit());
    map.put(header + "logLevel", this.getLogLevel());
    map.put(header + "mcastAddress", this.getMcastAddress());
    map.put(header + "mcastPort", "inherited from distributed system");
    map.put(header + "memberTimeout", this.getMemberTimeout());
    map.put(header + "refreshInterval", this.getRefreshInterval());
    map.put(header + "remoteCommand", this.getRemoteCommand());
    map.put(header + "sslName", this.getSSLName());
    return map;
  }

//------------------------------------------------------------------------------
// Configuration
//------------------------------------------------------------------------------

  /**
   * Creates admin descriptions from the admin parameters in the test
   * configuration.
   */
  protected static void configure(TestConfig config) {

    ConfigHashtable tab = config.getParameters();

    // create a description for each admin name
    Vector names = tab.vecAt(AdminPrms.names, new HydraVector());

    for (int i = 0; i < names.size(); i++) {

      String name = (String)names.elementAt(i);

      // create admin description from test configuration parameters
      AdminDescription ad = createAdminDescription(name, config, i);

      // save configuration
      config.addAdminDescription(ad);
    }
  }

  /**
   * Creates the initial admin description using test configuration parameters.
   */
  private static AdminDescription createAdminDescription(String name,
                 TestConfig config, int index) {

    ConfigHashtable tab = config.getParameters();

    AdminDescription ad = new AdminDescription();
    ad.setName(name);

    // distributedSystem (generates gemfireDescription)
    {
      Long key = AdminPrms.distributedSystem;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str == null) {
        String s = BasePrms.nameForKey(key)
                 + " is a required field and has no default value";
        throw new HydraConfigException(s);
      } else if (str.equals(GemFirePrms.LONER)) {
        String s = BasePrms.nameForKey(key)
                 + " cannot be a " + GemFirePrms.LONER;
        throw new HydraConfigException(s);
      }
      ad.setDistributedSystem(str);

      GemFireDescription gfd = getGemFireDescription(str, key, config);
      ad.setGemFireDescription(gfd);
    }
    // ackSevereAlertThreshold (default to distributedSystem)
    {
      Long key = AdminPrms.ackSevereAlertThreshold;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = ad.getGemFireDescription().getAckSevereAlertThreshold();
      }
      ad.setAckSevereAlertThreshold(i);
    }
    // ackWaitThreshold (default to distributedSystem)
    {
      Long key = AdminPrms.ackWaitThreshold;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = ad.getGemFireDescription().getAckWaitThreshold();
      }
      ad.setAckWaitThreshold(i);
    }
    // disableTcp (inherit from distributedSystem)
    {
      Boolean bool = ad.getGemFireDescription().getDisableTcp();
      ad.setDisableTcp(bool);
    }
    // emailNotificationEnabled
    {
      Long key = AdminPrms.emailNotificationEnabled;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        // @todo switch these when source change occurs
        //bool = Boolean.valueOf(DistributedSystemConfig.DEFAULT_EMAIL_NOTIFICATION_ENABLED);
        bool = Boolean.valueOf(com.gemstone.gemfire.admin.jmx.AgentConfig.DEFAULT_EMAIL_NOTIFICATIONS_ENABLED);
      }
      ad.setEmailNotificationEnabled(bool);
    }
    // emailNotificationFrom
    {
      Long key = AdminPrms.emailNotificationFrom;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str == null) {
        str = "";
        // @todo switch these when source change occurs
        //str = DistributedSystemConfig.DEFAULT_EMAIL_NOTIFICATION_FROM;
        str = com.gemstone.gemfire.admin.jmx.AgentConfig.DEFAULT_EMAIL_FROM;
      }
      ad.setEmailNotificationFrom(str);
    }
    // emailNotificationHost
    {
      Long key = AdminPrms.emailNotificationHost;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str == null) {
        // @todo switch these when source change occurs
        //str = DistributedSystemConfig.DEFAULT_EMAIL_NOTIFICATION_HOST;
        str = com.gemstone.gemfire.admin.jmx.AgentConfig.DEFAULT_EMAIL_TO_LIST;
      } else {
        str = getEmailNotificationHost(str, key);
      }
      ad.setEmailNotificationHost(str);
    }
    // emailNotificationToList
    {
      Long key = AdminPrms.emailNotificationToList;
      String str = null;
      Vector v = tab.getVector(key, tab.getWild(key, index, null));
      if (v == null) {
        // @todo switch these when source change occurs
        //str = DistributedSystemConfig.DEFAULT_EMAIL_NOTIFICATION_TO_LIST;
        str = com.gemstone.gemfire.admin.jmx.AgentConfig.DEFAULT_EMAIL_TO_LIST;
      } else {
        str = getEmailNotificationToList(v);
      }
      ad.setEmailNotificationToList(str);
    }
    // enableNetworkPartitionDetection (default to distributedSystem)
    {
      Long key = AdminPrms.enableNetworkPartitionDetection;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        bool = ad.getGemFireDescription().getEnableNetworkPartitionDetection();
      }
      ad.setEnableNetworkPartitionDetection(bool);
    }
    // disableAutoReconnect (default to distributedSystem)
    {
      Long key = AdminPrms.disableAutoReconnect;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        bool = ad.getGemFireDescription().getDisableAutoReconnect();
      }
      ad.setDisableAutoReconnect(bool);
    }
    // defer locators
    // logDiskSpaceLimit
    {
      Long key = AdminPrms.logDiskSpaceLimit;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = new Integer(DistributedSystemConfig.DEFAULT_LOG_DISK_SPACE_LIMIT);
      }
      ad.setLogDiskSpaceLimit(i);
    }
    // defer logFile
    // logFileSizeLimit
    {
      Long key = AdminPrms.logFileSizeLimit;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = new Integer(DistributedSystemConfig.DEFAULT_LOG_FILE_SIZE_LIMIT);
      }
      ad.setLogFileSizeLimit(i);
    }
    // logLevel
    {
      Long key = AdminPrms.logLevel;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str == null) {
        str = DistributedSystemConfig.DEFAULT_LOG_LEVEL;
      }
      ad.setLogLevel(str);
    }
    // memberTimeout (default to distributedSystem)
    {
      Long key = AdminPrms.memberTimeout;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = ad.getGemFireDescription().getMemberTimeout();
      }
      ad.setMemberTimeout(i);
    }
    // multicastAddress (inherit from distributedSystem)
    {
      String str = ad.getGemFireDescription().getMcastAddress();
      ad.setMcastAddress(str);
    }
    // defer mcastPort
    // refreshInterval
    {
      Long key = AdminPrms.refreshInterval;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = DistributedSystemConfig.DEFAULT_REFRESH_INTERVAL;
      }
      ad.setRefreshInterval(i);
    }
    // remoteCommand
    {
      Long key = AdminPrms.remoteCommand;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str == null) {
        str = DistributedSystemConfig.DEFAULT_REMOTE_COMMAND;
      }
      ad.setRemoteCommand(str);
    }
    // sslName (generates sslDescription)
    {
      Long key = AdminPrms.sslName;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null && !str.equalsIgnoreCase(BasePrms.NONE)) {
        ad.setSSLName("SSLDescription." + str);
        ad.setSSLDescription(getSSLDescription(str, key, config));
      }
    }
    return ad;
  }

//------------------------------------------------------------------------------
// Email notification configuration support
//------------------------------------------------------------------------------

  /**
   * Returns the email notification host from the given string.
   */
  private static String getEmailNotificationHost(String str, Long key) {
    String host = EnvHelper.convertHostName(str);
    return HostHelper.getCanonicalHostName(host);
  }

  /**
   * Returns the email notification string from the given vector.
   */
  public static String getEmailNotificationToList(Vector v) {
    String str = "";
    for (int i = 0; i < v.size(); i++) {
      if (i > 0) str += ",";
      str += v.get(i);
    }
    return str;
  }

//------------------------------------------------------------------------------
// GemFire configuration support
//------------------------------------------------------------------------------

  /**
   * Returns the GemFire description whose distributed system matches the given
   * string.
   * @throws HydraConfigException if the given string is not listed in {@link
   *         GemFirePrms#names}.
   */
  private static GemFireDescription getGemFireDescription(String str, Long key,
                                                          TestConfig config) {
    Map gfds = config.getGemFireDescriptions();
    for (Iterator i = gfds.values().iterator(); i.hasNext();) {
      GemFireDescription gfd = (GemFireDescription)i.next();
      if (gfd.getDistributedSystem().equals(str)) {
        return gfd;
      }
    }
    String s = BasePrms.nameForKey(key) + " not found in "
             + BasePrms.nameForKey(GemFirePrms.names) + ": " + str;
    throw new HydraConfigException(s);
  }
}
