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

import com.gemstone.gemfire.admin.DistributedSystemConfig;
import com.gemstone.gemfire.admin.jmx.AgentConfig;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import java.io.Serializable;
import java.util.*;

/**
 * Encodes information needed to describe and create a management agent.
 */
public class AgentDescription extends AbstractDescription
implements Serializable {

  /** The logical name of this agent description */
  private String name;

  /** Remaining parameters, in alphabetical order */
  private String  adminName;
  private AdminDescription adminDescription; // from adminName
  private Boolean httpEnabled;
  private Boolean rmiEnabled;
  private Boolean rmiRegistryEnabled;
  private String  sslName;
  private SSLDescription sslDescription; // from sslName

//------------------------------------------------------------------------------
// Constructors
//------------------------------------------------------------------------------

  public AgentDescription() {
  }

//------------------------------------------------------------------------------
// Accessors, in aphabetical order after name
//------------------------------------------------------------------------------

  /**
   * Returns the logical name of this agent description.
   */
  public String getName() {
    return this.name;
  }

  /**
   * Sets the logical name of this agent description.
   */
  private void setName(String str) {
    this.name = str;
  }

  /**
   * Returns the admin name.
   */
  public String getAdminName() {
    return this.adminName;
  }

  /**
   * Sets the admin name.
   */
  private void setAdminName(String str) {
    this.adminName = str;
  }

  /**
   * Returns the admin description.
   */
  protected AdminDescription getAdminDescription() {
    return this.adminDescription;
  }

  /**
   * Sets the admin description.
   */
  private void setAdminDescription(AdminDescription ad) {
    this.adminDescription = ad;
  }

  /**
   * Returns the HTTP enabled.
   */
  public Boolean getHttpEnabled() {
    return this.httpEnabled;
  }

  /**
   * Sets the HTTP enabled.
   */
  private void setHttpEnabled(Boolean b) {
    this.httpEnabled = b;
  }

  /**
   * Returns the RMI enabled.
   */
  public Boolean getRmiEnabled() {
    return this.rmiEnabled;
  }

  /**
   * Sets the RMI enabled.
   */
  private void setRmiEnabled(Boolean b) {
    this.rmiEnabled = b;
  }

  /**
   * Returns the RMI registry enabled.
   */
  public Boolean getRmiRegistryEnabled() {
    return this.rmiRegistryEnabled;
  }

  /**
   * Sets the RMI registry enabled.
   */
  private void setRmiRegistryEnabled(Boolean b) {
    this.rmiRegistryEnabled = b;
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
   * Returns the ssl description.
   */
  private SSLDescription getSSLDescription() {
    return this.sslDescription;
  }

  /**
   * Sets the ssl description.
   */
  private void setSSLDescription(SSLDescription sd) {
    this.sslDescription = sd;
  }

//------------------------------------------------------------------------------
// Agent configuration
//------------------------------------------------------------------------------

  /**
   * Configures the agent using this agent description.
   * <p>
   * If SSL is configured and enabled, sets SSL-related properties on the VM
   * as a side-effect.
   */
  public void configure(AgentConfig a, int port) {
    this.getAdminDescription().configure(a);
    a.setAutoConnect(false);
    // Add email properties (@todo remove these when Bug 39009 is fixed).
    a.setEmailNotificationEnabled(this.getAdminDescription().getEmailNotificationEnabled().booleanValue());
    a.setEmailNotificationFrom(this.getAdminDescription().getEmailNotificationFrom());
    a.setEmailNotificationHost(this.getAdminDescription().getEmailNotificationHost());
    a.setEmailNotificationToList(this.getAdminDescription().getEmailNotificationToList());
    //
    a.setHttpEnabled(this.getHttpEnabled().booleanValue());
    a.setRmiEnabled(this.getRmiEnabled().booleanValue());
    a.setRmiPort(port);
    a.setRmiRegistryEnabled(this.getRmiRegistryEnabled().booleanValue());
    SSLDescription sd = this.getSSLDescription();
    if (sd != null && sd.getSSLEnabled().booleanValue()) {
      a.setAgentSSLCiphers(sd.getSSLCiphers());
      a.setAgentSSLEnabled(sd.getSSLEnabled().booleanValue());
      a.setAgentSSLProtocols(sd.getSSLProtocols());
      a.setAgentSSLRequireAuth(sd.getSSLRequireAuthentication().booleanValue());
      // set the VM-level properties related to SSL as a side-effect
      String s = DistributionConfig.GEMFIRE_PREFIX;
      System.setProperty(s + AgentConfig.SSL_CIPHERS_NAME,
                             sd.getSSLCiphers());
      System.setProperty(s + AgentConfig.SSL_ENABLED_NAME,
                             sd.getSSLEnabled().toString());
      System.setProperty(s + AgentConfig.SSL_PROTOCOLS_NAME,
                             sd.getSSLProtocols());
      System.setProperty(s + AgentConfig.SSL_REQUIRE_AUTHENTICATION_NAME,
                             sd.getSSLRequireAuthentication().toString());
    }
  }

  /**
   * Returns the agent as a string.  For use only by {@link AgentHelper
   * #agentToString(Agent)}.
   */
  protected static synchronized String agentToString(AgentConfig a) {
    StringBuffer buf = new StringBuffer();
    buf.append(AdminDescription.adminToString((DistributedSystemConfig)a));
    buf.append("\n  httpEnabled: " + a.isHttpEnabled());
    buf.append("\n  rmiEnabled: " + a.isRmiEnabled());
    buf.append("\n  rmiPort: " + a.getRmiPort());
    buf.append("\n  rmiRegistryEnabled: " + a.isRmiRegistryEnabled());
    buf.append("\n  sslEnabled: " + a.isAgentSSLEnabled());
    buf.append("\n  sslCiphers: " + a.getAgentSSLCiphers());
    buf.append("\n  sslProtocols: " + a.getAgentSSLProtocols());
    buf.append("\n  sslRequireAuthentication: " + a.isAgentSSLRequireAuth());
      
    // Add email properties (@todo remove these when Bug 39009 is fixed).
    buf.append("\n  emailNotificationEnabled: " + a.isEmailNotificationEnabled());
    buf.append("\n  emailNotificationFrom: " + a.getEmailNotificationFrom());
    buf.append("\n  emailNotificationHost: " + a.getEmailNotificationHost());
    buf.append("\n  emailNotificationToList: " + a.isAgentSSLRequireAuth());
    return buf.toString();
  }

//------------------------------------------------------------------------------
// Printing
//------------------------------------------------------------------------------

  public SortedMap toSortedMap() {

    SortedMap map = new TreeMap();
    String header = this.getClass().getName() + "." + this.getName() + ".";
    map.put(header + "adminName", this.getAdminName());
    map.put(header + "httpEnabled", this.getHttpEnabled());
    map.put(header + "rmiEnabled", this.getRmiEnabled());
    map.put(header + "rmiPort", "autogenerated");
    map.put(header + "rmiRegistryEnabled", this.getRmiRegistryEnabled());
    map.put(header + "sslName", this.getSSLName());
    return map;
  }

//------------------------------------------------------------------------------
// Configuration
//------------------------------------------------------------------------------

  /**
   * Creates agent descriptions from the agent parameters in the test
   * configuration.
   */
  protected static void configure(TestConfig config) {

    ConfigHashtable tab = config.getParameters();

    // create a description for each agent name
    Vector names = tab.vecAt(AgentPrms.names, new HydraVector());

    for (int i = 0; i < names.size(); i++) {

      String name = (String)names.elementAt(i);

      // create agent description from test configuration parameters
      AgentDescription ad = createAgentDescription(name, config, i);

      // save configuration
      config.addAgentDescription(ad);
    }
  }

  /**
   * Creates the initial agent description using test configuration parameters.
   */
  private static AgentDescription createAgentDescription(String name,
                 TestConfig config, int index) {

    ConfigHashtable tab = config.getParameters();

    AgentDescription ad = new AgentDescription();
    ad.setName(name);

    // adminName (generates adminDescription)
    {
      Long key = AgentPrms.adminName;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str == null) {
        String s = BasePrms.nameForKey(key)
                 + " is a required field and has no default value";
        throw new HydraConfigException(s);
      }
      ad.setAdminName(str);

      AdminDescription admind = getAdminDescription(str, key, config);
      ad.setAdminDescription(admind);
    }
    // httpEnabled
    {
      Long key = AgentPrms.httpEnabled;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        bool = Boolean.FALSE;
      }
      ad.setHttpEnabled(bool);
    }
    // rmiEnabled
    {
      Long key = AgentPrms.rmiEnabled;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        bool = Boolean.valueOf(AgentConfig.DEFAULT_RMI_ENABLED);
      }
      ad.setRmiEnabled(bool);
    }
    // defer rmiPort
    // rmiRegistryEnabled
    {
      Long key = AgentPrms.rmiRegistryEnabled;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        bool = Boolean.valueOf(AgentConfig.DEFAULT_RMI_REGISTRY_ENABLED);
      }
      ad.setRmiRegistryEnabled(bool);
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
// Admin configuration support
//------------------------------------------------------------------------------

  /**
   * Returns the admin description for the given string.
   * @throws HydraConfigException if the given string is not listed in {@link
   *         AdminPrms#names}.
   */
  private static AdminDescription getAdminDescription(String str, Long key,
                                                      TestConfig config) {
    AdminDescription admind = config.getAdminDescription(str);
    if (admind == null) {
      String s = BasePrms.nameForKey(key) + " not found in "
               + BasePrms.nameForKey(AdminPrms.names) + ": " + str;
      throw new HydraConfigException(s);
    } else {
      return admind;
    }
  }
}
