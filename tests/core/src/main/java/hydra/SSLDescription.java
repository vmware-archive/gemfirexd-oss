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

import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import java.io.Serializable;
import java.util.*;

/**
 * Encodes information needed to describe and create an SSL configuration.
 */
public class SSLDescription extends AbstractDescription
implements Serializable {

  /** The logical name of this SSL description */
  private String name;

  /** Remaining parameters, in alphabetical order */
  private String keyStore;
  private String keyStorePassword;
  private String sslCiphers;
  private Boolean sslEnabled;
  private String sslProtocols;
  private Boolean sslRequireAuthentication;
  private String trustStore;
  private String trustStorePassword;

  /** Cached fields */
  private transient Properties distributedSystemProperties;
  private transient Properties systemProperties;

//------------------------------------------------------------------------------
// Constructors
//------------------------------------------------------------------------------

  public SSLDescription() {
  }

//------------------------------------------------------------------------------
// Accessors, in aphabetical order after name
//------------------------------------------------------------------------------

  /**
   * Returns the logical name of this SSL description.
   */
  public String getName() {
    return this.name;
  }

  /**
   * Sets the logical name of this SSL description.
   */
  private void setName(String str) {
    this.name = str;
  }

  /**
   * Returns the key store (with unexpanded environment variables).
   */
  private String getKeyStore() {
    return this.keyStore;
  }

  /**
   * Sets the key store (with unexpanded environment variables).
   */
  private void setKeyStore(String str) {
    this.keyStore = str;
  }

  /**
   * Returns the key store password.
   */
  private String getKeyStorePassword() {
    return this.keyStorePassword;
  }

  /**
   * Sets the key store password.
   */
  private void setKeyStorePassword(String str) {
    this.keyStorePassword = str;
  }

  /**
   * Returns the SSL ciphers.
   */
  protected String getSSLCiphers() {
    return this.sslCiphers;
  }

  /**
   * Sets the SSL ciphers.
   */
  private void setSSLCiphers(String str) {
    this.sslCiphers = str;
  }

  /**
   * Returns the SSL enabled.
   */
  protected Boolean getSSLEnabled() {
    return this.sslEnabled;
  }

  /**
   * Sets the SSL enabled.
   */
  private void setSSLEnabled(Boolean bool) {
    this.sslEnabled = bool;
  }

  /**
   * Returns the SSL protocols.
   */
  protected String getSSLProtocols() {
    return this.sslProtocols;
  }

  /**
   * Sets the SSL protocols.
   */
  private void setSSLProtocols(String str) {
    this.sslProtocols = str;
  }

  /**
   * Returns the SSL require authentication.
   */
  protected Boolean getSSLRequireAuthentication() {
    return this.sslRequireAuthentication;
  }

  /**
   * Sets the SSL require authentication.
   */
  private void setSSLRequireAuthentication(Boolean bool) {
    this.sslRequireAuthentication = bool;
  }

  /**
   * Returns the trust store (with unexpanded environment variables).
   */
  private String getTrustStore() {
    return this.trustStore;
  }

  /**
   * Sets the trust store (with unexpanded environment variables).
   */
  private void setTrustStore(String str) {
    this.trustStore = str;
  }

  /**
   * Returns the trust store password.
   */
  private String getTrustStorePassword() {
    return this.trustStorePassword;
  }

  /**
   * Sets the trust store password.
   */
  private void setTrustStorePassword(String str) {
    this.trustStorePassword = str;
  }

//------------------------------------------------------------------------------
// Properties creation
//------------------------------------------------------------------------------

  /**
   * Returns the cached instance of system properties for this description.
   * This method should only be called if SSL is enabled.
   */
  protected synchronized Properties getSystemProperties(String mcastPort) {
    if (this.systemProperties == null) {
        Properties p = new Properties();

        // gemfire properties
        String s = DistributionConfig.GEMFIRE_PREFIX;
        p.setProperty(s + DistributionConfig.SSL_CIPHERS_NAME,
                          getSSLCiphers());
        p.setProperty(s + DistributionConfig.SSL_ENABLED_NAME,
                          getSSLEnabled().toString());
        p.setProperty(s + DistributionConfig.SSL_PROTOCOLS_NAME,
                          getSSLProtocols());
        p.setProperty(s + DistributionConfig.SSL_REQUIRE_AUTHENTICATION_NAME,
                          getSSLRequireAuthentication().toString());
        // in case this is a master-managed locator
        p.setProperty(s + DistributionConfig.MCAST_PORT_NAME,
                          mcastPort);

        // javax.net.ssl properties
        s = "javax.net.ssl.";
        p.setProperty(s + "keyStore",
                           getPath(SSLPrms.keyStore, getKeyStore()));
        p.setProperty(s + "keyStorePassword",
                           getKeyStorePassword());
        p.setProperty(s + "trustStore",
                           getPath(SSLPrms.trustStore, getTrustStore()));
        p.setProperty(s + "trustStorePassword",
                           getTrustStorePassword());

        this.systemProperties = p;
    }
    return this.systemProperties;
  }

  /**
   * Sets the system properties for this description using the cached instance.
   * Properties are only set if SSL is enabled.
   */
  protected synchronized void setSystemProperties(String mcastPort) {
    Properties p = getSystemProperties(mcastPort);
    if (p != null) {
      for (Iterator i = p.keySet().iterator(); i.hasNext();) {
        String key = (String)i.next();
        String val = (String)p.getProperty(key);
        System.setProperty(key, val);
      }
    }
  }

  /**
   * Returns the cached instance of distributed system properties for this
   * description.
   */
  public synchronized Properties getDistributedSystemProperties() {
    if (this.distributedSystemProperties == null) {
      Properties p = new Properties();
      p.setProperty(DistributionConfig.SSL_CIPHERS_NAME,
                    getSSLCiphers());
      p.setProperty(DistributionConfig.SSL_ENABLED_NAME,
                    getSSLEnabled().toString());
      p.setProperty(DistributionConfig.SSL_PROTOCOLS_NAME,
                    getSSLProtocols());
      p.setProperty(DistributionConfig.SSL_REQUIRE_AUTHENTICATION_NAME,
                    getSSLRequireAuthentication().toString());

      this.distributedSystemProperties = p;
    }
    return this.distributedSystemProperties;
  }

//------------------------------------------------------------------------------
// Printing
//------------------------------------------------------------------------------

  public SortedMap toSortedMap() {
    SortedMap map = new TreeMap();
    String header = this.getClass().getName() + "." + this.getName() + ".";
    map.put(header + "keyStore", this.getKeyStore());
    map.put(header + "keyStorePassword", this.getKeyStorePassword());
    map.put(header + "sslCiphers", this.getSSLCiphers());
    map.put(header + "sslEnabled", this.getSSLEnabled());
    map.put(header + "sslProtocols", this.getSSLProtocols());
    map.put(header + "sslRequireAuthentication",
                      this.getSSLRequireAuthentication());
    map.put(header + "trustStore", this.getTrustStore());
    map.put(header + "trustStorePassword", this.getTrustStorePassword());
    return map;
  }

//------------------------------------------------------------------------------
// Configuration
//------------------------------------------------------------------------------

  /**
   * Creates SSL descriptions from the SSL parameters in the test
   * configuration.
   */
  protected static void configure(TestConfig config) {

    ConfigHashtable tab = config.getParameters();

    // create a description for each SSL name
    Vector names = tab.vecAt(SSLPrms.names, new HydraVector());

    for (int i = 0; i < names.size(); i++) {

      String name = (String)names.elementAt(i);

      // create SSL description from test configuration parameters
      // and do hydra-level validation
      SSLDescription sd = createSSLDescription(name, config, i);

      // save configuration
      config.addSSLDescription(sd);
    }
  }

  /**
   * Creates the initial SSL description using test configuration
   * parameters and does hydra-level validation.
   */
  private static SSLDescription createSSLDescription(String name,
                 TestConfig config, int index) {

    ConfigHashtable tab = config.getParameters();

    SSLDescription sd = new SSLDescription();
    sd.setName(name);

    // keyStore
    {
      Long key = SSLPrms.keyStore;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str == null) {
        String s = BasePrms.nameForKey(key) + " is a required field";
        throw new HydraConfigException(s);
      }
      sd.setKeyStore(str);
    }
    // keyStorePassword
    {
      Long key = SSLPrms.keyStorePassword;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str == null) {
        String s = BasePrms.nameForKey(key) + " is a required field";
        throw new HydraConfigException(s);
      }
      sd.setKeyStorePassword(str);
    }
    // sslCiphers
    {
      Long key = SSLPrms.sslCiphers;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str == null) {
        str = DistributionConfig.DEFAULT_SSL_CIPHERS;
      }
      sd.setSSLCiphers(str);
    }
    // sslEnabled
    {
      Long key = SSLPrms.sslEnabled;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        bool = Boolean.valueOf(DistributionConfig.DEFAULT_SSL_ENABLED);
      }
      sd.setSSLEnabled(bool);
    }
    // sslProtocols
    {
      Long key = SSLPrms.sslProtocols;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str == null) {
        str = DistributionConfig.DEFAULT_SSL_PROTOCOLS;
      }
      sd.setSSLProtocols(str);
    }
    // sslRequireAuthentication
    {
      Long key = SSLPrms.sslRequireAuthentication;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        bool = Boolean.valueOf(
                   DistributionConfig.DEFAULT_SSL_REQUIRE_AUTHENTICATION);
      }
      sd.setSSLRequireAuthentication(bool);
    }
    // trustStore
    {
      Long key = SSLPrms.trustStore;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str == null) {
        str = sd.getKeyStore();
      }
      sd.setTrustStore(str);
    }
    // trustStorePassword
    {
      Long key = SSLPrms.trustStorePassword;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str == null) {
        str = sd.getKeyStorePassword();
      }
      sd.setTrustStorePassword(str);
    }
    return sd;
  }
}
