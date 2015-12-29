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

import com.gemstone.gemfire.internal.LogWriterImpl;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.security.*;
import java.io.Serializable;
import java.util.*;

/**
 * Encodes information needed to describe and create a security scheme.
 */
public class SecurityDescription extends AbstractDescription
implements Serializable {

  /** The logical name of this security description */
  private String name;

  /** Remaining parameters, in alphabetical order */
  private String clientAccessor;
  private String clientAccessorPP;
  private String clientAuthInit;
  private String clientAuthenticator;
  private Map clientExtraPrms;
  private String clientExtraPrmsClass;
  private String logLevel;
  private String peerAuthInit;
  private String peerAuthenticator;
  private Map peerExtraPrms;
  private String peerExtraPrmsClass;
  private Integer peerVerifymemberTimeout;

  /** Cached fields */
  private transient Properties clientExtraProperties;
  private transient Properties peerExtraProperties;
  private transient Properties distributedSystemProperties;

//------------------------------------------------------------------------------
// Constructors
//------------------------------------------------------------------------------

  public SecurityDescription() {
  }

//------------------------------------------------------------------------------
// Accessors, in aphabetical order after name
//------------------------------------------------------------------------------

  /**
   * Returns the logical name of this security description.
   */
  public String getName() {
    return this.name;
  }

  /**
   * Sets the logical name of this security description.
   */
  private void setName(String str) {
    this.name = str;
  }

  /**
   * Returns the method name of the client accessor.
   */
  private String getClientAccessor() {
    return this.clientAccessor;
  }

  /**
   * Sets the method name of the client accessor.
   */
  private void setClientAccessor(String str) {
    this.clientAccessor = str;
  }

  /**
   * Returns the method name of the client accessor pp.
   */
  private String getClientAccessorPP() {
    return this.clientAccessorPP;
  }

  /**
   * Sets the method name of the client accessor pp.
   */
  private void setClientAccessorPP(String str) {
    this.clientAccessorPP = str;
  }

  /**
   * Returns the method name of the client authentication initializer.
   */
  private String getClientAuthInit() {
    return this.clientAuthInit;
  }

  /**
   * Sets the method name of the client authentication initializer.
   */
  private void setClientAuthInit(String str) {
    this.clientAuthInit = str;
  }

  /**
   * Returns the method name of the client authenticator.
   */
  private String getClientAuthenticator() {
    return this.clientAuthenticator;
  }

  /**
   * Sets the method name of the client authenticator.
   */
  private void setClientAuthenticator(String str) {
    this.clientAuthenticator = str;
  }

  /**
   * Returns the client extra prms class.
   */
  private synchronized String getClientExtraPrmsClass() {
    return this.clientExtraPrmsClass;
  }

  /**
   * Sets the client extra prms class.
   */
  private void setClientExtraPrmsClass(String str) {
    this.clientExtraPrmsClass = str;
  }

  /**
   * Returns the client extra prms.
   */
  private synchronized Map getClientExtraPrms() {
    return this.clientExtraPrms;
  }

  /**
   * Sets the client extra prms.
   */
  private void setClientExtraPrms(Map map) {
    this.clientExtraPrms = map;
  }

  /**
   * Returns the log level for the security log.
   */
  private String getLogLevel() {
    return this.logLevel;
  }

  /**
   * Sets the log level for the security log.
   */
  private void setLogLevel(String str) {
    this.logLevel = str;
  }

  /**
   * Returns the method name of the peer authentication initializer.
   */
  private String getPeerAuthInit() {
    return this.peerAuthInit;
  }

  /**
   * Sets the method name of the peer authentication initializer.
   */
  private void setPeerAuthInit(String str) {
    this.peerAuthInit = str;
  }

  /**
   * Returns the method name of the peer authenticator.
   */
  private String getPeerAuthenticator() {
    return this.peerAuthenticator;
  }

  /**
   * Sets the method name of the peer authenticator.
   */
  private void setPeerAuthenticator(String str) {
    this.peerAuthenticator = str;
  }

  /**
   * Returns the peer extra prms class.
   */
  private synchronized String getPeerExtraPrmsClass() {
    return this.peerExtraPrmsClass;
  }

  /**
   * Sets the peer extra prms class.
   */
  private void setPeerExtraPrmsClass(String str) {
    this.peerExtraPrmsClass = str;
  }

  /**
   * Returns the peer extra prms.
   */
  private synchronized Map getPeerExtraPrms() {
    return this.peerExtraPrms;
  }

  /**
   * Sets the peer extra prms.
   */
  private void setPeerExtraPrms(Map map) {
    this.peerExtraPrms = map;
  }

  /**
   * Returns the peer verifymember timeout.
   */
  private Integer getPeerVerifymemberTimeout() {
    return this.peerVerifymemberTimeout;
  }

  /**
   * Sets the peer verifymember timeout.
   */
  private void setPeerVerifymemberTimeout(Integer i) {
    this.peerVerifymemberTimeout = i;
  }

//------------------------------------------------------------------------------
// Properties creation
//------------------------------------------------------------------------------

  /**
   * For internal hydra use only.  Hydra client-side method only.
   * <p>
   * Returns the name of the security log file in the system directory.
   */
  protected String getSecurityLogFileName(GemFireDescription gfd) {
    return gfd.getSysDirName() + "/security.log";
  }

  /**
   * Returns the cached instance of security-related properties for this
   * description.
   */
  public synchronized Properties getDistributedSystemProperties(
                                    GemFireDescription gfd) {
    if (this.distributedSystemProperties == null) {
      Properties p = new Properties();

      p.setProperty(DistributionConfig.SECURITY_CLIENT_ACCESSOR_NAME,
                    getClientAccessor());
      p.setProperty(DistributionConfig.SECURITY_CLIENT_ACCESSOR_PP_NAME,
                    getClientAccessorPP());
      p.setProperty(DistributionConfig.SECURITY_CLIENT_AUTH_INIT_NAME,
                    getClientAuthInit());
      p.setProperty(DistributionConfig.SECURITY_CLIENT_AUTHENTICATOR_NAME,
                    getClientAuthenticator());
      addProperties(getClientExtraProperties(), p);

      p.setProperty(DistributionConfig.SECURITY_LOG_FILE_NAME,
                    getSecurityLogFileName(gfd));
      p.setProperty(DistributionConfig.SECURITY_LOG_LEVEL_NAME,
                    getLogLevel());

      p.setProperty(DistributionConfig.SECURITY_PEER_AUTH_INIT_NAME,
                    getPeerAuthInit());
      p.setProperty(DistributionConfig.SECURITY_PEER_AUTHENTICATOR_NAME,
                    getPeerAuthenticator());
      p.setProperty(DistributionConfig.SECURITY_PEER_VERIFYMEMBER_TIMEOUT_NAME,
                     String.valueOf(this.getPeerVerifymemberTimeout()));
      addProperties(getPeerExtraProperties(), p);

      this.distributedSystemProperties = p;
    }
    return this.distributedSystemProperties;
  }

  /**
   * Returns the client extra properties, or null if there are none.
   */
  public synchronized Properties getClientExtraProperties() {
    if (this.clientExtraProperties == null) {
      this.clientExtraProperties = getExtraProperties(this.clientExtraPrmsClass,
                                                      this.clientExtraPrms);
    }
    return this.clientExtraProperties;
  }

  /**
   * Returns the peer extra properties, or null if there are none.
   */
  public synchronized Properties getPeerExtraProperties() {
    if (this.peerExtraProperties == null) {
      this.peerExtraProperties = getExtraProperties(this.peerExtraPrmsClass,
                                                    this.peerExtraPrms);
    }
    return this.peerExtraProperties;
  }

  /**
   * Returns the extra properties for the given parameter map, allowing for
   * overrides in task attributes.  Makes sure the parameter class is loaded
   * for the master-managed locator case.
   */
  private Properties getExtraProperties(String prmclass, Map prms) {
    Properties p = null;
    if (prmclass != null && prms != null) {
      loadClass(prmclass);
      p = new Properties();
      for (Iterator i = prms.keySet().iterator(); i.hasNext();) {
        Long key = (Long)i.next();
        String defaultVal = (String)prms.get(key);
        String setVal = TestConfig.tab().stringAt(key, defaultVal);
        RemoteTestModule mod = RemoteTestModule.getCurrentThread();
        if (mod != null) {
          ConfigHashtable tasktab = mod.getCurrentTask().getTaskAttributes();
          if (tasktab != null) {
            setVal = tasktab.stringAt(key, setVal);
          }
        }
        if (setVal.startsWith("$JTESTS")) { // expand path, versioned
          setVal = EnvHelper.expandPath(setVal);
        } else if (setVal.startsWith("$")) { // expand path
          setVal = EnvHelper.expandEnvVars(setVal);
        }
        p.setProperty(convertSecurityPrm(key), setVal);
      }
    }
    return p;
  }

//------------------------------------------------------------------------------
// Printing
//------------------------------------------------------------------------------

  public SortedMap toSortedMap() {
    SortedMap map = new TreeMap();
    String header = this.getClass().getName() + "." + this.getName() + ".";
    map.put(header + "clientAccessor", this.getClientAccessor());
    map.put(header + "clientAccessorPP", this.getClientAccessorPP());
    map.put(header + "clientAuthInit", this.getClientAuthInit());
    map.put(header + "clientAuthenticator", this.getClientAuthenticator());
    if (this.clientExtraPrmsClass != null) {
      map.put(header + "clientExtraProperties", this.getClientExtraPrmsClass()
         + " defaults: " + convertSecurityPrms(this.getClientExtraPrms()));
    }
    map.put(header + "logFile", "autogenerated: security.log in system directory");
    map.put(header + "logLevel", this.getLogLevel());
    map.put(header + "peerAuthInit", this.getPeerAuthInit());
    map.put(header + "peerAuthenticator", this.getPeerAuthenticator());
    if (this.peerExtraPrmsClass != null) {
      map.put(header + "peerExtraProperties", this.getPeerExtraPrmsClass()
         + " defaults: " + convertSecurityPrms(this.getPeerExtraPrms()));
    }
    map.put(header + "peerVerifymemberTimeout", this.getPeerVerifymemberTimeout());
    return map;
  }

//------------------------------------------------------------------------------
// Configuration
//------------------------------------------------------------------------------

  /**
   * Creates security descriptions from the security parameters in the test
   * configuration.
   */
  protected static void configure(TestConfig config) {

    ConfigHashtable tab = config.getParameters();

    // create a description for each security scheme name
    Vector names = tab.vecAt(SecurityPrms.names, new HydraVector());

    for (int i = 0; i < names.size(); i++) {

      String name = (String)names.elementAt(i);

      // create security description from test configuration parameters
      // and do hydra-level validation
      SecurityDescription sd = createSecurityDescription(name, config, i);

      // save configuration
      config.addSecurityDescription(sd);
    }
  }

  /**
   * Creates the security description using test configuration parameters and
   * does hydra-level validation.
   */
  private static SecurityDescription createSecurityDescription(String name,
                 TestConfig config, int index) {

    ConfigHashtable tab = config.getParameters();

    SecurityDescription sd = new SecurityDescription();
    sd.setName(name);

    // clientAccessor
    {
      Long key = SecurityPrms.clientAccessor;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str == null || str.equalsIgnoreCase(BasePrms.NONE)) {
        str = DistributionConfig.DEFAULT_SECURITY_CLIENT_ACCESSOR;
      }
      sd.setClientAccessor(parseMethod(key, str, null, AccessControl.class));
    }
    // clientAccessorPP
    {
      Long key = SecurityPrms.clientAccessorPP;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str == null || str.equalsIgnoreCase(BasePrms.NONE)) {
        str = DistributionConfig.DEFAULT_SECURITY_CLIENT_ACCESSOR_PP;
      }
      sd.setClientAccessorPP(parseMethod(key, str, null, AccessControl.class));
    }
    // clientAuthInit
    {
      Long key = SecurityPrms.clientAuthInit;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str == null || str.equalsIgnoreCase(BasePrms.NONE)) {
        str = DistributionConfig.DEFAULT_SECURITY_CLIENT_AUTH_INIT;
      }
      sd.setClientAuthInit(parseMethod(key, str, null, AuthInitialize.class));
    }
    // clientAuthenticator
    {
      Long key = SecurityPrms.clientAuthenticator;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str == null || str.equalsIgnoreCase(BasePrms.NONE)) {
        str = DistributionConfig.DEFAULT_SECURITY_CLIENT_AUTHENTICATOR;
      }
      sd.setClientAuthenticator(parseMethod(key, str, null,
                                            Authenticator.class));
    }
    // clientExtraProperties
    {
      Long key = SecurityPrms.clientExtraProperties;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null && !str.equalsIgnoreCase(BasePrms.NONE)) {
        Class cls = getClass(key, str);
        sd.setClientExtraPrmsClass(cls.getName());
        sd.setClientExtraPrms(getParametersAndDefaults(key, cls));
      }
    }
    // logLevel
    {
      Long key = SecurityPrms.logLevel;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str == null) {
        str = LogWriterImpl.levelToString(DistributionConfig.DEFAULT_LOG_LEVEL);
      }
      sd.setLogLevel(str);
    }
    // peerAuthInit
    {
      Long key = SecurityPrms.peerAuthInit;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str == null || str.equalsIgnoreCase(BasePrms.NONE)) {
        str = DistributionConfig.DEFAULT_SECURITY_PEER_AUTH_INIT;
      }
      sd.setPeerAuthInit(parseMethod(key, str, null, AuthInitialize.class));
    }
    // peerAuthenticator
    {
      Long key = SecurityPrms.peerAuthenticator;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str == null || str.equalsIgnoreCase(BasePrms.NONE)) {
        str = DistributionConfig.DEFAULT_SECURITY_PEER_AUTHENTICATOR;
      }
      sd.setPeerAuthenticator(parseMethod(key, str, null,
                                          Authenticator.class));
    }
    // peerExtraProperties
    {
      Long key = SecurityPrms.peerExtraProperties;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null && !str.equalsIgnoreCase(BasePrms.NONE)) {
        Class cls = getClass(key, str);
        sd.setPeerExtraPrmsClass(cls.getName());
        sd.setPeerExtraPrms(getParametersAndDefaults(key, cls));
      }
    }
    // peerVerifymemberTimeout
    {
      Long key = SecurityPrms.peerVerifymemberTimeout;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = new Integer(DistributionConfig.DEFAULT_SECURITY_PEER_VERIFYMEMBER_TIMEOUT);
      }
      sd.setPeerVerifymemberTimeout(i);
    }
    return sd;
  }

//------------------------------------------------------------------------------
// Extra property support
//------------------------------------------------------------------------------

  /**
   * Converts the map of parameter key-value pairs to a sorted map of security
   * property-value pairs.
   */
  private SortedMap convertSecurityPrms(Map prms) {
    SortedMap map = new TreeMap();
    for (Iterator i = prms.keySet().iterator(); i.hasNext();) {
      Long key = (Long)i.next();
      map.put(convertSecurityPrm(key), prms.get(key));
    }
    return map;
  }

  /**
   * Converts the security parameter key to its security property name.
   */
  private String convertSecurityPrm(Long key) {
    return DistributionConfig.SECURITY_PREFIX_NAME + convertPrm(key);
  }
}
