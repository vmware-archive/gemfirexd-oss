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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Vector;

import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolFactory;
import com.gemstone.gemfire.internal.cache.PoolFactoryImpl;

/**
 * Encodes information needed to describe and create a pool as used by clients.
 */
public class PoolDescription extends AbstractDescription
implements Serializable {

  private static final long serialVersionUID = 1L;

  /** Contact type for locators. */
  private static final int SERVER_LOCATOR = 0;

  /** Contact type for bridge servers. */
  private static final int CACHE_SERVER = 1;

  /** The logical name of this pool description and actual name of the pool */
  private String name;

  /** Remaining parameters, in alphabetical order */
  private String  contactClass;
  private String  contactMethod;
  private Integer contactNum;
  private String  distributedSystem;
  private Integer freeConnectionTimeout;
  private Long    idleTimeout;
  private Integer loadConditioningInterval;
  private Integer minConnections;
  private Integer maxConnections;
  private Boolean multiuserAuthentication;
  private Long    pingInterval;
  private Boolean prSingleHopEnabled;
  private Integer readTimeout;
  private Integer retryAttempts;
  private String  serverGroup;
  private Integer socketBufferSize;
  private Integer statisticInterval;
  private Integer subscriptionAckInterval;
  private Boolean subscriptionEnabled;
  private Integer subscriptionMessageTrackingTimeout;
  private Integer subscriptionRedundancy;
  private Boolean threadLocalConnections;

//------------------------------------------------------------------------------
// Constructors
//------------------------------------------------------------------------------

  public PoolDescription() {
  }

//------------------------------------------------------------------------------
// Accessors, in aphabetical order after name
//------------------------------------------------------------------------------

  /**
   * Returns the logical name of this pool description and actual name of the
   * pool.
   */
  public String getName() {
    return this.name;
  }

  /**
   * Sets the logical name of this pool description and actual name of the pool.
   */
  private void setName(String str) {
    this.name = str;
  }

  /**
   * Returns the contact algorithm signature.
   */
  private String getContactAlgorithm() {
    return this.getContactClass() + "."
         + this.getContactMethod() + "(int, String)";
  }

  /**
   * Returns the contact class.
   */
  private String getContactClass() {
    return this.contactClass;
  }

  /**
   * Sets the contact class.
   */
  private void setContactClass(String str) {
    this.contactClass = str;
  }

  /**
   * Returns the contact method.
   */
  private String getContactMethod() {
    return this.contactMethod;
  }

  /**
   * Sets the contact method.
   */
  private void setContactMethod(String str) {
    this.contactMethod = str;
  }

  /**
   * Returns the contact num.
   */
  private Integer getContactNum() {
    return this.contactNum;
  }

  /**
   * Sets the contact num.
   */
  private void setContactNum(Integer i) {
    this.contactNum = i;
  }
  
  /**
   * Returns the distributed system.
   */
  private String getDistributedSystem() {
    return this.distributedSystem;
  }

  /**
   * Sets the distributed system.
   */
  private void setDistributedSystem(String str) {
    this.distributedSystem = str;
  }

  /**
   * Returns the free connection timeout.
   */
  private Integer getFreeConnectionTimeout() {
    return this.freeConnectionTimeout;
  }

  /**
   * Sets the free connection timeout.
   */
  private void setFreeConnectionTimeout(Integer i) {
    this.freeConnectionTimeout = i;
  }

  /**
   * Returns the idle timeout.
   */
  private Long getIdleTimeout() {
    return this.idleTimeout;
  }

  /**
   * Sets the idle timeout.
   */
  private void setIdleTimeout(Long l) {
    this.idleTimeout = l;
  }
  
  /**
   * Returns the load conditioning interval.
   */
  private Integer getLoadConditioningInterval() {
    return this.loadConditioningInterval;
  }

  /**
   * Sets the load conditioning interval.
   */
  private void setLoadConditioningInterval(Integer i) {
    this.loadConditioningInterval = i;
  }

  /**
   * Returns the min connections.
   */
  public Integer getMinConnections() {
    return this.minConnections;
  }

  /**
   * Sets the min connections.
   */
  public void setMinConnections(Integer i) {
    this.minConnections = i;
  }

  /**
   * Returns the max connections.
   */
  public Integer getMaxConnections() {
    return this.maxConnections;
  }

  /**
   * Sets the max connections.
   */
  public void setMaxConnections(Integer i) {
    this.maxConnections = i;
  }

  /**
   * Returns the multiuser authentication.
   */
  private Boolean getMultiuserAuthentication() {
    return this.multiuserAuthentication;
  }

  /**
   * Sets the multiuser authentication.
   */
  private void setMultiuserAuthentication(Boolean bool) {
    this.multiuserAuthentication = bool;
  }  
  
  /**
   * Returns the ping interval.
   */
  public Long getPingInterval() {
    return this.pingInterval;
  }

  /**
   * Sets the ping interval.
   */
  public void setPingInterval(Long l) {
    this.pingInterval = l;
  }
  
  /**
   * Returns the PR single hop enabled.
   */
  private Boolean getPRSingleHopEnabled() {
    return this.prSingleHopEnabled;
  }

  /**
   * Sets the PR single hop enabled.
   */
  private void setPRSingleHopEnabled(Boolean bool) {
    this.prSingleHopEnabled = bool;
  }

  /**
   * Returns the read timeout.
   */
  public Integer getReadTimeout() {
    return this.readTimeout;
  }

  /**
   * Sets the read timeout.
   */
  private void setReadTimeout(Integer i) {
    this.readTimeout = i;
  }

  /**
   * Returns the retry attempts.
   */
  public Integer getRetryAttempts() {
    return this.retryAttempts;
  }

  /**
   * Sets the retry attempts.
   */
  public void setRetryAttempts(Integer i) {
    this.retryAttempts = i;
  }

  /**
   * Returns the server group.
   */
  public String getServerGroup() {
    return this.serverGroup;
  }

  /**
   * Sets the server group.
   */
  private void setServerGroup(String str) {
    this.serverGroup = str;
  }

  /**
   * Returns the socket buffer size.
   */
  private Integer getSocketBufferSize() {
    return this.socketBufferSize;
  }

  /**
   * Sets the socket buffer size.
   */
  private void setSocketBufferSize(Integer i) {
    this.socketBufferSize = i;
  }
  
  /**
   * Returns the statistic interval.
   */
  public Integer getStatisticInterval() {
    return this.statisticInterval;
  }

  /**
   * Sets the statistic interval.
   */
  public void setStatisticInterval(Integer v) {
    this.statisticInterval = v;
  }

  /**
   * Returns the subscription ack interval.
   */
  private Integer getSubscriptionAckInterval() {
    return this.subscriptionAckInterval;
  }

  /**
   * Sets the subscription ack interval.
   */
  private void setSubscriptionAckInterval(Integer i) {
    this.subscriptionAckInterval = i;
  }

  /**
   * Returns the subscription enabled.
   */
  private Boolean getSubscriptionEnabled() {
    return this.subscriptionEnabled;
  }

  /**
   * Sets the subscription enabled.
   */
  private void setSubscriptionEnabled(Boolean bool) {
    this.subscriptionEnabled = bool;
  }

  /**
   * Returns the subscription message tracking timeout.
   */
  private Integer getSubscriptionMessageTrackingTimeout() {
    return this.subscriptionMessageTrackingTimeout;
  }

  /**
   * Sets the subscription message tracking timeout.
   */
  private void setSubscriptionMessageTrackingTimeout(Integer i) {
    this.subscriptionMessageTrackingTimeout = i;
  }

  /**
   * Returns the subscription redundancy.
   */
  private Integer getSubscriptionRedundancy() {
    return this.subscriptionRedundancy;
  }

  /**
   * Sets the subscription redundancy.
   */
  private void setSubscriptionRedundancy(Integer i) {
    this.subscriptionRedundancy = i;
  }

  /**
   * Returns the thread local connections.
   */
  private Boolean getThreadLocalConnections() {
    return this.threadLocalConnections;
  }

  /**
   * Sets the thread local connections.
   */
  private void setThreadLocalConnections(Boolean bool) {
    this.threadLocalConnections = bool;
  }
  
//------------------------------------------------------------------------------
// Pool configuration
//------------------------------------------------------------------------------

  /**
   * Configures the pool factory using this pool description.
   */
  public void configure(PoolFactory f) {
    // configure the factory
    List contacts = getContacts(this.getContactClass(), this.getContactMethod(),
                                this.getContactNum(),
                                this.getDistributedSystem());
    int contactType = getContactType(contacts, this.getContactAlgorithm());
    if (contactType == SERVER_LOCATOR) {
      for (Iterator i = contacts.iterator(); i.hasNext();) {
        Contact contact = (Contact)i.next();
        f.addLocator(contact.getAddress(), contact.getPort());
      }
    } else if (contactType == CACHE_SERVER) {
      for (Iterator i = contacts.iterator(); i.hasNext();) {
        Contact contact = (Contact)i.next();
        f.addServer(contact.getAddress(), contact.getPort());
      }
    } else {
      String s = "Illegal contact type: " + contactType;
      throw new HydraInternalException(s);
    }
    f.setFreeConnectionTimeout(this.getFreeConnectionTimeout().intValue());
    f.setIdleTimeout(this.getIdleTimeout().longValue());
    f.setLoadConditioningInterval(this.getLoadConditioningInterval().intValue());
    f.setMinConnections(this.getMinConnections().intValue());
    f.setMaxConnections(this.getMaxConnections().intValue());
    f.setMultiuserAuthentication(this.getMultiuserAuthentication().booleanValue());
    f.setPingInterval(this.getPingInterval().longValue());
    f.setPRSingleHopEnabled(this.getPRSingleHopEnabled().booleanValue());
    f.setReadTimeout(this.getReadTimeout().intValue());
    f.setRetryAttempts(this.getRetryAttempts().intValue());
    f.setServerGroup(this.getServerGroup());
    f.setSocketBufferSize(this.getSocketBufferSize().intValue());
    f.setStatisticInterval(this.getStatisticInterval().intValue());
    f.setSubscriptionAckInterval(this.getSubscriptionAckInterval().intValue());
    f.setSubscriptionEnabled(this.getSubscriptionEnabled().booleanValue());
    f.setSubscriptionMessageTrackingTimeout(this.getSubscriptionMessageTrackingTimeout().intValue());
    f.setSubscriptionRedundancy(this.getSubscriptionRedundancy().intValue());
    f.setThreadLocalConnections(this.getThreadLocalConnections().booleanValue());
  }

  /**
   * Configures the client cache factory using this pool description.
   */
  public void configure(ClientCacheFactory f) {
    // configure the factory
    List contacts = getContacts(this.getContactClass(), this.getContactMethod(),
                                this.getContactNum(),
                                this.getDistributedSystem());
    int contactType = getContactType(contacts, this.getContactAlgorithm());
    if (contactType == SERVER_LOCATOR) {
      for (Iterator i = contacts.iterator(); i.hasNext();) {
        Contact contact = (Contact)i.next();
        f.addPoolLocator(contact.getAddress(), contact.getPort());
      }
    } else if (contactType == CACHE_SERVER) {
      for (Iterator i = contacts.iterator(); i.hasNext();) {
        Contact contact = (Contact)i.next();
        f.addPoolServer(contact.getAddress(), contact.getPort());
      }
    } else {
      String s = "Illegal contact type: " + contactType;
      throw new HydraInternalException(s);
    }
    f.setPoolFreeConnectionTimeout(this.getFreeConnectionTimeout().intValue());
    f.setPoolIdleTimeout(this.getIdleTimeout().longValue());
    f.setPoolLoadConditioningInterval(this.getLoadConditioningInterval().intValue());
    f.setPoolMinConnections(this.getMinConnections().intValue());
    f.setPoolMaxConnections(this.getMaxConnections().intValue());
    f.setPoolMultiuserAuthentication(this.getMultiuserAuthentication().booleanValue());
    f.setPoolPingInterval(this.getPingInterval().longValue());
    f.setPoolPRSingleHopEnabled(this.getPRSingleHopEnabled().booleanValue());
    f.setPoolReadTimeout(this.getReadTimeout().intValue());
    f.setPoolRetryAttempts(this.getRetryAttempts().intValue());
    f.setPoolServerGroup(this.getServerGroup());
    f.setPoolSocketBufferSize(this.getSocketBufferSize().intValue());
    f.setPoolStatisticInterval(this.getStatisticInterval().intValue());
    f.setPoolSubscriptionAckInterval(this.getSubscriptionAckInterval().intValue());
    f.setPoolSubscriptionEnabled(this.getSubscriptionEnabled().booleanValue());
    f.setPoolSubscriptionMessageTrackingTimeout(this.getSubscriptionMessageTrackingTimeout().intValue());
    f.setPoolSubscriptionRedundancy(this.getSubscriptionRedundancy().intValue());
    f.setPoolThreadLocalConnections(this.getThreadLocalConnections().booleanValue());
  }

  /**
   * Returns the pool as a string.  For use only by {@link PoolHelper
   * #poolToString(Pool)}.
   */
  protected static synchronized String poolToString(Pool p) {
    return poolToString(p.getName(), p);
  }

  /**
   * Returns the pool as a string.  Serves calling methods for both factory
   * attributes and actual pool.
   */
  private static synchronized String poolToString(String poolName, Pool p) {
    StringBuffer buf = new StringBuffer();
    buf.append("\n  freeConnectionTimeout: " + p.getFreeConnectionTimeout());
    buf.append("\n  idleTimeout: " + p.getIdleTimeout());
    buf.append("\n  loadConditioningInterval: " + p.getLoadConditioningInterval());
    buf.append("\n  locators: " + p.getLocators());
    buf.append("\n  minConnections: " + p.getMinConnections());
    buf.append("\n  maxConnections: " + p.getMaxConnections());
    buf.append("\n  multiuserAuthentication: " + p.getMultiuserAuthentication());
    buf.append("\n  pingInterval: " + p.getPingInterval());
    buf.append("\n  poolName: " + poolName);
    buf.append("\n  prSingleHopEnabled: " + p.getPRSingleHopEnabled());
    buf.append("\n  readTimeout: " + p.getReadTimeout());
    buf.append("\n  retryAttempts: " + p.getRetryAttempts());
    buf.append("\n  servers: " + p.getServers());
    buf.append("\n  serverGroup: " + p.getServerGroup());
    buf.append("\n  socketBufferSize: " + p.getSocketBufferSize());
    buf.append("\n  statisticInterval: " + p.getStatisticInterval());
    buf.append("\n  subscriptionAckInterval: " + p.getSubscriptionAckInterval());
    buf.append("\n  subscriptionEnabled: " + p.getSubscriptionEnabled());
    buf.append("\n  subscriptionMessageTrackingTimeout: " + p.getSubscriptionMessageTrackingTimeout());
    buf.append("\n  subscriptionRedundancy: " + p.getSubscriptionRedundancy());
    buf.append("\n  threadLocalConnections: " + p.getThreadLocalConnections());
    return buf.toString();
  }

  /**
   * Returns the named pool factory as a string.  For use only by {@link
   * PoolHelper#poolFactoryToString(String, PoolFactory)}.
   */
  protected static synchronized String poolFactoryToString(String poolName,
                                                           PoolFactory f) {
    Pool pa = ((PoolFactoryImpl)f).getPoolAttributes();
    return poolToString(poolName, pa);
  }

  /**
   * Returns whether the factory attributes are the same as the pool.
   */
  protected static boolean equals(PoolFactory f, Pool p) {
    Pool pa = ((PoolFactoryImpl)f).getPoolAttributes();
    if (pa.getFreeConnectionTimeout() != p.getFreeConnectionTimeout() ||
        pa.getIdleTimeout() != p.getIdleTimeout() ||
        pa.getLoadConditioningInterval() != p.getLoadConditioningInterval() ||
        pa.getLocators().size() != p.getLocators().size() ||
        pa.getMinConnections() != p.getMinConnections() ||
        pa.getMaxConnections() != p.getMaxConnections() ||
        pa.getMultiuserAuthentication() != p.getMultiuserAuthentication() ||
        pa.getPingInterval() != p.getPingInterval() ||
        pa.getPRSingleHopEnabled() != p.getPRSingleHopEnabled() ||
        pa.getReadTimeout() != p.getReadTimeout() ||
        pa.getRetryAttempts() != p.getRetryAttempts() ||
        pa.getServers().size() != p.getServers().size() ||
        !pa.getServerGroup().equals(p.getServerGroup()) ||
        pa.getSocketBufferSize() != p.getSocketBufferSize() ||
        pa.getStatisticInterval() != p.getStatisticInterval() ||
        pa.getSubscriptionAckInterval() != p.getSubscriptionAckInterval() ||
        pa.getSubscriptionEnabled() != p.getSubscriptionEnabled() ||
        pa.getSubscriptionMessageTrackingTimeout() != p.getSubscriptionMessageTrackingTimeout() ||
        pa.getSubscriptionRedundancy() != p.getSubscriptionRedundancy() ||
        pa.getThreadLocalConnections() != p.getThreadLocalConnections()) {
      return false;
    }
    return true;
  }

  /**
   * Returns the result of invoking the contact algorithm.
   */
  private List getContacts(String contactClass, String contactMethod,
                           Integer numContacts, String ds) {
    Object[] args = {numContacts, ds};
    MethExecutorResult result =
        MethExecutor.execute(contactClass, contactMethod, args);
    if (result.getStackTrace() != null){
      throw new HydraRuntimeException(result.toString());
    }
    return (List)result.getResult();
  }

  /**
   * Returns the contact type, validating along the way.
   */
  private int getContactType(List contacts, String contactAlgorithm) {
    if (contacts.size() == 0) {
      String s = "Contact algorithm " + contactAlgorithm
               + " returned no contacts";
      throw new HydraRuntimeException(s);
    }
    int type = -1;
    for (Iterator i = contacts.iterator(); i.hasNext();) {
      Object contact = i.next();
      if (contact instanceof DistributedSystemHelper.Endpoint) {
        if (type == -1) {
          type = SERVER_LOCATOR;
        } else if (type == SERVER_LOCATOR) {
          if (!((DistributedSystemHelper.Endpoint)contact).isServerLocator()) {
            String s = "Locator is not a server locator: " + contact;
            throw new HydraRuntimeException(s);
          }
        } else if (type == CACHE_SERVER) {
          String s = "Contact algorithm " + contactAlgorithm
                   + " returned a mix of server locators and bridge servers: "
                   + contacts;
        } else {
          throw new HydraInternalException("Unexpected type: " + type);
        }
      } else if (contact instanceof BridgeHelper.Endpoint) {
        if (type == -1) {
          type = CACHE_SERVER;
        } else if (type == SERVER_LOCATOR) {
          String s = "Contact algorithm " + contactAlgorithm
                   + " returned a mix of server locators and bridge servers: "
                   + contacts;
        } else if (type == CACHE_SERVER) {
          // copacetic
        } else {
          throw new HydraInternalException("Unexpected type: " + type);
        }
      } else {
        String s = "Contact algorithm " + contactAlgorithm
                 + " returned illegal type: " + contact.getClass().getName();
        throw new HydraRuntimeException(s);
      }
    }
    return type;
  }

//------------------------------------------------------------------------------
// Printing
//------------------------------------------------------------------------------

  public SortedMap toSortedMap() {
    SortedMap map = new TreeMap();
    String header = this.getClass().getName() + "." + this.getName() + ".";
    map.put(header + "contactAlgorithm", this.getContactAlgorithm());
    if (this.getContactNum().intValue() == PoolPrms.ALL_AVAILABLE) {
      map.put(header + "contactNum", "all available");
    } else {
      map.put(header + "contactNum", this.getContactNum());
    }
    if (this.getDistributedSystem() == null) {
      map.put(header + "distributedSystem", "all available");
    } else {
      map.put(header + "distributedSystem", this.getDistributedSystem());
    }
    map.put(header + "freeConnectionTimeout", this.getFreeConnectionTimeout());
    map.put(header + "idleTimeout", this.getIdleTimeout());
    map.put(header + "loadConditioningInterval", this.getLoadConditioningInterval());
    map.put(header + "minConnections", this.getMinConnections());
    map.put(header + "maxConnections", this.getMaxConnections());
    map.put(header + "multiuserAuthentication", this.getMultiuserAuthentication());
    map.put(header + "pingInterval", this.getPingInterval());
    map.put(header + "prSingleHopEnabled", this.getPRSingleHopEnabled());
    map.put(header + "readTimeout", this.getReadTimeout());
    map.put(header + "retryAttempts", this.getRetryAttempts());
    map.put(header + "serverGroup", this.getServerGroup());
    map.put(header + "socketBufferSize", this.getSocketBufferSize());
    map.put(header + "statisticInterval", this.getStatisticInterval());
    map.put(header + "subscriptionAckInterval", this.getSubscriptionAckInterval());
    map.put(header + "subscriptionEnabled", this.getSubscriptionEnabled());
    map.put(header + "subscriptionMessageTrackingTimeout", this.getSubscriptionMessageTrackingTimeout());
    map.put(header + "subscriptionRedundancy", this.getSubscriptionRedundancy());
    map.put(header + "threadLocalConnections", this.getThreadLocalConnections());
    return map;
  }

//------------------------------------------------------------------------------
// Configuration
//------------------------------------------------------------------------------

  /**
   * Creates pool descriptions from the pool parameters in the test
   * configuration.
   */
  protected static void configure(TestConfig config) {

    ConfigHashtable tab = config.getParameters();

    // create a description for each pool name
    Vector names = tab.vecAt(PoolPrms.names, new HydraVector());

    for (int i = 0; i < names.size(); i++) {

      String name = (String)names.elementAt(i);

      // create pool description from test configuration parameters
      PoolDescription pd = createPoolDescription(name, config, i);

      // save configuration
      config.addPoolDescription(pd);
    }
  }

  /**
   * Creates the pool description using test configuration parameters and
   * product defaults.
   */
  private static PoolDescription createPoolDescription(String name,
                                           TestConfig config, int index) {

    ConfigHashtable tab = config.getParameters();

    PoolDescription pd = new PoolDescription();
    pd.setName(name);

    // contactAlgorithm
    {
      Long key = PoolPrms.contactAlgorithm;
      Vector strs = tab.vecAtWild(key, index, null);
      pd.setContactClass(getContactClass(strs, key));
      pd.setContactMethod(getContactMethod(strs, key));
    }
    // contactNum
    {
      Long key = PoolPrms.contactNum;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      pd.setContactNum(getContactNum(i, key));
    }
    // distributedSystem
    {
      Long key = PoolPrms.distributedSystem;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null) {
        pd.setDistributedSystem(getDistributedSystem(str, key, config));
      }
    }
    // freeConnectionTimeout
    {
      Long key = PoolPrms.freeConnectionTimeout;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = new Integer(PoolFactory.DEFAULT_FREE_CONNECTION_TIMEOUT);
      }
      pd.setFreeConnectionTimeout(i);
    }
    // idleTimeout
    {
      Long key = PoolPrms.idleTimeout;
      Long i = tab.getLong(key, tab.getWild(key, index, null));
      if (i == null) {
        i = new Long(PoolFactory.DEFAULT_IDLE_TIMEOUT);
      }
      pd.setIdleTimeout(i);
    }
    // loadConditioningInterval
    {
      Long key = PoolPrms.loadConditioningInterval;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = new Integer(PoolFactory.DEFAULT_LOAD_CONDITIONING_INTERVAL);
      }
      pd.setLoadConditioningInterval(i);
    }
    // minConnections
    {
      Long key = PoolPrms.minConnections;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = new Integer(PoolFactory.DEFAULT_MIN_CONNECTIONS);
      }
      pd.setMinConnections(i);
    }
    // maxConnections
    {
      Long key = PoolPrms.maxConnections;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = new Integer(PoolFactory.DEFAULT_MAX_CONNECTIONS);
      }
      pd.setMaxConnections(i);
    }
    // multiuserAuthentication
    {
      Long key = PoolPrms.multiuserAuthentication;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        bool = Boolean.valueOf(PoolFactory.DEFAULT_MULTIUSER_AUTHENTICATION);
      }
      pd.setMultiuserAuthentication(bool);
    }
    // pingInterval
    {
      Long key = PoolPrms.pingInterval;
      Long i = tab.getLong(key, tab.getWild(key, index, null));
      if (i == null) {
        i = new Long(PoolFactory.DEFAULT_PING_INTERVAL);
      }
      pd.setPingInterval(i);
    }
    // prSingleHopEnabled
    {
      Long key = PoolPrms.prSingleHopEnabled;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        bool = Boolean.valueOf(PoolFactory.DEFAULT_PR_SINGLE_HOP_ENABLED);
      }
      pd.setPRSingleHopEnabled(bool);
    }
    // readTimeout
    {
      Long key = PoolPrms.readTimeout;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = new Integer(PoolFactory.DEFAULT_READ_TIMEOUT);
      }
      pd.setReadTimeout(i);
    }
    // retryAttempts
    {
      Long key = PoolPrms.retryAttempts;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = new Integer(PoolFactory.DEFAULT_RETRY_ATTEMPTS);
      }
      pd.setRetryAttempts(i);
    }
    // serverGroup
    {
      Long key = PoolPrms.serverGroup;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str == null) {
        pd.setServerGroup(PoolFactory.DEFAULT_SERVER_GROUP);
      } else {
        pd.setServerGroup(getServerGroup(str, key, config));
      }
    }
    // socketBufferSize
    {
      Long key = PoolPrms.socketBufferSize;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = new Integer(PoolFactory.DEFAULT_SOCKET_BUFFER_SIZE);
      }
      pd.setSocketBufferSize(i);
    }
    // statisticInterval
    {
      Long key = PoolPrms.statisticInterval;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = new Integer(PoolFactory.DEFAULT_STATISTIC_INTERVAL);
      }
      pd.setStatisticInterval(i);
    }
    // subscriptionAckInterval
    {
      Long key = PoolPrms.subscriptionAckInterval;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = new Integer(PoolFactory.DEFAULT_SUBSCRIPTION_ACK_INTERVAL);
      }
      pd.setSubscriptionAckInterval(i);
    }
    // subscriptionEnabled
    {
      Long key = PoolPrms.subscriptionEnabled;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        bool = Boolean.valueOf(PoolFactory.DEFAULT_SUBSCRIPTION_ENABLED);
      }
      pd.setSubscriptionEnabled(bool);
    }
    // subscriptionMessageTrackingTimeout
    {
      Long key = PoolPrms.subscriptionMessageTrackingTimeout;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = new Integer(PoolFactory.DEFAULT_SUBSCRIPTION_MESSAGE_TRACKING_TIMEOUT);
      }
      pd.setSubscriptionMessageTrackingTimeout(i);
    }
    // subscriptionRedundancy
    {
      Long key = PoolPrms.subscriptionRedundancy;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = new Integer(PoolFactory.DEFAULT_SUBSCRIPTION_REDUNDANCY);
      }
      pd.setSubscriptionRedundancy(i);
    }
    // threadLocalConnections
    {
      Long key = PoolPrms.threadLocalConnections;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        bool = Boolean.valueOf(PoolFactory.DEFAULT_THREAD_LOCAL_CONNECTIONS);
      }
      pd.setThreadLocalConnections(bool);
    }
    return pd;
  }

//------------------------------------------------------------------------------
// Contact configuration support
//------------------------------------------------------------------------------

  /**
   * Returns the contact class for the given contact algorithm.
   * @throws HydraConfigException if the algorithm is malformed.
   */
  private static String getContactClass(Vector strs, Long key) {
    if (strs == null || strs.size() == 0) { // default
      return "hydra.PoolHelper";
    } else if (strs.size() == 2) {
      return (String)strs.get(0);
    } else {
      String s = BasePrms.nameForKey(key) + " has illegal value: " + strs;
      throw new HydraConfigException(s);
    }
  }

  /**
   * Returns the contact method for the given contact algorithm.
   * @throws HydraConfigException if the algorithm is malformed.
   */
  private static String getContactMethod(Vector strs, Long key) {
    if (strs == null || strs.size() == 0) { // default
      return "getRandomContacts";
    } else if (strs.size() == 2) {
      return (String)strs.get(1);
    } else {
      String s = BasePrms.nameForKey(key) + " has illegal value: " + strs;
      throw new HydraConfigException(s);
    }
  }

  /**
   * Returns the contact number for the given integer.
   * @throws HydraConfigException if the integer has an illegal value.
   */
  private static Integer getContactNum(Integer i, Long key) {
    if (i == null || i.intValue() == PoolPrms.ALL_AVAILABLE) {
      return new Integer(PoolPrms.ALL_AVAILABLE);
    } else if (i.intValue() > 0) {
      return i;
    } else {
      String s = BasePrms.nameForKey(key) + " has illegal value: " + i;
      throw new HydraConfigException(s);
    }
  }

//------------------------------------------------------------------------------
// Distributed system configuration support
//------------------------------------------------------------------------------

  /**
   * Checks whether the given string is a valid distributed system and returns
   * it.
   * @throws HydraConfigException if the given string is not listed in {@link
   *         GemFirePrms#distributedSystem}.
   * Returns the distributed system for the given string.  Checks whether it
   * is a valid distributed system.
   */
  private static String getDistributedSystem(String str, Long key,
                                             TestConfig config) {
    Map gfds = config.getGemFireDescriptions();
    if (gfds != null) {
      for (Iterator i = gfds.values().iterator(); i.hasNext();) {
        GemFireDescription gfd = (GemFireDescription)i.next();
        if (gfd.getDistributedSystem().equals(str)) {
          return str;
        }
      }
    }
    String s = BasePrms.nameForKey(key) + " not found in any "
             + BasePrms.nameForKey(GemFirePrms.distributedSystem) + ": " + str;
    throw new HydraConfigException(s);
  }
  
  

//------------------------------------------------------------------------------
// Server group configuration support
//------------------------------------------------------------------------------

  /**
   * Checks whether the given string is a valid server group and returns it.
   * @throws HydraConfigException if the given string is not listed in {@link
   *         BridgePrms#groups}.
   */
  private static String getServerGroup(String str, Long key, TestConfig config)
  {
    Map bds = config.getBridgeDescriptions();
    if (bds != null) {
      for (Iterator i = bds.values().iterator(); i.hasNext();) {
        BridgeDescription bd = (BridgeDescription)i.next();
        if (Arrays.asList(bd.getGroups()).contains(str)) {
          return str;
        }
      }
    }
    String s = BasePrms.nameForKey(key) + " not found in any "
             + BasePrms.nameForKey(BridgePrms.groups) + ": " + str;
    throw new HydraConfigException(s);
  }
}
