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

import com.gemstone.gemfire.cache.util.Gateway;
import com.gemstone.gemfire.cache.util.GatewayHub;
import java.io.Serializable;
import java.util.*;

/**
 * Encodes information needed to describe and create a gateway hub.
 */
public class GatewayHubDescription extends AbstractDescription
implements Serializable {

  private static final long serialVersionUID = 1L;

  /** The logical name of this gateway hub description */
  private String name;

  /** Remaining parameters, in alphabetical order */
  private Boolean acceptGatewayConnections;
  private Boolean asynchronousGatewayDistributionEnabled;
  private Boolean haEnabled;
  private Integer maximumTimeBetweenPings;
  private Integer socketBufferSize;
  private String startupPolicy;

//------------------------------------------------------------------------------
// Constructors
//------------------------------------------------------------------------------

  public GatewayHubDescription() {
  }

//------------------------------------------------------------------------------
// Accessors, in aphabetical order after name
//------------------------------------------------------------------------------

  /**
   * Returns the logical name of this gateway hub description.
   */
  public String getName() {
    return this.name;
  }

  /**
   * Sets the logical name of this gateway hub description.
   */
  private void setName(String str) {
    this.name = str;
  }

  /**
   * Returns the accept gateway connections.
   */
  protected Boolean getAcceptGatewayConnections() {
    return this.acceptGatewayConnections;
  }

  /**
   * Sets the accept gateway connections.
   */
  private void setAcceptGatewayConnections(Boolean bool) {
    this.acceptGatewayConnections = bool;
  }

  /**
   * Returns the asynchronous gateway distribution enabled.
   */
  private Boolean getAsynchronousGatewayDistributionEnabled() {
    return this.asynchronousGatewayDistributionEnabled;
  }

  /**
   * Sets the asynchronous gateway distribution enabled.
   */
  private void setAsynchronousGatewayDistributionEnabled(Boolean bool) {
    this.asynchronousGatewayDistributionEnabled = bool;
  }

  /**
   * Sets the asynchronous gateway distribution enabled system property, if
   * configured to do so.  For use by {@link GatewayHubHelper} only.
   */
  protected void setAsynchronousGatewayDistributionEnabledProperty() {
    Boolean sval = this.getAsynchronousGatewayDistributionEnabled();
    if (sval != null) {
      String p = GatewayHubPrms.ASYNC_DIST_PROP_NAME;
      String pval = System.getProperty(p);
      if (pval != null && Boolean.getBoolean(p) != sval.booleanValue()) {
        Log.getLogWriter().warning("Overriding -D" + p + " with " + sval);
      }
      System.setProperty(p, String.valueOf(sval));
    }
  }

  /**
   * Returns the HA enabled.
   */
  protected Boolean getHAEnabled() {
    return this.haEnabled;
  }

  /**
   * Sets the HA enabled.
   */
  private void setHAEnabled(Boolean bool) {
    this.haEnabled = bool;
  }

  /**
   * Returns the maximum time between pings.
   */
  private Integer getMaximumTimeBetweenPings() {
    return this.maximumTimeBetweenPings;
  }

  /**
   * Sets the maximum time between pings.
   */
  private void setMaximumTimeBetweenPings(Integer i) {
    this.maximumTimeBetweenPings = i;
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
   * Returns the startup policy.
   */
  private String getStartupPolicy() {
    return this.startupPolicy;
  }

  /**
   * Sets the startup policy.
   */
  private void setStartupPolicy(String s) {
    this.startupPolicy = s;
  }

//------------------------------------------------------------------------------
// Gateway hub configuration
//------------------------------------------------------------------------------

  /**
   * Configures the gateway hub using this gateway hub description.  The id
   * and port are already set, and asynchronousGatewayDistributionEnabled is
   * set via a system property.
   */
  protected void configure(GatewayHub gh) {
    gh.setMaximumTimeBetweenPings(this.getMaximumTimeBetweenPings().intValue());
    gh.setSocketBufferSize(this.getSocketBufferSize().intValue());
    gh.setStartupPolicy(this.getStartupPolicy());
  }

  /**
   * Returns the gateway hub as a string.  For use only by {@link
   * GatewayHubHelper#gatewayHubToString(GatewayHub)}.
   */
  protected static synchronized String gatewayHubToString(GatewayHub gh) {
    StringBuffer buf = new StringBuffer();
    boolean async = Boolean.getBoolean(GatewayHubPrms.ASYNC_DIST_PROP_NAME);
    buf.append("\nGatewayHub (")
       .append(async ? "Asynchronous " : "Synchronous ");
    try {
       buf.append(gh.isPrimary() ? " Primary" : "Secondary ");
    } catch (UnsupportedOperationException e) {
       // ignore when isPrimary() invoked during xml generation (GatewayHubCreation)
    }
    buf.append(")");
    buf.append("\n  id: " + gh.getId());
    buf.append("\n  port: " + gh.getPort());
    buf.append("\n  maximumTimeBetweenPings: " + gh.getMaximumTimeBetweenPings());
    buf.append("\n  socketBufferSize: " + gh.getSocketBufferSize());
    buf.append("\n  startupPolicy: " + gh.getStartupPolicy());
    List gateways = gh.getGateways();
    if (gateways != null) {
      for (Iterator i = gateways.iterator(); i.hasNext();) {
        Gateway gateway = (Gateway)i.next();
        buf.append("\nGateway");
        buf.append(GatewayDescription.gatewayToString(gateway));
      }
    }
    return buf.toString();
  }

//------------------------------------------------------------------------------
// Printing
//------------------------------------------------------------------------------

  public SortedMap toSortedMap() {
    SortedMap map = new TreeMap();
    String header = this.getClass().getName() + "." + this.getName() + ".";
    map.put(header + "acceptGatewayConnections",
                      this.getAcceptGatewayConnections());
    if (this.getAsynchronousGatewayDistributionEnabled() == null) {
      map.put(header + "asynchronousGatewayDistributionEnabled",
                "-D" + GatewayHubPrms.ASYNC_DIST_PROP_NAME);
    } else {
      map.put(header + "asynchronousGatewayDistributionEnabled",
                        this.getAsynchronousGatewayDistributionEnabled());
    }
    map.put(header + "haEnabled", this.getHAEnabled());
    map.put(header + "maximumTimeBetweenPings",
                      this.getMaximumTimeBetweenPings());
    map.put(header + "socketBufferSize", this.getSocketBufferSize());
    map.put(header + "startupPolicy", this.getStartupPolicy());
    return map;
  }

//------------------------------------------------------------------------------
// Configuration
//------------------------------------------------------------------------------

  /**
   * Creates gateway hub descriptions from the gateway hub parameters in the
   * test configuration.
   */
  protected static void configure(TestConfig config) {

    ConfigHashtable tab = config.getParameters();

    // create a description for each bridge name
    Vector names = tab.vecAt(GatewayHubPrms.names, new HydraVector());

    for (int i = 0; i < names.size(); i++) {

      String name = (String)names.elementAt(i);

      // create gateway hub description from test configuration parameters
      GatewayHubDescription ghd = createGatewayHubDescription(name, tab, i);

      // save configuration
      config.addGatewayHubDescription(ghd);
    }
  }

  /**
   * Creates the gateway hub description using test configuration parameters
   * and product defaults.
   */
  private static GatewayHubDescription createGatewayHubDescription(String name,
                 ConfigHashtable tab, int index) {

    GatewayHubDescription ghd = new GatewayHubDescription();
    ghd.setName(name);

    // acceptGatewayConnections
    {
      Long key = GatewayHubPrms.acceptGatewayConnections;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        ghd.setAcceptGatewayConnections(Boolean.TRUE);
      } else {
        ghd.setAcceptGatewayConnections(bool);
      }
    }
    // asynchronousGatewayDistributionEnabled
    {
      Long key = GatewayHubPrms.asynchronousGatewayDistributionEnabled;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool != null) {
        ghd.setAsynchronousGatewayDistributionEnabled(bool);
      }
    }
    // haEnabled
    {
      Long key = GatewayHubPrms.haEnabled;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        ghd.setHAEnabled(Boolean.TRUE);
      } else {
        ghd.setHAEnabled(bool);
      }
    }
    // maximumTimeBetweenPings
    {
      Long key = GatewayHubPrms.maximumTimeBetweenPings;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = new Integer(GatewayHub.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS);
      }
      ghd.setMaximumTimeBetweenPings(i);
    }
    // socketBufferSize
    {
      Long key = GatewayHubPrms.socketBufferSize;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = new Integer(GatewayHub.DEFAULT_SOCKET_BUFFER_SIZE);
      }
      ghd.setSocketBufferSize(i);
    }
    // startupPolicy
    {
      Long key = GatewayHubPrms.startupPolicy;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str == null) {
        str = GatewayHub.DEFAULT_STARTUP_POLICY;
      } else {
        str = getStartupPolicy(str, key);
      }
      ghd.setStartupPolicy(str);
    }
    return ghd;
  }

//------------------------------------------------------------------------------
// Startup policy configuration support
//------------------------------------------------------------------------------

  /**
   * Returns the startup policy for the given string.
   */
  private static String getStartupPolicy(String str, Long key) {
    if (str.equalsIgnoreCase(GatewayHub.STARTUP_POLICY_NONE)) {
      return GatewayHub.STARTUP_POLICY_NONE;
    } else if (str.equalsIgnoreCase(GatewayHub.STARTUP_POLICY_PRIMARY)) {
      return GatewayHub.STARTUP_POLICY_PRIMARY;
    } else if (str.equalsIgnoreCase(GatewayHub.STARTUP_POLICY_SECONDARY)) {
      return GatewayHub.STARTUP_POLICY_SECONDARY;
    } else {
      String s = BasePrms.nameForKey(key) + " has illegal value: " + str;
      throw new HydraConfigException(s);
    }
  }
}
