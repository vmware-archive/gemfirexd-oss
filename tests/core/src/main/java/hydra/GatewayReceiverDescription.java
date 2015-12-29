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

import com.gemstone.gemfire.cache.wan.GatewayReceiver;
import com.gemstone.gemfire.cache.wan.GatewayReceiverFactory;
import com.gemstone.gemfire.cache.wan.GatewayTransportFilter;
import java.io.Serializable;
import java.util.*;

/**
 * Encodes information needed to describe and create a gateway receiver.
 */
public class GatewayReceiverDescription extends AbstractDescription
implements Serializable {

  private static final long serialVersionUID = 1L;

  /** The logical name of this gateway receiver description */
  private String name;

  /** Remaining parameters, in alphabetical order */
  private List gatewayTransportFilters;
  private Integer endPort;
  private Integer maximumTimeBetweenPings;
  private Integer numInstances;
  private Integer socketBufferSize;
  private Integer startPort;

//------------------------------------------------------------------------------
// Constructors
//------------------------------------------------------------------------------

  public GatewayReceiverDescription() {
  }

//------------------------------------------------------------------------------
// Accessors, in aphabetical order after name
//------------------------------------------------------------------------------

  /**
   * Returns the logical name of this gateway receiver description.
   */
  public String getName() {
    return this.name;
  }

  /**
   * Sets the logical name of this gateway receiver description.
   */
  private void setName(String str) {
    this.name = str;
  }

  /**
   * Returns the end port.
   */
  private Integer getEndPort() {
    return this.endPort;
  }

  /**
   * Sets the end port.
   */
  private void setEndPort(Integer i) {
    this.endPort = i;
  }

  /**
   * Returns the classnames of the gateway transport filters.
   */
  private List getGatewayTransportFilters() {
    return this.gatewayTransportFilters;
  }

  /**
   * Sets the classnames of the gateway transport filters.
   */
  private void setGatewayTransportFilters(List classnames) {
    this.gatewayTransportFilters = classnames;
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
   * Returns the number of instances.
   */
  protected Integer getNumInstances() {
    return this.numInstances;
  }

  /**
   * Sets the number of instances.
   */
  private void setNumInstances(Integer i) {
    this.numInstances = i;
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
   * Returns the start port.
   */
  private Integer getStartPort() {
    return this.startPort;
  }

  /**
   * Sets the start port.
   */
  private void setStartPort(Integer i) {
    this.startPort = i;
  }

//------------------------------------------------------------------------------
// Gateway receiver configuration
//------------------------------------------------------------------------------

  /**
   * Configures a gateway receiver using this gateway receiver description
   * and the given port.
   */
  protected void configure(GatewayReceiverFactory f) {
    List<GatewayTransportFilter> gtfs =
                                 this.getGatewayTransportFilterInstances();
    if (TestConfig.tab().booleanAt(Prms.useIPv6)) {
      String ipv6 = HostHelper.getHostAddress(HostHelper.getIPv6Address());
      if (ipv6 == null) {
        String s = "IPv6 address is not available for host "
                 + HostHelper.getLocalHost();
        throw new HydraRuntimeException(s);
      }
      f.setBindAddress(ipv6);
    }
    f.setEndPort(this.getEndPort().intValue());
    if (gtfs != null) {
      for (GatewayTransportFilter gtf : gtfs) {
        f.addGatewayTransportFilter(gtf);
      }
    }
    f.setMaximumTimeBetweenPings(this.getMaximumTimeBetweenPings().intValue());
    f.setSocketBufferSize(this.getSocketBufferSize().intValue());
    f.setStartPort(this.getStartPort().intValue());
  }

  /**
   * Returns the gateway receiver as a string.  For use only by {@link
   * GatewayReceiverHelper#gatewayReceiverToString(Gateway)}.
   */
  protected static synchronized String gatewayReceiverToString(GatewayReceiver gr) {
    StringBuffer buf = new StringBuffer();
    buf.append("\n  bindAddress: " + gr.getBindAddress());
    buf.append("\n  endPort: " + gr.getEndPort());
    buf.append("\n  gatewayTransportFilters: "
              + gatewayTransportFiltersFor(gr.getGatewayTransportFilters()));
    buf.append("\n  host: " + gr.getHost());
    buf.append("\n  maximumTimeBetweenPings: " + gr.getMaximumTimeBetweenPings());
    buf.append("\n  port: " + gr.getPort());
    buf.append("\n  socketBufferSize: " + gr.getSocketBufferSize());
    buf.append("\n  startPort: " + gr.getStartPort());
    return buf.toString();
  }

//------------------------------------------------------------------------------
// Printing
//------------------------------------------------------------------------------

  public SortedMap toSortedMap() {
    SortedMap map = new TreeMap();
    String header = this.getClass().getName() + "." + this.getName() + ".";
    map.put(header + "endPort", this.getEndPort());
    map.put(header + "gatewayTransportFilters", this.getGatewayTransportFilters());
    map.put(header + "maximumTimeBetweenPings", this.getMaximumTimeBetweenPings());
    map.put(header + "numInstances", this.getNumInstances());
    map.put(header + "socketBufferSize", this.getSocketBufferSize());
    map.put(header + "startPort", this.getStartPort());
    return map;
  }

//------------------------------------------------------------------------------
// Configuration
//------------------------------------------------------------------------------

  /**
   * Creates gateway receiver descriptions from the gateway receiver parameters
   * in the test configuration.
   */
  protected static void configure(TestConfig config) {

    ConfigHashtable tab = config.getParameters();

    // create a description for each gateway receiver name
    Vector names = tab.vecAt(GatewayReceiverPrms.names, new HydraVector());

    for (int i = 0; i < names.size(); i++) {

      String name = (String)names.elementAt(i);

      // create gateway receiver description from test configuration parameters
      GatewayReceiverDescription grd = createGatewayReceiverDescription(
                                                        name, config, i);

      // save configuration
      config.addGatewayReceiverDescription(grd);
    }
  }

  /**
   * Creates the gateway receiver description using test configuration
   * parameters and product defaults.
   */
  private static GatewayReceiverDescription createGatewayReceiverDescription(
                 String name, TestConfig config, int index) {

    ConfigHashtable tab = config.getParameters();

    GatewayReceiverDescription grd = new GatewayReceiverDescription();
    grd.setName(name);

    // endPort
    {
      Long key = GatewayReceiverPrms.endPort;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = Integer.valueOf(GatewayReceiver.DEFAULT_END_PORT);
      }
      grd.setEndPort(i);
    }
    // gateway transport filters
    {
      Long key = GatewayReceiverPrms.gatewayTransportFilters;
      Vector strs = tab.vecAtWild(key, index, null);
      if (strs != null) {
        for (Iterator i = strs.iterator(); i.hasNext();) {
          String str = tab.getString(key, i.next());
          if (str == null || str.equalsIgnoreCase(BasePrms.NONE)) {
            i.remove();
          }
        }
        if (strs.size() > 0) {
          grd.setGatewayTransportFilters(new ArrayList(strs));
        }
      }
    }
    // maximumTimeBetweenPings
    {
      Long key = GatewayReceiverPrms.maximumTimeBetweenPings;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = Integer.valueOf(GatewayReceiver.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS);
      }
      grd.setMaximumTimeBetweenPings(i);
    }
    // numInstances
    {
      Long key = GatewayReceiverPrms.numInstances;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = 1;
      }
      grd.setNumInstances(i);
    }
    // socketBufferSize
    {
      Long key = GatewayReceiverPrms.socketBufferSize;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = GatewayReceiver.DEFAULT_SOCKET_BUFFER_SIZE;
      }
      grd.setSocketBufferSize(i);
    }
    // startPort
    {
      Long key = GatewayReceiverPrms.startPort;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = Integer.valueOf(GatewayReceiver.DEFAULT_START_PORT);
      }
      grd.setStartPort(i);
    }
    return grd;
  }

//------------------------------------------------------------------------------
// Gateway transport filter support
//------------------------------------------------------------------------------

  /**
   * Returns the classnames for the gateway transport filters in the list.
   */
  private static List gatewayTransportFiltersFor(List<GatewayTransportFilter> gtfs) {
    List classnames = new ArrayList();
    for (Object gtf : gtfs) {
      classnames.add(gtf.getClass().getName());
    }
    return classnames;
  }

  /**
   * Returns a gateway transport filter instance for each classname.
   * @throws HydraConfigException if instantiation fails or a class does not
   *                              implement GatewayTransportFilter.
   */
  private List<GatewayTransportFilter> getGatewayTransportFilterInstances() {
    List<String> classnames = this.getGatewayTransportFilters();
    if (classnames == null) {
      return null;
    }
    Long key = GatewaySenderPrms.gatewayTransportFilters;
    List<GatewayTransportFilter> instances = new ArrayList();
    for (String classname : classnames) {
      try {
        GatewayTransportFilter gtf =
              (GatewayTransportFilter)getInstance(key, classname);
        instances.add(gtf);
      } catch (ClassCastException e) {
        String s = BasePrms.nameForKey(key)
                 + " does not implement GatewayTransportFilter: " + classname;
        throw new HydraConfigException(s);
      }
    }
    return instances;
  }
}
