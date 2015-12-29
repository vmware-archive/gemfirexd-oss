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
package com.gemstone.gemfire.internal.cache.xmlcache;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.wan.GatewayReceiver;
import com.gemstone.gemfire.cache.wan.GatewayTransportFilter;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

public class GatewayReceiverCreation implements GatewayReceiver {

  private final Cache cache;

  private String host;

  private int startPort;
  
  private int endPort;
  
  private String portRange;

  private List<GatewayTransportFilter> transFilter = new ArrayList<GatewayTransportFilter>();

  private int maxTimeBetweenPings;

  private int socketBufferSize;

  private String bindAddress;
  
  private CacheServer receiver;

  private final String id;
  
  private final LogWriterI18n logger ;

  @SuppressWarnings("deprecation")
  public GatewayReceiverCreation(Cache cache, String id, int startPort,
      int endPort, int timeBetPings, int buffSize, String bindAdd,
      List<GatewayTransportFilter> filters, String hostnameForSenders) {
    this.cache = cache;
    this.id = id;
    this.logger = this.cache.getLoggerI18n();
    
    /*
     * If user has set hostNameForSenders then it should take precedence over
     * bindAddress. If user hasn't set either hostNameForSenders or bindAddress
     * then getLocalHost().getHostName() should be used.
     */
    if (hostnameForSenders == null || hostnameForSenders.isEmpty()) {
      if (bindAdd == null || bindAdd.isEmpty()) {
        try {
          if (this.logger.warningEnabled()) {
            this.logger
                .warning(LocalizedStrings.GatewayReceiverImpl_USING_LOCAL_HOST);
          }
          this.host = SocketCreator.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
          throw new IllegalStateException(
              LocalizedStrings.GatewayReceiverImpl_COULD_NOT_GET_HOST_NAME
                  .toLocalizedString(),
              e);
        }
      } else {
        this.host = bindAdd;
      }
    } else {
      this.host = hostnameForSenders;
    }
    

    this.startPort = startPort;
    this.endPort = endPort;
    this.maxTimeBetweenPings = timeBetPings;
    this.socketBufferSize = buffSize;
    this.bindAddress = bindAdd;
    this.transFilter = filters;
  }

  public String getId() {
    return this.id;
  }

  public List<GatewayTransportFilter> getGatewayTransportFilters() {
    return this.transFilter;
  }

  public int getMaximumTimeBetweenPings() {
    return this.maxTimeBetweenPings;
  }

  public int getPort() {
    return this.startPort;
  }

  public String getPortRange() {
    return this.portRange;
  }
  
  public int getSocketBufferSize() {
    return this.socketBufferSize;
  }

  public void setMaximumTimeBetweenPings(int time) {
    this.maxTimeBetweenPings = time;
  }

  public void setStartPort(int port) {
    this.startPort = port;
  }
  
  public void setEndPort(int port) {
    this.endPort = port;
  }
  
  public void setSocketBufferSize(int socketBufferSize) {
    this.socketBufferSize = socketBufferSize;
  }

  public String getHost() {
    return this.host;
  }

  public String getBindAddress() {
    return this.bindAddress;
  }

  public void setBindAddress(String address) {
    this.bindAddress = address;
  }

  public void start() throws IOException {
    if (receiver == null) {
      //add a cache server and set its port to random port. See defect 45630 for more details.
      receiver = ((CacheCreation)this.cache).addCacheServer(true);
      receiver.setPort(endPort + 1);
    }
  }

  public void stop() {

  }

  public boolean isRunning(){
    return false;
  }
  
  public void addGatewayTransportFilter(GatewayTransportFilter filter) {
    this.transFilter.add(filter);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.wan.GatewayReceiver#getStartPort()
   */
  public int getStartPort() {
    return this.startPort;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.wan.GatewayReceiver#getEndPort()
   */
  public int getEndPort() {
    return this.endPort;
  }

}
