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

package com.gemstone.gemfire.internal.cache.wan;

import java.util.ArrayList;
import java.util.List;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.wan.GatewayReceiver;
import com.gemstone.gemfire.cache.wan.GatewayReceiverFactory;
import com.gemstone.gemfire.cache.wan.GatewayTransportFilter;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ResourceEvent;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.GatewayReceiverCreation;
import com.gemstone.gemfire.management.ManagementException;

/**
 * @author Suranjan Kumar
 * @author Yogesh Mahajan
 * 
 * @since 7.0
 */
public class GatewayReceiverFactoryImpl implements GatewayReceiverFactory {

  private int startPort = GatewayReceiver.DEFAULT_START_PORT;
  
  private int endPort = GatewayReceiver.DEFAULT_END_PORT;
  
  private int timeBetPings = GatewayReceiver.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS;

  private int socketBuffSize = GatewayReceiver.DEFAULT_SOCKET_BUFFER_SIZE;

  private String bindAdd= GatewayReceiver.DEFAULT_BIND_ADDRESS; 
  
  private String hostnameForSenders = GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS;  

  private final List<GatewayTransportFilter> filters = new ArrayList<GatewayTransportFilter>();
  
  private final Cache cache;

  public GatewayReceiverFactoryImpl(Cache cache) {
   this.cache = cache;
  }

  @Override
  public GatewayReceiverFactory addGatewayTransportFilter(
      GatewayTransportFilter filter) {
    this.filters.add(filter);
    return this;
  }

  @Override
  public GatewayReceiverFactory removeGatewayTransportFilter(
      GatewayTransportFilter filter) {
    this.filters.remove(filter);
    return this;
  }

  @Override
  public GatewayReceiverFactory setMaximumTimeBetweenPings(int time) {
    this.timeBetPings = time;
    return this;
  }

  @Override
  public GatewayReceiverFactory setStartPort(int port) {
    this.startPort = port;
    return this;
  }
  
  @Override
  public GatewayReceiverFactory setEndPort(int port) {
    this.endPort = port;
    return this;
  }
  
  @Override
  public GatewayReceiverFactory setSocketBufferSize(int size) {
    this.socketBuffSize = size;
    return this;
  }

  @Override
  public GatewayReceiverFactory setBindAddress(String address) {
    this.bindAdd = address;
    return this;
  }
  
  public GatewayReceiverFactory setHostnameForSenders(String address) {
    this.hostnameForSenders = address;
    return this;
  } 

  @Override
  public GatewayReceiver create() {
    return create("defaultGatewayReceiver");
  }

  @Override
  public GatewayReceiver create(String id) {
    if (this.startPort > this.endPort) {
      throw new IllegalStateException("Please specify either "
          + "start port a value which is less than end port.");
    }
    GatewayReceiver recv = null;
    if (this.cache instanceof GemFireCacheImpl) {
      final GemFireCacheImpl gfc = (GemFireCacheImpl)this.cache;
      recv = new GatewayReceiverImpl(gfc, id, this.startPort, this.endPort,
          this.timeBetPings, this.socketBuffSize, this.bindAdd, this.filters,
          this.hostnameForSenders);
      ((GemFireCacheImpl)cache).addGatewayReceiver(recv);
      InternalDistributedSystem system = gfc.getDistributedSystem();
      try {
        system.handleResourceEvent(ResourceEvent.GATEWAYRECEIVER_CREATE, recv);
      } catch (ManagementException ex) {
        // javax.management.InstanceAlreadyExistsException can be thrown if we
        // create multiple GatewayReceivers on a node
      }
    } else if (this.cache instanceof CacheCreation) {
      recv = new GatewayReceiverCreation(this.cache, id, this.startPort, this.endPort,
          this.timeBetPings, this.socketBuffSize, this.bindAdd, this.filters,
          this.hostnameForSenders);
      ((CacheCreation)cache).addGatewayReceiver(recv);
    }
    return recv;
  }
}
