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
package com.pivotal.gemfirexd.internal.iapi.sql.dictionary;

import com.pivotal.gemfirexd.internal.catalog.UUID;

public class GfxdGatewayReceiverDescriptor extends TupleDescriptor {

  private final UUID id;

  public String rcvrId;

  public String serverGroup;

  private int startPort;

  private int endPort;

  private int runningPort;

  private int maxTimeBetPings;

  private int socketBufferSize;

  private String bindAdd;

  private String hostNameForSenders;

  public GfxdGatewayReceiverDescriptor(DataDictionary dd, UUID id,
      String rcvrId, String serverGroup, Integer startPort, Integer endPort,
      Integer runningPort, Integer socketBufferSize, Integer maxTimeBetPings,
      String bindAdd, String hostNameForSenders) {

    super(dd);
    this.id = id;
    this.rcvrId = rcvrId;
    this.serverGroup = serverGroup;
    this.startPort = startPort;
    this.endPort = endPort;
    this.runningPort = runningPort;
    this.socketBufferSize = socketBufferSize;
    this.maxTimeBetPings = maxTimeBetPings;
    this.bindAdd = bindAdd;
    this.hostNameForSenders = hostNameForSenders;

  }

  public Integer getSocketBufferSize() {
    return this.socketBufferSize;
  }

  public Integer getMaxTimeBetweenPings() {
    return this.maxTimeBetPings;
  }

  public Integer getStartPort() {
    return this.startPort;
  }

  public Integer getEndPort() {
    return this.endPort;
  }

  public int getRunningPort() {
    return this.runningPort;
  }

  public String getServerGroup() {
    return this.serverGroup;
  }

  public String getBindAddress() {
    return this.bindAdd;
  }

  public String getId() {
    return this.rcvrId;
  }

  public UUID getUUID() {
    return this.id;
  }

  public String getDescriptorType() 
  {
          return "GatewayReceiver";
  }
  
  /** @see TupleDescriptor#getDescriptorName */
  public String getDescriptorName() { 
    return this.rcvrId; 
  }
  
  public String getHostNameForSenders() {
    return hostNameForSenders;
  }
  
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("GfxdGatewayReceiverDescriptor{");
    sb.append("id=" + rcvrId);
    sb.append("startPort=" + startPort);
    sb.append("endPort=" + endPort);
    sb.append("runningPort=" + runningPort);
    sb.append("serverGroup=" + serverGroup);
    sb.append("socketBufferSize=" + socketBufferSize);
    sb.append(",maxTimeBetweenPings=" + maxTimeBetPings);
    sb.append(",bindAdd=" + bindAdd);
    sb.append(",hostNameForSenders=" + hostNameForSenders);
    sb.append("}");
    return sb.toString();
  }



}
