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
package com.pivotal.gemfirexd.internal.impl.sql.compile;

import java.util.Iterator;
import java.util.Map;

import com.gemstone.gemfire.cache.wan.GatewayReceiver;
import com.pivotal.gemfirexd.internal.engine.ddl.ServerGroupsTableAttribute;
import com.pivotal.gemfirexd.internal.engine.sql.catalog.DistributionDescriptor;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ConstantAction;

/**
 * Statement Node for CREATE GATEWAYRECEIVER DDL
 * 
 * @author Yogesh Mahajan
 * @since 1.0.2
 * 
 */
public class CreateGatewayReceiverNode extends DDLStatementNode {

  private int startPort = GatewayReceiver.DEFAULT_START_PORT;

  private int endPort = GatewayReceiver.DEFAULT_END_PORT;

  public static final String SOCKETBUFFERSIZE = "socketbuffersize";

  public static final String MAXTIMEBETWEENPINGS = "maxtimebetweenpings";

  public static final String STARTPORT = "startport";

  public static final String ENDPORT = "endport";

  public static final String BINDADDRESS = "bindaddress";
  
  public static final String HOSTNAMEFORSENDERS = "hostnameforsenders";

  private int maxTimeBetPings = GatewayReceiver.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS;

  private int socketBufferSize = GatewayReceiver.DEFAULT_SOCKET_BUFFER_SIZE;

  private String bindAddress = GatewayReceiver.DEFAULT_BIND_ADDRESS;
  
  private String hostNameForSenders = GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS;

  private ServerGroupsTableAttribute serverGroups;

  private String id;

  public CreateGatewayReceiverNode() {
  }

  public String getName() {
    return "CreateGatewayReceiver";
  }

  @Override
  public void init(Object arg1, Object arg2, Object arg3)
      throws StandardException {
    Map attrs = (Map)arg1;
    Iterator<Map.Entry> entryItr = attrs.entrySet().iterator();
    while (entryItr.hasNext()) {
      Map.Entry entry = entryItr.next();
      String key = (String)entry.getKey();
      Object vn = entry.getValue();
      if (key.equalsIgnoreCase(STARTPORT)) {
        this.startPort = (Integer)vn;
      }
      else if (key.equalsIgnoreCase(ENDPORT)) {
        this.endPort = (Integer)vn;
      }
      else if (key.equalsIgnoreCase(SOCKETBUFFERSIZE)) {
        this.socketBufferSize = (Integer)vn;
      }
      else if (key.equalsIgnoreCase(MAXTIMEBETWEENPINGS)) {
        this.maxTimeBetPings = (Integer)vn;
      }
      else if (key.equalsIgnoreCase(BINDADDRESS)) {
        this.bindAddress = (String)vn;
      }
      else if (key.equalsIgnoreCase(HOSTNAMEFORSENDERS)) {
        this.hostNameForSenders = (String)vn;
      }
    }
    this.id = (String)arg2;
    this.serverGroups = (ServerGroupsTableAttribute)arg3;

    DistributionDescriptor.checkAvailableDataStore(
        getLanguageConnectionContext(),
        this.serverGroups != null ? this.serverGroups.getServerGroupSet()
            : null, "CREATE GATEWAYRECEIVER ");

 
  }

  @Override
  public ConstantAction makeConstantAction() throws StandardException {
	    return  getGenericConstantActionFactory().getCreateGatewayReceiverConstantAction(
	    		id, serverGroups, startPort, endPort, bindAddress, maxTimeBetPings, socketBufferSize, hostNameForSenders);
  }

  @Override
  public String statementToString() {
    return "CREATE GATEWAYRECEIVER";
  }

  public static void dummy() {
  }
}
