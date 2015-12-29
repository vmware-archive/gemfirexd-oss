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
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.pivotal.gemfirexd.internal.engine.ddl.ServerGroupsTableAttribute;
import com.pivotal.gemfirexd.internal.engine.sql.catalog.DistributionDescriptor;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ConstantAction;

/**
 * 
 * Statement node for CRETAE GATEWAYSEDNER DDL
 * 
 * @author Yogesh Mahajan
 * @since 1.0
 * 
 */
public class CreateGatewaySenderNode extends DDLStatementNode {

  private String senderId;

  private int remoteDsId;

  private boolean enableBatchConflation = GatewaySender.DEFAULT_BATCH_CONFLATION;

  private boolean enablePersistence = GatewaySender.DEFAULT_PERSISTENCE_ENABLED;

  private String diskStoreName;
  
  private boolean diskSynchronous = false;// Keeping disk sync to be falseGatewaySender.DEFAULT_DISK_SYNCHRONOUS;

  private ServerGroupsTableAttribute serverGroups;

  private int socketBufferSize = GatewaySender.DEFAULT_SOCKET_BUFFER_SIZE;

  private boolean manualStart = GatewaySender.DEFAULT_MANUAL_START;

  private int socketReadTimeout = GatewaySender.DEFAULT_SOCKET_READ_TIMEOUT;

  private int batchSize = GatewaySender.DEFAULT_BATCH_SIZE;

  private int batchTimeInterval = GatewaySender.DEFAULT_BATCH_TIME_INTERVAL;

  private int maximumQueueMemory = GatewaySender.DEFAULT_MAXIMUM_QUEUE_MEMORY;

  private int alertThreshold = GatewaySender.DEFAULT_ALERT_THRESHOLD;

  private boolean isParallel = false;

  public static final String SOCKETBUFFERSIZE = "socketbuffersize";

  public static final String MANUALSTART = "manualstart";

  public static final String SOCKETREADTIMEOUT = "socketreadtimeout";

  public static final String ENABLEBATCHCONFLATION = "enablebatchconflation";

  public static final String BATCHSIZE = "batchsize";

  public static final String BATCHTIMEINTERVAL = "batchtimeinterval";

  public static final String ENABLEPERSISTENCE = "enablepersistence";
  
  public static final String DISKSYNCHRONOUS = "disksynchronous";

  public static final String DISKSTORENAME = "diskstorename";
  
  public static final String MAXQUEUEMEMORY = "maxqueuememory";

  public static final String ALERTTHRESHOLD = "alertthreshold";

  public static final String ISPARALLEL = "isparallel";

  public CreateGatewaySenderNode() {
  }

  public String getName() {
    return "CreateGatewaySender";
  }

  @Override
  public void init(Object arg1, Object arg2, Object arg3, Object arg4)
      throws StandardException {

    this.senderId = (String)arg1;
    this.remoteDsId = (Integer)arg2;
    this.serverGroups = (ServerGroupsTableAttribute)arg3;
    @SuppressWarnings("unchecked")
    final Map<Object, Object> attrs = (Map<Object, Object>)arg4;
    final Iterator<Map.Entry<Object, Object>> entryItr = attrs.entrySet()
        .iterator();
    while (entryItr.hasNext()) {
      Map.Entry<Object, Object> entry = entryItr.next();
      String key = (String)entry.getKey();
      Object vn = entry.getValue();
      if (key.equalsIgnoreCase(SOCKETBUFFERSIZE)) {
        this.socketBufferSize = (Integer)vn;
      }
      else if (key.equalsIgnoreCase(MANUALSTART)) {
        this.manualStart = (Boolean)vn;
      }
      else if (key.equalsIgnoreCase(SOCKETREADTIMEOUT)) {
        this.socketReadTimeout = (Integer)vn;
      }
      else if (key.equalsIgnoreCase(ENABLEBATCHCONFLATION)) {
        this.enableBatchConflation = (Boolean)vn;
      }
      else if (key.equalsIgnoreCase(BATCHSIZE)) {
        this.batchSize = (Integer)vn;
      }
      else if (key.equalsIgnoreCase(BATCHTIMEINTERVAL)) {
        this.batchTimeInterval = (Integer)vn;
      }
      else if (key.equalsIgnoreCase(ENABLEPERSISTENCE)) {
        this.enablePersistence = (Boolean)vn;
      }
      else if (key.equalsIgnoreCase(DISKSYNCHRONOUS)) {
        this.diskSynchronous = (Boolean)vn;
      }
      else if (key.equalsIgnoreCase(DISKSTORENAME)) {
        this.diskStoreName = (String)vn;
      }
      else if (key.equalsIgnoreCase(MAXQUEUEMEMORY)) {
        this.maximumQueueMemory = (Integer)vn;
      }
      else if (key.equalsIgnoreCase(ALERTTHRESHOLD)) {
        this.alertThreshold = (Integer)vn;
      }
      else if (key.equalsIgnoreCase(ISPARALLEL)) {
        this.isParallel = (Boolean)vn;
      }
    }
    DistributionDescriptor.checkAvailableDataStore(
        getLanguageConnectionContext(),
        this.serverGroups != null ? this.serverGroups.getServerGroupSet()
            : null, "CREATE GATEWAYSENDER " + this.senderId);
  }

  @Override
  public ConstantAction makeConstantAction() {
    return  getGenericConstantActionFactory().getCreateGatewaySenderConstantAction(
			senderId, serverGroups, socketBufferSize, manualStart, socketReadTimeout, enableBatchConflation,
			batchSize, batchTimeInterval, enablePersistence, diskSynchronous, diskStoreName,
			maximumQueueMemory, alertThreshold, remoteDsId, isParallel);
  }

  @Override
  public String statementToString() {
    return "CREATE GATEWAYSENDER";
  }

  public static void dummy() {
  }
}
