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

import com.gemstone.gemfire.cache.asyncqueue.internal.AsyncEventQueueFactoryImpl;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.pivotal.gemfirexd.internal.engine.ddl.ServerGroupsTableAttribute;
import com.pivotal.gemfirexd.internal.engine.sql.catalog.DistributionDescriptor;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ConstantAction;

/**
 * Statement Node for CREATE ASYNCEVENTLISTENER DDL
 * 
 * @author Yogesh Mahajan
 * @since 1.0.2
 * 
 */
public class CreateAsyncEventListenerNode extends DDLStatementNode {

  private String id;

  private String className;

  private String initParams;

  public int remoteDs = GatewaySender.DEFAULT_DISTRIBUTED_SYSTEM_ID;

  public int socketBufferSize = GatewaySender.DEFAULT_SOCKET_BUFFER_SIZE;

  // TODO: merge: [sumedh] why is manual start true for AsyncEventListener?
  public boolean manualStart = true; // for gfxd, it should true, for gfe it false

  public int socketReadTimeout = GatewaySender.DEFAULT_SOCKET_READ_TIMEOUT;

  public boolean isBatchConflationEnabled = GatewaySender.DEFAULT_BATCH_CONFLATION;

  public int batchSize = GatewaySender.DEFAULT_BATCH_SIZE;

  public int batchTimeInterval =
      AsyncEventQueueFactoryImpl.DEFAULT_BATCH_TIME_INTERVAL;

  public boolean isPersistenceEnabled = GatewaySender.DEFAULT_PERSISTENCE_ENABLED;
  
  private boolean diskSynchronous = false;//GatewaySender.DEFAULT_DISK_SYNCHRONOUS;

  public String diskStoreName;

  public int maximumQueueMemory = GatewaySender.DEFAULT_MAXIMUM_QUEUE_MEMORY;

  public int alertThreshold = GatewaySender.DEFAULT_ALERT_THRESHOLD;
  
  public boolean isParallel;

  private ServerGroupsTableAttribute serverGroups;

  public CreateAsyncEventListenerNode() {
  }

  public String getName() {
    return "CreateAsyncEventListener";
  }

  @Override
  public void init(Object arg1, Object arg2, Object arg3, Object arg4,
      Object arg5) throws StandardException {

    this.id = (String)arg1;
    this.className = (String)arg2;
    this.initParams = (String)arg3;
    this.serverGroups = (ServerGroupsTableAttribute)arg4;

    @SuppressWarnings("unchecked")
    final Map<Object, Object> attrs = (Map<Object, Object>)arg5;
    final Iterator<Map.Entry<Object, Object>> entryItr = attrs.entrySet()
        .iterator();
    while (entryItr.hasNext()) {
      Map.Entry<Object, Object> entry = entryItr.next();
      String key = (String)entry.getKey();
      Object vn = entry.getValue();
      if (key.equalsIgnoreCase(CreateGatewaySenderNode.MANUALSTART)) {
        this.manualStart = (Boolean)vn;
      }
      else if (key
          .equalsIgnoreCase(CreateGatewaySenderNode.ENABLEBATCHCONFLATION)) {
        this.isBatchConflationEnabled = (Boolean)vn;
      }
      else if (key.equalsIgnoreCase(CreateGatewaySenderNode.BATCHSIZE)) {
        this.batchSize = (Integer)vn;
      }
      else if (key.equalsIgnoreCase(CreateGatewaySenderNode.BATCHTIMEINTERVAL)) {
        this.batchTimeInterval = (Integer)vn;
      }
      else if (key.equalsIgnoreCase(CreateGatewaySenderNode.ENABLEPERSISTENCE)) {
        this.isPersistenceEnabled = (Boolean)vn;
      }
      else if (key.equalsIgnoreCase(CreateGatewaySenderNode.DISKSYNCHRONOUS)) {
        this.diskSynchronous = (Boolean)vn;
      }
      else if (key.equalsIgnoreCase(CreateGatewaySenderNode.DISKSTORENAME)) {
        this.diskStoreName = (String)vn;
      }
      else if (key.equalsIgnoreCase(CreateGatewaySenderNode.MAXQUEUEMEMORY)) {
        this.maximumQueueMemory = (Integer)vn;
      }
      else if (key.equalsIgnoreCase(CreateGatewaySenderNode.ALERTTHRESHOLD)) {
        this.alertThreshold = (Integer)vn;
      }
      else if (key.equalsIgnoreCase(CreateGatewaySenderNode.ISPARALLEL)) {
        this.isParallel = (Boolean)vn;
      }
    }
    DistributionDescriptor.checkAvailableDataStore(
        getLanguageConnectionContext(),
        this.serverGroups != null ? this.serverGroups.getServerGroupSet()
            : null, "CREATE ASYNCEVENTLISTENER " + this.id);

  }

  @Override
  public ConstantAction makeConstantAction() {
    return getGenericConstantActionFactory()
        .getCreateAsyncEventListenerConstantAction(id, serverGroups,
            manualStart, isBatchConflationEnabled, batchSize, batchTimeInterval,
            isPersistenceEnabled, diskSynchronous, diskStoreName, maximumQueueMemory,
            alertThreshold, isParallel, className, initParams);
  }

  @Override
  public String statementToString() {
    return "CREATE ASYNCEVENTLISTENER";
  }

  public static void dummy() {
  }
}
