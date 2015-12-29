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
package com.pivotal.gemfirexd.internal.impl.sql.execute;

import java.util.Set;

import com.gemstone.gemfire.cache.wan.GatewaySenderFactory;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.pivotal.gemfirexd.internal.catalog.UUID;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.ddl.ServerGroupsTableAttribute;
import com.pivotal.gemfirexd.internal.engine.ddl.wan.WanProcedures;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.store.ServerGroupUtils;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDictionary;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.SchemaDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.GfxdGatewaySenderDescriptor;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;
import com.pivotal.gemfirexd.internal.shared.common.SharedUtils;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;

public class CreateGatewaySenderConstantAction extends DDLConstantAction {

  final private String senderId;

  final private ServerGroupsTableAttribute serverGroups;

  final private int socketBufferSize;

  final private boolean manualStart;

  final private int socketReadTimeout;

  final private boolean enableBatchConflation;

  final private boolean enablePersistence;
  
  final private boolean diskSynchronous;

  final private int batchSize;

  final private int batchTimeInterval;

  private String diskStoreName;

  final private int maximumQueueMemory;

  final private int alertThreshold;

  final private int remoteDsId;
  
  final private boolean isParallel;
  
  final private boolean enableBULKDMLStr;

  public static final String REGION_PREFIX_FOR_CONFLATION =
      "__GFXD_INTERNAL_GATEWAYSENDER_";

  CreateGatewaySenderConstantAction(String senderId,
      ServerGroupsTableAttribute serverGroups, int socketBufferSize,
      boolean manualStart, int socketReadTimeout,
      boolean enableBatchConflation, int batchSize, int batchTimeInterval,
      boolean enablePersistence, boolean diskSync, String diskStoreName, int maximumQueueMemory,
      int alertThreshold, int remoteDsId, boolean isParallel) {
    this.senderId = senderId;
    this.serverGroups = serverGroups;
    this.socketBufferSize = socketBufferSize;
    this.manualStart = manualStart;
    this.socketReadTimeout = socketReadTimeout;
    this.enableBatchConflation = enableBatchConflation;
    this.batchSize = batchSize;
    this.batchTimeInterval = batchTimeInterval;
    this.enablePersistence = enablePersistence;
    this.diskSynchronous = diskSync;
    this.diskStoreName = diskStoreName;
    this.maximumQueueMemory = maximumQueueMemory;
    this.alertThreshold = alertThreshold;
    this.remoteDsId = remoteDsId;
    this.isParallel = isParallel;
    this.enableBULKDMLStr = false;
  }

  // Override the getSchemaName/getObjectName to enable
  // DDL conflation of CREATE and DROP GATEWAYSENDER statements.
  @Override
  public final String getSchemaName() {
    // gateways have no schema, so return 'SYS'
    return SchemaDescriptor.STD_SYSTEM_SCHEMA_NAME;
  }

  /** Create a table name out of the senderId. */
  @Override
  public final String getTableName() {
    return REGION_PREFIX_FOR_CONFLATION + senderId;
  }

  @Override
  public void executeConstantAction(Activation activation)
      throws StandardException {
    // If this node does not store data, return success, nothing to do
    if (!isParallel && !ServerGroupUtils.isDataStore()) {
      SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_CONGLOM,
          "Skipping create gateway sender for " + senderId + " on JVM of kind "
              + GemFireXDUtils.getMyVMKind());
      return;
    }
    // If this node needs to create the GATEWAYSENDER cache object, do so
    final Set<DistributedMember> members;
    if (serverGroups != null) {
      members = GemFireXDUtils.getGfxdAdvisor().adviseOperationNodes(
          serverGroups.getServerGroupSet());
    }
    else {
      members = GemFireXDUtils.getGfxdAdvisor().adviseOperationNodes(null);
    }
    final DistributedMember myId = Misc.getGemFireCache().getMyId();
    boolean self = members.remove(myId);
    GemFireCacheImpl cache = Misc.getGemFireCache();
    // Check to see if this cache already contains a gateway sender with this
    // name=id
    if (cache.getGatewaySender(senderId) != null) {
      // Throw 'object-already-exists' error here
      // TODO : also detect two gatewaysenders with different ids but identical
      // remotedsids
      throw StandardException.newException(SQLState.LANG_OBJECT_ALREADY_EXISTS,
          "GATEWAYSENDER", senderId);
    }
    if (self) {
      GatewaySenderFactory factory = cache.createGatewaySenderFactory();
      factory.setSocketBufferSize(socketBufferSize);
      factory.setManualStart(manualStart);
      factory.setSocketReadTimeout(socketReadTimeout);
      factory.setBatchConflationEnabled(enableBatchConflation);
      factory.setBatchSize(batchSize);
      factory.setBatchTimeInterval(batchTimeInterval);
      factory.setPersistenceEnabled(enablePersistence);
      factory.setDiskSynchronous(this.diskSynchronous);
      factory.setParallel(isParallel);

      if (diskStoreName != null) {
        diskStoreName = diskStoreName.toUpperCase();
        // check if disk store exists, else throw exception
        if (GemFireCacheImpl.getInstance().findDiskStore(diskStoreName) == null) {
          throw StandardException.newException(SQLState.DISK_STORE_ABSENT,
              diskStoreName);
        }
        factory.setDiskStoreName(diskStoreName);
      }
      else {
        // set default disk store
        factory.setDiskStoreName(GfxdConstants.GFXD_DEFAULT_DISKSTORE_NAME);
      }
      factory.setMaximumQueueMemory(maximumQueueMemory);
      factory.setAlertThreshold(alertThreshold);
      if(isParallel){
        factory.addGatewayEventFilter(WanProcedures.getParallelWanFilter());
      } else{
          factory.addGatewayEventFilter(WanProcedures.getSerialWanFilter(enableBULKDMLStr));
      }
      factory.create(senderId, remoteDsId);
    }
    // Finally, add row to GATEWAYSENDERS for all nodes
    LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
    DataDictionary dd = lcc.getDataDictionary();
    TransactionController tc = lcc.getTransactionExecute();
    dd.startWriting(lcc);
    UUID id = dd.getUUIDFactory().recreateUUID(senderId);

    String servers = SharedUtils.toCSV(serverGroups.getServerGroupSet());
    GfxdGatewaySenderDescriptor ghd = new GfxdGatewaySenderDescriptor(dd, id,
        senderId, remoteDsId, servers, socketBufferSize, manualStart,
        socketReadTimeout, enableBatchConflation, batchSize, batchTimeInterval,
        enablePersistence, diskSynchronous, diskStoreName, maximumQueueMemory, alertThreshold,
        !manualStart);
    dd.addDescriptor(ghd, null, DataDictionary.GATEWAYSENDERS_CATALOG_NUM,
        false, tc);
    SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_CONGLOM,
        "CreateGatewaySenderNode:: inserted GatewaySender "
            + "configuration for " + senderId + " in SYS table");
  }

  @Override
  public String toString() {
    return "CREATE GATEWAYSENDER " + senderId;
  }
}
