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

import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.ClassPathLoader;
import com.pivotal.gemfirexd.internal.catalog.UUID;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.access.GemFireTransaction;
import com.pivotal.gemfirexd.internal.engine.access.operations.AsyncQueueCreateOperation;
import com.pivotal.gemfirexd.internal.engine.ddl.ServerGroupsTableAttribute;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.store.ServerGroupUtils;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.depend.DependencyManager;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDictionary;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.SchemaDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.GfxdAsyncEventListenerDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ConstantAction;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;
import com.pivotal.gemfirexd.internal.impl.services.reflect.JarLoader;
import com.pivotal.gemfirexd.internal.shared.common.SharedUtils;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;

public class CreateAsyncEventListenerConstantAction extends DDLConstantAction {

  private final String id;

  private final ServerGroupsTableAttribute serverGroups;

  private final AsyncQueueCreateOperation queueCreateOp;

  public static final String REGION_PREFIX_FOR_CONFLATION =
      "__GFXD_INTERNAL_ASYNCLISTENER_";

  // Constructor for this constant action
  // Initialize all fields
  CreateAsyncEventListenerConstantAction(String id,
      ServerGroupsTableAttribute serverGroups, boolean manualStart,
      boolean enableBatchConflation, int batchSize, int batchTimeInterval,
      boolean enablePersistence, boolean diskSync, String diskStoreName, int maximumQueueMemory,
      int alertThreshold, boolean isParallel, String className, String initParams) {
    this.queueCreateOp = new AsyncQueueCreateOperation(id, manualStart,
        enableBatchConflation, batchSize, batchTimeInterval, enablePersistence, diskSync,
        diskStoreName, maximumQueueMemory, alertThreshold, isParallel,
        className, initParams);
    this.id = id;
    this.serverGroups = serverGroups;
  }

  // OBJECT METHODS

  @Override
  public String toString() {
    return constructToString("CREATE ASYNCEVENTLISTENER", id);
  }

  // INTERFACE METHODS

  // Override the getSchemaName/getObjectName to enable
  // DDL conflation of CREATE and DROP ASYNCEVENTLISTENER statements.
  @Override
  public final String getSchemaName() {
    // Async Eventlisteners have no schema, so return 'SYS'
    return SchemaDescriptor.STD_SYSTEM_SCHEMA_NAME;
  }

  @Override
  public final String getTableName() {
    return REGION_PREFIX_FOR_CONFLATION + id;
  }

  /**
   * This is the guts of the Execution-time logic for CREATE ASYNCEVENTLISTENER.
   * 
   * @see ConstantAction#executeConstantAction
   * 
   * @exception StandardException
   *              Thrown on failure
   */
  @Override
  public void executeConstantAction(Activation activation)
      throws StandardException {
    final LanguageConnectionContext lcc = activation
        .getLanguageConnectionContext();
    // first initialize the default private key used for internal encryption
    // of user provided passwords
    try {
      GemFireXDUtils.initializePrivateKey(
          GfxdConstants.PASSWORD_PRIVATE_KEY_ALGO_DEFAULT,
          GfxdConstants.PASSWORD_PRIVATE_KEY_SIZE_DEFAULT, lcc);
    } catch (Exception ex) {
      throw StandardException.newException(
          SQLState.UNEXPECTED_EXCEPTION_FOR_ASYNC_LISTENER, ex, id,
          ex.toString());
    }
    // If this node is not hosting data, return success, nothing to do
    if (!this.queueCreateOp.isParallel &&!ServerGroupUtils.isDataStore()) {
      SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_CONGLOM,
          "Skipping create gateway receiver for " + id + " on JVM of kind "
              + GemFireXDUtils.getMyVMKind());
      return;
    }

    // Are we on the list of members that need to start the listener?
    final Set<DistributedMember> members;
    final String servers;
    if (serverGroups != null) {
      members = GemFireXDUtils.getGfxdAdvisor().adviseOperationNodes(
          serverGroups.getServerGroupSet());
      servers = SharedUtils.toCSV(serverGroups.getServerGroupSet());
    }
    else {
      members = GemFireXDUtils.getGfxdAdvisor().adviseOperationNodes(null);
      servers = "";
    }
    final DistributedMember myId = Misc.getGemFireCache().getMyId();
    boolean self = members.remove(myId);
    Class<?> c = null;
    try {
      c = ClassPathLoader.getLatest().forName(this.queueCreateOp.className);
      if (self) {
        GemFireTransaction gft = (GemFireTransaction)lcc.getTransactionExecute();
        gft.logAndDo(this.queueCreateOp);
      }
    } catch (Exception ex) {
      throw StandardException.newException(
          SQLState.UNEXPECTED_EXCEPTION_FOR_ASYNC_LISTENER, ex, id,
          ex.toString());
    }
    // Now we need to create the row in ASYNCEVENTLISTENERS on all nodes
    // Indicate we're going to change the data dictionary
    DataDictionary dd = lcc.getDataDictionary();
    TransactionController tc = lcc.getTransactionExecute();
    dd.startWriting(lcc);

    UUID uid = dd.getUUIDFactory().recreateUUID(id);
    GfxdAsyncEventListenerDescriptor ghd = new GfxdAsyncEventListenerDescriptor(
        dd, uid, id, queueCreateOp.className, servers,
        queueCreateOp.manualStart, queueCreateOp.enableBatchConflation,
        queueCreateOp.batchSize, queueCreateOp.batchTimeInterval,
        queueCreateOp.enablePersistence, queueCreateOp.diskSync, queueCreateOp.diskStoreName,
        queueCreateOp.maximumQueueMemory, queueCreateOp.alertThreshold,
        !queueCreateOp.manualStart, queueCreateOp.initParams);
    dd.addDescriptor(ghd, null, DataDictionary.ASYNCEVENTLISTENERS_CATALOG_NUM,
        false, tc);

    Misc.getMemStore().getUUID_IDMap().put(uid, id);
    DependencyManager dm = dd.getDependencyManager();
    ClassLoader loader = c.getClassLoader();
    if (loader instanceof JarLoader) {
      dm.addDependency(ghd, (JarLoader)loader, lcc.getContextManager());
    }
    SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_CONGLOM,
        "CreateAsyncEventListenerNode:: inserted AsyncEventListener "
            + "configuration for " + id + " in SYS table");
  }
}
