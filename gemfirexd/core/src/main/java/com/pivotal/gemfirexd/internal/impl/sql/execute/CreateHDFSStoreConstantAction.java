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

import com.gemstone.gemfire.cache.hdfs.HDFSStoreFactory;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.access.GemFireTransaction;
import com.pivotal.gemfirexd.internal.engine.access.operations.HDFSStoreCreateOperation;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.store.ServerGroupUtils;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.SchemaDescriptor;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;

/**
 * 
 * @author jianxiachen
 *
 */

public class CreateHDFSStoreConstantAction extends DDLConstantAction {

  final private String hdfsStoreName;
  
  final private HDFSStoreFactory hsf;

  public static final String REGION_PREFIX_FOR_CONFLATION =
      "__GFXD_INTERNAL_HDFSSTORE_";

  CreateHDFSStoreConstantAction(String hdfsStoreName, HDFSStoreFactory hsf) {
    this.hdfsStoreName = hdfsStoreName;
    this.hsf = hsf;
  }

  // Override the getSchemaName/getObjectName to enable
  // DDL conflation of CREATE and DROP HDFSSTORE statements.
  @Override
  public final String getSchemaName() {
    // HDFS stores have no schema, so return 'SYS'
    return SchemaDescriptor.STD_SYSTEM_SCHEMA_NAME;
  }

  @Override
  public final String getTableName() {
    return REGION_PREFIX_FOR_CONFLATION + hdfsStoreName;
  }

  @Override
  public String toString() {
    return constructToString("CREATE HDFSSTORE ", hdfsStoreName);
  }

  @Override
  public void executeConstantAction(Activation activation)
      throws StandardException {
    HDFSStoreCreateOperation startOp = new HDFSStoreCreateOperation(hsf,
        hdfsStoreName);
    LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
    GemFireTransaction gft = (GemFireTransaction)lcc.getTransactionExecute();
    gft.logAndDo(startOp);
  }
  
  public final String getHDFSStoreName() {
    return hdfsStoreName;
  }

}
