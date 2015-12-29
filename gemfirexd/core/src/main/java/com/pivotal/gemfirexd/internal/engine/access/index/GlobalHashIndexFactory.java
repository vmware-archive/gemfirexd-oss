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

package com.pivotal.gemfirexd.internal.engine.access.index;

import java.util.Properties;

import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.access.MemConglomerate;
import com.pivotal.gemfirexd.internal.engine.access.MemConglomerateFactory;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.store.access.ColumnOrdering;
import com.pivotal.gemfirexd.internal.iapi.store.access.conglomerate.ConglomerateFactory;
import com.pivotal.gemfirexd.internal.iapi.store.access.conglomerate.TransactionManager;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;

/**
 * @author yjing
 * 
 */
public class GlobalHashIndexFactory extends MemConglomerateFactory {

  private static final String FORMATUUIDSTRING =
    "C6CEEEF3-DAD3-11d0-BB01-0060973F0942";

  public GlobalHashIndexFactory() {
    super(FORMATUUIDSTRING, GfxdConstants.GLOBAL_HASH_INDEX_TYPE, null,
        ConglomerateFactory.GLOBAL_HASH_FACTORY_ID);
  }

  /*
   ** Methods of ConglomerateFactory
   */

  /**
   * Create the conglomerate and return a conglomerate object for it.
   * 
   * @see ConglomerateFactory#createConglomerate
   * 
   * @exception StandardException
   *              Standard exception policy.
   */
  @Override
  public MemConglomerate createConglomerate(TransactionManager xact_mgr,
      int segment, long containerId, DataValueDescriptor[] template,
      ColumnOrdering[] columnOrder, int[] collationIds, Properties properties,
      int temporaryFlag) throws StandardException {
    return new GlobalHashIndex();
  }
}
