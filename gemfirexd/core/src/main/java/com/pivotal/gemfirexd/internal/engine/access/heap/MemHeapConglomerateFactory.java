
/*

 Derived from source files from the Derby project.

 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to you under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.

 */

/*
 * Changes for GemFireXD distributed data platform.
 *
 * Portions Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
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

/*
 This file was based on the MemStore patch written by Knut Magne, published
 under the Derby issue DERBY-2798 and released under the same license,
 ASF, as described above. The MemStore patch was in turn based on Derby source
 files.
*/

package com.pivotal.gemfirexd.internal.engine.access.heap;

import java.util.Properties;

import com.pivotal.gemfirexd.internal.engine.access.MemConglomerate;
import com.pivotal.gemfirexd.internal.engine.access.MemConglomerateFactory;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.store.access.ColumnOrdering;
import com.pivotal.gemfirexd.internal.iapi.store.access.conglomerate.ConglomerateFactory;
import com.pivotal.gemfirexd.internal.iapi.store.access.conglomerate.TransactionManager;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;

/**
 * The heap conglomerate factory manages heap conglomerates implemented on the
 * raw store.
 */
public class MemHeapConglomerateFactory extends MemConglomerateFactory {

  private static final String IMPLEMENTATIONID = "heap";

  private static final String FORMATUUIDSTRING =
    "D2976090-D9F5-11d0-B54D-00A024BF8878";

  public MemHeapConglomerateFactory() {
    super(FORMATUUIDSTRING, IMPLEMENTATIONID, null,
        ConglomerateFactory.HEAP_FACTORY_ID);
  }

  /*
   ** Methods of ConglomerateFactory
   */

  /**
   * Create the conglomerate and return a conglomerate object for it.
   * 
   * @exception StandardException
   *              Standard exception policy.
   * 
   * @see ConglomerateFactory#createConglomerate
   */
  @Override
  public MemConglomerate createConglomerate(TransactionManager xact_mgr,
      int segment, long containerId, DataValueDescriptor[] template,
      ColumnOrdering[] columnOrder, int[] collationIds, Properties properties,
      int temporaryFlag) throws StandardException {
    return new MemHeap();
  }
}
