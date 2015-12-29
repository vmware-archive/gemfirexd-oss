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

package com.pivotal.gemfirexd.internal.engine.access;

import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableBitSet;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.store.access.Qualifier;
import com.pivotal.gemfirexd.internal.iapi.store.access.conglomerate.ScanManager;
import com.pivotal.gemfirexd.internal.iapi.store.raw.LockingPolicy;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;

/**
 * An extension to {@link ScanManager} for GemFireXD that adds methods for GFXD
 * specific functionality.
 * 
 * @author swale
 */
public interface MemScanController extends ScanManager {

  public void init(GemFireTransaction tran, MemConglomerate conglomerate,
      int openMode, int lockLevel, LockingPolicy locking,
      FormatableBitSet scanColumnList, DataValueDescriptor[] startKeyValue,
      int startSearchOperator, Qualifier[][] qualifier,
      DataValueDescriptor[] stopKeyValue, int stopSearchOperator,
      Activation act) throws StandardException;
}
