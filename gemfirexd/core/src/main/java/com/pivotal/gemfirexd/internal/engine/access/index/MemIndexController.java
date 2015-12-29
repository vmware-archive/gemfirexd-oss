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


import com.pivotal.gemfirexd.internal.engine.access.GemFireTransaction;
import com.pivotal.gemfirexd.internal.engine.access.MemConglomerate;
import com.pivotal.gemfirexd.internal.engine.access.MemConglomerateController;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.engine.store.offheap.OffHeapResourceHolder;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableBitSet;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.store.access.RowLocationRetRowSource;
import com.pivotal.gemfirexd.internal.iapi.store.access.SpaceInfo;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;
import com.pivotal.gemfirexd.internal.iapi.store.raw.ContainerHandle;
import com.pivotal.gemfirexd.internal.iapi.store.raw.LockingPolicy;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation;
import com.pivotal.gemfirexd.internal.impl.store.raw.data.SpaceInformation;

/**
 * @author yjing
 */
public class MemIndexController implements MemConglomerateController {

  protected final OpenMemIndex open_conglom;

  public MemIndexController() {
    this.open_conglom = new OpenMemIndex();
  }

  public final void init(GemFireTransaction tran, MemConglomerate conglomerate,
      int openMode, int lockLevel, LockingPolicy locking)
      throws StandardException {
    this.open_conglom.init(tran, (MemIndex)conglomerate, openMode, lockLevel,
        locking);
    postInitialize();
  }

  public int getType() {
    return MemConglomerate.HASH1INDEX;
  }

  public final boolean isClosed() {
    return this.open_conglom.isClosed();
  }

  protected void postInitialize() {
  }

  protected int doInsert(DataValueDescriptor[] row) throws StandardException {
    return 0;
  }

  public final void checkConsistency() throws StandardException {
  }

  public final void close() throws StandardException {
    final GemFireTransaction tran = this.open_conglom.getTransaction();
    if (tran != null) {
      tran.closeMe(this);
      this.open_conglom.close();
    }
  }

  final protected long load(GemFireTransaction tran,
      MemIndex indexConglomerate, boolean createConglom,
      RowLocationRetRowSource rowSource) throws StandardException {

    int mode = (ContainerHandle.MODE_FORUPDATE | ContainerHandle.MODE_UNLOGGED);
    if (createConglom) {
      mode |= ContainerHandle.MODE_CREATE_UNLOGGED;
    }
    init(tran, indexConglomerate, mode, TransactionController.MODE_RECORD, null);

    int count = 0;
    DataValueDescriptor[] row;
    while ((row = rowSource.getNextRowFromRowSource().getRowArray()) != null) {
      doInsert(row);
      ++count;
    }
    return count;
  }

  public final boolean closeForEndTransaction(boolean closeHeldScan)
      throws StandardException {
    if (closeHeldScan) {
      // close the scan as part of the commit/abort
      close();
      return true;
    }
    else {
      return false;
    }
  }

  public void debugConglomerate() throws StandardException {
  }

  public boolean delete(RowLocation loc) throws StandardException {
    throw new AssertionError("not expected to be called");
  }

  public RowLocation fetch(RowLocation loc, DataValueDescriptor[] destRow,
      FormatableBitSet validColumns, boolean faultIn) throws StandardException {
    throw new AssertionError("not expected to be called");
  }

  public RowLocation fetch(RowLocation loc, ExecRow destRow,
      FormatableBitSet validColumns, boolean faultIn) throws StandardException {
    throw new AssertionError("not expected to be called");
  }
  
  public RowLocation fetch(RowLocation loc, ExecRow destRow,
      FormatableBitSet validColumns, boolean faultIn, OffHeapResourceHolder offheapOwner)
          throws StandardException {
    throw new AssertionError("not expected to be called"); 
  }


  public RowLocation fetch(RowLocation loc, DataValueDescriptor[] destRow,
      FormatableBitSet validColumns, boolean waitForLock, boolean faultIn)
      throws StandardException {
    throw new AssertionError("not expected to be called");
  }

  public SpaceInfo getSpaceInfo() throws StandardException {
	  // We have no space information for GemFireXD mem heap objects
      SpaceInformation spaceInfo = new SpaceInformation(0,0,0);
      return spaceInfo;
  }

  public int insert(DataValueDescriptor[] row) throws StandardException {
    return this.doInsert(row);
  }

  public RowLocation insertAndFetchLocation(DataValueDescriptor[] row,
      RowLocation destRowLocation) throws StandardException {
    throw new AssertionError("not expected to be called");
  }

  public boolean isKeyed() {
    return true;
  }

  public boolean lockRow(RowLocation loc, int lock_oper, boolean wait,
      int lock_duration) throws StandardException {
    throw new AssertionError("not expected to be called");
  }

  public boolean lockRow(long page_num, int record_id, int lock_oper,
      boolean wait, int lock_duration) throws StandardException {
    throw new AssertionError("not expected to be called");
  }

  public RowLocation newRowLocationTemplate() throws StandardException {
    throw new AssertionError("not expected to be called");
  }

  public boolean replace(RowLocation loc, DataValueDescriptor[] row,
      FormatableBitSet validColumns) throws StandardException {
    throw new AssertionError("not expected to be called");
  }

  public void unlockRowAfterRead(RowLocation loc, boolean forUpdate,
      boolean row_qualified) throws StandardException {
    throw new AssertionError("not expected to be called");
  }

  public Properties getInternalTablePropertySet(Properties props)
      throws StandardException {
    if (props == null) {
      props = new Properties();
    }
    getTableProperties(props);
    return props;
  }

  public void getTableProperties(Properties props) throws StandardException {
    GemFireContainer container = this.open_conglom.getConglomerate().container;
    if (container != null) { // can be null for Hash1Index
      this.open_conglom.getConglomerate().container
          .getContainerProperties(props);
    }
  }
}
