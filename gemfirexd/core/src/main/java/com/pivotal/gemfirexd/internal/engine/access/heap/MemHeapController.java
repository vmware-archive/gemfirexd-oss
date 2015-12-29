
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
 * Changes for GemFireXD distributed data platform (some marked by "GemStone changes")
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

import com.gemstone.gemfire.cache.EntryDestroyedException;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.RegionEntry;
import com.gemstone.gemfire.internal.cache.TXStateInterface;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.access.GemFireTransaction;
import com.pivotal.gemfirexd.internal.engine.access.MemConglomerate;
import com.pivotal.gemfirexd.internal.engine.access.MemConglomerateController;
import com.pivotal.gemfirexd.internal.engine.access.index.GlobalExecRowLocation;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.engine.store.offheap.OffHeapResourceHolder;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableBitSet;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.store.access.RowLocationRetRowSource;
import com.pivotal.gemfirexd.internal.iapi.store.access.RowUtil;
import com.pivotal.gemfirexd.internal.iapi.store.access.SpaceInfo;
import com.pivotal.gemfirexd.internal.iapi.store.access.conglomerate.TransactionManager;
import com.pivotal.gemfirexd.internal.iapi.store.raw.LockingPolicy;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation;
import com.pivotal.gemfirexd.internal.impl.sql.execute.ValueRow;
import com.pivotal.gemfirexd.internal.impl.store.raw.data.SpaceInformation;

/**
 * DOCUMENT ME!
 * 
 * @author $author$
 */
public final class MemHeapController implements MemConglomerateController {

  private GemFireTransaction tran;

  private TXStateInterface txState;

  private GemFireContainer gfContainer;

  private int openMode;

  public void init(GemFireTransaction tran, MemConglomerate conglomerate,
      int openMode, int lockLevel, LockingPolicy locking)
      throws StandardException {
    this.tran = tran;
    this.gfContainer = conglomerate.getGemFireContainer();
    assert this.gfContainer != null;
    conglomerate.openContainer(tran, openMode, lockLevel, locking);
    this.openMode = openMode;
    this.txState = this.gfContainer.getActiveTXState(this.tran);
  }

  public boolean isClosed() {
    return (this.tran == null);
  }

  public boolean delete(RowLocation loc) throws StandardException {
    // assert loc instanceof ExecRowLocation:
    // "the ExecRowLocation class expected!";
    final RegionEntry entry = loc.getRegionEntry();
    return MemHeapScanController.delete(this.tran, this.txState,
        this.gfContainer, entry, loc.getBucketID());
  }

  public RowLocation fetch(RowLocation loc, DataValueDescriptor[] destRow,
      FormatableBitSet validColumns, boolean faultIn) throws StandardException {
    // TODO: PERF: temporary implementation until all callers modified to pass
    // ExecRow
    ValueRow vrow = new ValueRow(destRow.length);
    vrow.setRowArray(destRow);
    return fetch(loc, vrow, validColumns, faultIn);
  }

  public int getType() {
    return MemConglomerate.HEAP;
  }

  /**
   * @see RowUtil for descriptions of parameters
   */
  public RowLocation fetch(final RowLocation loc, ExecRow destRow,
      FormatableBitSet validColumns, boolean faultIn) throws StandardException {
    return RowUtil.fetch(loc, destRow, validColumns, faultIn, this.gfContainer,
        null, null, 0, this.tran);
  }
  
  public RowLocation fetch(final RowLocation loc, ExecRow destRow,
      FormatableBitSet validColumns, boolean faultIn, OffHeapResourceHolder offheapOwner) throws StandardException {
    return RowUtil.fetch(loc, destRow, validColumns, faultIn, this.gfContainer,
        null, null, 0, offheapOwner);
  }

  public RowLocation fetch(RowLocation loc, DataValueDescriptor[] row,
      FormatableBitSet validColumns, boolean waitForLock, boolean faultIn) {
    throw new AssertionError("not expected to be called");
  }

  public boolean replace(RowLocation loc, DataValueDescriptor[] row,
      FormatableBitSet validColumns) throws StandardException {

    final RowLocation memloc = loc;
    boolean updated = true;

    if (GemFireXDUtils.TraceConglomUpdate) {
      SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_CONGLOM_UPDATE,
          "MemHeapController::replace: ExecRowLocation=" + memloc + " entry="
              + memloc.getRegionEntry() + " bucketId=" + memloc.getBucketID());
    }
    try {
      this.gfContainer.replacePartialRow(memloc.getRegionEntry(), validColumns,
          row, memloc.getBucketID(), this.tran, this.txState,
          this.tran.getLanguageConnectionContext());
    } catch (EntryDestroyedException ede) {
      // can occur as the row may be concurrently deleted
      updated = false;
    } catch (EntryNotFoundException enfe) {
      // Fix for Bug 40161. It is possible that by the time region.put is in
      // progress the entry is deleted & so the put is internally converted into
      // create. But since the new value is delta, which needs old value to be
      // meaningful, will not be there throwing Exception with message
      // "Cannot apply a delta without an existing value".
      updated = false;
    }
    return updated;
  }

  /**
   * Private/Protected methods of This class:***********************************
   * **************************************
   */

  /**
   * Insert a new row into the heap and return the region key of the row.
   * 
   * @param row
   *          The row to insert.
   * 
   * @return the new slotId
   * 
   * @exception StandardException
   *              Standard exception policy.
   */
  private Object doInsert(DataValueDescriptor[] row) throws StandardException {
    return this.gfContainer.insertRow(row, this.tran, this.txState,
        this.tran.getLanguageConnectionContext(), false /*isPutDML*/);
  }

  /**
   * DOCUMENT ME!
   * 
   * @param xact_manager
   *          DOCUMENT ME!
   * @param heap
   *          DOCUMENT ME!
   * @param createConglom
   *          DOCUMENT ME!
   * @param rowSource
   *          DOCUMENT ME!
   * 
   * @return DOCUMENT ME!
   * 
   * @throws StandardException
   *           DOCUMENT ME!
   */
  protected long load(TransactionManager xact_manager, MemHeap heap,
      boolean createConglom, RowLocationRetRowSource rowSource)
      throws StandardException {
    return 0;
  }

  /**
   * Public Methods of This class:**********************************************
   * ***************************
   */

  public int insert(DataValueDescriptor[] row) throws StandardException {
    doInsert(row);
    return 0;
  }

  public RowLocation insertAndFetchLocation(DataValueDescriptor[] row,
      RowLocation templateRowLocation) throws StandardException {

    Object regionKey = doInsert(row);

    // [sumedh]: setFrom is only required for SYS tables since it is used only
    // by indexes and actual setting is done by GfxdIndexManager; instead we
    // directly get the RegionEntry for use by DataDictionary tables here,
    // while bucketId is not relevant for those tables.
    RegionAttributes<?, ?> attrs = this.gfContainer.getRegionAttributes();
    if (attrs.getScope().isLocal() && attrs.getDataPolicy().withStorage()) {
      RegionEntry entry = ((LocalRegion.NonTXEntry) this.gfContainer
          .getRegion().getEntry(regionKey)).getRegionEntry();
      if (templateRowLocation instanceof GlobalExecRowLocation) {
        ((GlobalExecRowLocation) templateRowLocation).setFrom(entry);
      } else {
        templateRowLocation = (RowLocation) entry;
      }
      return templateRowLocation;
    } else {
      return null;
    }
  }

  public boolean lockRow(RowLocation loc, int lock_operation, boolean wait,
      int lock_duration) throws StandardException {
    throw new AssertionError("not expected to be called");
  }

  public void unlockRowAfterRead(RowLocation loc, boolean forUpdate,
      boolean row_qualified) throws StandardException {
    throw new AssertionError("not expected to be called");
  }

  public boolean lockRow(long page_num, int record_id, int lock_operation,
      boolean wait, int lock_duration) throws StandardException {
    throw new AssertionError("not expected to be called");
  }

  public void checkConsistency() throws StandardException {
  }

  public void debugConglomerate() throws StandardException {
  }

  public SpaceInfo getSpaceInfo() throws StandardException {
    // We have no space information for GemFireXD mem heap objects
    SpaceInformation spaceInfo = new SpaceInformation(0, 0, 0);
    return spaceInfo;
  }

  /**
   * DOCUMENT ME!
   * 
   * @param prop
   *          DOCUMENT ME!
   * 
   * @return DOCUMENT ME!
   * 
   * @throws StandardException
   *           DOCUMENT ME!
   */
  public Properties getInternalTablePropertySet(Properties props)
      throws StandardException {
    if (props == null) {
      props = new Properties();
    }
    getTableProperties(props);
    return props;
  }

  /**
   * DOCUMENT ME!
   * 
   * @param prop
   *          DOCUMENT ME!
   * 
   * @throws StandardException
   *           DOCUMENT ME!
   */
  public void getTableProperties(Properties props) throws StandardException {
    this.gfContainer.getContainerProperties(props);
  }

  public void close() throws StandardException {
    if (!isClosed()) {
      closeCC();
    }
  }

  public boolean closeForEndTransaction(boolean closeHeldScan)
      throws StandardException {
    if (!isClosed()) {
      if (closeHeldScan) {
        // close the controller as part of the commit/abort
        closeCC();
        return true;
      }
    }
    return false;
  }

  private void closeCC() throws StandardException {
    this.tran.closeMe(this);
    this.tran = null;
  }

  public boolean isKeyed() {
    return false;
  }

  public RowLocation newRowLocationTemplate() throws StandardException {
    return MemHeapScanController.newRowLocationTemplate(this.gfContainer,
        this.openMode);
  }
  
}
