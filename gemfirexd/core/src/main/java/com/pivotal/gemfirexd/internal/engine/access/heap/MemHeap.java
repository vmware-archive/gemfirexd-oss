
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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import java.util.Properties;

import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.access.GemFireTransaction;
import com.pivotal.gemfirexd.internal.engine.access.MemConglomerate;
import com.pivotal.gemfirexd.internal.engine.access.MemConglomerateController;
import com.pivotal.gemfirexd.internal.engine.access.MemScanController;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.cache.ClassSize;
import com.pivotal.gemfirexd.internal.iapi.services.io.ArrayInputStream;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatIdUtil;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableBitSet;
import com.pivotal.gemfirexd.internal.iapi.services.io.Storable;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.store.access.ColumnOrdering;
import com.pivotal.gemfirexd.internal.iapi.store.access.DynamicCompiledOpenConglomInfo;
import com.pivotal.gemfirexd.internal.iapi.store.access.Qualifier;
import com.pivotal.gemfirexd.internal.iapi.store.access.RowLocationRetRowSource;
import com.pivotal.gemfirexd.internal.iapi.store.access.RowUtil;
import com.pivotal.gemfirexd.internal.iapi.store.access.StaticCompiledOpenConglomInfo;
import com.pivotal.gemfirexd.internal.iapi.store.access.StoreCostController;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;
import com.pivotal.gemfirexd.internal.iapi.store.access.conglomerate.Conglomerate;
import com.pivotal.gemfirexd.internal.iapi.store.access.conglomerate.TransactionManager;
import com.pivotal.gemfirexd.internal.iapi.store.raw.ContainerHandle;
import com.pivotal.gemfirexd.internal.iapi.store.raw.ContainerKey;
import com.pivotal.gemfirexd.internal.iapi.store.raw.LockingPolicy;
import com.pivotal.gemfirexd.internal.iapi.store.raw.Transaction;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.impl.store.access.conglomerate.ConglomerateUtil;
import com.pivotal.gemfirexd.internal.impl.store.access.conglomerate.GenericConglomerate;
import com.pivotal.gemfirexd.internal.shared.common.StoredFormatIds;


/**
 * A heap object corresponds to an instance of a heap conglomerate. It caches
 * information which makes it fast to open heap controllers from it.
 */
public final class MemHeap extends GenericConglomerate implements
    MemConglomerate {

  /** DOCUMENT ME! */
  private static final int BASE_MEMORY_USAGE = ClassSize
      .estimateBaseFromCatalog(MemHeap.class);

  /** DOCUMENT ME! */
  private static final int CONTAINER_KEY_MEMORY_USAGE = ClassSize
    .estimateBaseFromCatalog(ContainerKey.class);

  /** DOCUMENT ME! */
  protected ContainerKey id;

  /** the container */
  protected GemFireContainer container;

  /** The format id's of each of the columns in the heap table. */
  int[] format_ids;

  /** DOCUMENT ME! */
  //private int[] collation_ids;

  /*
   ** Methods of Heap.
   */

  /* Constructors for This class: */

  /**
   * Zero arg constructor for Monitor to create empty object.
   */
  public MemHeap() {
  }

  /* Private/Protected methods of This class: */

  /**
   * DOCUMENT ME!
   *
   * @return  DOCUMENT ME!
   */
  public int estimateMemoryUsage() {
    int sz = BASE_MEMORY_USAGE;

    if (null != id) {
      sz += CONTAINER_KEY_MEMORY_USAGE;
    }

    if (null != format_ids) {
      sz += format_ids.length * ClassSize.getIntSize();
    }

    return sz;
  } // end of estimateMemoryUsage

  /**
   * Create a heap conglomerate.
   * <p>
   * Create a heap conglomerate. This method is called from the heap factory to
   * create a new instance of a heap.
   * <p>
   * 
   * @exception StandardException
   *              Standard exception policy.
   */
  public void create(GemFireTransaction tran, int segmentId, long containerId,
      DataValueDescriptor[] template, ColumnOrdering[] columnOrder,
      int[] collationIds, Properties properties, int tmpFlag)
      throws StandardException {

    // Make sure the container id was actually created.
    if (((tmpFlag & TransactionController.IS_TEMPORARY) !=
        TransactionController.IS_TEMPORARY) && containerId < 0) {
      throw StandardException.newException(SQLState.HEAP_CANT_CREATE_CONTAINER);
    }

    // Heap requires a template representing every column in the table.
    if ((template == null) || (template.length == 0)) {
      throw StandardException
          .newException(SQLState.HEAP_COULD_NOT_CREATE_CONGLOMERATE);
    }

    // get format id's from each column in template and store it in the
    // conglomerate state.
    this.format_ids = ConglomerateUtil.createFormatIds(template);

    // get collation ids from input collation ids, store it in the
    // conglom state.
    /*
    collation_ids = ConglomerateUtil.createCollationIds(format_ids.length,
                                                        collationIds);

     */

    // set the template row for GemFireContainer
    properties.put(GfxdConstants.PROP_TEMPLATE_ROW, template);

    // Keep track of what segment the container's in.
    this.id = ContainerKey.valueOf(segmentId, containerId);
  }

  @Override
  public boolean requiresContainer() {
    return true;
  }

  /*
   ** Methods of Conglomerate
   */

  /**
   * Add a column to the heap conglomerate.
   *
   * <p>This routine update's the in-memory object version of the Heap
   * Conglomerate to have one more column of the type described by the input
   * template column.
   *
   * @param  column_id  The column number to add this column at.
   * @param  template_column  An instance of the column to be added to table.
   *
   * @exception  StandardException  Standard exception policy.
   */
  public void addColumn(TransactionManager xactManager,
                        int column_id,
                        Storable template_column,
                        int collation_id) throws StandardException {

    // currently a no-op since it is expected to be invoked only when there
    // is no data
    //throw (StandardException.newException(SQLState.HEAP_UNIMPLEMENTED_FEATURE));
  }

  /**
   * Drop this heap.
   *
   * @see  Conglomerate#drop
   *
   * @exception  StandardException  Standard exception policy.
   */
  public void drop(TransactionManager xactManager) throws StandardException {
    xactManager.getRawStoreXact().dropContainer(this.id);
  }

  /**
   * Retrieve the maximum value row in an ordered conglomerate.
   *
   * <p>Returns true and fetches the rightmost row of an ordered conglomerate
   * into "fetchRow" if there is at least one row in the conglomerate. If there
   * are no rows in the conglomerate it returns false.
   *
   * <p>Non-ordered conglomerates will not implement this interface, calls will
   * generate a StandardException.
   *
   * <p>RESOLVE - this interface is temporary, long term equivalent (and more)
   * functionality will be provided by the openBackwardScan() interface.
   *
   * @param  conglomId  The identifier of the conglomerate to open the scan for.
   * @param  openMode  Specifiy flags to control opening of table.
   *                    OPENMODE_FORUPDATE - if set open the table for update
   *                    otherwise open table shared.
   * @param  lockLevel  One of (MODE_TABLE, MODE_RECORD, or MODE_NONE).
   * @param  isolation_level  The isolation level to lock the conglomerate at.
   *                          One of (ISOLATION_READ_COMMITTED or
   *                          ISOLATION_SERIALIZABLE).
   * @param  scanColumnList  A description of which columns to return from every
   *                         fetch in the scan. template, and scanColumnList
   *                         work together to describe the row to be returned by
   *                         the scan - see RowUtil for description of how these
   *                         three parameters work together to describe a "row".
   * @param  fetchRow  The row to retrieve the maximum value into.
   *
   * @return  boolean indicating if a row was found and retrieved or not.
   *
   * @exception  StandardException  Standard exception policy.
   */
  public boolean fetchMaxOnBTree(TransactionManager xactManager,
                                 Transaction rawtran,
                                 long conglomId,
                                 int openMode,
                                 int lockLevel,
                                 LockingPolicy locking,
                                 int isolation_level,
                                 FormatableBitSet scanColumnList,
                                 DataValueDescriptor[] fetchRow)
  throws StandardException {

    // no support for max on a heap table.
    throw (StandardException.newException(SQLState.HEAP_UNIMPLEMENTED_FEATURE));
  }

  /**
   * Get the id of the container of the conglomerate.
   *
   * <p>Will have to change when a conglomerate could have more than one
   * container. The ContainerKey is a combination of the container id and
   * segment id.
   *
   * @return  The ContainerKey.
   */
  public ContainerKey getId() {
    return (id);
  }

  public long getContainerid() {
    return (id.getContainerId());
  }

  /**
   * @see {@link MemConglomerate#getGemFireContainer()}
   */
  public GemFireContainer getGemFireContainer() {
    return this.container;
  }

  /**
   * @see {@link MemConglomerate#setGemFireContainer(GemFireContainer)}
   */
  public void setGemFireContainer(GemFireContainer container) {
    this.container = container;
  }

  public int getType() {
    return HEAP;
  }

  /**
   * Convenience method to get the GemFire Region
   */
  public LocalRegion getRegion() {
    return getGemFireContainer().getRegion();
  }

  /**
   * Return dynamic information about the conglomerate to be dynamically reused
   * in repeated execution of a statement.
   *
   * <p>The dynamic info is a set of variables to be used in a given
   * ScanController or ConglomerateController. It can only be used in one
   * controller at a time. It is up to the caller to insure the correct thread
   * access to this info. The type of info in this is a scratch template for
   * btree traversal, other scratch variables for qualifier evaluation, ...
   *
   * <p>
   *
   * @return  The dynamic information.
   *
   * @param  conglomId  The identifier of the conglomerate to open.
   *
   * @exception  StandardException  Standard exception policy.
   */
  public DynamicCompiledOpenConglomInfo getDynamicCompiledConglomInfo()
      throws StandardException {
    // not used by GemFireXD
    throw new UnsupportedOperationException("not expected to be called");
  }

  /**
   * Return static information about the conglomerate to be included in a a
   * compiled plan.
   *
   * <p>The static info would be valid until any ddl was executed on the
   * conglomid, and would be up to the caller to throw away when that happened.
   * This ties in with what language already does for other invalidation of
   * static info. The type of info in this would be containerid and array of
   * format id's from which templates can be created. The info in this object is
   * read only and can be shared among as many threads as necessary.
   *
   * <p>
   *
   * @return  The static compiled information.
   *
   * @param  conglomId  The identifier of the conglomerate to open.
   *
   * @exception  StandardException  Standard exception policy.
   */
  public StaticCompiledOpenConglomInfo getStaticCompiledConglomInfo(
    TransactionController tc,
    long conglomId) throws StandardException {
    return (this);
  }

  /**
   * Is this conglomerate temporary?
   *
   * <p>
   *
   * @return  whether conglomerate is temporary or not.
   */
  public boolean isTemporary() {
    return (id.getSegmentId() == ContainerHandle.TEMPORARY_SEGMENT);
  }

  /**
   * Bulk load into the conglomerate.
   *
   * <p>
   *
   * @see  Conglomerate#load
   *
   * @exception  StandardException  Standard exception policy.
   */
  public long load(TransactionManager xactManager, boolean createConglom,
      RowLocationRetRowSource rowSource) throws StandardException {
    throw (StandardException.newException(SQLState.HEAP_UNIMPLEMENTED_FEATURE));
  }

  /**
   * Open a heap controller.
   *
   * <p>
   *
   * @see  Conglomerate#open
   *
   * @exception  StandardException  Standard exception policy.
   */
  public MemConglomerateController open(TransactionManager xactManager,
      Transaction rawtran, boolean hold, int openMode, int lockLevel,
      LockingPolicy locking, StaticCompiledOpenConglomInfo staticInfo,
      DynamicCompiledOpenConglomInfo dynamicInfo) throws StandardException {
    return open((GemFireTransaction)xactManager, openMode, lockLevel, locking);
  }

  /**
   * Open a heap controller.
   *
   * @exception  StandardException  Standard exception policy.
   */
  public MemConglomerateController open(GemFireTransaction tran, int openMode,
      int lockLevel, LockingPolicy locking) throws StandardException {
    final MemHeapController cc = new MemHeapController();
    cc.init(tran, this, openMode, lockLevel, locking);
    return cc;
  }

  /**
   * Open a heap scan controller.
   *
   * <p>
   *
   * @see  Conglomerate#openScan
   *
   * @exception  StandardException  Standard exception policy.
   */
  public MemScanController openScan(TransactionManager xactManager,
      Transaction rawtran, boolean hold, int openMode, int lockLevel,
      LockingPolicy locking, int isolation_level,
      FormatableBitSet scanColumnList, DataValueDescriptor[] startKeyValue,
      int startSearchOperator, Qualifier[][] qualifier,
      DataValueDescriptor[] stopKeyValue, int stopSearchOperator,
      StaticCompiledOpenConglomInfo staticInfo,
      DynamicCompiledOpenConglomInfo dynamicInfo) throws StandardException {

    return openScan(xactManager, rawtran, hold, openMode, lockLevel, locking,
        isolation_level, scanColumnList, startKeyValue, startSearchOperator,
        qualifier, stopKeyValue, stopSearchOperator, staticInfo, dynamicInfo,
        null /*local data set */);
  }

  /**
   * Open a heap scan controller with local data set provided by gemfire
   * function service.
   * <p>
   *
   * @see  Conglomerate#openScan
   *
   * @exception  StandardException  Standard exception policy.
   */
  public MemScanController openScan(TransactionManager xactManager,
      Transaction rawtran, boolean hold, int openMode, int lockLevel,
      LockingPolicy locking, int isolation_level,
      FormatableBitSet scanColumnList, DataValueDescriptor[] startKeyValue,
      int startSearchOperator, Qualifier[][] qualifier,
      DataValueDescriptor[] stopKeyValue, int stopSearchOperator,
      StaticCompiledOpenConglomInfo staticInfo,
      DynamicCompiledOpenConglomInfo dynamicInfo, Activation act)
      throws StandardException {

    // Heap scans do not suppport start and stop scan positions (these
    // only make sense for ordered storage structures).
    if (!RowUtil.isRowEmpty(startKeyValue) || !RowUtil.isRowEmpty(stopKeyValue)) {
      throw StandardException.newException(SQLState.HEAP_UNIMPLEMENTED_FEATURE);
    }

    final GemFireTransaction tran = (GemFireTransaction)xactManager;
    MemHeapScanController sc = new MemHeapScanController();
    sc.init(tran, this, openMode, lockLevel, null, scanColumnList,
        startKeyValue, startSearchOperator, qualifier, stopKeyValue,
        stopSearchOperator, act);
    return sc;
  }

  public boolean openContainer(GemFireTransaction tran, int openMode,
      int lockLevel, LockingPolicy locking) throws StandardException {
    if (isTemporary()) {
      openMode |= ContainerHandle.MODE_TEMP_IS_KEPT;
    }
    return this.container.open(tran, openMode);
  }

  public void purgeConglomerate(TransactionManager xactManager,
                                Transaction rawtran) throws StandardException {
    throw (StandardException.newException(SQLState.HEAP_UNIMPLEMENTED_FEATURE));
  }

  public void compressConglomerate(TransactionManager xactManager,
                                   Transaction rawtran)
  throws StandardException {
  }

  /**
   * Open a heap compress scan.
   * 
   * <p>
   * 
   * @see Conglomerate#defragmentConglomerate
   * 
   * @exception StandardException
   *              Standard exception policy.
   */
  public MemScanController defragmentConglomerate(
      TransactionManager xactManager, Transaction rawtran, boolean hold,
      int openMode, int lockLevel, LockingPolicy locking, int isolation_level)
      throws StandardException {
    return null;
  }

  /**
   * Return an open StoreCostController for the conglomerate.
   *
   * <p>Return an open StoreCostController which can be used to ask about the
   * estimated row counts and costs of ScanController and ConglomerateController
   * operations, on the given conglomerate.
   *
   * <p>
   *
   * @param  xactManager  The TransactionController under which this operation
   *                       takes place.
   * @param  rawtran  raw transaction context in which scan is managed.
   *
   * @return  The open StoreCostController.
   *
   * @exception  StandardException  Standard exception policy.
   *
   * @see  StoreCostController
   */
  public StoreCostController openStoreCost(TransactionManager xactManager,
      Transaction rawtran) throws StandardException {
    final GemFireTransaction tran = (GemFireTransaction)xactManager;
    MemHeapCostController cc = new MemHeapCostController();
    cc.init(tran, this, ContainerHandle.MODE_READONLY, 0, null);
    return cc;
  }

  /**
   * Print this heap.
   */
  @Override
  public String toString() {
    return (this.id == null) ? "null"
        : (this.id.toString() + ' ' + this.container);
  }

  /**
   * Public Methods of StaticCompiledOpenConglomInfo Interface:
   * ********************************************************
   */

  /**
   * return the "Conglomerate".
   *
   * <p>For heap just return "this", which both implements Conglomerate and
   * StaticCompiledOpenConglomInfo.
   *
   * <p>
   *
   * @return  this
   */
  public DataValueDescriptor getConglom() {
    return (this);
  }

  /**
   * Methods of Storable (via Conglomerate) Storable interface, implies
   * Externalizable, TypedFormat************************************************
   * *************************
   */

  /**
   * Return my format identifier.
   *
   * @see  com.pivotal.gemfirexd.internal.iapi.services.io.TypedFormat#getTypeFormatId
   */
  public int getTypeFormatId() {
    // throw new UnsupportedOperationException("not storable");

         // return StoredFormatIds.ACCESS_MEMHEAP;
    return StoredFormatIds.ACCESS_HEAP_V2_ID;
    
  }

  /**
   * Return whether the value is null or not.
   *
   * @see  com.pivotal.gemfirexd.internal.iapi.services.io.Storable#isNull
   */
  public boolean isNull() {
    return id == null;
  }

  /**
   * Restore the in-memory representation to the null value.
   *
   * @see  com.pivotal.gemfirexd.internal.iapi.services.io.Storable#restoreToNull
   */
  public void restoreToNull() {
    id = null;
  }

  /**
   * Store the stored representation of the column value in the stream.
   */
  public void writeExternal(ObjectOutput out) throws IOException {

    // write the format id of this conglomerate
    FormatIdUtil.writeFormatIdInteger(out, this.getTypeFormatId());

    out.writeInt((int) id.getSegmentId());
    out.writeLong(id.getContainerId());

    // write number of columns in heap.
    out.writeInt(format_ids.length);

    // write out array of format id's
    ConglomerateUtil.writeFormatIdArray(format_ids, out);
  }

  /**
   * Restore the in-memory representation from the stream.
   *
   * @see  java.io.Externalizable#readExternal
   */
  public void readExternal(ObjectInput in) throws IOException {

    // read the format id of this conglomerate.
    FormatIdUtil.readFormatIdInteger(in);

    int segmentid = in.readInt();
    long containerid = in.readLong();

    this.id = ContainerKey.valueOf(segmentid, containerid);

    // read the number of columns in the heap.
    int num_columns = in.readInt();

    // read the array of format ids.
    format_ids = ConglomerateUtil.readFormatIdArray(num_columns, in);
  }

  /**
   * DOCUMENT ME!
   *
   * @param  in  DOCUMENT ME!
   *
   * @throws  IOException  DOCUMENT ME!
   */
  public void readExternalFromArray(ArrayInputStream in) throws IOException {

    // read the format id of this conglomerate.
    FormatIdUtil.readFormatIdInteger(in);

    int segmentid = in.readInt();
    long containerid = in.readLong();

    this.id = ContainerKey.valueOf(segmentid, containerid);

    // read the number of columns in the heap.
    int num_columns = in.readInt();

    // read the array of format ids.
    format_ids = ConglomerateUtil.readFormatIdArray(num_columns, in);
  }

}