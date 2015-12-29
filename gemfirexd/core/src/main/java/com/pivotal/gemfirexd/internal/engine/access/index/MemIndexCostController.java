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

import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.access.GemFireTransaction;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableBitSet;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.CompilerContext;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.store.access.StoreCostController;
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation;

public abstract class MemIndexCostController implements StoreCostController {

  /** DOCUMENT ME! */
  protected long num_rows;
  /** DOCUMENT ME! */
  protected long row_size;

  protected OpenMemIndex open_conglom;

  public MemIndexCostController() {
    this.open_conglom = new OpenMemIndex();
  }

  /**
   * Initialize the cost controller.
   * 
   * <p>
   * Let super.init() do it's work and then get the initial stats about the
   * table from raw store.
   * 
   * @param memstore
   * 
   * @exception StandardException
   *              Standard exception policy.
   */
  final void init(GemFireTransaction tran, MemIndex conglomerate, boolean hold,
      int openMode, int lockLevel) throws StandardException {
    this.open_conglom.init(tran, conglomerate, openMode, lockLevel, null);
    boolean queryHDFS = false;
    LanguageConnectionContext lcc = Misc.getLanguageConnectionContext();
    if (lcc != null) {
      queryHDFS = lcc.getQueryHDFS();
      CompilerContext cc = (CompilerContext) (lcc.getContextManager().getContext(CompilerContext.CONTEXT_ID));
      final GemFireContainer baseContainer = this.open_conglom.getBaseContainer();
      final LocalRegion region = baseContainer.getRegion();
      if (region != null && region instanceof PartitionedRegion) {
        if (cc.getHasQueryHDFS()) {
          ((PartitionedRegion) region).setQueryHDFS(cc.getQueryHDFS());
        }
        else {
          ((PartitionedRegion) region).setQueryHDFS(queryHDFS);
        }
      }
    }
    postInit();
  }

  protected void postInit() 
       throws StandardException {
  }

  /**
   * Return the cost of calling ConglomerateController.fetch().
   *
   * <p>Return the estimated cost of calling ConglomerateController.fetch() on
   * the current conglomerate. This gives the cost of finding a record in the
   * conglomerate given the exact RowLocation of the record in question.
   *
   * <p>The validColumns describes what kind of row is being fetched, ie. it may
   * be cheaper to fetch a partial row than a complete row.
   *
   * <p>
   *
   * @param  validColumns  A description of which columns to return from row on
   *                       the page into "templateRow." templateRow, and
   *                       validColumns work together to describe the row to be
   *                       returned by the fetch - see RowUtil for description
   *                       of how these three parameters work together to
   *                       describe a fetched "row".
   * @param  access_type  Describe the type of access the query will be
   *                      performing to the ConglomerateController.
   *                      STORECOST_CLUSTERED - The location of one fetch is
   *                      likely clustered "close" to the next fetch. For
   *                      instance if the query plan were to sort the
   *                      RowLocations of a heap and then use those RowLocations
   *                      sequentially to probe into the heap, then this flag
   *                      should be specified. If this flag is not set then
   *                      access to the table is assumed to be random - ie. the
   *                      type of access one gets if you scan an index and probe
   *                      each row in turn into the base table is "random".
   *
   * @return  The cost of the fetch.
   *
   * @exception  StandardException  Standard exception policy.
   *
   * @see  RowUtil
   */
  public double getFetchFromRowLocationCost(FormatableBitSet validColumns, int access_type)
      throws StandardException {
        throw StandardException.newException(
            SQLState.BTREE_UNIMPLEMENTED_FEATURE);
   }

  public void close() throws StandardException {
  }

  public RowLocation newRowLocationTemplate() throws StandardException {
    return null;
  }

  /**
   * DOCUMENT ME!
   *
   * @return  DOCUMENT ME!
   *
   * @throws  StandardException  DOCUMENT ME!
   */
  public long getEstimatedRowCount() throws StandardException {
    return this.num_rows;
  }

  /**
   * DOCUMENT ME!
   *
   * @param  count  DOCUMENT ME!
   *
   * @throws  StandardException  DOCUMENT ME!
   */
  public void setEstimatedRowCount(long count) throws StandardException {
    this.num_rows = count;
  }

  public double getCardinality() {
    return 1.0d;
  }
}
