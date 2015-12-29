
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

package com.pivotal.gemfirexd.internal.engine.distributed.metadata;

import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.store.RowFormatter;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.store.access.ColumnOrdering;
import com.pivotal.gemfirexd.internal.impl.sql.compile.OrderByList;
import com.pivotal.gemfirexd.internal.impl.sql.compile.OrderByNode;
import com.pivotal.gemfirexd.internal.impl.sql.compile.ResultColumnList;

/**
 * This class represents a orderby clause present in the query.
 * It wraps details about the column index relative to projection
 * of the query, whether ordering is ascending/descending etc.
 * 
 * @author Soubhik
 * @since GemFireXD
 * @see OrderByNode#computeQueryInfo()
 */
public class OrderByQueryInfo extends AbstractQueryInfo implements SecondaryClauseQueryInfo{
  
  private final ColumnOrdering[] columnOrdering;
  
  /**
   * This row format will be one-to-one stripped down version of resultColumns
   * with which the Data Store node will return rows to query nodes.
   * 
   * e.g. "Select count(1) from Table Group By Col1" 
   * Data Store node will ship the group by columns in the resultSets. 
   */
  private final ExecRow expectedRemoteExecRow;
  private final RowFormatter rowFormatter;

  private int[] projectMapping ;

  public OrderByQueryInfo(QueryInfoContext qic, OrderByList ordering,
      ResultColumnList rclist, ResultColumnList parentRCL)
      throws StandardException {

    columnOrdering = ordering.getColumnOrdering();
    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TraceRSIter) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_RSITER,
            "OrderByQueryInfo column ordering size " + columnOrdering.length);
      }
    }

    if(parentRCL != null && rclist.size() != parentRCL.size()) {
      
      projectMapping = new int[parentRCL.size()];
      
      for(int i = 0; i < parentRCL.size(); i++) {
         projectMapping[i] = i+1;
      }
    }
    
    expectedRemoteExecRow = rclist.buildEmptyRow();
    rowFormatter = getRowFormatterFromRCL(rclist, null);
  }

  @Override
  public ColumnOrdering[] getColumnOrdering() {
    return columnOrdering;
  }
  
  @Override
  public ExecRow getInComingProjectionExecRow() {
    return expectedRemoteExecRow;
  }

  @Override
  public final RowFormatter getRowFormatter() {
    return this.rowFormatter;
  }

  @Override
  public int[] getProjectMapping() {
    return projectMapping;
  }
}
