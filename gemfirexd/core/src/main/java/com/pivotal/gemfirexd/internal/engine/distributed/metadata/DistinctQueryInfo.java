
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
import com.pivotal.gemfirexd.internal.impl.sql.compile.ResultColumn;
import com.pivotal.gemfirexd.internal.impl.sql.compile.ResultColumnList;
import com.pivotal.gemfirexd.internal.impl.sql.execute.IndexColumnOrder;

public class DistinctQueryInfo extends AbstractQueryInfo implements SecondaryClauseQueryInfo{
  private ColumnOrdering[] ordering;
  
  private final ExecRow expectedRemoteExecRow;
  private final RowFormatter rowFormatter;

  private int[] projectMapping;

  /**
   * 
   * @param rclist
   * @throws StandardException
   */
  public DistinctQueryInfo(QueryInfoContext qic, 
                           ResultColumnList rclist 
                          ,ResultColumnList parentRCL
                          ,boolean hasColumnOrdering) throws StandardException {
    
 
       
    if(hasColumnOrdering) {
      deriveColumnOrdering(rclist, parentRCL);
    }
    
    if(parentRCL != null && rclist.size() != parentRCL.size()) {
      
      projectMapping = new int[parentRCL.size()];
      
      for(int i = 0; i < parentRCL.size(); i++) {
         projectMapping[i] = i+1;
      }
      
      rclist.mergeResultColumnList(parentRCL, false);
    }
    
    expectedRemoteExecRow = parentRCL != null ? 
                              parentRCL.buildEmptyRow() : 
                                rclist.buildEmptyRow();
    rowFormatter = getRowFormatterFromRCL(parentRCL != null ? parentRCL
        : rclist, null);
  }

  /**
   * Distinct will sort and evaluate on all columns of its RCL (rclist).
   * see ExpressionClassBuilder#getColumnOrdering(ResultColumnList rclist)
   * 
   * @param rclist
   */
  private void deriveColumnOrdering(ResultColumnList rclist, ResultColumnList parentRCL) throws StandardException {
    int numCols = (rclist == null) ? 0 : rclist.size();
    
    IndexColumnOrder[] orderByColOrdering = parentRCL != null ? parentRCL.getColumnOrdering() : null;
    ordering = new IndexColumnOrder[numCols];
    
    /* The child RCL i.e. rclist will dictate distinct
     * evaluation whereas parentRCL's RCs will decide ordering of rows 
     * when ***orderbyAndDistinct is merged***.
     * 
     * NOTE: parentRCL.getColumnOrdering() is derived from OrderByList which is limited to
     * parentRCL and orderBy columns are put first in parentRCL. @see OrderByList#reorderRCL
     * 
     * e.g. select distinct address, country from TESTTABLE order by country
     * e.g. select country, address from TESTTABLE group by country, address order by address nulls first, country
     * 
     * So, we take the columnOrdering of the orderBy list first and append the missing
     * columns of rclist to it. also, parentRCL and rclist is not necessarily in-order but 
     * we do distinct evaluation in rclist order for rest of the columns.
     */
    int index = 0;
    if( orderByColOrdering != null) {
      for (int i = 0; i < orderByColOrdering.length; i++)
      {
        ordering[i] = orderByColOrdering[i];
      }
      
      index = orderByColOrdering.length;
    }

    for (; index < numCols; index++)
    {
        ResultColumn rclistRC = rclist.getResultColumn(index+1);
        ResultColumn rc = parentRCL != null ? 
                            parentRCL.findParentResultColumn(rclistRC) :
                              rclistRC;
                       
        ordering[index] = new IndexColumnOrder( rc != null ?
                                                  rc.getVirtualColumnId()-1 :
                                                   rclistRC.getVirtualColumnId()-1
                                              );
    }
    
    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TraceRSIter) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_RSITER,
            "DistinctQueryInfo column ordering size " + ordering.length);
      }
    }    
  }
  
  @Override
  public ColumnOrdering[] getColumnOrdering() {
    return ordering;
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
