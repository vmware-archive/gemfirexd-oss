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
/**
 * 
 */
package com.pivotal.gemfirexd.internal.engine.distributed.metadata;

import java.util.Set;

import com.gemstone.gemfire.LogWriter;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.shared.common.ResolverUtils;

/**
 * @author Asif
 *
 */
public interface QueryInfoConstants {
  public int AND_JUNCTION =1;
  public int OR_JUNCTION =2;
  public int EXPRESSION_NODE_COL_POS = -2;
  
  public static final QueryInfo DUMMY = new AbstractQueryInfo() {
    @Override
    public final boolean isSelect() {
      return false;
    }
    
    @Override
    public final boolean isUpdate() {
      return false;
    }
    
    @Override
    public final boolean isInsert() {
      return false;
    }

    @Override
    public final boolean isDelete() {
      return false;
    }
    
    public final boolean createGFEActivation() throws StandardException {
      return false;
    }
  };
  public static final QueryInfo NON_PRUNABLE = new AbstractQueryInfo() {
    @Override
    public void computeNodes(Set<Object> routingKeys, Activation activation, boolean forSingleHopPreparePhase)
        throws StandardException
    {
      assert routingKeys.size() == 1;
      assert routingKeys.contains(ResolverUtils.TOK_ALL_NODES);

      final LogWriter logger = Misc.getCacheLogWriter();
      if (logger.fineEnabled()) {
        logger.fine("NON_PRUNABLE::computeNodes: "
            + "After prunning nodes size is " + routingKeys.size());
        logger.fine("NON_PRUNABLE::computeNodes: "
            + "After prunning nodes are " + routingKeys);
      }
    }
  }; 
  
  public static final TableQueryInfo UNINITIALIZED = new TableQueryInfo();
  
  public static final GroupByQueryInfo DUMMYGROUPBYQI = new GroupByQueryInfo(true);
}
