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

package com.pivotal.gemfirexd.internal.impl.sql.compile;

import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.Visitable;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.VisitorAdaptor;
import com.pivotal.gemfirexd.internal.impl.sql.compile.SelectNode;

/**
 * To temporarily throw exception if we have a View, with Group By on a
 * Partitioned table. If so, do not allow such expression.
 * 
 * @author vivekb
 */
public final class VerifyViewExpressionVisitor extends VisitorAdaptor {
  private boolean foundGroupByExpression = false;
  private boolean foundPartitionedTable = false;

  /**
   * @return throwUnsupportedException
   */
  public boolean isThrowUnsupportedException() {
    return foundGroupByExpression && foundPartitionedTable;
  }

  @Override
  public boolean skipChildren(Visitable node) throws StandardException {
    return isThrowUnsupportedException();
  }

  @Override
  public boolean stopTraversal() {
    return isThrowUnsupportedException();
  }

  @Override
  public Visitable visit(Visitable node) throws StandardException {
    if (node instanceof SelectNode) {
      foundGroupByExpression = foundGroupByExpression
          || ((SelectNode)node).groupByList != null;
    }
    else if (node instanceof FromBaseTable) {
      foundPartitionedTable = foundPartitionedTable
          || ((FromBaseTable)node).isPartitionedRegion();
    }
    return node;
  }
}
