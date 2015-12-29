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

package com.pivotal.gemfirexd.internal.engine.expression;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.Visitable;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.VisitorAdaptor;
import com.pivotal.gemfirexd.internal.impl.sql.compile.BaseColumnNode;
import com.pivotal.gemfirexd.internal.impl.sql.compile.ColumnReference;

/**
 * Get the index of each column reference in the expression so as to use that
 * when DVD[] is passed for evaluation of expression.
 * 
 * @author swale
 */
public final class GfxdExprNodeVisitor extends VisitorAdaptor {

  private final Map<String, Integer> columnToIndexMap;
  private final ArrayList<String> usedCols;
  private int idx;

  public GfxdExprNodeVisitor(Map<String, Integer> columnToIndexMap) {
    this.columnToIndexMap = columnToIndexMap;
    this.usedCols = new ArrayList<String>();
    this.idx = 0;
  }

  @Override
  public boolean skipChildren(Visitable node) throws StandardException {
    return false;
  }

  @Override
  public boolean stopTraversal() {
    return false;
  }

  @Override
  public Visitable visit(Visitable node) throws StandardException {
    if (node instanceof ColumnReference) {
      String columnName = ((ColumnReference)node).getColumnName();
      this.usedCols.add(columnName);
      columnToIndexMap.put(columnName, Integer.valueOf(this.idx));
      this.idx++;
    }
    else if (node instanceof BaseColumnNode) {
      String columnName = ((BaseColumnNode)node).getColumnName();
      this.usedCols.add(columnName);
      this.columnToIndexMap.put(columnName, Integer.valueOf(this.idx));
      this.idx++;
    }
    return node;
  }

  public List<String> getUsedColumns() {
    return this.usedCols;
  }
}
