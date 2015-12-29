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

import com.gemstone.gnu.trove.THashSet;
import com.gemstone.gnu.trove.TObjectHashingStrategy;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.Visitable;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.VisitorAdaptor;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;

/**
 * 
 * @author soubhikc
 * 
 */
public class CollectAndEliminateColumnsVisitor extends VisitorAdaptor {

  private boolean collectColumnReferences = true;

  private final TObjectHashingStrategy comparisonStrategy = new TObjectHashingStrategy() {

    private static final long serialVersionUID = 1L;

    @Override
    public int computeHashCode(Object o) {
      assert o instanceof String || o instanceof ResultColumn : o;

      if(o instanceof ResultColumn) {
        return ((ResultColumn)o).exposedName.hashCode();
      }
      
      return ((String)o).hashCode();
    }

    @Override
    public boolean equals(Object o1, Object o2) {

      assert o1 instanceof String && o2 instanceof String
          || o1 instanceof ResultColumn && o2 instanceof ResultColumn;

      if(o1 instanceof ResultColumn) {
        try {
          return ((ResultColumn)o1).isEquivalent((ResultColumn)o2);
        } catch (StandardException e) {
          // this confuses us whether to include the ResultColumn in question or not.
          // so, need to definitely resolve this error.
          SanityManager.DEBUG_PRINT("error", "ResultColumn comparison error : ", e);
          return false;
        }
      }
      return ((String)o1).equals(o2);
    }

  };

  private final THashSet columnReferences = new THashSet(comparisonStrategy);

  private boolean columnFound = false;

  @Override
  public boolean skipChildren(Visitable node) throws StandardException {
    return columnFound || (node instanceof ResultColumnList);
  }

  @Override
  public boolean stopTraversal() {
    return columnFound;
  }

  @Override
  public Visitable visit(Visitable node) throws StandardException {

    if (collectColumnReferences) {
      if (node instanceof ColumnReference) {
        columnReferences.putIfAbsent(((ColumnReference)node).getColumnName());
      }
      else if (node instanceof ResultColumn) {
        ResultColumn rc = (ResultColumn)node;
        columnReferences.putIfAbsent(rc.name);
        columnReferences.putIfAbsent(rc.exposedName);
        String srcCol = rc.getSourceColumnName();
        if (srcCol != null) {
          columnReferences.putIfAbsent(srcCol);
        }
        
        if (rc.isNameGenerated || rc.isGenerated || rc.isExpanded
            || rc.isGeneratedForUnmatchedColumnInInsert()
            || rc.getExpression() == null) {
          columnReferences.putIfAbsent(rc);
        }
      }
      else if (node instanceof BaseColumnNode) {
        columnReferences.putIfAbsent(((BaseColumnNode)node).getColumnName());
      }
    }
    else {
      if (node instanceof ColumnReference) {
        columnFound = columnReferences.contains(((ColumnReference)node)
            .getColumnName());
      }
      else if (node instanceof ResultColumn) {

        ResultColumn rc = (ResultColumn)node;
        String colName = rc.name;
        if (colName != null) {
          columnFound = columnReferences.contains(colName);
        }
        if (!columnFound) {
          colName = rc.exposedName;
          if (colName != null) {
            columnFound = columnReferences.contains(colName);
          }
          if (!columnFound) {
            colName = rc.getSourceColumnName();
            if (colName != null) {
              columnFound = columnReferences.contains(colName);
            }
          }
        }
      }
      else if (node instanceof BaseColumnNode) {
        columnFound = columnReferences.contains(((BaseColumnNode)node)
            .getColumnName());
      }
    }

    return node;
  }

  public void stopCollectingColumnReferences() {
    collectColumnReferences = false;
  }

  public boolean isColumnUsed(ResultColumn rc) throws StandardException {

    rc.accept(this);

    if (columnFound) {
      columnFound = false;
      return true;
    }

    return false;
  }

}
