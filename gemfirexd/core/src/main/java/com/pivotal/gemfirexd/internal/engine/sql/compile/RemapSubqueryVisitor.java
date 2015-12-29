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
package com.pivotal.gemfirexd.internal.engine.sql.compile;

import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.Visitable;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.Visitor;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.VisitorAdaptor;
import com.pivotal.gemfirexd.internal.iapi.util.JBitSet;
import com.pivotal.gemfirexd.internal.impl.sql.compile.ColumnReference;
import com.pivotal.gemfirexd.internal.impl.sql.compile.ResultColumn;

public class RemapSubqueryVisitor extends VisitorAdaptor {
  private final boolean remap;

  private final JBitSet outerTables;
  
  private final boolean finalRemap ;
  public RemapSubqueryVisitor(boolean remap, JBitSet outerTables, boolean finalRemap) {
    this.remap = remap;
    this.outerTables = outerTables;
    this.finalRemap = finalRemap;
   

  }

  public Visitable visit(Visitable node) throws StandardException {

    if (node instanceof ColumnReference) {
      ColumnReference cr = (ColumnReference)node;
      if (!cr.getFinalRemapDoneForSubqueryPred()) {

        ResultColumn rc = cr.getSource();
        int crTableNum;
        if (rc == null || (crTableNum = rc.getTableNumber()) == -1) {
          crTableNum = cr.getTableNumber();
        }

        if (crTableNum < outerTables.size() && crTableNum >= 0
            && outerTables.get(crTableNum)) {
          // if (this.tableNumOptimized == crTableNum) {
          if (remap) {
            cr.remapColumnReferences();
            if (this.finalRemap) {
              cr.setFinalRemapDoneForSubqueryPred();
            }
          }
          else {
            cr.unRemapColumnReferences();
          }
          // }
        }
      }

    }

    return node;
  }

  public boolean skipChildren(Visitable node) {
    return false;

  }

  public boolean stopTraversal() {
    return false;
  }

}
