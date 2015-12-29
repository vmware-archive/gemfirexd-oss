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
package com.pivotal.gemfirexd.internal.engine.procedure.coordinate;

import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.Visitable;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.VisitorAdaptor;
import com.pivotal.gemfirexd.internal.iapi.util.JBitSet;
import com.pivotal.gemfirexd.internal.impl.sql.compile.ColumnReference;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;

final class ReferencedColumnsVisitor extends VisitorAdaptor {

  private JBitSet columnMap;
  
  public ReferencedColumnsVisitor(JBitSet columnMap) {
     this.columnMap=columnMap;
  }
  public boolean skipChildren(Visitable node) throws StandardException {
     return false;
  }

  public boolean stopTraversal() {
    
    return false;
  }

  public Visitable visit(Visitable node) throws StandardException {
    if (node instanceof ColumnReference) {
      int columnNum = ((ColumnReference)node).getColumnNumber();
      if (columnNum > this.columnMap.size()) {
        // todo throw the correct exception
        SanityManager.THROWASSERT("the column number " + columnNum
            + " does not exist!");
      }
      this.columnMap.set(columnNum);
    }
    return node;
  }
}
