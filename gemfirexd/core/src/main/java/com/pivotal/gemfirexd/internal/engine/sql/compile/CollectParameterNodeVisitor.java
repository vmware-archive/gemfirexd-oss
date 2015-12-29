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

import com.pivotal.gemfirexd.internal.engine.distributed.metadata.QueryInfoContext;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.Visitable;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.VisitorAdaptor;
import com.pivotal.gemfirexd.internal.impl.sql.compile.ColumnReference;
import com.pivotal.gemfirexd.internal.impl.sql.compile.ParameterNode;
import com.pivotal.gemfirexd.internal.impl.sql.compile.ValueNode;

/**
 * This is to invoke ComputeRoutingObjects on each ParameterNode
 * under any expression tree depth of a RC.
 * 
 * @author soubhikc
 *
 */
public class CollectParameterNodeVisitor extends VisitorAdaptor {

  private QueryInfoContext _qic;
  
  public CollectParameterNodeVisitor(QueryInfoContext qic) {
    this._qic = qic;
  }
  
  public boolean skipChildren(Visitable node) throws StandardException {
    return false;
  }

  public boolean stopTraversal() {
    return false;
  }
  
  public Visitable visit(Visitable node) throws StandardException {
    
    if(node instanceof ParameterNode) {
      ((ParameterNode)node).computeQueryInfo(_qic);
      return node;
    }
    
    if(node instanceof ParameterizedConstantNode) {
      ((ParameterizedConstantNode)node).computeQueryInfo(_qic);
      return node;
    }
    
    if(node instanceof ColumnReference) {
      ValueNode expression = ((ColumnReference)node).getSource();
      
      /* expression can be null while remapToNew in ColumnReference
       * genExpressionOperands.
       */
      if(expression != null) {
        expression.accept(this);
      }
    }

    return node;
  }
  
}
