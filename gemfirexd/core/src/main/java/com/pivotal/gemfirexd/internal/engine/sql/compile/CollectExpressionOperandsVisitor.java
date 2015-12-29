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

import java.util.ArrayList;
import java.util.Vector;

import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.C_NodeTypes;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.NodeFactory;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.Visitable;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.VisitorAdaptor;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.impl.sql.compile.AggregateNode;
import com.pivotal.gemfirexd.internal.impl.sql.compile.ColumnReference;
import com.pivotal.gemfirexd.internal.impl.sql.compile.ResultColumn;
import com.pivotal.gemfirexd.internal.impl.sql.compile.ResultColumnList;
import com.pivotal.gemfirexd.internal.impl.sql.compile.ValueNode;
import com.pivotal.gemfirexd.internal.impl.sql.compile.ValueNodeList;

/**
 * This visits the expression tree and collects each operands of
 * BinaryOperatorNode and flattens it into an RCL.
 * 
 * When remapToNew is true, BinaryOperatorNode's left and right
 * operands  will be replaced with the RC of the expandedRCL.
 * This helps in column mapping during projection derivation.
 * 
 * see GroupByQueryInf#generateProjectMapping
 * @author soubhikc
 * 
 */
public final class CollectExpressionOperandsVisitor extends VisitorAdaptor {

  private final NodeFactory nf;

  private final ResultColumnList expandedRCL;

  private final boolean isCompilationAsDataStoreNode;

  private final ArrayList<Integer> numOperands;

  private final HasAggregateVisitor checkAgg;

  private ValueNodeList otherExpressions;

  public CollectExpressionOperandsVisitor(LanguageConnectionContext lcc,
      Vector[] aggregateInfoList, boolean isCompilationAsDataStoreNode)
      throws StandardException {
    if (lcc != null) {
      this.nf = lcc.getLanguageConnectionFactory().getNodeFactory();
      this.expandedRCL = (ResultColumnList)nf.getNode(
          C_NodeTypes.RESULT_COLUMN_LIST, lcc.getContextManager());
      this.numOperands = new ArrayList<Integer>();
    }
    else {
      this.nf = null;
      this.expandedRCL = null;
      this.numOperands = null;
    }
    
    this.isCompilationAsDataStoreNode = isCompilationAsDataStoreNode;
    this.checkAgg = new HasAggregateVisitor();
  }
  
  /**
   * Visit only HasAggregateVisitor
   * @return if any aggregate
   * @throws StandardException 
   */
  public boolean visitHasAggregateVisitorOnly(ValueNode node)
      throws StandardException {
    this.checkAgg.reset();
    if (node != null) {
      node.accept(checkAgg);
    }
    return this.checkAgg.hasAggregates();
  }

  public boolean skipChildren(Visitable node) throws StandardException {
    
    if( ResultColumnList.class.isInstance(node)) {
      /*
       * skip children for RC(s) in the list. As we
       * are returning 'false', RCL's children will 
       * be traversed but not any further.
       */
      return false;
    }
    
    return true;
  }

  public boolean stopTraversal() {
    return false;
  }
  
  public Visitable visit(Visitable node) throws StandardException {
    if(! (node instanceof ValueNode)) {
      return node;
    }
    
    try {
      
        ValueNode vn = null;
        ResultColumn parentRC = null;
        
        if( node instanceof ResultColumn) {
          parentRC = (ResultColumn) node;
          vn = parentRC;
        }
        else {
          vn = (ValueNode) node;
        }
    
        vn.accept(checkAgg);
        
        int before = expandedRCL.size();
        if(  checkAgg.hasAggregates() ) {
           vn.genExpressionOperands(expandedRCL
                                   , parentRC
                                   , ! isCompilationAsDataStoreNode);
        }
        else if(parentRC != null) {
           expandedRCL.addResultColumn(parentRC);
        }
        int after = expandedRCL.size();
    
        if(parentRC != null) {
          
            /* Issue is with CastNode as parentRC, it cannot delegate below to CastOperands
             * as 'null' indicates take expansion action. So now consider following query:
             *   select cast( avg(amount) as REAL), cast( sum(amount) as REAL) from tab
             * where 1st cast( sum/count ) should pass parentRC down so that rc.markExpand()
             * is done by BinaryOperatorNode but 2nd cast( sum ) shouldn't pass parentRC as
             * CastOperand is not a complex expression. 
             * 
             * So, in essence if outerRCL grows more than by 1 column then surely it got expanded.
             * TODO: when column re-use happens then outerRCL can grow by 1 even if complex expression 
             * is wrapped in CastNode.  
             */
              
              if( !parentRC.isExpanded() && (after - before) > 1) {
                  parentRC.markExpanded();
              }
              
        }
        /* this must be a valueNode hanging around in havingClause
         * lets add it to VNL. 
         */
        else if(  checkAgg.hasAggregates() ) {
          
           if(otherExpressions == null) {
             otherExpressions = new ValueNodeList();
           }
           
           otherExpressions.addValueNode(vn);
        }
          
        numOperands.add( after - before );
        
    } finally {
      checkAgg.reset();
    }
    
    return node;
  }
  
  public ResultColumnList getResultColumns() {
    expandedRCL.resetVirtualColumnIds();
    return expandedRCL;
  }
  
  public ValueNodeList getOtherExpressions() {
    return otherExpressions;
  }
  
  public ArrayList<Integer> getNumOperands() {
    return numOperands;
  }
  
}

final class HasAggregateVisitor extends VisitorAdaptor {
  
  private boolean isAggregateFound;
  
  public HasAggregateVisitor() {
    isAggregateFound = false;
  }

  public boolean skipChildren(Visitable node) throws StandardException {
    return false;
  }

  public boolean stopTraversal() {
    return isAggregateFound;
  }
  
  public boolean hasAggregates() {
    return isAggregateFound;
  }
  
  public void reset() {
    isAggregateFound = false;
  }

  public Visitable visit(Visitable node) throws StandardException {
    
    if(AggregateNode.class.isInstance(node)) {
       isAggregateFound = true;
       return node;
     }
     
     if(ColumnReference.class.isInstance(node)) {
       ColumnReference cr = (ColumnReference) node;
       isAggregateFound = cr.getColumnName().startsWith("##aggregate result");
       if(isAggregateFound) {
         return node;
       }
       cr.getSource().accept(this);
     }
      
    return node;
  }
  
}

