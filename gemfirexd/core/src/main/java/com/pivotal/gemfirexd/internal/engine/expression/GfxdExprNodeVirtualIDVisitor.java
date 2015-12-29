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

import java.util.Map;

import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.Visitable;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.Visitor;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.VisitorAdaptor;
import com.pivotal.gemfirexd.internal.impl.sql.compile.BinaryOperatorNode;
import com.pivotal.gemfirexd.internal.impl.sql.compile.CastNode;
import com.pivotal.gemfirexd.internal.impl.sql.compile.ColumnReference;
import com.pivotal.gemfirexd.internal.impl.sql.compile.ConstantNode;
import com.pivotal.gemfirexd.internal.impl.sql.compile.CurrentDatetimeOperatorNode;
import com.pivotal.gemfirexd.internal.impl.sql.compile.JavaToSQLValueNode;
import com.pivotal.gemfirexd.internal.impl.sql.compile.JavaValueNode;
import com.pivotal.gemfirexd.internal.impl.sql.compile.MethodCallNode;
import com.pivotal.gemfirexd.internal.impl.sql.compile.NonStaticMethodCallNode;
import com.pivotal.gemfirexd.internal.impl.sql.compile.ResultColumn;
import com.pivotal.gemfirexd.internal.impl.sql.compile.SQLToJavaValueNode;
import com.pivotal.gemfirexd.internal.impl.sql.compile.StaticClassFieldReferenceNode;
import com.pivotal.gemfirexd.internal.impl.sql.compile.TernaryOperatorNode;
import com.pivotal.gemfirexd.internal.impl.sql.compile.UnaryOperatorNode;

/**
 * {@link Visitor} to assign virtual column IDs to the columns using
 * {@link ResultColumn#setVirtualColumnId(int)} and also build a canonicalized
 * expression string using pre-order traversal for display and comparison.
 * Also allows for passing a child {@link Visitor} that will be invoked in
 * addition to this visitor in the same order.
 * 
 * @author swale
 */
public final class GfxdExprNodeVirtualIDVisitor extends VisitorAdaptor {

  private final Map<String, Integer> columnToIndexMap;

  private final StringBuilder canonicalizedExpression;

  private final Visitor childVisitor;

  private final String exprType;

  public GfxdExprNodeVirtualIDVisitor(Map<String, Integer> columnToIndexMap,
      Visitor childVisitor, String exprType) {
    this.columnToIndexMap = columnToIndexMap;
    this.canonicalizedExpression = new StringBuilder();
    this.childVisitor = childVisitor;
    this.exprType = exprType;
  }

  @Override
  public boolean skipChildren(Visitable node) throws StandardException {
    return (node instanceof BinaryOperatorNode
        || node instanceof TernaryOperatorNode
        || (node instanceof MethodCallNode
            && !(node instanceof NonStaticMethodCallNode)));
  }

  @Override
  public boolean stopTraversal() {
    return false;
  }

  @Override
  public Visitable visit(Visitable node) throws StandardException {
    if (node instanceof ColumnReference) {
      final ColumnReference colRef = (ColumnReference)node;
      if (this.columnToIndexMap != null) {
        final int virtID = this.columnToIndexMap.get(colRef.getColumnName())
            .intValue();
        colRef.getSource().setVirtualColumnId(virtID + 1);
      }
      this.canonicalizedExpression.append(colRef.getColumnName());
      acceptChildVisitor(node);
    }
    else if (node instanceof BinaryOperatorNode) {
      final BinaryOperatorNode opNode = (BinaryOperatorNode)node;
      final boolean parens = (this.canonicalizedExpression.length() > 0);
      if (parens) {
        this.canonicalizedExpression.append('(');
      }
      opNode.getLeftOperand().accept(this);
      this.canonicalizedExpression.append(' ')
          .append(opNode.getOperatorString()).append(' ');
      acceptChildVisitor(opNode);
      opNode.getRightOperand().accept(this);
      if (parens) {
        this.canonicalizedExpression.append(')');
      }
    }
    else if (node instanceof UnaryOperatorNode) {
      final UnaryOperatorNode opNode = (UnaryOperatorNode)node;
      this.canonicalizedExpression.append(opNode.getOperatorString());
      acceptChildVisitor(opNode);
    }
    else if (node instanceof TernaryOperatorNode) {
      final TernaryOperatorNode opNode = (TernaryOperatorNode)node;
      this.canonicalizedExpression.append(opNode.getMethodName()).append('(');
      acceptChildVisitor(opNode);
      if (opNode.getReceiver() != null) {
        opNode.getReceiver().accept(this);
        this.canonicalizedExpression.append(',');
      }
      if (opNode.getLeftOperand() != null) {
        opNode.getLeftOperand().accept(this);
        this.canonicalizedExpression.append(',');
      }
      if (opNode.getRightOperand() != null) {
        opNode.getRightOperand().accept(this);
        this.canonicalizedExpression.append(',');
      }
      closeParen();
    }
    else if (node instanceof ConstantNode) {
      this.canonicalizedExpression.append(((ConstantNode)node).getValue());
      acceptChildVisitor(node);
    }
    else if (node instanceof NonStaticMethodCallNode) {
      // nothing to be done
      acceptChildVisitor(node);
    }
    else if (node instanceof MethodCallNode) {
      final MethodCallNode opNode = (MethodCallNode)node;
      this.canonicalizedExpression.append(opNode.getMethodName()).append('(');
      acceptChildVisitor(node);
      for (JavaValueNode child : opNode.getMethodParms()) {
        if (child != null) {
          child.accept(this);
          this.canonicalizedExpression.append(',');
        }
      }
      closeParen();
    }
    else if (node instanceof StaticClassFieldReferenceNode) {
      final StaticClassFieldReferenceNode opNode =
        (StaticClassFieldReferenceNode)node;
      this.canonicalizedExpression.append(opNode.getJavaClassName())
          .append('.').append(opNode.getFieldName());
      acceptChildVisitor(node);
    }
    else if (node instanceof JavaToSQLValueNode) {
      // nothing to be done
      acceptChildVisitor(node);
    }
    else if (node instanceof SQLToJavaValueNode) {
      // nothing to be done
      acceptChildVisitor(node);
    }
    else if (node instanceof CastNode) {
      // nothing to be done
      acceptChildVisitor(node);
    }
    else if (node instanceof CurrentDatetimeOperatorNode) {
      final CurrentDatetimeOperatorNode opNode =
          (CurrentDatetimeOperatorNode)node;
        this.canonicalizedExpression.append(opNode.getMethodName());
        acceptChildVisitor(node);
    }
    else {
      if (this.childVisitor != null) {
        this.canonicalizedExpression.append(node.toString());
        acceptChildVisitor(node);
      }
      else {
        throw StandardException.newException(SQLState.LANG_SYNTAX_ERROR,
            "unknown expression in " + this.exprType + ": " + node + '('
                + node.getClass() + ')');
      }
    }
    return node;
  }

  public String getCanonicalizedExpression() {
    return this.canonicalizedExpression.toString();
  }

  private void acceptChildVisitor(Visitable node) throws StandardException {
    if (this.childVisitor != null && !this.childVisitor.skipChildren(node)) {
      this.childVisitor.visit(node);
    }
  }

  private void closeParen() {
    final int lastIndex = this.canonicalizedExpression.length() - 1;
    if (this.canonicalizedExpression.charAt(lastIndex) == ',') {
      this.canonicalizedExpression.setCharAt(lastIndex, ')');
    }
    else {
      this.canonicalizedExpression.append(')');
    }
  }
}
