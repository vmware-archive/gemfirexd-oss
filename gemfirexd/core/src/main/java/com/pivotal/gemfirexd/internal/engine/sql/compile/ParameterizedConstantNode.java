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

import java.util.Vector;

import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.DMLQueryInfo;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.ParameterizedConstantQueryInfo;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.QueryInfo;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.QueryInfoContext;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.ClassName;
import com.pivotal.gemfirexd.internal.iapi.services.classfile.VMOpcode;
import com.pivotal.gemfirexd.internal.iapi.services.compiler.MethodBuilder;
import com.pivotal.gemfirexd.internal.iapi.services.loader.ClassFactory;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.Visitable;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.VisitorAdaptor;
import com.pivotal.gemfirexd.internal.iapi.store.access.Qualifier;
import com.pivotal.gemfirexd.internal.iapi.types.DataTypeDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.JSQLType;
import com.pivotal.gemfirexd.internal.impl.sql.GenericStatement;
import com.pivotal.gemfirexd.internal.impl.sql.compile.ConstantNode;
import com.pivotal.gemfirexd.internal.impl.sql.compile.ExpressionClassBuilder;
import com.pivotal.gemfirexd.internal.impl.sql.compile.FromList;
import com.pivotal.gemfirexd.internal.impl.sql.compile.PredicateList;
import com.pivotal.gemfirexd.internal.impl.sql.compile.ResultColumn;
import com.pivotal.gemfirexd.internal.impl.sql.compile.ResultColumnList;
import com.pivotal.gemfirexd.internal.impl.sql.compile.SubqueryList;
import com.pivotal.gemfirexd.internal.impl.sql.compile.ValueNode;
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;

/**
 * 
 * @see GenericStatement#prepMinion and rescanStatementCache.
 * 
 * @author soubhikc
 */
public final class ParameterizedConstantNode extends ValueNode {

  private ConstantNode constantNode;

  private int constantNumber;

  

  /**
   ** Pointer to the array in the CompilerContext that holds array of types for
   * all the constants as parameters.. When each constant is bound (setType gets
   * called), it fills in its type descriptor in this array. Note that the array
   * is allocated in the parser, but the individual elements are not filled in
   * until their corresponding parameters are bound.
   * 
   * This array is not read in this class, but is read from the CompilerContext
   * on completion of compiling the statement.
   * 
   */
  // TODO
  // * In some case a parameter node may exist but is not a visible user
  // parameter,
  // * in this case typeServices will be null so that setting its type will not
  // * modify the user's set.
  // */
  private DataTypeDescriptor[] userParameterTypes;
  
  //private DataTypeDescriptor[] originalParameterTypes;

  @Override
  public void init(Object constantCount, Object constantValueNode)
      throws StandardException {

    if (SanityManager.DEBUG) {
      if (!(constantValueNode instanceof ConstantNode)
          || constantValueNode == null) {
        SanityManager.THROWASSERT("Unexpected constant " + constantValueNode
            + " in node " + getNodeType());
      }
    }
    if (SanityManager.DEBUG) {
      //if (getCompilerContext().isPreparedStatement()) {
      if (!getCompilerContext().isOptimizeLiteralAllowed()) {
        SanityManager.THROWASSERT("Shouldn't have this type for constants "
            + "in call to EmbedConnection.prepareStatement() "
            + this.constantNode
            + " statement "
            + getLanguageConnectionContext().getStatementContext()
                .getStatementText());
      }
    }

    constantNode = (ConstantNode)constantValueNode;
    constantNumber = ((Integer)constantCount).intValue();

    
  }

  final public ConstantNode constantNode() {
    return this.constantNode;
  }

  @Override
  public ValueNode bindExpression(FromList fromList, SubqueryList subqueryList,
      Vector aggregateVector) throws StandardException {

    if (SanityManager.DEBUG) {
      //if (getCompilerContext().isPreparedStatement()) {
      if (!getCompilerContext().isOptimizeLiteralAllowed()) {
        SanityManager.THROWASSERT("Shouldn't have this type for constants "
            + "in call to EmbedConnection.prepareStatement() "
            + this.constantNode
            + " statement "
            + getLanguageConnectionContext().getStatementContext()
                .getStatementText());
      }
    }

    if(userParameterTypes != null && userParameterTypes[constantNumber] == null) {
      //try getting from bind, but atleast one such test is there where we get ClasscastException
     // this.originalParameterTypes[constantNumber] = this.constantNode.getTypeServices();
        ValueNode n = constantNode.bindExpression(fromList, subqueryList,
          aggregateVector);
        setType(n.getTypeServices());  
    }
    

    return this;
  }

  @Override
  public void generateExpression(ExpressionClassBuilder acb, MethodBuilder mb)
      throws StandardException {

    if (SanityManager.DEBUG) {
      //if (getCompilerContext().isPreparedStatement()) {
      if (!getCompilerContext().isOptimizeLiteralAllowed()) {
        SanityManager.THROWASSERT("Shouldn't have this type for constants "
            + "in call to EmbedConnection.prepareStatement() "
            + this.constantNode
            + " statement "
            + getLanguageConnectionContext().getStatementContext()
                .getStatementText());
      }
    }

    mb.pushThis();
    mb.push(constantNumber); // arg

    mb.callMethod(VMOpcode.INVOKEVIRTUAL, ClassName.BaseActivation,
        "getParameter", ClassName.DataValueDescriptor, 1);

    /* Cast the result to its specific interface */
    mb.cast(getTypeCompiler(constantNode.getTypeServices().getTypeId())
        .interfaceName());
    // mb.cast(getTypeCompiler().interfaceName());
  }

  @Override
  public String toString() {
    return "ParameterizedConstantNode::" + constantNode.toString();
  }
  
  /**
   * @see ValueNode#isParameterizedConstantNode()
   */
  @Override
  public boolean isParameterizedConstantNode() {
    return true;
  }

  /**
   * Return whether or not this expression tree represents a constant
   * expression.
   * 
   * @return Whether or not this expression tree represents a constant
   *         expression.
   */
  @Override
  public boolean isConstantExpression() {
    return true;
  }

  /** @see ValueNode#constantExpression */
  @Override
  public boolean constantExpression(PredicateList whereClause) {
    return true;
  }

  /**
   * Return the variant type for the underlying expression.
   * 
   * The variant type can be:
   * 
   *              VARIANT           - variant within a scan
   *                                  (method calls and non-static field access)
   *              SCAN_INVARIANT    - invariant within a scan
   *                                  (column references from outer tables)
   *              QUERY_INVARIANT   - invariant within the life of a query
   *                                  (constant expressions)
   * 
   * @return      The variant type for the underlying expression.
   */
  @Override
  protected int getOrderableVariantType() {
    // Parameterized nodes are invariant for the life of the query
    return Qualifier.QUERY_INVARIANT;
  }

  /**
   * @see ValueNode#setType
   */
  @Override
  public void setType(DataTypeDescriptor descriptor) throws StandardException{
//   if(this.originalParameterTypes[constantNumber] == null) {
//     this.originalParameterTypes[constantNumber] = this.constantNode.getTypeServices();
//   }
    DataTypeDescriptor inputConstantType = constantNode.getTypeServices();
   
    if(inputConstantType.isNumericType(inputConstantType.getJDBCTypeId()) 
        && descriptor.isNumericType(descriptor.getJDBCTypeId())) {
      if(inputConstantType.getTypeId().typePrecedence() > descriptor.getTypeId().typePrecedence()) {
        return;
      }
    }
    if (userParameterTypes != null) {
      userParameterTypes[constantNumber] = descriptor;
    }

    super.setType(descriptor);

    if (descriptor.getTypeId() != constantNode.getTypeId()) {
     // DataValueDescriptor targetType = descriptor.getNull();
     // targetType.setValue(constantNode.getValue());
    
      constantNode.setType(descriptor);
      //constantNode.setValue(targetType);
    } 
    
    
  }
  
  
  
  
  /**
   * @see ValueNode#getTypeServices
   */
  @Override
  public DataTypeDescriptor getTypeServices() {
    
      return constantNode.getTypeServices();
    
  }
  
  public        JSQLType        getJSQLType()
  {          
    return new JSQLType(constantNode.getTypeServices());
  }

  /**
   * Set the descriptor array
   * 
   * @param descriptors
   *          The array of DataTypeServices to fill in when the parameters are
   *          bound.
   */

  public void setDescriptors(DataTypeDescriptor[] descriptors) {
    userParameterTypes = descriptors;
  }
  
//  public void setOriginalDescriptors(DataTypeDescriptor[] descriptors) {
//    this.originalParameterTypes = descriptors;
//  }

  @Override
  protected boolean isEquivalent(ValueNode o) throws StandardException {
    if (isSameNodeType(o)) {
      ParameterizedConstantNode other = (ParameterizedConstantNode)o;
      return constantNode.isEquivalent(other.constantNode);
    }

    return false;
  }

  /* don't use this; use cc.switchOptimizeLiteral(false) instead to never
     create ParameterizedConstantNodes in the first place
  public static QueryTreeNode rollbackParameterizedConstantsToConstantsIfAny(
      QueryTreeNode qt) throws StandardException {

    Visitable result = qt.accept(ParameterizedConstantReverter.REPLACER);
    if (SanityManager.DEBUG) {
      if (!ValueNode.class.isInstance(result)
          && !StatementNode.class.isInstance(result)) {
        SanityManager
            .THROWASSERT("Until now only handled for StatementNode, "
                + "ValueNode & ParameterizedConstantNode. This is a new condition.");
      }
    }

    return (QueryTreeNode)result;
  }
  */

  @Override
  public QueryInfo computeQueryInfo(QueryInfoContext qic)
      throws StandardException {
        QueryInfo queryInfo = qic.getRootQueryInfo(); 
        if(queryInfo.isSelect()) {
          if(((DMLQueryInfo)queryInfo).isPreparedStatementQuery() ){
                  return this.constantNode.computeQueryInfo(qic);
          }
        }
    qic.foundParameter( constantNumber-qic.getAbsoluteStart());
    return new ParameterizedConstantQueryInfo(constantNumber, constantNumber-qic.getAbsoluteStart());
  }

  @Override
  public ValueNode genExpressionOperands(ResultColumnList outerResultColumns,
      ResultColumn parentRC, boolean remapToNew) throws StandardException {

    this.constantNode = (ConstantNode) this.constantNode.genExpressionOperands(outerResultColumns,
        parentRC, remapToNew);
    
    return this;
  }

  @Override
  public boolean requiresTypeFromContext() {
    return true;
  }
  
  public int getConstantNumber() {
    return constantNumber;
  }
  
  //Gemstone changes BEGIN
  /**
   * Convert this object to a String for special formatting during EXPLAIN.
   *   This is done as part of PROJECT/PROJECT-RESTRICT and FILTER EXPLAIN output
   *
   * @return      This object as a String
   */

  public String printExplainInfo()
  {
    // Print 'CONSTANT:' before value to clarify for user
    return "CONSTANT:"+constantNode.printExplainInfo();
  }
  
  @Override
  public String ncjGenerateSql() {
    return constantNode.ncjGenerateSql();
  }
  //Gemstone changes END
}

/**
 * A visitor to revert back to ConstantNode in case of constraint check
 * generation.
 */
class ParameterizedConstantReverter extends VisitorAdaptor {

  static final ParameterizedConstantReverter REPLACER = new ParameterizedConstantReverter();

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

    if (node instanceof ParameterizedConstantNode) {
      if (SanityManager.DEBUG) {
        if (GemFireXDUtils.TraceStatementMatching) {
          SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_STATEMENT_MATCHING,
              "Replacing ParameterizedConstantNode with ConstantNode " + node);
        }
      }
      return ((ParameterizedConstantNode)node).constantNode();
    }

    return node;
  }
} // end of ParameterizedConstantReverter
