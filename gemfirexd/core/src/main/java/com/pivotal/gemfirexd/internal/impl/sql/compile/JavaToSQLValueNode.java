/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.compile.JavaToSQLValueNode

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

/*
 * Changes for GemFireXD distributed data platform (some marked by "GemStone changes")
 *
 * Portions Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
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

package	com.pivotal.gemfirexd.internal.impl.sql.compile;

// GemStone changes BEGIN
// GemStone changes END






import java.lang.reflect.Modifier;

import com.pivotal.gemfirexd.internal.engine.distributed.metadata.QueryInfo;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.QueryInfoContext;
import com.pivotal.gemfirexd.internal.engine.procedure.coordinate.DistributedProcedureCallNode;
import com.pivotal.gemfirexd.internal.engine.procedure.coordinate.ProcedureProxy;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.compiler.LocalField;
import com.pivotal.gemfirexd.internal.iapi.services.compiler.MethodBuilder;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.StatementType;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.TypeCompiler;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.Visitable;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.Visitor;
import com.pivotal.gemfirexd.internal.iapi.types.DataTypeDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.StringDataValue;
import com.pivotal.gemfirexd.internal.iapi.types.TypeId;
import com.pivotal.gemfirexd.internal.iapi.util.JBitSet;
import com.pivotal.gemfirexd.internal.impl.sql.compile.ExpressionClassBuilder;

import java.util.Vector;

/**
 * This node type converts a value from the Java domain to the SQL domain.
 */

public class JavaToSQLValueNode extends ValueNode
{
	JavaValueNode	javaNode;

	// Gemstone changes BEGIN
	ProcedureProxy procProxy;
	public ProcedureProxy getProcedureProxy() {
	  return this.procProxy;
	}
	// Gemstone changes END
	/**
	 * Initializer for a JavaToSQLValueNode
	 *
	 * @param value		The Java value to convert to the SQL domain
	 */
	public void init(Object value)
	{
		this.javaNode = (JavaValueNode) value;
	}

	/**
	 * Preprocess an expression tree.  We do a number of transformations
	 * here (including subqueries, IN lists, LIKE and BETWEEN) plus
	 * subquery flattening.
	 * NOTE: This is done before the outer ResultSetNode is preprocessed.
	 *
	 * @param	numTables			Number of tables in the DML Statement
	 * @param	outerFromList		FromList from outer query block
	 * @param	outerSubqueryList	SubqueryList from outer query block
	 * @param	outerPredicateList	PredicateList from outer query block
	 *
	 * @return		The modified expression
	 *
	 * @exception StandardException		Thrown on error
	 */
	public ValueNode preprocess(int numTables,
								FromList outerFromList,
								SubqueryList outerSubqueryList,
								PredicateList outerPredicateList) 
						throws StandardException
	{
		javaNode.preprocess(numTables,
							outerFromList, outerSubqueryList,
							outerPredicateList);

		return this;
	}

	/**
	 * Do code generation for this conversion of a value from the Java to
	 * the SQL domain.
	 *
	 * @param acb	The ExpressionClassBuilder for the class we're generating
	 * @param mb the method  the expression will go into
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void generateExpression(ExpressionClassBuilder acb,
											MethodBuilder mb)
									throws StandardException
	{
		TypeId			resultType;
		String						resultTypeName;

		/*
		** Tell the Java node that it's value is being returned to the
		** SQL domain.  This way, it knows whether the checking for a null
		** receiver is to be done at the Java level or the SQL level.
		*/
		javaNode.returnValueToSQLDomain();

		/* Generate the receiver, if any. */
		boolean hasReceiver = javaNode.generateReceiver(acb, mb);

		/*
		** If the java expression has a receiver, we want to check whether
		** it's null before evaluating the whole expression (to avoid
		** a NullPointerException.
		*/
		if (hasReceiver)
		{
			/*
			** There is a receiver.  Generate a null SQL value to return
			** in case the receiver is null.  First, create a field to hold
			** the null SQL value.
			*/
			String nullValueClass = getTypeCompiler().interfaceName();
			LocalField nullValueField =
				acb.newFieldDeclaration(Modifier.PRIVATE, nullValueClass);
			/*
			** There is a receiver.  Generate the following to test
			** for null:
			**
			**		(receiverExpression == null) ? 
			*/

			mb.conditionalIfNull();
			mb.getField(nullValueField);
			acb.generateNullWithExpress(mb, getTypeCompiler(), 
					getTypeServices().getCollationType());


			/*
			** We have now generated the expression to test, and the
			** "true" side of the ?: operator.  Finish the "true" side
			** so we can generate the "false" side.
			*/
			mb.startElseCode();
		}
		
		resultType = getTypeId();
		TypeCompiler tc = getTypeCompiler();

		resultTypeName = tc.interfaceName();

		/* Allocate an object for re-use to hold the result of the conversion */
		LocalField field = acb.newFieldDeclaration(Modifier.PRIVATE, resultTypeName);

		/* Generate the expression for the Java value under us */
		javaNode.generateExpression(acb, mb);

		/* Generate the SQL value, which is always nullable */
		acb.generateDataValue(mb, tc, 
				getTypeServices().getCollationType(), field);

		/*
		** If there was a receiver, the return value will be the result
		** of the ?: operator.
		*/
		if (hasReceiver)
		{
			mb.completeConditional();
		}
	}

	/**
	 * Prints the sub-nodes of this object.  See QueryTreeNode for
	 * how tree printing is supposed to work.
	 *
	 * @param depth		The depth of this node in the tree
	 */

	public void printSubNodes(int depth)
	{
		if (SanityManager.DEBUG)
		{
			super.printSubNodes(depth);

			printLabel(depth, "javaNode: ");
			javaNode.treePrint(depth + 1);
		}
	}

	/**
	 * Get the JavaValueNode that lives under this JavaToSQLValueNode.
	 *
	 * @return	The JavaValueNode that lives under this node.
	 */

	public JavaValueNode getJavaValueNode()
	{
		return javaNode;
	}

	/** 
	 * @see QueryTreeNode#disablePrivilegeCollection
	 */
	public void disablePrivilegeCollection()
	{
		super.disablePrivilegeCollection();
		if (javaNode != null)
			javaNode.disablePrivilegeCollection();
	}

	/**
	 * Bind this expression.  This means binding the sub-expressions,
	 * as well as figuring out what the return type is for this expression.
	 *
	 * @param fromList		The FROM list for the query this
	 *				expression is in, for binding columns.
	 * @param subqueryList		The subquery list being built as we find
	 *							SubqueryNodes
	 * @param aggregateVector	The aggregate vector being built as we find AggregateNodes
	 *
	 * @return	The new top of the expression tree.
	 *
	 * @exception StandardException		Thrown on error
	 */

	public ValueNode bindExpression(FromList fromList, SubqueryList subqueryList,
		Vector	aggregateVector) 
			throws StandardException
	{
		// method invocations are not allowed in ADD TABLE clauses.
		// And neither are field references. 
		javaNode.checkReliability(this);

		/* Bind the expression under us */
		javaNode = javaNode.bindExpression(fromList, subqueryList, aggregateVector);

// Gemstone changes BEGIN
		if (javaNode.getStatementType() == StatementType.DISTRIBUTED_PROCEDURE_CALL) {
		  // TODO: [sumedh] looks like a basic bug to me -- a runtime
		  // entity ProcedureProxy being embedded in a compile-time entity
		  this.procProxy = ((DistributedProcedureCallNode)javaNode).getProcedureProxy();
		}
// Gemstone changes END
		DataTypeDescriptor dts = javaNode.getDataType();
		if (dts == null)
		{
			throw StandardException.newException(SQLState.LANG_NO_CORRESPONDING_S_Q_L_TYPE, 
				javaNode.getJavaTypeName());
		}
        
        setType(dts);
		
        // For functions returning string types we should set the collation to match the 
        // java method's schema DERBY-2972. This is propogated from 
        // RoutineAliasInfo to javaNode.
        if (dts.getTypeId().isStringTypeId()) {
            this.setCollationInfo(javaNode.getCollationType(),
                    StringDataValue.COLLATION_DERIVATION_IMPLICIT);
        }

		return this;
	}

	/**
	 * Remap all ColumnReferences in this tree to be clones of the
	 * underlying expression.
	 *
	 * @return ValueNode			The remapped expression tree.
	 *
	 * @exception StandardException			Thrown on error
	 */
	public ValueNode remapColumnReferencesToExpressions()
		throws StandardException
	{
		javaNode = javaNode.remapColumnReferencesToExpressions();
		return this;
	}

	/**
	 * Categorize this predicate.  Initially, this means
	 * building a bit map of the referenced tables for each predicate.
	 * If the source of this ColumnReference (at the next underlying level) 
	 * is not a ColumnReference or a VirtualColumnNode then this predicate
	 * will not be pushed down.
	 *
	 * For example, in:
	 *		select * from (select 1 from s) a (x) where x = 1
	 * we will not push down x = 1.
	 * NOTE: It would be easy to handle the case of a constant, but if the
	 * inner SELECT returns an arbitrary expression, then we would have to copy
	 * that tree into the pushed predicate, and that tree could contain
	 * subqueries and method calls.
	 *
	 * @param referencedTabs	JBitSet with bit map of referenced FromTables
	 * @param simplePredsOnly	Whether or not to consider method
	 *							calls, field references and conditional nodes
	 *							when building bit map
	 *
	 * @return boolean		Whether or not source.expression is a ColumnReference
	 *						or a VirtualColumnNode.
	 *
	 * @exception StandardException			Thrown on error
	 */
	public boolean categorize(JBitSet referencedTabs, boolean simplePredsOnly)
		throws StandardException
	{
		return javaNode.categorize(referencedTabs, simplePredsOnly);
	}

	/**
	 * Return the variant type for the underlying expression.
	 * The variant type can be:
	 *		VARIANT				- variant within a scan
	 *							  (method calls and non-static field access)
	 *		SCAN_INVARIANT		- invariant within a scan
	 *							  (column references from outer tables)
	 *		QUERY_INVARIANT		- invariant within the life of a query
	 *							  (constant expressions)
	 *
	 * @return	The variant type for the underlying expression.
	 * @exception StandardException	thrown on error
	 */
	protected int getOrderableVariantType() throws StandardException
	{
		return javaNode.getOrderableVariantType();
	}

	/**
	 * Accept a visitor, and call v.visit()
	 * on child nodes as necessary.  
	 * 
	 * @param v the visitor
	 *
	 * @exception StandardException on error
	 */
	public Visitable accept(Visitor v) 
		throws StandardException
	{
		Visitable returnNode = v.visit(this);
	
		if (v.skipChildren(this))
		{
			return returnNode;
		}

		if (javaNode != null && !v.stopTraversal())
		{
			javaNode = (JavaValueNode)javaNode.accept(v);
		}
		
		return returnNode;
	}
        
	/**
	 * {@inheritDoc}
	 */
    protected boolean isEquivalent(ValueNode o)
    {
    	// anything in the java domain is not equiavlent.
    	return false;
    }
    //GemStone changes BEGIN
    
    //Asif: This is a dummy implementation to allow tests to pass. It needs
    //to be implmented correctly
    @Override
    public QueryInfo computeQueryInfo(QueryInfoContext qic) throws StandardException {      
      //Since isNull  Operator is not going to contribute to nodes prunning at this point,
      // it is safe to return null;      
      return null;
      
    }

    @Override
    public ValueNode genExpressionOperands(ResultColumnList outerResultColumns, ResultColumn parentRC, boolean remapToNew) 
       throws StandardException 
    {
        if(parentRC != null) {
          outerResultColumns.addResultColumn(parentRC);
          return this;
        }
        
      
        if(javaNode instanceof SQLToJavaValueNode) {
           javaNode = ((SQLToJavaValueNode)javaNode).genExpressionOperands(outerResultColumns, null, remapToNew);
        }
        
        return this;
    }
    
    @Override
    public String ncjGenerateSql() {
      if (javaNode instanceof StaticMethodCallNode) {
        StaticMethodCallNode smCall = (StaticMethodCallNode)javaNode;
        if (smCall.javaClassName.contains("Like")) {
          return "false";
        }
      }
      return super.ncjGenerateSql();
    }
    //GemStone changes END
}
