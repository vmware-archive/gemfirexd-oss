/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.compile.CallStatementNode

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

import java.lang.reflect.Modifier;

import com.pivotal.gemfirexd.internal.catalog.types.RoutineAliasInfo;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.procedure.coordinate.ProcedureProxy;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.ClassName;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.classfile.VMOpcode;
import com.pivotal.gemfirexd.internal.iapi.services.compiler.MethodBuilder;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultDescription;
import com.pivotal.gemfirexd.internal.iapi.sql.StatementType;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.C_NodeTypes;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.CompilerContext;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.Visitable;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.Visitor;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.Authorizer;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDictionary;
import com.pivotal.gemfirexd.internal.impl.sql.GenericStatement;

/**
 * An CallStatementNode represents a CALL <procedure> statement.
 * It is the top node of the query tree for that statement.
 * A procedure call is very simple.
 * 
 * CALL [<schema>.]<procedure>(<args>)
 * 
 * <args> are either constants or parameter markers.
 * This implementation assumes that no subqueries or aggregates
 * can be in the argument list.
 * 
 * A procedure is always represented by a MethodCallNode.
 *
 */
public class CallStatementNode extends DMLStatementNode
{	
	/**
	 * The method call for the Java procedure. Guaranteed to be
	 * a JavaToSQLValueNode wrapping a MethodCallNode by checks
	 * in the parser.
	 */
	private JavaToSQLValueNode	methodCall;


	/**
	 * Initializer for a CallStatementNode.
	 *
	 * @param methodCall		The expression to "call"
	 */

	public void init(Object methodCall)
	{
		super.init(null);
		this.methodCall = (JavaToSQLValueNode) methodCall;
		this.methodCall.getJavaValueNode().markForCallStatement();
	}

	/**
	 * Convert this object to a String.  See comments in QueryTreeNode.java
	 * for how this should be done for tree printing.
	 *
	 * @return	This object as a String
	 */

	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			return "CALL " + methodCall.toString() + "\n" +
				super.toString();
		}
		else
		{
			return "";
		}
	}

	public String statementToString()
	{
		return "CALL";
	}

	/**
	 * Prints the sub-nodes of this object.  See QueryTreeNode.java for
	 * how tree printing is supposed to work.
	 *
	 * @param depth		The depth of this node in the tree
	 */

	public void printSubNodes(int depth)
	{
		if (SanityManager.DEBUG)
		{
			super.printSubNodes(depth);

			if (methodCall != null)
			{
				printLabel(depth, "methodCall: ");
				methodCall.treePrint(depth + 1);
			}
		}
	}

	/**
	 * Bind this UpdateNode.  This means looking up tables and columns and
	 * getting their types, and figuring out the result types of all
	 * expressions, as well as doing view resolution, permissions checking,
	 * etc.
	 * <p>
	 * Binding an update will also massage the tree so that
	 * the ResultSetNode has a single column, the RID.
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void bindStatement() throws StandardException
	{
		DataDictionary dd = getDataDictionary();

		if (SanityManager.DEBUG)
			SanityManager.ASSERT((dd != null), "Failed to get data dictionary");

		final CompilerContext cc = getCompilerContext();
		cc.pushCurrentPrivType(getPrivType());
		methodCall = (JavaToSQLValueNode) methodCall.bindExpression(
							(FromList) getNodeFactory().getNode(
								C_NodeTypes.FROM_LIST,
								getNodeFactory().doJoinOrderOptimization(),
								getContextManager()), 
							null,
							null);

		// Disallow creation of BEFORE triggers which contain calls to 
		// procedures that modify SQL data. 
  		checkReliability();

		cc.popCurrentPrivType();
		cc.setOriginalExecFlags((short)(cc.getOriginalExecFlags()
		    | GenericStatement.IS_CALLABLE_STATEMENT));
	}

	/**
	 * Optimize a DML statement (which is the only type of statement that
	 * should need optimizing, I think). This method over-rides the one
	 * in QueryTreeNode.
	 *
	 * This method takes a bound tree, and returns an optimized tree.
	 * It annotates the bound tree rather than creating an entirely
	 * new tree.
	 *
	 * Throws an exception if the tree is not bound, or if the binding
	 * is out of date.
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void optimizeStatement() throws StandardException
	{
		DataDictionary dd = getDataDictionary();

		if (SanityManager.DEBUG)
		SanityManager.ASSERT((dd != null), "Failed to get data dictionary");

		/* Preprocess the method call tree */
		methodCall = (JavaToSQLValueNode) methodCall.preprocess(
								getCompilerContext().getNumTables(),
								(FromList) getNodeFactory().getNode(
									C_NodeTypes.FROM_LIST,
									getNodeFactory().doJoinOrderOptimization(),
									getContextManager()),
								(SubqueryList) null,
								(PredicateList) null);

	}

	/**
	 * Code generation for CallStatementNode.
	 * The generated code will contain:
	 *		o  A generated void method for the user's method call.
	 *
	 * @param acb	The ActivationClassBuilder for the class being built
	 * @param mb	The method for the execute() method to be built
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void generate(ActivationClassBuilder acb,
								MethodBuilder mb)
							throws StandardException
	{
		JavaValueNode		methodCallBody;

		/* generate the parameters */
		generateParameterValueSet(acb);
		this.optimizeForOffHeap(true);
		/* 
		 * Skip over the JavaToSQLValueNode and call generate() for the JavaValueNode.
		 * (This skips over generated code which is unnecessary since we are throwing
		 * away any return value and which won't work with void methods.)
		 * generates:
		 *     <methodCall.generate(acb)>;
		 * and adds it to userExprFun
		 */
		methodCallBody = methodCall.getJavaValueNode();

		/*
		** Tell the method call that its return value (if any) will be
		** discarded.  This is so it doesn't generate the ?: operator
		** that would return null if the receiver is null.  This is
		** important because the ?: operator cannot be made into a statement.
		*/
		methodCallBody.markReturnValueDiscarded();

		// this sets up the method
		// generates:
		// 	void userExprFun {
		//     method_call(<args>);
		//  }
		//
		//  An expression function is used to avoid reflection.
		//  Since the arguments to a procedure are simple, this
		// will be the only expression function and so it will
		// be executed directly as e0.
		MethodBuilder userExprFun = acb.newGeneratedFun("void", Modifier.PUBLIC);
		userExprFun.addThrownException("java.lang.Exception");
		methodCallBody.generate(acb, userExprFun);
		userExprFun.endStatement();
		userExprFun.methodReturn();
		userExprFun.complete();

// GemStone changes BEGIN
		// set the object name in activation
		acb.pushThisAsActivation(mb);
		mb.push(((MethodCallNode)methodCallBody).getMethodName()); // arg one
		mb.callMethod(VMOpcode.INVOKEINTERFACE, null, "setObjectName",
		    "void", 1);
// GemStone changes END
		acb.pushGetResultSetFactoryExpression(mb);
		acb.pushMethodReference(mb, userExprFun); // first arg
		acb.pushThisAsActivation(mb); // arg 2
		mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getCallStatementResultSet", ClassName.ResultSet, 2);
	}

	// Gemstone changes BEGIN
	public ProcedureProxy getProcedureProxy() {
	  return this.methodCall.getProcedureProxy();
	}
	
	public int getStatementType() {
	    return StatementType.CALL_STATEMENT;
	}
	// Gemstone changes END
	public ResultDescription makeResultDescription()
	{
		return null;
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
		if (v.skipChildren(this))
		{
			return v.visit(this);
		}

		Visitable returnNode = super.accept(v);

		if (!v.stopTraversal())
		{
			methodCall = (JavaToSQLValueNode) methodCall.accept(v);
		}

		return returnNode;
	}

	/**
	 * Set default privilege of EXECUTE for this node. 
	 */
	int getPrivType()
	{
		return Authorizer.EXECUTE_PRIV;
	}
	
	/**
	 * This method checks if the called procedure allows modification of SQL 
	 * data. If yes, it cannot be compiled if the reliability is 
	 * <code>CompilerContext.MODIFIES_SQL_DATA_PROCEDURE_ILLEGAL</code>. This 
	 * reliability is set for BEFORE triggers in the create trigger node. This 
	 * check thus disallows creation of BEFORE triggers which contain calls to 
	 * procedures that modify SQL data in the trigger action statement.  
	 * 
	 * @throws StandardException
	 */
	private void checkReliability() throws StandardException {
		if(getSQLAllowedInProcedure() == RoutineAliasInfo.MODIFIES_SQL_DATA &&
				getCompilerContext().getReliability() == CompilerContext.MODIFIES_SQL_DATA_PROCEDURE_ILLEGAL) 
			throw StandardException.newException(SQLState.LANG_UNSUPPORTED_TRIGGER_PROC);
	}
	
	/**
	 * This method checks the SQL allowed by the called procedure. This method 
	 * should be called only after the procedure has been resolved.
	 * 
	 * @return	SQL allowed by the procedure
	 */
	private short getSQLAllowedInProcedure() {
		RoutineAliasInfo routineInfo = ((MethodCallNode)methodCall.getJavaValueNode()).routineInfo;
		
		// If this method is called before the routine has been resolved, routineInfo will be null 
		if (SanityManager.DEBUG)
			SanityManager.ASSERT((routineInfo != null), "Failed to get routineInfo");

		return routineInfo.getSQLAllowed();
	}
}
