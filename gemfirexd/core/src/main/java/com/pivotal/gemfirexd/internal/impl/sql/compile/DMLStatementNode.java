/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.compile.DMLStatementNode

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

import java.util.ArrayList;
import java.util.Vector;

import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultColumnDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultDescription;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.C_NodeTypes;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.CompilerContext;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.Optimizer;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.Visitable;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.Visitor;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.VisitorAdaptor;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.Authorizer;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDictionary;
import com.pivotal.gemfirexd.internal.iapi.util.JBitSet;

/**
 * A DMLStatementNode represents any type of DML statement: a cursor declaration,
 * an INSERT statement, and UPDATE statement, or a DELETE statement.  All DML
 * statements have result sets, but they do different things with them.  A
 * SELECT statement sends its result set to the client, an INSERT statement
 * inserts its result set into a table, a DELETE statement deletes from a
 * table the rows corresponding to the rows in its result set, and an UPDATE
 * statement updates the rows in a base table corresponding to the rows in its
 * result set.
 *
 */
// Gemstone changes BEGIN
public abstract class DMLStatementNode extends StatementNode
//Gemstone changes END
{

	/**
	 * The result set is the rows that result from running the
	 * statement.  What this means for SELECT statements is fairly obvious.
	 * For a DELETE, there is one result column representing the
	 * key of the row to be deleted (most likely, the location of the
	 * row in the underlying heap).  For an UPDATE, the row consists of
	 * the key of the row to be updated plus the updated columns.  For
	 * an INSERT, the row consists of the new column values to be
	 * inserted, with no key (the system generates a key).
	 *
	 * The parser doesn't know anything about keys, so the columns
	 * representing the keys will be added after parsing (perhaps in
	 * the binding phase?).
	 *
	 */
	ResultSetNode	resultSet;

	// GemStone changes BEGIN
	protected Optimizer optimizer;
	// GemStone changes END

	/**
	 * Initializer for a DMLStatementNode
	 *
	 * @param resultSet	A ResultSetNode for the result set of the
	 *			DML statement
	 */

	@Override
  public void init(Object resultSet)
	{
		this.resultSet = (ResultSetNode) resultSet;
	}

	/**
	 * Prints the sub-nodes of this object.  See QueryTreeNode.java for
	 * how tree printing is supposed to work.
	 *
	 * @param depth		The depth of this node in the tree
	 */

	@Override
  public void printSubNodes(int depth)
	{
		if (SanityManager.DEBUG)
		{
			super.printSubNodes(depth);
			if (resultSet != null)
			{
				printLabel(depth, "resultSet: ");
				resultSet.treePrint(depth + 1);
			}
		}
	}

	/**
	 * Get the ResultSetNode from this DML Statement.
	 * (Useful for view resolution after parsing the view definition.)
	 *
	 * @return ResultSetNode	The ResultSetNode from this DMLStatementNode.
	 */
	public ResultSetNode getResultSetNode()
	{
		return resultSet;
	}

	/**
	 * Bind this DMLStatementNode.  This means looking up tables and columns and
	 * getting their types, and figuring out the result types of all
	 * expressions, as well as doing view resolution, permissions checking,
	 * etc.
	 *
	 * @param dataDictionary	The DataDictionary to use to look up
	 *				columns, tables, etc.
	 *
	 * @return	The bound query tree
	 *
	 * @exception StandardException		Thrown on error
	 */

	QueryTreeNode bind(DataDictionary dataDictionary)
					 throws StandardException
	{
		// We just need select privilege on most columns and tables
		getCompilerContext().pushCurrentPrivType(getPrivType());
		try {
			/*
			** Bind the tables before binding the expressions, so we can
			** use the results of table binding to look up columns.
			*/
			bindTables(dataDictionary);

			/* Bind the expressions */
			bindExpressions();
		}
		finally
		{
			getCompilerContext().popCurrentPrivType();
		}

		return this;
	}

	/**
	 * Bind only the underlying ResultSets with tables.  This is necessary for
	 * INSERT, where the binding order depends on the underlying ResultSets.
	 * This means looking up tables and columns and
	 * getting their types, and figuring out the result types of all
	 * expressions, as well as doing view resolution, permissions checking,
	 * etc.
	 *
	 * @param dataDictionary	The DataDictionary to use to look up
	 *				columns, tables, etc.
	 *
	 * @return	The bound query tree
	 *
	 * @exception StandardException		Thrown on error
	 */

	public QueryTreeNode bindResultSetsWithTables(DataDictionary dataDictionary)
					 throws StandardException
	{
		/* Okay to bindly bind the tables, since ResultSets without tables
		 * know to handle the call.
		 */
		bindTables(dataDictionary);

		/* Bind the expressions in the underlying ResultSets with tables */
		bindExpressionsWithTables();

		return this;
	}

	/**
	 * Bind the tables in this DML statement.
	 *
	 * @param dataDictionary	The data dictionary to use to look up the tables
	 *
	 * @exception StandardException		Thrown on error
	 */

	protected void bindTables(DataDictionary dataDictionary)
			throws StandardException
	{
		/* Bind the tables in the resultSet 
		 * (DMLStatementNode is above all ResultSetNodes, so table numbering
		 * will begin at 0.)
		 * In case of referential action on delete , the table numbers can be
		 * > 0 because the nodes are create for dependent tables also in the 
		 * the same context.
		 */

		resultSet = resultSet.bindNonVTITables(
						dataDictionary,
						(FromList) getNodeFactory().getNode(
							C_NodeTypes.FROM_LIST,
							getNodeFactory().doJoinOrderOptimization(),
							getContextManager()));
		resultSet = resultSet.bindVTITables(
						(FromList) getNodeFactory().getNode(
							C_NodeTypes.FROM_LIST,
							getNodeFactory().doJoinOrderOptimization(),
							getContextManager()));
	}

	/**
	 * Bind the expressions in this DML statement.
	 *
	 * @exception StandardException		Thrown on error
	 */

	protected void bindExpressions()
			throws StandardException
	{
		FromList fromList = (FromList) getNodeFactory().getNode(
								C_NodeTypes.FROM_LIST,
								getNodeFactory().doJoinOrderOptimization(),
								getContextManager());

		/* Bind the expressions under the resultSet */
		resultSet.bindExpressions(fromList);
		
		/* Verify that all underlying ResultSets reclaimed their FromList */
		if (SanityManager.DEBUG)
		SanityManager.ASSERT(fromList.size() == 0,
			"fromList.size() is expected to be 0, not " + fromList.size() +
			" on return from RS.bindExpressions()");
	}

	/**
	 * Bind the expressions in the underlying ResultSets with tables.
	 *
	 * @exception StandardException		Thrown on error
	 */

	protected void bindExpressionsWithTables()
			throws StandardException
	{
		FromList fromList = (FromList) getNodeFactory().getNode(
								C_NodeTypes.FROM_LIST,
								getNodeFactory().doJoinOrderOptimization(),
								getContextManager());

		/* Bind the expressions under the resultSet */
		resultSet.bindExpressionsWithTables(fromList);

		/* Verify that all underlying ResultSets reclaimed their FromList */
		if (SanityManager.DEBUG)
		SanityManager.ASSERT(fromList.size() == 0,
			"fromList.size() is expected to be 0, not " + fromList.size() +
			" on return from RS.bindExpressions()");
	}

	/**
	 * Returns the type of activation this class
	 * generates.
	 * 
	 * @return either (NEED_ROW_ACTIVATION | NEED_PARAM_ACTIVATION) or
	 *			(NEED_ROW_ACTIVATION) depending on params
	 *
	 */
	@Override
  int activationKind()
	{
		Vector parameterList = getCompilerContext().getParameterList();
		/*
		** We need rows for all types of DML activations.  We need parameters
		** only for those that have parameters.
		*/
		if (parameterList != null && parameterList.size() > 0)
		{
			return StatementNode.NEED_PARAM_ACTIVATION;
		}
		else
		{
			return StatementNode.NEED_ROW_ACTIVATION;
		}
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
	@Override
  public void optimizeStatement() throws StandardException
	{
		optimizeStatement(null, null);
	}

	/**
	 * This overload variant of optimizeStatement is used by subclass
	 * CursorNode (as well as a minion for the no-arg variant).
	 *
	 * @param offset     Any OFFSET row count, or null
	 * @param fetchFirst Any FETCH FIRST row count or null
	 *
	 * @exception StandardException		Thrown on error
	 * @see DMLStatementNode#optimizeStatement()
	 */
	protected void optimizeStatement(ValueNode offset, ValueNode fetchFirst)
			throws StandardException
	{
	        boolean isOffsetOrFetchNext = offset != null || fetchFirst != null;
	        getCompilerContext().setIsOffsetOrFetchNext();
	        
		resultSet = resultSet.preprocess(getCompilerContext().getNumTables(),
										 null,
										 (FromList) null);
		resultSet = resultSet.optimize(getDataDictionary(), null, 1.0d);
		
		// GemStone changes BEGIN
		optimizer = resultSet.getOptimizerImpl();
		optimizer.saveBestJoinOrderPrIDs();
		// GemStone changes END

		resultSet = resultSet.modifyAccessPaths();

		// Any OFFSET/FETCH FIRST narrowing must be done *after* any rewrite of
		// the query tree (if not, underlying GROUP BY fails), but *before* the
		// final scroll insensitive result node set is added - that one needs
		// to sit on top - so now is the time.
		// 
		// This example statement fails if we wrap *before* the optimization
		// above:
		//     select max(a) from t1 group by b fetch first row only
		//
		// A java.sql.ResultSet#previous on a scrollable result set will fail
		// if we don't wrap *after* the ScrollInsensitiveResultSetNode below.
		//
		// We need only wrap the RowCountNode set if at least one of the
		// clauses is present.
		
		if (isOffsetOrFetchNext) {
			resultSet = wrapRowCountNode(resultSet, offset, fetchFirst);
		}

		/* If this is a cursor, then we
		 * need to generate a new ResultSetNode to enable the scrolling
		 * on top of the tree before modifying the access paths.
		 */
		if (this instanceof CursorNode)
		{
			ResultColumnList				siRCList;
			ResultColumnList				childRCList;
			ResultSetNode					siChild = resultSet;

			/* We get a shallow copy of the ResultColumnList and its 
			 * ResultColumns.  (Copy maintains ResultColumn.expression for now.)
			 */
			siRCList = resultSet.getResultColumns();
			childRCList = siRCList.copyListAndObjects();
			resultSet.setResultColumns(childRCList);

			/* Replace ResultColumn.expression with new VirtualColumnNodes
			 * in the ScrollInsensitiveResultSetNode's ResultColumnList.  (VirtualColumnNodes include
			 * pointers to source ResultSetNode, this, and source ResultColumn.)
			 */
			siRCList.genVirtualColumnNodes(resultSet, childRCList);

			/* Finally, we create the new ScrollInsensitiveResultSetNode */
			resultSet = (ResultSetNode) getNodeFactory().
							getNode(
								C_NodeTypes.SCROLL_INSENSITIVE_RESULT_SET_NODE,
								resultSet, 
								siRCList,
								null,
								getContextManager());
			// Propagate the referenced table map if it's already been created
			if (siChild.getReferencedTableMap() != null)
			{
				resultSet.setReferencedTableMap((JBitSet) siChild.getReferencedTableMap().clone());
			}
			
		}
	}

	private ResultSetNode wrapRowCountNode(
		ResultSetNode resultSet,
		ValueNode offset,
		ValueNode fetchFirst) throws StandardException {
//GemStone changes BEGIN   
                /**
                 * On remote data nodes, do not apply row count (to process offset and
                 * fetch-next) if its order by is nullified (for partitioned region).
                 */
                {
                  final CompilerContext cc = getCompilerContext();
                  if (cc != null && !cc.createQueryInfo() && cc.isOrderByListNullified()) {
                    return resultSet;
                  }
                }      
//GemStone changes END 
          
		ResultSetNode topRS = resultSet;
		ResultColumnList selectRCs =
			topRS.getResultColumns().copyListAndObjects();
		selectRCs.genVirtualColumnNodes(topRS, topRS.getResultColumns());
	
		return (RowCountNode)getNodeFactory().getNode(
			C_NodeTypes.ROW_COUNT_NODE,
			topRS,
			selectRCs,
			offset,
			fetchFirst,
			getContextManager());
	}


	/**
	 * Make a ResultDescription for use in a PreparedStatement.
	 *
	 * ResultDescriptions are visible to JDBC only for cursor statements.
	 * For other types of statements, they are only used internally to
	 * get descriptions of the base tables being affected.  For example,
	 * for an INSERT statement, the ResultDescription describes the
	 * rows in the table being inserted into, which is useful when
	 * the values being inserted are of a different type or length
	 * than the columns in the base table.
	 *
	 * @return	A ResultDescription for this DML statement
	 */

	@Override
  public ResultDescription makeResultDescription()
	{
	    ResultColumnDescriptor[] colDescs = resultSet.makeResultDescriptors();
		String statementType = statementToString();

	    return getExecutionFactory().getResultDescription(
                           colDescs, statementType );
	}

	/**
	 * Generate the code to create the ParameterValueSet, if necessary,
	 * when constructing the activation.  Also generate the code to call
	 * a method that will throw an exception if we try to execute without
	 * all the parameters being set.
	 * 
	 * @param acb	The ActivationClassBuilder for the class we're building
	 */

	void generateParameterValueSet(ActivationClassBuilder acb)
		throws StandardException
	{
		Vector<ValueNode> parameterList = getCompilerContext().getParameterList();
		int	numberOfParameters = (parameterList == null) ? 0 : parameterList.size();

		if (numberOfParameters <= 0 || !getCompilerContext().isPreparedStatement())
			return;

			ParameterNode.generateParameterValueSet
				( acb, numberOfParameters, parameterList);
	}

	/**
	 * A read statement is atomic (DMLMod overrides us) if there
	 * are no work units, and no SELECT nodes, or if its SELECT nodes 
	 * are all arguments to a function.  This is admittedly
	 * a bit simplistic, what if someone has: <pre>
	 * 	VALUES myfunc(SELECT max(c.commitFunc()) FROM T) 
	 * </pre>
	 * but we aren't going too far out of our way to
	 * catch every possible wierd case.  We basically
	 * want to be permissive w/o allowing someone to partially
	 * commit a write. 
	 * 
	 * @return true if the statement is atomic
	 *
	 * @exception StandardException on error
	 */	
	@Override
  public boolean isAtomic() throws StandardException
	{
		/*
		** If we have a FromBaseTable then we have
		** a SELECT, so we want to consider ourselves
		** atomic.  Don't drill below StaticMethodCallNodes
		** to allow a SELECT in an argument to a method
		** call that can be atomic.
		*/
		HasNodeVisitor visitor = new HasNodeVisitor(FromBaseTable.class, StaticMethodCallNode.class);
													
		this.accept(visitor);
		if (visitor.hasNode())
		{
			return true;
		}

		return false;
	}

	/**
	 * Accept a visitor, and call v.visit()
	 * on child nodes as necessary.  
	 * 
	 * @param v the visitor
	 *
	 * @exception StandardException on error
	 */
	@Override
  public Visitable accept(Visitor v) 
		throws StandardException
	{
		if (v.skipChildren(this))
		{
			return v.visit(this);
		}

		if (resultSet != null && !v.stopTraversal())
		{
			resultSet = (ResultSetNode)resultSet.accept(v);
		}

		return this;
	}

	/**
	 * Return default privilege needed for this node. Other DML nodes can override
	 * this method to set their own default privilege.
	 *
	 * @return true if the statement is atomic
	 */
	int getPrivType()
	{
		return Authorizer.SELECT_PRIV;
	}

  // GemStone changes BEGIN
  @Override
  public StringBuilder makeShortDescription(final StringBuilder buffer) {
    if (resultSet == null) {
      return buffer;
    }

    String statementType = statementToString().replaceAll(" ", "_");
    // statementType
    buffer.append(statementType.toLowerCase()).append("_");

    StringBuilder columnList = new StringBuilder();
    ResultColumnList rcl = resultSet.getResultColumns();

    if (!(this instanceof DeleteNode)) {
      for (int idx = 0; idx <= rcl.size() - 1; idx++) {
        ResultColumn rc = (ResultColumn) rcl.elementAt(idx);
        if (rc == null || rc.isGenerated()
            || (this instanceof UpdateNode && !rc.updated())) {
          continue;
        }
        if (idx != 0) {
          columnList.append(",");
        }
        columnList.append(rc.getName().toLowerCase());
      }
    }

    if (this instanceof DMLModStatementNode) {
      buffer.append(columnList);
      buffer.append("_").append(((DMLModStatementNode)this).targetTableName);
    }
    else if (this instanceof CursorNode) {
      final StringBuilder tableList = new StringBuilder();

      final Visitor v = new VisitorAdaptor() {
        private int numTables = 0;
        private ArrayList<String> visitedTables = new ArrayList<String>();

        @Override
        public boolean skipChildren(Visitable node) throws StandardException {
          /*
           * SubQuery table list is handled within the visit method &
           * from base table is only what we are interested in.
           */
          return (node instanceof FromSubquery || node instanceof FromBaseTable);
        }

        @Override
        public boolean stopTraversal() {
          return tableList.length() >= GfxdConstants.MAX_STATS_DESCRIPTION_LENGTH;
        }

        @Override
        public Visitable visit(Visitable node) throws StandardException {
          String tabName = null;
          if (node instanceof FromBaseTable) {
            tabName = ((FromBaseTable)node).getActualTableName().toString();
          }
          else if (node instanceof FromVTI) {
            tabName = ((FromVTI)node).getExposedTableName().toString();
          }
          else if (node instanceof FromSubquery) {
            int parentNumTables = numTables;
            ArrayList<String> parentVisitedTables = visitedTables;
            try {
              numTables = 0;
              visitedTables = new ArrayList<String>();
              tableList.append("(_SELECT");
              ((FromSubquery)node).getSubquery().accept(this);
              tableList.append("_)");
            }
            finally {
              numTables = parentNumTables;
              visitedTables = parentVisitedTables;
            }
          }
          
          if (tabName != null && !visitedTables.contains(tabName)) {
            if (numTables == 0) {
              tableList.append("__");
            }
            else {
              tableList.append(",");
            }

            tableList.append(tabName);
            visitedTables.add(tabName);

            numTables++;
          }
          return node;
        }

      };

      boolean columnListAdded = false;
      try {
        this.resultSet.accept(v);
        if ((columnList.length() + tableList.length() + buffer.length()) > GfxdConstants.MAX_STATS_DESCRIPTION_LENGTH) {
          int reduceLength = columnList.length() - tableList.length()
              - buffer.length();
          if (reduceLength > 0) {
            buffer.append(columnList.subSequence(0, reduceLength));
          }
        }
        else {
          buffer.append(columnList);
        }
        columnListAdded = true;
        buffer.append(tableList);
      } catch (StandardException e) {
        if (SanityManager.DEBUG) {
          if (GemFireXDUtils.TraceStatsGeneration) {
            SanityManager.DEBUG_PRINT("warning"
                + GfxdConstants.TRACE_STATS_GENERATION,
                "Error while extracting table information " + this, e);
          }
        }
      } finally {
        if (!columnListAdded) {
          buffer.append(columnList);
        }
      }
    }

    return buffer;
  }
  
  @Override
  public void optimizeForOffHeap(boolean shouldOptimize) { 
    if(GemFireXDUtils.isOffHeapEnabled()) { 
      if(this.resultSet != null) {
        this.resultSet.optimizeForOffHeap(shouldOptimize);
      }
    }
  }
  // GemStone changes END
}
