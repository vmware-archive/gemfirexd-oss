/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.compile.GroupByList

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







import com.pivotal.gemfirexd.internal.engine.sql.compile.VerifySysfunctionExpressionVisitor;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.Limits;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.C_NodeTypes;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.CostEstimate;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.RequiredRowOrdering;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.RowOrdering;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDictionary;
import com.pivotal.gemfirexd.internal.iapi.store.access.SortCostController;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.util.JBitSet;
import com.pivotal.gemfirexd.internal.iapi.util.ReuseFactory;
import com.pivotal.gemfirexd.internal.impl.sql.compile.ActivationClassBuilder;

import java.util.Vector;

/**
 * A GroupByList represents the list of expressions in a GROUP BY clause in
 * a SELECT statement.
 *
 */
//GemStone changes BEGIN
//public class GroupByList extends OrderedColumnList 
public class GroupByList extends OrderedColumnList implements RequiredRowOrdering

{
	int		numGroupingColsAdded = 0;
	
	private boolean sortNeeded = true;
	private Object[] resultRow;
	private ResultSetNode resultToSort;
	private SortCostController scc;
	private int estimatedRowSize;

	/**
		Add a column to the list

		@param column	The column to add to the list
	 */
	public void addGroupByColumn(GroupByColumn column)
	{
		addElement(column);
	}

	/**
		Get a column from the list

		@param position	The column to get from the list
	 */
	public GroupByColumn getGroupByColumn(int position)
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(position >=0 && position < size(),
					"position (" + position +
					") expected to be between 0 and " + size());
		}
		return (GroupByColumn) elementAt(position);
	}

	/**
		Print the list.

		@param depth		The depth at which to indent the sub-nodes
	 */
	public void printSubNodes(int depth)
	{
		if (SanityManager.DEBUG)
		{
			for (int index = 0; index < size(); index++)
			{
				( (GroupByColumn) elementAt(index) ).treePrint(depth);
			}
		}
	}

	/**
	 * Get the number of grouping columns that need to be added to the SELECT list.
	 *
	 * @return int	The number of grouping columns that need to be added to
	 *				the SELECT list.
	 */
	public int getNumNeedToAddGroupingCols()
	{
		return numGroupingColsAdded;
	}

	/**
	 *  Bind the group by list.  Verify:
	 *		o  Number of grouping columns matches number of non-aggregates in
	 *		   SELECT's RCL.
	 *		o  Names in the group by list are unique
	 *		o  Names of grouping columns match names of non-aggregate
	 *		   expressions in SELECT's RCL.
	 *
	 * @param select		The SelectNode
	 * @param aggregateVector	The aggregate vector being built as we find AggregateNodes
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void bindGroupByColumns(SelectNode select,
								   Vector	aggregateVector)
					throws StandardException
	{
		FromList		 fromList = select.getFromList();
		ResultColumnList selectRCL = select.getResultColumns();
		SubqueryList	 dummySubqueryList =
									(SubqueryList) getNodeFactory().getNode(
													C_NodeTypes.SUBQUERY_LIST,
													getContextManager());
		//GemStone changes BEGIN
		final VerifySysfunctionExpressionVisitor checkJavaValueNode = new VerifySysfunctionExpressionVisitor();
                //GemStone changes END
		int				 numColsAddedHere = 0;
		int				 size = size();

		/* Only 32677 columns allowed in GROUP BY clause */
		if (size > Limits.DB2_MAX_ELEMENTS_IN_GROUP_BY)
		{
			throw StandardException.newException(SQLState.LANG_TOO_MANY_ELEMENTS,
			    Limits.DB2_MAX_ELEMENTS_IN_GROUP_BY /* GemStoneAddition */);
		}

		/* Bind the grouping column */
		for (int index = 0; index < size; index++)
		{
			GroupByColumn groupByCol = (GroupByColumn) elementAt(index);
			groupByCol.bindExpression(fromList,
									  dummySubqueryList, aggregateVector);
		}

		
		int				rclSize = selectRCL.size();
		for (int index = 0; index < size; index++)
		{
			boolean				matchFound = false;
			GroupByColumn		groupingCol = (GroupByColumn) elementAt(index);

			/* Verify that this entry in the GROUP BY list matches a
			 * grouping column in the select list.
			 */
			for (int inner = 0; inner < rclSize; inner++)
			{
				ResultColumn selectListRC = (ResultColumn) selectRCL.elementAt(inner);
//GemStone changes BEGIN
//  supporting expression in GroupBy
//				if (!(selectListRC.getExpression() instanceof ColumnReference)) {
//					continue;
//				}
//				
//				ColumnReference selectListCR = (ColumnReference) selectListRC.getExpression();

                                if (selectListRC.getExpression().isEquivalent(groupingCol.getColumnExpression())) { 
//                                if (selectListCR.isEquivalent(groupingCol.getColumnExpression())) { 
//GemStone changes END
					/* Column positions for grouping columns are 0-based */
					groupingCol.setColumnPosition(inner + 1);

					/* Mark the RC in the SELECT list as a grouping column */
					selectListRC.markAsGroupingColumn();
					matchFound = true;
					break;
				}
			}
			/* If no match found in the SELECT list, then add a matching
			 * ResultColumn/ColumnReference pair to the SelectNode's RCL.
			 * However, don't add additional result columns if the query
			 * specified DISTINCT, because distinct processing considers
			 * the entire RCL and including extra columns could change the
			 * results: e.g. select distinct a,b from t group by a,b,c
			 * should not consider column c in distinct processing (DERBY-3613)
			 */
			if (! matchFound && !select.hasDistinct() 
			    //GemStone changes BEGIN
			    //disable the following check as DERBY-883 is resolved in GroupByNode#addUnAggColumns()
			    //&& groupingCol.getColumnExpression() instanceof ColumnReference
			    ) 
			{
			  ValueNode groupingExpr = groupingCol.getColumnExpression(); 
                          //GemStone changes END
			    	// only add matching columns for column references not 
			    	// expressions yet. See DERBY-883 for details. 
				ResultColumn newRC;

				/* Get a new ResultColumn */
				newRC = (ResultColumn) getNodeFactory().getNode(
								C_NodeTypes.RESULT_COLUMN,
								groupingCol.getColumnName(),
								//GemStone changes BEGIN
								groupingExpr instanceof ColumnReference ? 
								   groupingCol.getColumnExpression().getClone() :
								     groupingExpr,
								//GemStone changes END
								getContextManager());
				newRC.setVirtualColumnId(selectRCL.size() + 1);
				newRC.markGenerated();
				newRC.markAsGroupingColumn();

				/* Add the new RC/CR to the RCL */
				selectRCL.addElement(newRC);

				/* Set the columnPosition in the GroupByColumn, now that it
				* has a matching entry in the SELECT list.
				*/
				groupingCol.setColumnPosition(selectRCL.size());
				
				// a new hidden or generated column is added to this RCL
				// i.e. that the size() of the RCL != visibleSize(). 
				// Error checking done later should be aware of this 
				// special case.
				selectRCL.setCountMismatchAllowed(true);

				/*
				** Track the number of columns that we have added
				** in this routine.  We track this separately
				** than the total number of columns added by this
				** object (numGroupingColsAdded) because we
				** might be bound (though not gagged) more than
				** once (in which case numGroupingColsAdded will
				** already be set).
				*/
				numColsAddedHere++;
			}
			if (groupingCol.getColumnExpression() instanceof JavaToSQLValueNode) 
			{
			  //GemStone changes BEGIN
			  //fix for #42854
			  groupingCol.getColumnExpression().accept(checkJavaValueNode);
			  if(!checkJavaValueNode.hasSysFun())
			  //GemStone changes END
				// disallow any expression which involves native java computation. 
				// Not possible to consider java expressions for equivalence.
				throw StandardException.newException(					
						SQLState.LANG_INVALID_GROUPED_SELECT_LIST);
			}
		}

		/* Verify that no subqueries got added to the dummy list */
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(dummySubqueryList.size() == 0,
				"dummySubqueryList.size() is expected to be 0");
		}

		numGroupingColsAdded+= numColsAddedHere;
		
		this.resultToSort = select;
	}

	

	/**
	 * Find the matching grouping column if any for the given expression
	 * 
	 * @param node an expression for which we are trying to find a match
	 * in the group by list.
	 * 
	 * @return the matching GroupByColumn if one exists, null otherwise.
	 * 
	 * @throws StandardException
	 */
	public GroupByColumn findGroupingColumn(ValueNode node)
	        throws StandardException
	{
		int sz = size();
		for (int i = 0; i < sz; i++) 
		{
			GroupByColumn gbc = (GroupByColumn)elementAt(i);
			if (gbc.getColumnExpression().isEquivalent(node))
			{
				return gbc;
			}
		}
		return null;
	}
	
	/**
	 * Remap all ColumnReferences in this tree to be clones of the
	 * underlying expression.
	 *
	 * @exception StandardException			Thrown on error
	 */
	public void remapColumnReferencesToExpressions() throws StandardException
	{
		GroupByColumn	gbc;
		int				size = size();

		/* This method is called when flattening a FromTable.  We should
		 * not be flattening a FromTable if the underlying expression that
		 * will get returned out, after chopping out the redundant ResultColumns,
		 * is not a ColumnReference.  (See ASSERT below.)
		 */
		for (int index = 0; index < size; index++)
		{
			ValueNode	retVN;
			gbc = (GroupByColumn) elementAt(index);

			retVN = gbc.getColumnExpression().remapColumnReferencesToExpressions();

			if (SanityManager.DEBUG)
			{
				SanityManager.ASSERT(retVN instanceof ColumnReference,
					"retVN expected to be instanceof ColumnReference, not " +
					retVN.getClass().getName());
			}

			gbc.setColumnExpression(retVN);
		}
	}

	/**
	 * Print it out, baby
	 */
	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			StringBuilder	buf = new StringBuilder();

			for (int index = 0; index < size(); index++)
			{
				GroupByColumn	groupingCol = (GroupByColumn) elementAt(index);

				buf.append(groupingCol.toString());
			}
			return buf.toString();
		}
		else
		{
			return "";
		}
	}

	public void preprocess(
			int numTables, FromList fromList, SubqueryList whereSubquerys, 
			PredicateList wherePredicates) throws StandardException 
	{
		for (int index = 0; index < size(); index++)
		{
			GroupByColumn	groupingCol = (GroupByColumn) elementAt(index);
			groupingCol.setColumnExpression(
					groupingCol.getColumnExpression().preprocess(
							numTables, fromList, whereSubquerys, wherePredicates));
		}		
	}

  // GemStone changes BEGIN
  public boolean isDSIDOnly() throws StandardException {
    if (size() != 1) {
      return false;
    }

    GroupByColumn groupingCol = (GroupByColumn)elementAt(0);

    return groupingCol.hasDSID();
  }
  // GemStone changes END

  @Override
  public int sortRequired(RowOrdering rowOrdering) throws StandardException {
    return this.sortRequired(rowOrdering, null);
  }

  @Override
  public int sortRequired(RowOrdering rowOrdering, JBitSet tableMap)
      throws StandardException {

    int position = 0;
    int size = size();
    for (int loc = 0; loc < size; loc++)
    {
            GroupByColumn gbc = getGroupByColumn(loc);


            ValueNode expr = gbc.getColumnExpression();

            if ( ! (expr instanceof ColumnReference))
            {
                    return RequiredRowOrdering.SORT_REQUIRED;
            }

            ColumnReference cr = (ColumnReference) expr;

            /*
            ** Check whether the table referred to is in the table map (if any).
            ** If it isn't, we may have an ordering that does not require
            ** sorting for the tables in a partial join order.  Look for
            ** columns beyond this column to see whether a referenced table
            ** is found - if so, sorting is required (for example, in a
            ** case like ORDER BY S.A, T.B, S.C, sorting is required).
            */
            if (tableMap != null)
            {
                    if ( ! tableMap.get(cr.getTableNumber()))
                    {
                            /* Table not in partial join order */
                            for (int remainingPosition = loc + 1;
                                     remainingPosition < size();
                                     remainingPosition++)
                            {
                                    GroupByColumn remaininggbc = getGroupByColumn(loc);

                                    
                                    ValueNode remainingexpr = remaininggbc.getColumnExpression();

                                    if (remainingexpr instanceof ColumnReference)
                                    {
                                            ColumnReference remainingcr =
                                                                            (ColumnReference) remainingexpr;
                                            if (tableMap.get(remainingcr.getTableNumber()))
                                            {
                                                    return RequiredRowOrdering.SORT_REQUIRED;
                                            }
                                    }
                            }

                            return RequiredRowOrdering.NOTHING_REQUIRED;
                    }
            }

            if ( ! rowOrdering.alwaysOrdered(cr.getTableNumber()))
            {
                    /*
                    ** Check whether the ordering is ordered on this column in
                    ** this position.
                    */
                    if ( !( rowOrdering.orderedOnColumn(
                            
                                                    RowOrdering.ASCENDING ,
                            position,
                            cr.getTableNumber(),
                            cr.getColumnNumber()
                            ) || rowOrdering.orderedOnColumn(
                                       RowOrdering.DESCENDING , position,  cr.getTableNumber(),
                                       cr.getColumnNumber()
                                    ) ))
                    {
                            return RequiredRowOrdering.SORT_REQUIRED;
                    }

                    /*
                    ** The position to ask about is for the columns in tables
                    ** that are *not* always ordered.  The always-ordered tables
                    ** are not counted as part of the list of ordered columns
                    */
                    position++;
            }
    }

    return RequiredRowOrdering.NOTHING_REQUIRED;

  }
  public void setTargetResultSet(ResultSetNode target) {
    this.resultToSort = target;
  }

  @Override
  public void estimateCost(double estimatedInputRows, RowOrdering rowOrdering,
      CostEstimate resultCost) throws StandardException {
    
    /*
     ** Do a bunch of set-up the first time: get the SortCostController,
     ** the template row, the ColumnOrdering array, and the estimated
     ** row size.
     */
     if (scc == null)
     {
             scc = getCompilerContext().getSortCostController();

             resultRow =
                     resultToSort.getResultColumns().buildEmptyRow().getRowArray();
            
             estimatedRowSize =
                                     resultToSort.getResultColumns().getTotalColumnSize();
     }

     long inputRows = (long) estimatedInputRows;
     long exportRows = inputRows;
     double sortCost;

     sortCost = scc.getSortCost(
                                                             (DataValueDescriptor[]) resultRow,
                                                             getColumnOrdering(),
                                                             false,
                                                             inputRows,
                                                             exportRows,
                                                             estimatedRowSize
                                                             );

     resultCost.setCost(sortCost, estimatedInputRows, estimatedInputRows);
  }

  @Override
  public void sortNeeded() {
    this.sortNeeded = true;
    
  }

  @Override
  public void sortNotNeeded() {
    // TODO Auto-generated method stub
    this.sortNeeded = false;
  }

  @Override
  public boolean getSortNeeded() {
    
    return this.sortNeeded;
  }
}

//GemStone changes END
