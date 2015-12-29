/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.compile.GroupByColumn

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





import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.Visitable;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.Visitor;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.VisitorAdaptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDictionary;
import com.pivotal.gemfirexd.internal.iapi.types.TypeId;

import java.util.Vector;

/**
 * A GroupByColumn is a column in the GROUP BY clause.
 *
 */
public class GroupByColumn extends OrderedColumn 
{
	private ValueNode columnExpression;
	
	/**
	 * Initializer.
	 *
	 * @param colRef	The ColumnReference for the grouping column
	 */
	public void init(Object colRef) 
	{
		this.columnExpression = (ValueNode)colRef;
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
			return "Column Expression: "+columnExpression+super.toString();
		}
		else
		{
			return "";
		}
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

			if (columnExpression != null)
			{
				printLabel(depth, "colRef: ");
				columnExpression.treePrint(depth + 1);
			}
		}
	}

	/**
	 * Get the name of this column
	 *
	 * @return	The name of this column
	 */
	public String getColumnName() 
	{
		return columnExpression.getColumnName();
	}

	/**
	 * Bind this grouping column.
	 *
	 * @param fromList			The FROM list to use for binding
	 * @param subqueryList		The SubqueryList we are building as we hit
	 *							SubqueryNodes.
	 * @param aggregateVector	The aggregate vector we build as we hit 
	 *							AggregateNodes.
	 *
	 * @exception StandardException	Thrown on error
	 */

	public void bindExpression(
			FromList fromList, 
			SubqueryList subqueryList,
			Vector	aggregateVector) 
				throws StandardException
	{
		/* Bind the ColumnReference to the FromList */
		columnExpression = (ValueNode) columnExpression.bindExpression(fromList,
							  subqueryList,
							  aggregateVector);

		// Verify that we can group on the column
		if (columnExpression.isParameterNode()) 
		{
			throw StandardException.newException(SQLState.LANG_INVALID_COL_REF_GROUPED_SELECT_LIST,
					columnExpression);
		}
		/*
		 * Do not check to see if we can map user types
		 * to built-in types.  The ability to do so does
		 * not mean that ordering will work.  In fact,
		 * as of version 2.0, ordering does not work on
		 * user types.
		 */
		TypeId ctid = columnExpression.getTypeId();
		if (! ctid.orderable(getClassFactory()))
		{
			throw StandardException.newException(SQLState.LANG_COLUMN_NOT_ORDERABLE_DURING_EXECUTION, 
							ctid.getSQLTypeName());
		}
	}

	public ValueNode getColumnExpression() 
	{
		return columnExpression;
	}

	public void setColumnExpression(ValueNode cexpr) 
	{
		this.columnExpression = cexpr;
		
	}
	
	// GemStone changes BEGIN
        public boolean hasDSID() throws StandardException {
          final boolean hasDSID[] = new boolean[] { false };
      
          final VisitorAdaptor v = new VisitorAdaptor() {
            private int numColumns = 0;
      
            @Override
            public Visitable visit(Visitable node) throws StandardException {
              if (node instanceof StaticMethodCallNode) {
                hasDSID[0] = (((StaticMethodCallNode)node).hasDSID() && numColumns == 0);
              }
              else if (node instanceof ColumnReference
                  || node instanceof VirtualColumnNode
                  || node instanceof BaseColumnNode) {
                numColumns++;
              }
              return node;
            }
      
            @Override
            public boolean stopTraversal() {
              return hasDSID[0] || numColumns != 0;
            }
      
            @Override
            public boolean skipChildren(Visitable node) throws StandardException {
              return hasDSID[0] || numColumns != 0;
            }
          };
      
          this.columnExpression.accept(v);
      
          return hasDSID[0];
        }
        
        @Override
        ResultColumn getResultColumn()
        {
                return this.columnExpression.getSourceResultColumn();
        }
        // GemStone changes END
}
