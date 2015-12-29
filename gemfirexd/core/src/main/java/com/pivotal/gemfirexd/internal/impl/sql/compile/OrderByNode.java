/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.compile.OrderByNode

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

import com.pivotal.gemfirexd.internal.engine.distributed.metadata.OrderByQueryInfo;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.QueryInfo;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.QueryInfoContext;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.TableQueryInfo;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.compiler.MethodBuilder;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultColumnDescriptor;

/**
 * An OrderByNode represents a result set for a sort operation
 * for an order by list.  It is expected to only be generated at 
 * the end of optimization, once we have determined that a sort
 * is required.
 *
 */
public class OrderByNode extends SingleChildResultSetNode
{

	OrderByList		orderByList;

	/**
	 * Initializer for a OrderByNode.
	 *
	 * @param childResult	The child ResultSetNode
	 * @param orderByList	The order by list.
 	 * @param tableProperties	Properties list associated with the table
    *
	 * @exception StandardException		Thrown on error
	 */
	public void init(
						Object childResult,
						Object orderByList,
						Object tableProperties)
		throws StandardException
	{
		ResultSetNode child = (ResultSetNode) childResult;

		super.init(childResult, tableProperties);

		this.orderByList = (OrderByList) orderByList;

		ResultColumnList prRCList;

		/*
			We want our own resultColumns, which are virtual columns
			pointing to the child result's columns.

			We have to have the original object in the distinct node,
			and give the underlying project the copy.
		 */

		/* We get a shallow copy of the ResultColumnList and its 
		 * ResultColumns.  (Copy maintains ResultColumn.expression for now.)
		 */
		prRCList = child.getResultColumns().copyListAndObjects();
		resultColumns = child.getResultColumns();
		child.setResultColumns(prRCList);

		/* Replace ResultColumn.expression with new VirtualColumnNodes
		 * in the DistinctNode's RCL.  (VirtualColumnNodes include
		 * pointers to source ResultSetNode, this, and source ResultColumn.)
		 */
		resultColumns.genVirtualColumnNodes(this, prRCList);
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
			return childResult.toString() + "\n" + 
				"orderByList: " + 
				(orderByList != null ? orderByList.toString() : "null") + "\n" +
				super.toString();
		}
		else
		{
			return "";
		}
	}

	ResultColumnDescriptor[] makeResultDescriptors()
	{
	    return childResult.makeResultDescriptors();
	}

    /**
     * generate the distinct result set operating over the source
	 * resultset.
     *
	 * @exception StandardException		Thrown on error
     */
	public void generate(ActivationClassBuilder acb,
								MethodBuilder mb)
							throws StandardException
	{
		// Get the cost estimate for the child
		if (costEstimate == null)
		{
			costEstimate = childResult.getFinalCostEstimate();
		}

	    orderByList.generate(acb, mb, childResult);
	}
  
//GemStone changes BEGIN        
  /**
   * Returns the QueryInfo object reprsenting the queries order by
   * list.
   */
   
  @Override
  public QueryInfo computeQueryInfo(QueryInfoContext qic) throws StandardException {
    return new OrderByQueryInfo(qic, orderByList, resultColumns, 
                                                 (qic.getParentPRN() != null ? 
                                                    qic.getParentPRN().getResultColumns()
                                                     : null) );
  }

  @Override
  public ResultSetNode getInnerMostPRN() {
    return childResult.getInnerMostPRN();
  }
  
//TODO :Asif : Check if this can be optimized for offheap
  @Override
  protected void optimizeForOffHeap( boolean shouldOptimize) {
    shouldOptimize = shouldOptimize && !this.orderByList.getSortNeeded();
    this.childResult.optimizeForOffHeap(shouldOptimize);
  }
//GemStone changes END
  
}
