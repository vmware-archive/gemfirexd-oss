/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.compile.NormalizeResultSetNode

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









import com.pivotal.gemfirexd.internal.engine.distributed.metadata.QueryInfo;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.QueryInfoContext;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.SelectQueryInfo;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.ClassName;
import com.pivotal.gemfirexd.internal.iapi.services.classfile.VMOpcode;
import com.pivotal.gemfirexd.internal.iapi.services.compiler.MethodBuilder;
import com.pivotal.gemfirexd.internal.iapi.services.context.ContextManager;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultSet;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.CostEstimate;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.Optimizable;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.OptimizableList;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.OptimizablePredicate;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.OptimizablePredicateList;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.Optimizer;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.RequiredRowOrdering;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.Visitable;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.Visitor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDictionary;
import com.pivotal.gemfirexd.internal.iapi.util.JBitSet;
import com.pivotal.gemfirexd.internal.impl.sql.compile.ActivationClassBuilder;

import java.util.Properties;

/**
 * A NormalizeResultSetNode represents a normalization result set for any 
 * child result set that needs one.
 *
 */

public class NormalizeResultSetNode extends SingleChildResultSetNode
{
	/**
	 * this indicates if the normalize is being performed for an Update
	 * statement or not. The row passed to update also has
	 * before values of the columns being updated-- we need not 
	 * normalize these values. 
	 */
	private boolean forUpdate;

	/**
	 * Initializer for a NormalizeResultSetNode.
	 *
	 * @param childResult	The child ResultSetNode
	 * @param rcl			The RCL for the node
	 * @param tableProperties	Properties list associated with the table
	 * @param forUpdate 	tells us if the normalize operation is being
	 * performed on behalf of an update statement. 
	 */

	public void init(
							Object childResult,
							Object rcl,
							Object tableProperties,
							Object forUpdate)
	{
		super.init(childResult, tableProperties);
		resultColumns = (ResultColumnList) rcl;
		this.forUpdate = ((Boolean)forUpdate).booleanValue();
	}


    /**
     *
	 *
	 * @exception StandardException		Thrown on error
     */
	public void generate(ActivationClassBuilder acb,
								MethodBuilder mb)
							throws StandardException
	{
		int				erdNumber;

		if (SanityManager.DEBUG)
        SanityManager.ASSERT(resultColumns != null, "Tree structure bad");

		/* Get the next ResultSet #, so that we can number this ResultSetNode, its
		 * ResultColumnList and ResultSet.
		 */
		assignResultSetNumber();

		// build up the tree.

		// Generate the child ResultSet

		// Get the cost estimate for the child
		costEstimate = childResult.getFinalCostEstimate();

		erdNumber = acb.addItem(makeResultDescription());

		acb.pushGetResultSetFactoryExpression(mb);
		childResult.generate(acb, mb);
		mb.push(resultSetNumber);
		mb.push(erdNumber);
		mb.push(costEstimate.rowCount());
		mb.push(costEstimate.getEstimatedCost());
		mb.push(forUpdate);

		mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getNormalizeResultSet",
					ClassName.NoPutResultSet, 6);
	}

	/**
	 * set the Information gathered from the parent table that is 
	 * required to peform a referential action on dependent table.
	 */
	public void setRefActionInfo(long fkIndexConglomId, 
								 int[]fkColArray, 
								 String parentResultSetId,
								 boolean dependentScan)
	{
		childResult.setRefActionInfo(fkIndexConglomId,
								   fkColArray,
								   parentResultSetId,
								   dependentScan);
	}

	@Override
	public QueryInfo computeQueryInfo(QueryInfoContext qic)
	throws StandardException {
	  return childResult.computeQueryInfo(qic);	
	}
	
        //GemStone changes BEGIN
        @Override
        public QueryInfo computeSubSelectQueryInfo(QueryInfoContext qic) 
            throws StandardException 
        {
              return childResult.computeSubSelectQueryInfo(qic); 
        }
        //GemStone changes END
}
