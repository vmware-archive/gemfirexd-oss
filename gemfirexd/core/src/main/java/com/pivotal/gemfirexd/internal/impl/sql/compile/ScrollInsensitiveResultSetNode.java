/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.compile.ScrollInsensitiveResultSetNode

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

package	com.pivotal.gemfirexd.internal.impl.sql.compile;









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
 * A ScrollInsensitiveResultSetNode represents the insensitive scrolling cursor
 * functionality for any 
 * child result set that needs one.
 *
 */

public class ScrollInsensitiveResultSetNode  extends SingleChildResultSetNode
{
	/**
	 * Initializer for a ScrollInsensitiveResultSetNode.
	 *
	 * @param childResult	The child ResultSetNode
	 * @param rcl			The RCL for the node
	 * @param tableProperties	Properties list associated with the table
	 */

	public void init(
							Object childResult,
							Object rcl,
							Object tableProperties)
	{
		init(childResult, tableProperties);
		resultColumns = (ResultColumnList) rcl;
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

		int erdNumber = acb.addItem(makeResultDescription());

		acb.pushGetResultSetFactoryExpression(mb);

		childResult.generate(acb, mb);
		acb.pushThisAsActivation(mb);
		mb.push(resultSetNumber);
		mb.push(resultColumns.size());

		mb.pushThis();
		mb.callMethod(VMOpcode.INVOKEVIRTUAL, ClassName.BaseActivation, "getScrollable",
						"boolean", 0);

		mb.push(costEstimate.rowCount());
		mb.push(costEstimate.getEstimatedCost());

		mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getScrollInsensitiveResultSet",
						ClassName.NoPutResultSet, 7);
	}
}
