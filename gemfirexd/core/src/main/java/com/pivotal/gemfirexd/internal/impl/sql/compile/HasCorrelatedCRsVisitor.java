/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.compile.HasCorrelatedCRsVisitor

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
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.Visitable;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.VisitorAdaptor;
import com.pivotal.gemfirexd.internal.iapi.store.access.Qualifier;

/**
 * Find out if we have an correlated column reference
 * anywhere below us.  Stop traversal as soon as we find one.
 *
 */
public class HasCorrelatedCRsVisitor extends VisitorAdaptor
{
	private boolean hasCorrelatedCRs;

	/**
	 * Construct a visitor
	 */
	public HasCorrelatedCRsVisitor()
	{
	}



	////////////////////////////////////////////////
	//
	// VISITOR INTERFACE
	//
	////////////////////////////////////////////////

	/**
	 * If we have found the target node, we are done.
	 *
	 * @param node 	the node to process
	 *
	 * @return me
	 */
	public Visitable visit(Visitable node)
	{
		if (node instanceof ColumnReference)
		{
			if (((ColumnReference)node).getCorrelated())
			{
				hasCorrelatedCRs = true;
			}
		}
		else if (node instanceof VirtualColumnNode)
		{
			if (((VirtualColumnNode)node).getCorrelated())
			{
				hasCorrelatedCRs = true;
			}
		}
		else if (node instanceof MethodCallNode)
		{
			/* trigger action references are correlated
			 */
			if (((MethodCallNode)node).getMethodName().equals("getTriggerExecutionContext") ||
				((MethodCallNode)node).getMethodName().equals("TriggerOldTransitionRows") ||
				((MethodCallNode)node).getMethodName().equals("TriggerNewTransitionRows")
			   )
			{
				hasCorrelatedCRs = true;
			}
		}
		return node;
	}

	/**
	 * Stop traversal if we found the target node
	 *
	 * @return true/false
	 */
	public boolean stopTraversal()
	{
		return hasCorrelatedCRs;
	}

	public boolean skipChildren(Visitable v)
	{
		return false;
	}

	////////////////////////////////////////////////
	//
	// CLASS INTERFACE
	//
	////////////////////////////////////////////////
	/**
	 * Indicate whether we found the node in
	 * question
	 *
	 * @return true/false
	 */
	public boolean hasCorrelatedCRs()
	{
		return hasCorrelatedCRs;
	}

	/**
	 * Shortcut to set if hasCorrelatedCRs
	 *
	 *	@param	value	true/false
	 */
	public void setHasCorrelatedCRs(boolean value)
	{
		hasCorrelatedCRs = value;
	}
// GemStone changes BEGIN

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean supportsDeltaMerge() {
	  return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean initForDeltaState() {
	  return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Object getAndResetDeltaState() {
	  return Boolean.valueOf(this.hasCorrelatedCRs);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Visitable mergeDeltaState(final Object delta,
	    final Visitable node) {
	  if (!this.hasCorrelatedCRs) {
	    this.hasCorrelatedCRs = ((Boolean)delta).booleanValue();
	  }
	  return node;
	}
// GemStone changes END
}
