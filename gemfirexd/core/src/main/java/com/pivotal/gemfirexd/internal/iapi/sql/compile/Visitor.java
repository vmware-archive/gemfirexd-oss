/*

   Derby - Class com.pivotal.gemfirexd.internal.iapi.sql.compile.Visitor

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

package	com.pivotal.gemfirexd.internal.iapi.sql.compile;

import com.pivotal.gemfirexd.internal.iapi.error.StandardException;

/**
 * A visitor is an object that traverses the querytree
 * and performs some action. 
 *
 */
public interface Visitor
{
	/**
	 * This is the default visit operation on a 
	 * QueryTreeNode.  It just returns the node.  This
	 * will typically suffice as the default visit 
	 * operation for most visitors unless the visitor 
	 * needs to count the number of nodes visited or 
	 * something like that.
	 * <p>
	 * Visitors will overload this method by implementing
	 * a version with a signature that matches a specific
	 * type of node.  For example, if I want to do
	 * something special with aggregate nodes, then
	 * that Visitor will implement a 
	 * 		<I> visit(AggregateNode node)</I>
	 * method which does the aggregate specific processing.
	 *
	 * @param node 	the node to process
	 *
	 * @return a query tree node.  Often times this is
	 * the same node that was passed in, but Visitors that
	 * replace nodes with other nodes will use this to
	 * return the new replacement node.
	 *
	 * @exception StandardException may be throw an error
	 *	as needed by the visitor (i.e. may be a normal error
	 *	if a particular node is found, e.g. if checking 
	 *	a group by, we don't expect to find any ColumnReferences
	 *	that aren't under an AggregateNode -- the easiest
	 *	thing to do is just throw an error when we find the
	 *	questionable node).
	 */
	Visitable visit(Visitable node)
		throws StandardException;

	/**
	 * Method that is called to see
	 * if query tree traversal should be
	 * stopped before visiting all nodes.
	 * Useful for short circuiting traversal
	 * if we already know we are done.
	 *
	 * @return true/false
	 */
	boolean stopTraversal();

	/**
	 * Method that is called to indicate whether
	 * we should skip all nodes below this node
	 * for traversal.  Useful if we want to effectively
	 * ignore/prune all branches under a particular 
	 * node.  
	 * <p>
	 * Differs from stopTraversal() in that it
	 * only affects subtrees, rather than the
	 * entire traversal.
	 *
	 * @param node 	the node to process
	 * 
	 * @return true/false
	 */
	boolean skipChildren(Visitable node) throws StandardException;
// GemStone changes BEGIN

	/**
	 * return true if a merge of a delta using
	 * {@link #mergeDeltaState(Object, Visitable)} is possible
	 * for this visitor
	 */
	boolean supportsDeltaMerge();

	/**
	 * tell the visitor to record the changes in the current subtree
	 * separately; doesn't do anything if this is already invoked
	 * for a parent i.e. this is expected to get the delta only
	 * for one level of subtree; returns true if the intialization
	 * is possible and false otherwise
	 */
	boolean initForDeltaState();

	/**
	 * get the delta state for the subtree initialized via
	 * {@link #initForDeltaState()}
	 */
	Object getAndResetDeltaState();

	/**
	 * merge a previous delta recorded using
	 * {@link #getAndResetDeltaState()} into current state
	 */
	Visitable mergeDeltaState(Object delta, Visitable node);
// GemStone changes END
}	
