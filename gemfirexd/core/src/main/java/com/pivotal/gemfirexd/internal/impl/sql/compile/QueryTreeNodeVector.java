/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.compile.QueryTreeNodeVector

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

import com.gemstone.gnu.trove.THashMap;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.QueryInfo;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.QueryInfoContext;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.ValueQueryInfo;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.Visitable;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.Visitor;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Vector;

/**
 * QueryTreeNodeVector is the root class for all lists of query tree nodes.
 * It provides a wrapper for java.util.Vector. All
 * lists of query tree nodes inherit from QueryTreeNodeVector.
 *
 */

public abstract class QueryTreeNodeVector extends QueryTreeNode
// GemStone changes BEGIN
implements Iterable<QueryTreeNode>
{
        /* (original code)
{
	private Vector			v = new Vector();
	*/
	private ArrayList<QueryTreeNode> v = new ArrayList<QueryTreeNode>();
// GemStone changes END

	public final int size()
	{
		return v.size();
	}

// GemStone changes BEGIN
	public final Iterator<QueryTreeNode> iterator() {
	  return this.v.iterator();
	}

	public final
// GemStone changes END
	QueryTreeNode elementAt(int index)
	{
// GemStone changes END
		return v.get(index);
		/* (original code)
		return (QueryTreeNode) v.elementAt(index);
		*/
// GemStone changes END
	}

	public final void addElement(QueryTreeNode qt)
	{
// GemStone changes BEGIN
		v.add(qt);
		if (this.visitedMap != null) {
		  this.visitedMap.clear();
		}
		/* (original code)
		v.addElement(qt);
		*/
// GemStone changes END
	}

	final void removeElementAt(int index)
	{
// GemStone changes BEGIN
		v.remove(index);
		if (this.visitedMap != null) {
		  this.visitedMap.clear();
		}
		/* (original code)
		v.removeElementAt(index);
		*/
// GemStone changes END
	}

	final void removeElement(QueryTreeNode qt)
	{
// GemStone changes BEGIN
		v.remove(qt);
		if (this.visitedMap != null) {
		  this.visitedMap.clear();
		}
		/* (original code)
		v.removeElement(qt);
		*/
// GemStone changes END
	}

	final Object remove(int index)
	{
// GemStone changes BEGIN
	  if (this.visitedMap != null) {
	    this.visitedMap.clear();
	  }
		return v.remove(index);
		/* (original code)
		return((QueryTreeNode) (v.remove(index)));
		*/
// GemStone changes END
	}

	final int indexOf(QueryTreeNode qt)
	{
		return v.indexOf(qt);
	}

	final void setElementAt(QueryTreeNode qt, int index)
	{
// GemStone changes BEGIN
		v.set(index, qt);
		if (this.visitedMap != null) {
		  this.visitedMap.clear();
		}
		/* (original code)
		v.setElementAt(qt, index);
		*/
// GemStone changes END
	}

// GemStone changes BEGIN
	final
// GemStone changes END
	void destructiveAppend(QueryTreeNodeVector qtnv)
	{
		nondestructiveAppend(qtnv);
		qtnv.removeAllElements();
// GemStone changes BEGIN
		if (this.visitedMap != null) {
		  this.visitedMap.clear();
		}
// GemStone changes END
	}

// GemStone changes BEGIN
	final
// GemStone changes END
	void nondestructiveAppend(QueryTreeNodeVector qtnv)
	{
		int qtnvSize = qtnv.size();
		for (int index = 0; index < qtnvSize; index++)
		{
// GemStone changes BEGIN
			v.add(qtnv.elementAt(index));
			/* (original code)
			v.addElement(qtnv.elementAt(index));
			*/
// GemStone changes END
		}
// GemStone changes BEGIN
		if (this.visitedMap != null) {
		  this.visitedMap.clear();
		}
// GemStone changes END
	}
// GemStone changes BEGIN
	final public void removeAllElements()
// GemStone changes END
	{
// GemStone changes BEGIN
		v.clear();
		if (this.visitedMap != null) {
		  // removeAll invocation indicates that visitedMap
		  // will not be used anymore
		  this.visitedMap = null;
		}
		/* (original code)
		v.removeAllElements();
		*/
// GemStone changes END
	}

	final public void insertElementAt(QueryTreeNode qt, int index)
	{
// GemStone changes BEGIN
		v.add(index, qt);
		if (this.visitedMap != null) {
		  this.visitedMap.clear();
		}
		/* (original code)
		v.insertElementAt(qt, index);
		*/
// GemStone changes END
	}

	/**
	 * Format this list as a string
	 *
	 * We can simply iterate through the list.  Note each list member
	 * is a QueryTreeNode, and so should have its specialization of
	 * toString defined.
	 *
	 * @return	This list formatted as a String
	 */
	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			StringBuilder	buffer = new StringBuilder("");

			for (int index = 0; index < size(); index++)
			{
				buffer.append(elementAt(index).toString()).append("; ");
			}

			return buffer.toString();
		}
		else
		{
			return "";
		}
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
// GemStone changes BEGIN
	  // if we have recorded previous visit of this visitor, then use
	  // the cached value
	  final Object delta;
	  boolean deltaRecord = false;
	  if (v.supportsDeltaMerge()) {
	    if (this.visitedMap == null) {
	      this.visitedMap = new THashMap();
	    }
	    else if (this.visitedMap.size() > 0
	        && (delta = this.visitedMap.get(v.getClass())) != null) {
	      return v.mergeDeltaState(delta, this);
	    }
	    deltaRecord = v.initForDeltaState();
	  }
// GemStone changes END
		Visitable		returnNode = v.visit(this);
	
		if (v.skipChildren(this))
		{
// GemStone changes BEGIN
		  
			// if delta is being recorded, then cache it
			if (deltaRecord) {
			  this.visitedMap.put(v.getClass(),
			      v.getAndResetDeltaState());
			}
// GemStone changes END
			return returnNode;
		}

// GemStone changes BEGIN
		final int size = size();
		for (int index = 0; index < size; index++) {
		  final QueryTreeNode qn0 = elementAt(index);
		  final QueryTreeNode qn = (QueryTreeNode)qn0.accept(v);
		  if (qn != qn0) {
		    setElementAt(qn, index);
		  }
		}
		if (deltaRecord) {
		  this.visitedMap.put(v.getClass(), v.getAndResetDeltaState());
		}
		/* (original code)
		int size = size();
		for (int index = 0; index < size; index++)
		{
			setElementAt((QueryTreeNode)((QueryTreeNode) elementAt(index)).accept(v), index);
		}
		*/
// GemStone changes END
		
		return returnNode;
	}
// GemStone changes BEGIN
            private THashMap visitedMap;

            //Asif
            /**
             * Populates the QueryInfo array with the QueryInfo objects corresponding to the
             * parameters/constant values present in the IN List and returns a boolean true if any of the
             * value present in the List is a parameter , indicating a dynamic query.
             * If there is atleast one node which is not of type ValueQueryInfo , return null.
             */
            public Boolean populateQueryInfoArray(QueryInfoContext qic,
                ValueQueryInfo[] qiArr) throws StandardException {
              int size = qiArr.length;
              boolean isDynamic = false;
              QueryInfo temp;
              for(int i = 0; i < size; ++i) {
                temp = ((QueryTreeNode)v.get(i)).computeQueryInfo(qic);
                if(  !(temp instanceof ValueQueryInfo)) {
                  return null;
                }
                qiArr[i] = (ValueQueryInfo)temp;
                isDynamic = isDynamic || temp.isDynamic(); 
              }              
              return Boolean.valueOf(isDynamic);
            }
// GemStone changes END
}
