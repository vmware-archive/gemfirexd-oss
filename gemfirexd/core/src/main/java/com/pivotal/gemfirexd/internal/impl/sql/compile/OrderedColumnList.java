/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.compile.OrderedColumnList

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


import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.store.access.ColumnOrdering;
import com.pivotal.gemfirexd.internal.impl.sql.execute.IndexColumnOrder;

import java.util.Hashtable;

/**
 * List of OrderedColumns
 *
 */
public abstract class OrderedColumnList extends QueryTreeNodeVector
{
	/**
	 * Get an array of ColumnOrderings to pass to the store
	 */
	public IndexColumnOrder[] getColumnOrdering()
	{
		IndexColumnOrder[] ordering;
		int numCols = size();
		int actualCols;

		ordering = new IndexColumnOrder[numCols];

		/*
			order by is fun, in that we need to ensure
			there are no duplicates in the list.  later copies
			of an earlier entry are considered purely redundant,
			they won't affect the result, so we can drop them.
			We don't know how many columns are in the source,
			so we use a hashtable for lookup of the positions
		*/
		Hashtable hashColumns = new Hashtable();

		actualCols = 0;

		for (int i = 0; i < numCols; i++)
		{
			OrderedColumn oc = (OrderedColumn) elementAt(i);

			// order by (lang) positions are 1-based,
			// order items (store) are 0-based.
			int position = oc.getColumnPosition() - 1;

// GemStone changes BEGIN
			final Integer posInt = Integer.valueOf(position);
			// changed to use Integer.valueOf()
			/* (original code)
			Integer posInt = new Integer(position);
			*/
// GemStone changes END

			if (! hashColumns.containsKey(posInt))
			{
				ordering[i] = new IndexColumnOrder(position,
												oc.isAscending(),
												oc.isNullsOrderedLow());
				actualCols++;
				hashColumns.put(posInt, posInt);
			}
		}

		/*
			If there were duplicates removed, we need
			to shrink the array down to what we used.
		*/
		if (actualCols < numCols)
		{
			IndexColumnOrder[] newOrdering = new IndexColumnOrder[actualCols];
			System.arraycopy(ordering, 0, newOrdering, 0, actualCols);
			ordering = newOrdering;
		}

		return ordering;
	}
	
	//GemStone changes BEGIN
	
        public OrderedColumn getOrderedColumn(int position) {
          if (SanityManager.DEBUG)
          SanityManager.ASSERT(position >=0 && position < size());
          return (OrderedColumn) elementAt(position);
        }
	//GemStone changes END
}
