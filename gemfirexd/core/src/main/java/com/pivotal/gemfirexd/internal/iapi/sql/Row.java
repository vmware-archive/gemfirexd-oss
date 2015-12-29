/*

   Derby - Class com.pivotal.gemfirexd.internal.iapi.sql.Row

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

package com.pivotal.gemfirexd.internal.iapi.sql;

import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableBitSet;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;

/**
 * The Row interface provides methods to get information about the columns
 * in a result row.
 * It uses simple, position (1-based) access to get to columns.
 * Searching for columns by name should be done from the ResultSet
 * interface, where metadata about the rows and columns is available.
 * <p>
 *
 * @see ResultSet
 *
 * @see com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow
 */

public interface Row
{
	public int nColumns();

	/**
	 * Get a DataValueDescriptor in a Row by ordinal position (1-based).
	 *
	 * @param position	The ordinal position of the column.
	 *
     * @exception   StandardException Thrown on failure.
	 * @return		The DataValueDescriptor, null if no such column exists
	 */
	DataValueDescriptor	getColumn (int position) throws StandardException;

	/**
	 * Set a DataValueDescriptor in a Row by ordinal position (1-based).
	 *
	 * @param position	The ordinal position of the column.
	 */
	void	setColumn (int position, DataValueDescriptor value);
  
// GemStone changes BEGIN
  /**
   * Set DataValueDescriptors in a Row.
   * 
   * @param columns
   *          which columns from values to set, or null if all the values should
   *          be set.
   * @param values
   *          a sparse array of the values to set
   */
  public void setColumns(FormatableBitSet columns,
                         DataValueDescriptor[] values)
  throws StandardException;

  /**
   * Set the values in the row using the source row.
   */
  public void setColumns(FormatableBitSet columns,
                         ExecRow srcRow)
  throws StandardException;

  /**
   * Set the values in the row using the source row. The difference from
   * {@link #setColumns(FormatableBitSet, ExecRow)} is that this compacts the
   * columns in this {@link Row} mapping the first set bit in
   * <code>columns</code> to 1, next to 2 and so on.
   */
  public void setCompactColumns(FormatableBitSet columns, ExecRow srcRow,
      int[] baseColumnMap, boolean copyColumns) throws StandardException;

  /**
   * Set the values in the row using the source row.
   */
  public void setColumns(int[] columns,
                         boolean zeroBased,
                         ExecRow srcRow)
  throws StandardException;

  /**
   * Set the values for first n-columns in the row using the source row.
   */
  public void setColumns(int nCols,
                         ExecRow srcRow)
  throws StandardException;
    
// GemStone changes END
}
