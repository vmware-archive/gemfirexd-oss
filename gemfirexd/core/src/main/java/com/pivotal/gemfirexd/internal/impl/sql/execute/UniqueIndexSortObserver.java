/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.UniqueIndexSortObserver

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

package com.pivotal.gemfirexd.internal.impl.sql.execute;





import com.pivotal.gemfirexd.internal.engine.jdbc.GemFireXDRuntimeException;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.io.Storable;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.store.access.SortObserver;


/**
 * Unique index aggregator.  Enforces uniqueness when
 * creating a unique index or constraint.
 *
 */
class UniqueIndexSortObserver extends BasicSortObserver 
{
	private boolean		isConstraint;
	private String		indexOrConstraintName;
	private String 		tableName;

	public UniqueIndexSortObserver(boolean doClone, boolean isConstraint, 
				String indexOrConstraintName, ExecRow execRow, 
				boolean reuseWrappers, String tableName)
	{
		super(doClone, true, execRow, reuseWrappers);
		this.isConstraint = isConstraint;
		this.indexOrConstraintName = indexOrConstraintName;
		this.tableName = tableName;
	}

	/*
	 * Overridden from BasicSortObserver
	 */

	/**
	 * @see AggregateSortObserver#insertDuplicateKey
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ExecRow insertDuplicateKey(
    ExecRow   in, 
    ExecRow   dup)
		throws StandardException
	{
		StandardException se = null;
		se = StandardException.newException(
				SQLState.LANG_DUPLICATE_KEY_CONSTRAINT, indexOrConstraintName, tableName);
		throw se;
	}

// GemStone changes BEGIN
	@Override
	public boolean eliminateDuplicate(Object insertRow,
	    Object existingRow) {
	  try {
	    return insertDuplicateKey((ExecRow)insertRow,
	        (ExecRow)existingRow) == null;
	  } catch (StandardException se) {
	    throw new GemFireXDRuntimeException(se);
	  }
	}

	@Override
	public boolean canSkipDuplicate() {
	  return true;
	}
// GemStone changes END
}
