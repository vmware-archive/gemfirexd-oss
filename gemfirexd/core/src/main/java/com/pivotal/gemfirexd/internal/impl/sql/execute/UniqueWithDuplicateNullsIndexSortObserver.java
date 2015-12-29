/*
 
   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.UniqueWithDuplicateNullsIndexSortObserver
 
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
import com.pivotal.gemfirexd.internal.engine.store.RowFormatter;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;

/**
 * UniqueWithDuplicateNullsIndexSortObserver is implementation of BasicSortObserver for
 * eliminating non null duplicates from the backing index of unique constraint.
 * This class is implemented to check for special case of distinct sorting where
 * duplicate keys are allowed only if there is a null in the key part.
 */
public class UniqueWithDuplicateNullsIndexSortObserver extends BasicSortObserver {
    
    private boolean		isConstraint;
    private String		indexOrConstraintName;
    private String 		tableName;
    
    /**
     * Constructs an object of UniqueWithDuplicateNullsIndexSortObserver
     * 
     * 
     * 
     * @param doClone If true, then rows that are retained
     * 		by the sorter will be cloned.  This is needed
     * 		if language is reusing row wrappers.
     * @param isConstraint is this part of a constraint
     * @param indexOrConstraintName name of index of constraint
     * @param execRow	ExecRow to use as source of clone for store.
     * @param reuseWrappers	Whether or not we can reuse the wrappers
     * @param tableName name of the table
     */
    public UniqueWithDuplicateNullsIndexSortObserver(
    boolean doClone, 
    boolean isConstraint,
    String  indexOrConstraintName, 
    ExecRow execRow,
    boolean reuseWrappers, 
    String  tableName) {
        super(doClone, false, execRow, reuseWrappers);
        this.isConstraint = isConstraint;
        this.indexOrConstraintName = indexOrConstraintName;
        this.tableName = tableName;
    }
    
    /**
     * Methods to check if the duplicate key can be inserted or not. It throws 
     * exception if the duplicates has no null part in the key. 
     * @param in new key
     * @param dup the new key is duplicate of this key
     * @return DVD [] if there is at least one null in
     * the key else thorws StandardException
     * @throws StandardException is the duplicate key has all non null parts
     */
    public ExecRow insertDuplicateKey(ExecRow in,
            ExecRow dup) throws StandardException {
        for (int i = 1, nCols = in.nColumns(); i <= nCols; i++) {
            if (in.isNull(i) == RowFormatter.OFFSET_AND_WIDTH_IS_NULL) {
                return super.insertDuplicateKey(in, dup);
            }
        }
        StandardException se = null;
        se = StandardException.newException(
                SQLState.LANG_DUPLICATE_KEY_CONSTRAINT,
                indexOrConstraintName, tableName);
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
