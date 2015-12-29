/*

   Derby - Class com.pivotal.gemfirexd.internal.iapi.store.access.SortController

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

package com.pivotal.gemfirexd.internal.iapi.store.access;

import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;


/**

  A sort controller is an interface for inserting rows
  into a sort.
  <p>
  A sort is created with the createSort method of
  TransactionController. The rows are read back with
  a scan controller returned from the openSortScan
  method of TranscationController.


  @see TransactionController#openSort
  @see ScanController

**/

public interface SortController
{
	/**
	Inform SortController that all the rows have
    been inserted into it. 
	**/
	void completedInserts();

	/**
    Insert a row into the sort.
	 * @param row TODO

    @exception StandardException Standard exception policy.
    **/
// GemStone changes BEGIN  
	/*void insert(ExecRow row) throws StandardException;*/
	/**
	 * 
	 * @param row
	 * @return returns true if the row was inserted in the sort buffer or false in case it was not ( for eg if it was a duplicate)
	 *  @exception StandardException Standard exception policy.
	 */
	boolean insert(ExecRow row) throws StandardException;

	/** estimated memory usage of the sorter */
	long estimateMemoryUsage(ExecRow sortResultRow)
	    throws StandardException;
// GemStone changes END

    /**
     * Return SortInfo object which contains information about the current
     * state of the sort.
     * <p>
     *
     * @see SortInfo
     *
	 * @return The SortInfo object which contains info about current sort.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    SortInfo getSortInfo()
		throws StandardException;


}
