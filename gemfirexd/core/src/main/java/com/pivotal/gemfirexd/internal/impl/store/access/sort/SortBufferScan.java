/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.store.access.sort.SortBufferScan

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

package com.pivotal.gemfirexd.internal.impl.store.access.sort;


import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecIndexRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.store.access.conglomerate.TransactionManager;

/**

  A sort scan that just reads rows out of a sorter.

**/

public class SortBufferScan extends SortScan
{
	/**
	The sorter we're returning rows from.
	**/
	protected SortBuffer sortBuffer;

	/*
	 * Constructors.
	 */

	SortBufferScan(
    MergeSort           sort, 
    TransactionManager  tran, 
    SortBuffer          sortBuffer,
    boolean             hold)
	{
		super(sort, tran, hold);

        if (SanityManager.DEBUG)
            SanityManager.ASSERT(sortBuffer != null);

		this.sortBuffer = sortBuffer;
	}

	/*
	 * Methods of MergeSortScan
	 */

    /**
    Move to the next position in the scan.
	@see com.pivotal.gemfirexd.internal.iapi.store.access.ScanController#next
    **/
    public boolean next()
		throws StandardException
	{
        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(
                sortBuffer != null, 
                "next() called on scan after scan was closed.");
        }

		super.current = sortBuffer.removeFirst();
		return (super.current != null);
	}

    /**
    Close the scan.
    **/
    public boolean closeForEndTransaction(boolean closeHeldScan)
    {
        if (closeHeldScan || !hold)
        {
            close();
            return(true);
        }
        else
        {
            return(false);
        }

    }

    /**
    Close the scan.
	@see com.pivotal.gemfirexd.internal.iapi.store.access.ScanController#close
    **/
    public void close()
	{
		if (super.sort != null)
		{
			sort.doneScanning(this, sortBuffer);			
		}
		super.close();
		sortBuffer = null;
	}

    // GemStone changes BEGIN
    public void setCurrentRakInfoInInputArray(ExecRow inputRow) {
      if (this.current != null && inputRow != null) {
        inputRow.setAllRegionAndKeyInfo(this.current.getAllRegionAndKeyInfo());
      }
    }
    // GemStone changes END
}
