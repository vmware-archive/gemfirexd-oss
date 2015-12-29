/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.store.access.sort.SortBufferRowSource

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

package com.pivotal.gemfirexd.internal.impl.store.access.sort;




import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableBitSet;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.store.access.SortObserver;
import com.pivotal.gemfirexd.internal.iapi.store.access.conglomerate.ScanControllerRowSource;
import com.pivotal.gemfirexd.internal.iapi.store.access.conglomerate.TransactionManager;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation;

/**
	Wrapping the output of a SortBuffer in a RowSource for the benefit of the
	createAndLoadConglomerate and loadConglomerate interface.

	Scan implements ScanController, this class just implements the
	RowSource interface. 

*/
public class SortBufferRowSource extends Scan
		implements ScanControllerRowSource
{
	/**
	The Sort buffer where rows come from
	**/
	SortBuffer sortBuffer = null;

	/**
	The TransactionManager that opened this scan.
	**/
	protected TransactionManager tran = null;

	private int			 maxFreeListSize;
	private boolean		 writingToDisk;
	private SortObserver sortObserver;

	/*
	 * Constructors.
	 */

	SortBufferRowSource(
    SortBuffer          sortBuffer, 
    TransactionManager  tran, 
    SortObserver        sortObserver,
    boolean             writingToDisk, 
    int                 maxFreeListSize)
	{
		super();
		this.sortBuffer = sortBuffer;
		this.tran = tran;
		this.sortObserver = sortObserver;
		this.writingToDisk = writingToDisk;
		this.maxFreeListSize = maxFreeListSize;
	}

	/* Private/Protected methods of This class: */
    /* Public Methods of This class: */
    /* Public Methods of RowSource class: */

    public ExecRow getNextRowFromRowSource()
    {
		if (sortBuffer == null)	// has been closed
			return null;

		ExecRow retval = sortBuffer.removeFirst();

		// Return the removed object to the free DataValueDescriptor[]
		if (retval != null && writingToDisk)
		{
			sortObserver.addToFreeList(retval, maxFreeListSize);
		}
		return retval;
	  }

	public boolean needsRowLocation()
	{
		return false;
	}

	/**
	 * @see com.pivotal.gemfirexd.internal.iapi.store.access.RowSource#needsToClone
	 */
	public boolean needsToClone()
	{
		return false;
	}

	public void rowLocation(RowLocation rl)
	{
		if (SanityManager.DEBUG)
			SanityManager.THROWASSERT("unexpected call to RowSource.rowLocation");
	}


	/**
		All columns are always set from a sorter
	*/
	public FormatableBitSet getValidColumns()
	{
		return null;
	}

	/**
		Close the scan
	 */
	public void close()
	{
		if (sortBuffer != null)
		{
			sortBuffer.close();
			sortBuffer = null;
		}
		tran.closeMe(this);
	}

	/**
		Close the scan
	 */
	public boolean closeForEndTransaction(boolean closeHeldScan)
	{
        if (SanityManager.DEBUG)
            SanityManager.ASSERT(
                closeHeldScan, 
                "Sort scan should not be held open across commit.");

        close();
        return(true);
    }

	/**
		Close the rowSource
	 */
	public void closeRowSource()
	{
		close();
	}

	/*
	 * Disable illegal and dangerous scan controller interface call
	 */
	public boolean next() throws StandardException
	{
        throw StandardException.newException(
                SQLState.SORT_IMPROPER_SCAN_METHOD);
	}

    /**
     * Fetch the row at the current position of the Scan and does not apply the
     * qualifiers.
     *
     * This method will always throw an exception. 
     * (SQLState.SORT_IMPROPER_SCAN_METHOD)
     *
     * @see com.pivotal.gemfirexd.internal.iapi.store.access.ScanController#fetchWithoutQualify
     **/
    public void fetchWithoutQualify(DataValueDescriptor[] result) 
        throws StandardException
    {
        throw StandardException.newException(
                SQLState.SORT_IMPROPER_SCAN_METHOD);
    }

    /**
     * Fetch the row at the current position of the Scan.
     *
     * @see com.pivotal.gemfirexd.internal.iapi.store.access.ScanController#fetch
     **/
    public void fetch(DataValueDescriptor[] result) throws StandardException
	{
        throw StandardException.newException(
                SQLState.SORT_IMPROPER_SCAN_METHOD);
	}

    public final boolean fetchNext(DataValueDescriptor[] row) 
        throws StandardException
	{
        throw StandardException.newException(
                SQLState.SORT_IMPROPER_SCAN_METHOD);
	}
}
