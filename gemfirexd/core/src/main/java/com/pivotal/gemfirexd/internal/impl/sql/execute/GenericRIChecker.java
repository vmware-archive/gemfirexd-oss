/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.GenericRIChecker

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




import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableBitSet;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.ConglomerateDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecIndexRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.store.access.ConglomerateController;
import com.pivotal.gemfirexd.internal.iapi.store.access.DynamicCompiledOpenConglomInfo;
import com.pivotal.gemfirexd.internal.iapi.store.access.ScanController;
import com.pivotal.gemfirexd.internal.iapi.store.access.StaticCompiledOpenConglomInfo;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation;

import java.util.Enumeration;
import java.util.Hashtable;

/**
 * Generic implementation of a Referential Integrity
 * checker.  Abstract.
 */
public abstract class GenericRIChecker
{
	protected FKInfo					fkInfo;
	protected DynamicCompiledOpenConglomInfo[] fkDcocis;
	protected StaticCompiledOpenConglomInfo[] fkScocis;
	protected DynamicCompiledOpenConglomInfo refDcoci;
	protected StaticCompiledOpenConglomInfo refScoci;
	protected TransactionController		tc;

	private Hashtable 		scanControllers;
	private int				numColumns;
	private	IndexRow		indexQualifierRow;

	/**
	 * @param tc		the xact controller
	 * @param fkinfo	the foreign key information 
	 *
	 * @exception StandardException		Thrown on failure
	 */
	GenericRIChecker(TransactionController tc, FKInfo fkinfo)
		throws StandardException
	{
		this.fkInfo = fkinfo;
		this.tc = tc;
		scanControllers = new Hashtable();
		numColumns = fkInfo.colArray.length;
		indexQualifierRow = new IndexRow(numColumns);

		fkDcocis = new DynamicCompiledOpenConglomInfo[fkInfo.fkConglomNumbers.length];
		fkScocis = new StaticCompiledOpenConglomInfo[fkInfo.fkConglomNumbers.length];
		for (int index = 0; index < fkInfo.fkConglomNumbers.length; index++)
		{
			fkDcocis[index] = tc.getDynamicCompiledConglomInfo(fkInfo.fkConglomNumbers[index]);
			fkScocis[index] = tc.getStaticCompiledConglomInfo(fkInfo.fkConglomNumbers[index]);
		}
		refDcoci = tc.getDynamicCompiledConglomInfo(fkInfo.refConglomNumber);
		refScoci = tc.getStaticCompiledConglomInfo(fkInfo.refConglomNumber);
	}

	/**
	 * Check the validity of this row
	 *
	 * @param row	the row to check
	 *
	 * @exception StandardException on error
	 */
	abstract void doCheck(ExecRow row, boolean restrictCheckOnly) throws StandardException;

	public void doCheck(ExecRow row) throws StandardException
	{
		doCheck(row, false); //Check all the referential Actions
	}

	/**
	 * Get a scan controller positioned using searchRow as
	 * the start/stop position.  The assumption is that searchRow
	 * is of the same format as the index being opened. 
	 * The scan is set up to return no columns.
	 * NOTE: We only need an instantaneous lock on the
	 * table that we are probing as we are just checking
	 * for the existance of a row.  All updaters, whether
	 * to the primary or foreign key tables, will hold an
	 * X lock on the table that they are updating and will
	 * be probing the other table, so instantaneous locks
	 * will not change the semantics.
	 *
	 * RESOLVE:  Due to the current RI implementation 
	 * we cannot always get instantaneous locks.  We
	 * will call a method to find out what kind of
	 * locking to do until the implementation changes.
	 *
	 * @param conglomNumber		the particular conglomerate we 
	 *							are interested in
	 * @param searchRow			the row to match
	 *
	 * @exception StandardException on error
	 */
	protected ScanController getScanController(long conglomNumber,
											   StaticCompiledOpenConglomInfo scoci,
											   DynamicCompiledOpenConglomInfo dcoci, ExecRow searchRow)
		throws StandardException
	{
		int				isoLevel = getRICheckIsolationLevel();
		ScanController 	scan;
// GemStone changes BEGIN
		// changed to use valueOf()
		final Long hashKey = Long.valueOf(conglomNumber);
		/* (original code)
		Long			hashKey = new Long(conglomNumber);
		*/
// GemStone changes END

		/*
		** If we haven't already opened this scan controller,
		** we'll open it now and stick it in the hash table.
		*/
		if ((scan = (ScanController)scanControllers.get(hashKey)) == null)
		{
			setupQualifierRow(searchRow);
			scan = 
                tc.openCompiledScan(
                      false,                       				// hold 
                      0, 										// read only
                      TransactionController.MODE_RECORD,		// row locking
					  isoLevel,
                      (FormatableBitSet)null, 							// retrieve all fields
                      indexQualifierRow.getRowArray(),    		// startKeyValue
                      ScanController.GE,            			// startSearchOp
                      null,                         			// qualifier
                      indexQualifierRow.getRowArray(),    		// stopKeyValue
                      ScanController.GT,             			// stopSearchOp 
					  scoci,
					  dcoci
                      );
			scanControllers.put(hashKey, scan);
		}
		else
		{
			/*
			** If the base row is the same row as the previous	
			** row, this call to setupQualfierRow is redundant,
			** but it is safer this way so we'll take the
			** marginal performance hit (marginal relative
			** to the index scans that we are making).
			*/
			setupQualifierRow(searchRow);
			scan.reopenScan(
                      indexQualifierRow.getRowArray(),    	// startKeyValue
                      ScanController.GE,            		// startSearchOp
                      null,                         		// qualifier
                      indexQualifierRow.getRowArray(), 		// stopKeyValue
                      ScanController.GT             		// stopSearchOp
                      // GemStone changes BEGIN
                      , null
                      // GemStone changes END
                      );
		}

		return scan;
	}

	/*
	** Do reference copy for the qualifier row.  No cloning.
	** So we cannot get another row until we are done with
	** this one.
	*/
	private void setupQualifierRow(ExecRow baseRow)
	{
		DataValueDescriptor[] indexColArray = indexQualifierRow.getRowArray();
		DataValueDescriptor[] baseColArray = baseRow.getRowArray();

		for (int i = 0; i < numColumns; i++)
		{
			indexColArray[i] = baseColArray[fkInfo.colArray[i] - 1];
		}
	}

	/**
	 * Are any of the fields null in the row passed
	 * in.  The only fields that are checked are those
	 * corresponding to the colArray in fkInfo.
	 */
	boolean isAnyFieldNull(ExecRow baseRow)
	{
		DataValueDescriptor[] baseColArray = baseRow.getRowArray();

		for (int i = 0; i < numColumns; i++)
		{
			DataValueDescriptor storable = baseColArray[fkInfo.colArray[i] - 1];
			if (storable.isNull())
			{
				return true;
			}
		}
		return false;
	}
		
			
	/**
	 * Clean up all scan controllers
	 *
	 * @exception StandardException on error
	 */
	void close()
		throws StandardException
	{
		Enumeration e = scanControllers.elements();
		while (e.hasMoreElements())
		{
			ScanController scan = (ScanController)e.nextElement();
			scan.close();
		}
		scanControllers.clear();
	}

	/**
	 * Get the isolation level for the scan for
	 * the RI check.
	 *
	 * NOTE: The level will eventually be instantaneous
	 * locking once the implemenation changes.
	 *
	 * @return The isolation level for the scan for
	 * the RI check.
	 */
	int getRICheckIsolationLevel()
	{
		return TransactionController.ISOLATION_READ_COMMITTED_NOHOLDLOCK;
	}
}
