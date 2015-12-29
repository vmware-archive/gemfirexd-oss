/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.ForeignKeyRIChecker

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



// GemStone changes BEGIN
import com.pivotal.gemfirexd.internal.engine.access.index.MemIndex;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.StatementUtil;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecIndexRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.store.access.ScanController;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;

/**
 * A Referential Integrity checker for a foreign
 * key constraint.  It makes sure the foreign key is
 * intact.  This is used for a change to a foreign
 * key column.  see ReferencedKeyRIChecker for the code
 * that validates changes to referenced keys.
 */
public class ForeignKeyRIChecker extends GenericRIChecker
{
	/**
	 * @param tc		the xact controller
	 * @param fkinfo	the foreign key information 
	 *
	 * @exception StandardException		Thrown on failure
	 */
	ForeignKeyRIChecker(TransactionController tc, FKInfo fkinfo)
		throws StandardException
	{
		super(tc, fkinfo);

		if (SanityManager.DEBUG)
		{
			if (fkInfo.type != FKInfo.FOREIGN_KEY)
			{
				SanityManager.THROWASSERT("invalid type "+fkInfo.type+" for a ForeignKeyRIChecker");
			}
		} 
	}

	/**
	 * Check that the row either has a null column(s), or
	 * corresponds to a row in the referenced key.
	 * <p> 
	 * If the referenced key is found, then it is locked
	 * when this method returns.  The lock is held until
	 * the next call to doCheck() or close().
	 *
	 * @param row	the row to check
	 *
	 * @exception StandardException on unexped error, or
	 *		on a foreign key violation
	 */
	void doCheck(ExecRow row, boolean restrictCheckOnly) throws StandardException
	{

		if(restrictCheckOnly) //RESTRICT rule checks are not valid here.
			return; 

		/*
		** If any of the columns are null, then the
		** check always succeeds.
		*/
		if (isAnyFieldNull(row))
		{
			return;
		}

		/*
		** Otherwise, we had better find this row in the
		** referenced key
		*/
    // GemStone changes BEGIN
    // !!!:ezoerner:20080925 hack alert
    // Do a containsKey() on the foreign table region. (quick partial fix for #39543)
    // @todo If the foreign table is not partitioned on its primary key, we will
    // need to consult its global index instead (not yet implemented)
    
    // the following duplicates private behavior from superclass method
    // setupQualifierRow(ExecRow)
	/*	
    assert this.refScoci.getConglom() instanceof MemIndex;
    
    MemIndex backingIndex = (MemIndex)this.refScoci.getConglom();
   
   // System.out.println("XXXForeignKeyRIChecker:" + backingIndex.getIndexTypeName());
    if (backingIndex.getIndexTypeName().equals(MemIndex.LOCAL_HASH_INDEX)) {
      
      
      int numColumns = this.fkInfo.colArray.length;
      DataValueDescriptor[] indexRow = new DataValueDescriptor[numColumns];
      DataValueDescriptor[] baseRow = row.getRowArray();
      for (int i = 0; i < numColumns; i++) {
        indexRow[i] = baseRow[this.fkInfo.colArray[i] - 1];
      }
      GemFireKey gfKey = new GemFireKey(indexRow);
      //
     // System.out.println("XXXXForeignKeyRIChecker:gemfirekey="+gfKey.toString());
      if (!(backingIndex.getBaseContainer().getRegion().containsKey(gfKey))) {
       
        close();
        StandardException se = StandardException.newException(
            SQLState.LANG_FK_VIOLATION, fkInfo.fkConstraintNames[0],
            fkInfo.tableName, StatementUtil.typeName(fkInfo.stmtType), RowUtil
                .toString(row, fkInfo.colArray));

      //  throw se;
      }
    }
    else {
      // original derby code
      ScanController scan = getScanController(fkInfo.refConglomNumber,
          refScoci, refDcoci, row);
      if (!scan.next()) {
        close();
        StandardException se = StandardException.newException(
            SQLState.LANG_FK_VIOLATION, fkInfo.fkConstraintNames[0],
            fkInfo.tableName, StatementUtil.typeName(fkInfo.stmtType), RowUtil
                .toString(row, fkInfo.colArray));

        throw se;
      }
    }
    */
    // GemStone changes END

		
		/*
                 * * If we found the row, we are currently positioned on * the
                 * row when we leave this method. So we hold the * lock on the
                 * referenced key, which is very important.
                 */	
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
		return TransactionController.ISOLATION_READ_COMMITTED;
	}
}
