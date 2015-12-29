/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.LockTableConstantAction

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







import com.pivotal.gemfirexd.internal.catalog.UUID;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ConstantAction;
import com.pivotal.gemfirexd.internal.iapi.store.access.ConglomerateController;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;


/**
 *	This class describes actions that are ALWAYS performed for a
 *	LOCK TABLE Statement at Execution time.
 *
 */

public class LockTableConstantAction implements ConstantAction
{

	private final String					fullTableName;
	private final long					conglomerateNumber;
	private final boolean					exclusiveMode;
	
	// CONSTRUCTORS

	/**
	 * Make the ConstantAction for a LOCK TABLE statement.
	 *
	 *  @param fullTableName		Full name of the table.
	 *  @param conglomerateNumber	Conglomerate number for the heap
	 *  @param exclusiveMode		Whether or not to get an exclusive lock.
	 */
	LockTableConstantAction(String fullTableName,
									long conglomerateNumber, boolean exclusiveMode)
	{
		this.fullTableName = fullTableName;
		this.conglomerateNumber = conglomerateNumber;
		this.exclusiveMode = exclusiveMode;
	}

	// OBJECT METHODS

	public	String	toString()
	{
		// Do not put this under SanityManager.DEBUG - it is needed for
		// error reporting.
		return "LOCK TABLE " + fullTableName;
	}

	// INTERFACE METHODS


	/**
	 *	This is the guts of the Execution-time logic for LOCK TABLE.
	 *
	 *	@see ConstantAction#executeConstantAction
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	executeConstantAction( Activation activation )
						throws StandardException
	{
		ConglomerateController	cc;
		TransactionController	tc;

		/* Get a ConglomerateController for the base conglomerate */
		tc = activation.getTransactionController();

		try
		{
			cc = tc.openConglomerate(
	                conglomerateNumber,
                    false,
					(exclusiveMode) ?
						(TransactionController.OPENMODE_FORUPDATE | 
							TransactionController.OPENMODE_FOR_LOCK_ONLY) :
						TransactionController.OPENMODE_FOR_LOCK_ONLY,
			        TransactionController.MODE_TABLE,
                    TransactionController.ISOLATION_SERIALIZABLE);
			cc.close();
		}
		catch (StandardException se)
		{
			String msgId = se.getMessageId();
			if (msgId.equals(SQLState.DEADLOCK) || msgId.equals(SQLState.LOCK_TIMEOUT) || msgId.equals(SQLState.LOCK_TIMEOUT_LOG)) {
				String mode = (exclusiveMode) ? "EXCLUSIVE" : "SHARE";
				se = StandardException.newException(SQLState.LANG_CANT_LOCK_TABLE, se, fullTableName, mode);
			}

			throw se;
		}
	}

  // GemStone changes BEGIN
  @Override
  public boolean isCancellable() {
    return false;
  }
  // GemStone changes END
}
