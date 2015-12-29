/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.DropTriggerConstantAction

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
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.depend.DependencyManager;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDictionary;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.SPSDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.SchemaDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.TableDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.TriggerDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ConstantAction;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;

/**
 *	This class  describes actions that are ALWAYS performed for a
 *	DROP TRIGGER Statement at Execution time.
 *
 */
public class DropTriggerConstantAction extends DDLSingleTableConstantAction
{

	private final String			triggerName;
	private final SchemaDescriptor	sd;
// GemStone changes BEGIN
	private boolean onlyIfExists;
// GemStone changes END	

	// CONSTRUCTORS

	/**
	 *	Make the ConstantAction for a DROP TRIGGER statement.
	 *
	 * @param	sd					Schema that stored prepared statement lives in.
	 * @param	triggerName			Name of the Trigger
	 * @param	tableId				The table upon which the trigger is defined
	 *
	 */
	DropTriggerConstantAction
	(
		SchemaDescriptor	sd,
		String				triggerName,
		UUID				tableId
// GemStone changes BEGIN
		,boolean                 onlyIfExists
// GemStone changes END		
	)
	{
		super(tableId);
		this.sd = sd;
		this.triggerName = triggerName;
// GemStone changes BEGIN
		this.onlyIfExists = onlyIfExists;
// GemStone changes END		
		if (SanityManager.DEBUG 
// GemStone changes BEGIN		
		    && !onlyIfExists
// GemStone changes END		    
		    )
		{
			SanityManager.ASSERT(sd != null, "SchemaDescriptor is null");
		}
	}

	/**
	 *	This is the guts of the Execution-time logic for DROP STATEMENT.
	 *
	 *	@see ConstantAction#executeConstantAction
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	executeConstantAction( Activation activation )
						throws StandardException
	{
		TriggerDescriptor 			triggerd;

		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();

		/*
		** Inform the data dictionary that we are about to write to it.
		** There are several calls to data dictionary "get" methods here
		** that might be done in "read" mode in the data dictionary, but
		** it seemed safer to do this whole operation in "write" mode.
		**
		** We tell the data dictionary we're done writing at the end of
		** the transaction.
		*/
		dd.startWriting(lcc);

// GemStone changes BEGIN
		// Table descriptor will be null if the trigger descriptor was not available.
		// Check for if-exists condition and return.
		if(this.tableId == null && onlyIfExists){
		  invalidatePreparedStatement(activation, DependencyManager.DROP_TRIGGER);
		  return;
		}
		this.td = dd.getTableDescriptor(this.tableId);
		/* TableDescriptor td = dd.getTableDescriptor(tableId); */
// GemStone changes END
		if (td == null)
		{
			throw StandardException.newException(
								SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION,
								tableId.toString());
		}
// GemStone changes BEGIN
		// check for any open ResultSets
		lcc.verifyNoOpenResultSets(null, td,
		    DependencyManager.DROP_TRIGGER);
// GemStone changes END
		TransactionController tc = lcc.getTransactionExecute();
		lockTableForDDL(tc, td.getHeapConglomerateId(), true);
		// get td again in case table shape is changed before lock is acquired
		td = dd.getTableDescriptor(tableId);
		if (td == null)
		{
			throw StandardException.newException(
								SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION,
								tableId.toString());
		}

		/* 
		** Get the trigger descriptor.  We're responsible for raising
		** the error if it isn't found 
		*/
		triggerd = dd.getTriggerDescriptor(triggerName, sd);

		if (triggerd == null)
		{
			throw StandardException.newException(SQLState.LANG_OBJECT_NOT_FOUND_DURING_EXECUTION, "TRIGGER",
					(sd.getSchemaName() + "." + triggerName));
		}

		/* 
	 	** Prepare all dependents to invalidate.  (This is there chance
		** to say that they can't be invalidated.  For example, an open
		** cursor referencing a table/trigger that the user is attempting to
		** drop.) If no one objects, then invalidate any dependent objects.
		*/
        triggerd.drop(lcc);
        // Gemstone changes BEGIN
        com.pivotal.gemfirexd.internal.engine.access.index.GfxdIndexManager
        indexManager = com.pivotal.gemfirexd.internal.engine.access.index
          .GfxdIndexManager.getGfxdIndexManager(this.td, lcc);
        indexManager.removeTriggerExecutor(triggerd, lcc);
        // Gemstone changes END
	}

	public String toString()
	{
		// Do not put this under SanityManager.DEBUG - it is needed for
		// error reporting.
		return "DROP TRIGGER "+triggerName;
	}
// GemStone changes BEGIN

  private TableDescriptor td;

  @Override
  public final String getSchemaName() {
    return this.td == null ? null : this.td.getSchemaName();
  }

  @Override
  public final String getTableName() {
    return this.td == null ? null : this.td.getName();
  }

  @Override
  public final boolean isDropStatement() {
    return true;
  }

  @Override
  public final String getObjectName() {
    return this.triggerName;
  }

  @Override
  public boolean isCancellable() {
    return false;
  }

  @Override
  public boolean isDropIfExists(){
    return this.onlyIfExists;
  }
  
// GemStone changes END
}
