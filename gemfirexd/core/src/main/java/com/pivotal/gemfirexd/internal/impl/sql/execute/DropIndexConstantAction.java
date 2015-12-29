/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.DropIndexConstantAction

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








import com.gemstone.gemfire.internal.cache.TXStateInterface;
import com.pivotal.gemfirexd.internal.catalog.UUID;
// GemStone changes BEGIN
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.access.GemFireTransaction;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
// GemStone changes END
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.depend.Dependency;
import com.pivotal.gemfirexd.internal.iapi.sql.depend.DependencyManager;
import com.pivotal.gemfirexd.internal.iapi.sql.depend.Dependent;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.ConglomerateDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDescriptorGenerator;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDictionary;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.SchemaDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.TableDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ConstantAction;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;
import java.util.Enumeration;


/**
 *	This class  describes actions that are ALWAYS performed for a
 *	DROP INDEX Statement at Execution time.
 *
 */

class DropIndexConstantAction extends IndexConstantAction
{

	private String				fullIndexName;
	private long				tableConglomerateId;
// GemStone changes BEGIN	
	private boolean                         onlyIfExists;
// GemStone changes END
	
	// CONSTRUCTORS

	/**
	 *	Make the ConstantAction for a DROP INDEX statement.
	 *
	 *
	 *	@param	fullIndexName		Fully qualified index name
	 *	@param	indexName			Index name.
	 *	@param	tableName			The table name
	 *	@param	schemaName			Schema that index lives in.
	 *  @param  tableId				UUID for table
	 *  @param  tableConglomerateId	heap Conglomerate Id for table
	 *
	 */
	DropIndexConstantAction(
								String				fullIndexName,
								String				indexName,
								String				tableName,
								String				schemaName,
								UUID				tableId,
								long				tableConglomerateId
// GemStone changes BEGIN								
								,boolean                        onlyIfExists
// GemStone changes END								
	                                                        )
	{
		super(tableId, indexName, tableName, schemaName);
		this.fullIndexName = fullIndexName;
		this.tableConglomerateId = tableConglomerateId;
// GemStone changes BEGIN		
		this.onlyIfExists = onlyIfExists;
// GemStone changes END		
	}

	// OBJECT METHODS

	public	String	toString()
	{
		// Do not put this under SanityManager.DEBUG - it is needed for
		// error reporting.
		return "DROP INDEX "
// GemStone changes BEGIN		   
		      + (onlyIfExists ? "IF EXISTS ": "")
// GemStone changes END		      
		      + fullIndexName;
	}

	// INTERFACE METHODS


	/**
	 *	This is the guts of the Execution-time logic for DROP INDEX.
	 *
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	executeConstantAction(Activation activation)
						throws StandardException
	{
		TableDescriptor td = null;
		ConglomerateDescriptor cd;

		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		TransactionController tc = lcc.getTransactionExecute();

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
		if((tableId == null || tableConglomerateId == 0L) && onlyIfExists){
		  invalidatePreparedStatement(activation, DependencyManager.DROP_INDEX);
		  return;
		}
// GemStone changes END		
		// need to lock heap in exclusive mode first.  Because we can't first
		// shared lock the row in SYSCONGLOMERATES and later exclusively lock
		// it, this is potential deadlock (track 879).  Also td need to be
		// gotten after we get the lock, a concurrent thread could be modifying
		// table shape (track 3804, 3825)

		// older version (or target) has to get td first, potential deadlock
		if (tableConglomerateId == 0)
		{
			td = dd.getTableDescriptor(tableId);
			if (td == null)
			{
				throw StandardException.newException(
					SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION, tableName);
			}
			tableConglomerateId = td.getHeapConglomerateId();
		}
// GemStone changes BEGIN
		// check for any open ResultSets
		if (td == null) {
		  td = dd.getTableDescriptor(tableId);
		}
		lcc.verifyNoOpenResultSets(null, td,
		    DependencyManager.DROP_INDEX);
// GemStone changes END
		lockTableForDDL(tc, tableConglomerateId, true);

		td = dd.getTableDescriptor(tableId);
		if (td == null)
		{
			throw StandardException.newException(SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION, tableName);
		}

		/*
		** If the schema descriptor is null, then
		** we must have just read ourselves in.  
		** So we will get the corresponding schema
		** descriptor from the data dictionary.
		*/
		SchemaDescriptor sd = dd.getSchemaDescriptor(schemaName, tc, true) ;

		/* Get the conglomerate descriptor for the index, along
		 * with an exclusive row lock on the row in sys.sysconglomerates
		 * in order to ensure that no one else compiles against the
		 * index.
		 */
		cd = dd.getConglomerateDescriptor(indexName, sd, true);

		if (cd == null)
		{
// GemStone changes BEGIN		  
		    if(this.onlyIfExists){
		      invalidatePreparedStatement(activation, DependencyManager.DROP_INDEX);
		      return;
		    }
		    else{
// GemStone changes END		      
                      throw StandardException.newException(SQLState.LANG_INDEX_NOT_FOUND_DURING_EXECUTION, fullIndexName);
// GemStone changes BEGIN                      
		    }
// GemStone changes END		    
		}

		/* Since we support the sharing of conglomerates across
		 * multiple indexes, dropping the physical conglomerate
		 * for the index might affect other indexes/constraints
		 * which share the conglomerate.  The following call will
		 * deal with that situation by creating a new physical
		 * conglomerate to replace the dropped one, if necessary.
		 */
// GemStone changes BEGIN
    SanityManager.DEBUG_PRINT("info:" + GfxdConstants.TRACE_CONGLOM,
        "Dropping index with descriptor: " + cd);
    this.tableName = td.getName();
    dropConglomerate(cd, td, activation, lcc);
    if (GemFireXDUtils.TraceConglom) {
      SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_CONGLOM,
          "Dropped index with descriptor {" + cd + "} of table: "
              + this.schemaName + '.' + this.tableName);
    }
  }

  @Override
  public String getSchemaName() {
    return this.schemaName;
  }

  @Override
  public String getTableName() {
    return this.tableName;
  }

  @Override
  public boolean isDropStatement() {
    return true;
  }

  @Override
  public String getObjectName() {
    return this.indexName;
  }
	/* (original derby code)
		dropConglomerate(cd, td, activation, lcc);
		return;
	}
        */

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
