/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.DropViewConstantAction

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
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.depend.DependencyManager;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDictionary;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.SchemaDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.TableDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.ViewDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ConstantAction;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;

/**
 *	This class  describes actions that are ALWAYS performed for a
 *	DROP VIEW Statement at Execution time.
 *
 */

class DropViewConstantAction extends DDLConstantAction
{

	private String				fullTableName;
	private String				tableName;
	private SchemaDescriptor	sd;
// GemStone changes BEGIN
	private boolean                onlyIfExists;
// GemStone changes END	
	// CONSTRUCTORS

	/**
	 *	Make the ConstantAction for a DROP VIEW statement.
	 *
	 *
	 *	@param	fullTableName		Fully qualified table name
	 *	@param	tableName			Table name.
	 *	@param	sd					Schema that view lives in.
	 *
	 */
	DropViewConstantAction(
								String				fullTableName,
								String				tableName,
								SchemaDescriptor	sd
// GemStone changes BEGIN
								,boolean onlyIfExists
// GemStone changes END								
								)
	{
		this.fullTableName = fullTableName;
		this.tableName = tableName;
		this.sd = sd;

// GemStone changes BEGIN
		this.onlyIfExists = onlyIfExists;
// GemStone changes END		
		if (SanityManager.DEBUG && !onlyIfExists)
		{
			SanityManager.ASSERT(sd != null, "SchemaDescriptor is null");
		}
		
	}

	// OBJECT METHODS

	public	String	toString()
	{
		// Do not put this under SanityManager.DEBUG - it is needed for
		// error reporting.
		return "DROP VIEW " + fullTableName;
	}

	// INTERFACE METHODS


	/**
	 *	This is the guts of the Execution-time logic for DROP VIEW.
	 *
	 *	@see ConstantAction#executeConstantAction
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	executeConstantAction( Activation activation )
						throws StandardException
	{
		TableDescriptor td;
		ViewDescriptor vd;

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

		/* Get the table descriptor.  We're responsible for raising
		 * the error if it isn't found 
		 */

// GemStone changes BEGIN
                if(sd == null && onlyIfExists){
                  invalidatePreparedStatement(activation, DependencyManager.DROP_VIEW);
                  return;
                }
// GemStone changes END             
		
		td = dd.getTableDescriptor(tableName, sd,
                lcc.getTransactionExecute());

		if (td == null)
		{
// GemStone changes BEGIN
		  if(onlyIfExists){
		    invalidatePreparedStatement(activation, DependencyManager.DROP_VIEW);
		    return;
		  }
		  else{
// GemStone changes END		    
			throw StandardException.newException(SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION, fullTableName);
// GemStone changes BEGIN			
		  }
// GemStone changes END		  
		}

		/* Verify that TableDescriptor represents a view */
		if (td.getTableType() != TableDescriptor.VIEW_TYPE)
		{
			throw StandardException.newException(SQLState.LANG_DROP_VIEW_ON_NON_VIEW, fullTableName);
		}

		vd = dd.getViewDescriptor(td);

		vd.drop(lcc, sd, td);
	}
// GemStone changes BEGIN

  @Override
  public String getSchemaName() {
 // GemStone changes BEGIN    
    return sd == null ? null : this.sd.getSchemaName();
// GemStone changes END    
    // Original code
    // return this.sd.getSchemaName();
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
  public boolean isCancellable() {
    return false;
  }

  @Override
  public boolean isDropIfExists(){
    return this.onlyIfExists;
  }
  
// GemStone changes END
}
