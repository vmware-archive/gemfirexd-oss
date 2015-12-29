/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.DropSchemaConstantAction

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

import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDictionary;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.SchemaDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ConstantAction;
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState;

/**
 *	This class  describes actions that are ALWAYS performed for a
 *	DROP SCHEMA Statement at Execution time.
 *
 */

//GemStone changes BEGIN
//made public
public final
//GemStone changes END
class DropSchemaConstantAction extends DDLConstantAction
{


	private final String				schemaName;

	// GemStone changes BEGIN           
	private final boolean onlyIfExists;
	// GemStone changes END                         

	// CONSTRUCTORS

	/**
	 *	Make the ConstantAction for a DROP TABLE statement.
	 *
	 *	@param	schemaName			Table name.
	 *
	 */
	DropSchemaConstantAction(String	schemaName
// GemStone changes BEGIN	    
	                        ,boolean onlyIfExists
// GemStone changes END	                        
	                        )
	{
		this.schemaName = schemaName;
		this.onlyIfExists = onlyIfExists;
	}

	///////////////////////////////////////////////
	//
	// OBJECT SHADOWS
	//
	///////////////////////////////////////////////

	public	String	toString()
	{
		// Do not put this under SanityManager.DEBUG - it is needed for
		// error reporting.
		return "DROP SCHEMA "
// GemStone changes BEGIN		    
		      + (onlyIfExists ? "IF EXISTS ": "")
// GemStone changes END		      
		      + schemaName;
	}

	// INTERFACE METHODS


	/**
	 *	This is the guts of the Execution-time logic for DROP TABLE.
	 *
	 *	@see ConstantAction#executeConstantAction
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	executeConstantAction( Activation activation )
						throws StandardException
	{
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
		SchemaDescriptor sd = null;
		try{
	               sd = dd.getSchemaDescriptor(schemaName, null, true);
		}
		catch(StandardException e){
		  if(e.getSQLState().equals(SQLState.LANG_SCHEMA_DOES_NOT_EXIST) && this.onlyIfExists){
		    return;
		  }
		  else{
		    throw e;
		  }
		}
// GemStone changes END		
	        /* ORIGINAL CODE
		SchemaDescriptor sd = dd.getSchemaDescriptor(schemaName, null, true);
		*/
        sd.drop(lcc);

// GemStone changes BEGIN
    // drop schema from GemFireStore
    com.pivotal.gemfirexd.internal.engine.Misc.getMemStore().dropSchemaRegion(
        this.schemaName, lcc.getTransactionExecute());
  }

  @Override
  public boolean isReplayable() {
    return !GfxdConstants.SESSION_SCHEMA_NAME.equalsIgnoreCase(this.schemaName);
  }

  @Override
  public String getSchemaName() {
    return this.schemaName;
  }

  @Override
  public boolean isDropStatement() {
    return true;
  }

  @Override
  public boolean isCancellable() {
    return false;
  }

// GemStone changes END
}
