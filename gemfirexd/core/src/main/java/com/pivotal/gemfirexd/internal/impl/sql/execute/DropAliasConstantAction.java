/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.DropAliasConstantAction

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
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.depend.DependencyManager;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.AliasDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDictionary;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.SchemaDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ConstantAction;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecPreparedStatement;

/**
 *	This class performs actions that are ALWAYS performed for a
 *	DROP FUNCTION/PROCEDURE/SYNONYM statement at execution time.
 *  All of these SQL objects are represented by an AliasDescriptor.
 *
 */

//GemStone changes BEGIN
//made public
public final
//GemStone changes END
class DropAliasConstantAction extends DDLConstantAction
{

	private SchemaDescriptor	sd;
	private final String				aliasName;
	private final char				nameSpace;
// GemStone changes BEGIN
	private boolean        onlyIfExists;
// GemStone changes END
	// CONSTRUCTORS


	/**
	 *	Make the ConstantAction for a DROP  ALIAS statement.
	 *
	 *
	 *	@param	aliasName			Alias name.
	 *	@param	nameSpace			Alias name space.
	 *
	 */
	DropAliasConstantAction(SchemaDescriptor sd, String aliasName, char nameSpace
// GemStone changes BEGIN	    
	    ,boolean onlyIfExists
// GemStone changes END	    
	    )
	{
		this.sd = sd;
		this.aliasName = aliasName;
		this.nameSpace = nameSpace;
// GemStone changes BEGIN		
		this.onlyIfExists = onlyIfExists;
// GemStone changes END		
	}

	// OBJECT SHADOWS

	public	String	toString()
	{
		// Do not put this under SanityManager.DEBUG - it is needed for
		// error reporting.
		return	"DROP ALIAS " + aliasName;
	}

	// INTERFACE METHODS


	/**
	 *	This is the guts of the Execution-time logic for DROP ALIAS.
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
		if(sd == null && onlyIfExists){
		  invalidatePreparedStatement(activation, DependencyManager.DROP_METHOD_ALIAS);
		  return;
		}
// GemStone changes END		
		
// GemStone changes BEGIN
    // acquire exclusive lock on the alias object
    String lockObject = this.sd.getSchemaName() + '.' + this.aliasName;
    com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
        .lockObject(null, lockObject, true, lcc.getTransactionExecute());
// GemStone changes END
		/* Get the alias descriptor.  We're responsible for raising
		 * the error if it isn't found 
		 */
		AliasDescriptor ad = dd.getAliasDescriptor(sd.getUUID().toString(), aliasName, nameSpace);

		// RESOLVE - fix error message
		if (ad == null)
		{
// GemStone changes BEGIN
		  if(onlyIfExists){
		    invalidatePreparedStatement(activation, DependencyManager.DROP_METHOD_ALIAS);
		    return;
		  }
// GemStone changes BEGIN		  
		  else{
// GemStone changes END		    
			throw StandardException.newException(SQLState.LANG_OBJECT_NOT_FOUND, ad.getAliasType(nameSpace),  aliasName);
// GemStone changes BEGIN			
		  }
// GemStone changes END		  
		}

        adjustUDTDependencies( lcc, dd, ad, false );
        
        ad.drop(lcc);

// GemStone changes BEGIN
    // free the lock resources for the alias
    com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
        .freeLockResources(lockObject, lcc.getTransactionExecute());
  }

  @Override
  public final String getSchemaName() {
// GemStone changes BEGIN    
    return sd == null ? null : this.sd.getSchemaName();
// GemStone changes END    
    // Original code
    // return this.sd.getSchemaName();
  }

  @Override
  public final boolean isDropStatement() {
    return true;
  }

  @Override
  public final String getObjectName() {
    return this.aliasName;
  }
	/* } */

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
