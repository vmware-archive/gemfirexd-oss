/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.SetTransactionIsolationConstantAction

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
import java.sql.SQLException;

import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.iapi.services.context.ContextManager;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecutionContext;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnection;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnectionContext;
// GemStone changes END


import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ConstantAction;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;


/**
 *	This class  describes actions that are ALWAYS performed for a
 *	SET TRANSACTION ISOLATION Statement at Execution time.
 *
 */

class SetTransactionIsolationConstantAction implements ConstantAction
{

	private final int isolationLevel;

	// CONSTRUCTORS

	/**
	 *	Make the ConstantAction for a SET TRANSACTION ISOLATION statement.
	 *
	 *  @param isolationLevel	The new isolation level
	 */
	SetTransactionIsolationConstantAction(
								int		isolationLevel)
	{
		this.isolationLevel = isolationLevel;
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
		return "SET TRANSACTION ISOLATION LEVEL = " + isolationLevel;
	}

	// INTERFACE METHODS
	
	/**
	 *	This is the guts of the Execution-time logic for SET TRANSACTION ISOLATION.
	 *
	 *	@see ConstantAction#executeConstantAction
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	executeConstantAction( Activation activation )
						throws StandardException
	{
// GemStone changes BEGIN
		// using EmbedConnection's setIsolationLevel for proper upgrade
		final ContextManager cm = activation.getContextManager();
		final EmbedConnection conn = EmbedConnectionContext
		    .getEmbedConnection(cm);
		if (conn != null) {
		  try {
		    conn.setTransactionIsolation(ExecutionContext
		        .CS_TO_JDBC_ISOLATION_LEVEL_MAP[this.isolationLevel]);
		  } catch (SQLException sqle) {
		    throw Misc.wrapSQLException(sqle, sqle);
		  }
		}
		else {
		  activation.getLanguageConnectionContext()
		      .setIsolationLevel(this.isolationLevel);
		}
		/* (original code)
		activation.getLanguageConnectionContext().setIsolationLevel(isolationLevel);
		*/
// GemStone changes END
	}

  // GemStone changes BEGIN
  @Override
  public boolean isCancellable() {
    return false;
  }
  // GemStone changes END
}
