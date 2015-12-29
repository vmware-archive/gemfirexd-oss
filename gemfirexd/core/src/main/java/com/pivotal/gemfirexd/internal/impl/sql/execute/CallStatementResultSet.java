/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.CallStatementResultSet

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

import java.sql.ResultSet;
import java.sql.SQLException;

// GemStone changes BEGIN
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.jdbc.ConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.services.loader.GeneratedMethod;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDictionary;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;

/**
 * Call a Java procedure. This calls a generated method in the
 * activation which sets up the parameters and then calls the
 * Java method that the procedure resolved to.
 * <P>
 * Valid dynamic results returned by the procedure will be closed
 * as inaccessible when this is closed (e.g. a CALL within a trigger).
 * 
 * <BR>
 * Any code that requires the dynamic results to be accessible
 * (such as the JDBC Statement object executing the CALL) must
 * obtain the dynamic results from Activation.getDynamicResults()
 * and remove each ResultSet it will be handling by clearing the
 * reference in the object returned.
 * 
 * @see Activation#getDynamicResults()
 */
class CallStatementResultSet extends NoRowsResultSetImpl
{

	private final GeneratedMethod methodCall;
	public boolean isSysProc = false;
    /*
     * class interface
     *
     */
    CallStatementResultSet(
				GeneratedMethod methodCall,
				Activation a) 
			throws StandardException
    {
		super(a);
		this.methodCall = methodCall;
	}

	/**
     * Just invoke the method.
		@exception StandardException Standard Derby error policy
	*/
	public void open() throws StandardException
	{
		setup();

        LanguageConnectionContext lcc =
            activation.getLanguageConnectionContext();

        // Push the "authorization stack" of SQL 2003, vol 2, section
        // 4.34.1.1 and 4.27.3.
        lcc.pushCaller(activation);

        // Copy the current role into top cell of stack. Activations
        // inside nested connections look to this activation for
        // keeping its current role rather than rely on what's in lcc
        // (top level only).
        activation.setNestedCurrentRole(lcc.getCurrentRoleId(activation));

        try {
// GemStone changes BEGIN
      String objectName = this.activation.getObjectName();
      // for now be conservative and put the DataDictionary in read mode
      // since the actual procedure execution can end up taking the read lock
      // on DataDictionary causing reversal of lock order and deadlock
      // skip the DD lock for system procedures since SPS can take DD write
      // lock (bug #40995)
      DataDictionary dd = lcc.getDataDictionary();
      int schemaIndex = objectName.indexOf('.');
      if (schemaIndex > 0) {
        String schemaName = objectName.substring(0, schemaIndex);
        if (dd.isSystemSchemaName(schemaName)) {
          isSysProc = true;
        }
      }

      // for system procedures no need for any extra locking
      if (isSysProc) {
        this.methodCall.invoke(this.activation);
      }
      else {
        TransactionController tc = lcc.getTransactionExecute();
        int readMode = dd.startReading(lcc);

        try {
          // take and release read lock on the object (bug #40564)
          GemFireXDUtils.lockObject(null, objectName, false, tc);
          this.methodCall.invoke(this.activation);
        } finally {
          GemFireXDUtils.unlockObject(null, objectName, false, false, tc);
          dd.doneReading(readMode, lcc);
        }
      }
            /* methodCall.invoke(activation); */
// GemStone changes END
        }
        finally {
            activation.getLanguageConnectionContext().popCaller();
        }
    }

    /**
     * Need to explicitly close any dynamic result sets.
     * <BR>
     * If the dynamic results are not accessible then they
     * need to be destroyed (ie. closed) according the the
     * SQL Standard.
     * <BR>
     * An execution of a CALL statement through JDBC makes the
     * dynamic results accessible, in this case the closing
     * of the dynamic result sets is handled by the JDBC
     * statement object (EmbedStatement) that executed the CALL.
     * We cannot unify the closing of dynamic result sets to
     * this close, as in accessible case it is called during
     * the Statement.execute call, thus it would close the
     * dynamic results before the application has a change
     * to use them.
     * 
     * <BR>
     * With an execution of a CALL
     * statement as a trigger's action statement the dynamic
     * result sets are not accessible. In this case this close
     * method is called after the execution of the trigger's
     * action statement.
     * <BR>
     * <BR>
     * Section 4.27.5 of the TECHNICAL CORRIGENDUM 1 to the SQL 2003
     * Standard details what happens to dynamic result sets in detail,
     * the SQL 2003 foundation document is missing these details.
     */
    public void close(boolean cleanupOnError) throws StandardException
    {
        super.close(cleanupOnError);
        
        
        ResultSet[][] dynamicResults = getActivation().getDynamicResults();
        if (dynamicResults != null)
        {
            // Need to ensure all the result sets opened by this
            // CALL statement for this connection are closed.
            // If any close() results in an exception we need to keep going,
            // save any exceptions and then throw them once we are complete.
            StandardException errorOnClose = null;
            
            ConnectionContext jdbcContext = null;
            
            for (int i = 0; i < dynamicResults.length; i++)
            {
                ResultSet[] param = dynamicResults[i];
// GemStone changes BEGIN
                if (param == null) {
                  continue;
                }
// GemStone changes END
                ResultSet drs = param[0];
                
                // Can be null if the procedure never set this parameter
                // or if the dynamic results were processed by JDBC (EmbedStatement).
                if (drs == null)
                    continue;
                
                if (jdbcContext == null)
                    jdbcContext = (ConnectionContext)
                   lcc.getContextManager().getContext(ConnectionContext.CONTEXT_ID);
               
                try {
                    
                    // Is this a valid, open dynamic result set for this connection?
                    if (!jdbcContext.processInaccessibleDynamicResult(drs))
                    {
                        // If not just ignore it, not Derby's problem.
                        continue;
                    }
                    
                    drs.close();
                    
                } catch (SQLException e) {
                    
                    // Just report the first error
                    if (errorOnClose == null)
                    {
                        StandardException se = StandardException.plainWrapException(e);
                        errorOnClose = se;
                    }
                }
                finally {
                    // Remove any reference to the ResultSet to allow
                    // it and any associated resources to be garbage collected.
                    param[0] = null;
                }
            }
            
            if (errorOnClose != null)
                throw errorOnClose;
        }       
    }

	/**
	 * @see com.pivotal.gemfirexd.internal.iapi.sql.ResultSet#cleanUp
	 */
	public void	cleanUp(boolean cleanupOnError) throws StandardException
	{
			close(cleanupOnError);
	}

  // GemStone changes BEGIN
  @Override
  public void accept(ResultSetStatisticsVisitor visitor) {
    visitor.setNumberOfChildren(0);
    visitor.visit(this);
  }
  // GemStone changes END
}
