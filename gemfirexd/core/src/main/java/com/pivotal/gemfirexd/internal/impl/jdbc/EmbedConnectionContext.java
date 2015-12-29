/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnectionContext

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

//depot/main/java/com.pivotal.gemfirexd.internal.impl.jdbc/EmbedConnectionContext.java#24 - edit change 16899 (text)
package com.pivotal.gemfirexd.internal.impl.jdbc;

// This is the recommended super-class for all contexts.

import com.pivotal.gemfirexd.internal.iapi.error.ExceptionSeverity;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.jdbc.ConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.services.context.ContextImpl;
import com.pivotal.gemfirexd.internal.iapi.services.context.ContextManager;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultSet;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.StatementContext;

import java.sql.SQLException;
import java.util.Vector;
import java.util.Enumeration;
/**
 */
// GemStone changes BEGIN
// made public
public
// GemStone changes END
class EmbedConnectionContext extends ContextImpl 
		implements ConnectionContext
{

	/**
		We hold a soft reference to the connection so that when the application
		releases its reference to the Connection without closing it, its finalize
		method will be called, which will then close the connection. If a direct
		reference is used here, such a Connection will never be closed or garbage
		collected as modules hold onto the ContextManager and thus there would
		be a direct reference through this object.
	*/
// GemStone changed SoftReference to WeakReference
	private java.lang.ref.WeakReference	connRef;
	


	EmbedConnectionContext(ContextManager cm, EmbedConnection conn) {
		super(cm, ConnectionContext.CONTEXT_ID);

		connRef = new java.lang.ref.WeakReference(conn);
	}

	public void cleanupOnError(Throwable error) {

		if (connRef == null)
			return;

		EmbedConnection conn = (EmbedConnection) connRef.get();

		if (error instanceof StandardException) {

			StandardException se = (StandardException) error;
			if (se.getSeverity() < ExceptionSeverity.SESSION_SEVERITY) {

				// any error in auto commit mode that does not close the
				// session will cause a rollback, thus remvoing the need
				// for any commit. We could check this flag under autoCommit
				// being true but the flag is ignored when autoCommit is false
				// so why add the extra check
				if (conn != null) {
					conn.needCommit = false;
				}
				return;
			}
		}

		// This may be a transaction without connection.
		if (conn != null)
			conn.setInactive(); // make the connection inactive & empty

		connRef = null;
		popMe();
	}

	//public java.sql.Connection getEmbedConnection()
	//{
	///	return conn;
	//}

	/**
		Get a connection equivalent to the call
		<PRE>
		DriverManager.getConnection("jdbc:default:connection");
		</PRE>
	*/
	public java.sql.Connection getNestedConnection(boolean internal) throws SQLException {

		EmbedConnection conn = (EmbedConnection) connRef.get();

		if ((conn == null) || conn.isClosed())
			throw Util.noCurrentConnection();

		if (!internal) {
			StatementContext sc = conn.getLanguageConnection().getStatementContext();
			if ((sc == null) || (sc.getSQLAllowed() < com.pivotal.gemfirexd.internal.catalog.types.RoutineAliasInfo.MODIFIES_SQL_DATA))
				throw Util.noCurrentConnection();
		}

		return conn.getLocalDriver().getNewNestedConnection(conn);
	}

	/**
	 * Get a jdbc ResultSet based on the execution ResultSet.
	 *
	 * @param executionResultSet	a result set as gotten from execution
	 *	
	 */	
	public java.sql.ResultSet getResultSet
	(
		ResultSet 				executionResultSet
	) throws SQLException
	{
		EmbedConnection conn = (EmbedConnection) connRef.get();

		EmbedResultSet rs = conn.getLocalDriver().newEmbedResultSet(conn, executionResultSet, 
							false, (EmbedStatement) null, true);
		return rs;
	}

    /**
     * Process a ResultSet from a procedure to be a dynamic result,
     * but one that will be closed due to it being inaccessible. We cannot simply
     * close the ResultSet as it the nested connection that created
     * it might be closed, leading to its close method being a no-op.
     * This performs all the conversion (linking the ResultSet
     * to a valid connection) required but does not close
     * the ResultSet.
     * 
     *   @see EmbedStatement#processDynamicResult(EmbedConnection, java.sql.ResultSet, EmbedStatement)
     */
    public boolean processInaccessibleDynamicResult(java.sql.ResultSet resultSet) {
        EmbedConnection conn = (EmbedConnection) connRef.get();
        if (conn == null)
            return false;
        
        // Pass in null as the Statement to own the ResultSet since
        // we don't have one since the dynamic result will be inaccessible.
        return EmbedStatement.processDynamicResult(conn, resultSet, null) != null;
    }
// GemStone changes BEGIN

    public EmbedConnection getEmbedConnection() {
      return (EmbedConnection)this.connRef.get();
    }

    public static EmbedConnection getEmbedConnection(final ContextManager cm) {
      final EmbedConnectionContext ctx = (EmbedConnectionContext)cm
          .getContext(CONTEXT_ID);
      if (ctx != null) {
        return (EmbedConnection)ctx.connRef.get();
      }
      return null;
    }
// GemStone changes END
}
