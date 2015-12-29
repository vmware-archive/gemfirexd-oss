/*
 
   Derby - Class com.pivotal.gemfirexd.internal.jdbc.Driver40
 
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
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

package com.pivotal.gemfirexd.internal.jdbc;

import java.sql.DatabaseMetaData;

import com.gemstone.gnu.trove.THashMap;
import com.pivotal.gemfirexd.internal.iapi.jdbc.BrokeredConnection;
import com.pivotal.gemfirexd.internal.iapi.jdbc.BrokeredConnectionControl;
import com.pivotal.gemfirexd.internal.iapi.jdbc.BrokeredConnection40;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultSet;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnection;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnection30;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedPreparedStatement40;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedCallableStatement40;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnection40;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedResultSet;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedResultSet40;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedDatabaseMetaData40;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedResultSetMetaData;
import com.pivotal.gemfirexd.internal.impl.jdbc.SQLExceptionFactory40;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedStatement40;
import com.pivotal.gemfirexd.internal.iapi.jdbc.ResourceAdapter;
import com.pivotal.gemfirexd.internal.impl.jdbc.Util;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.util.Properties;

import com.pivotal.gemfirexd.internal.iapi.sql.ResultColumnDescriptor;


/** -- jdbc 2.0. extension -- */
import javax.sql.PooledConnection;
import javax.sql.XAConnection;

public class Driver40 extends Driver30 {
    
    @Override
    public Connection getNewNestedConnection(EmbedConnection conn) {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(conn instanceof EmbedConnection30,
                "conn expected to be instanceof EmbedConnection30");
        }
        return new EmbedConnection40(conn);
    }
    
// GemStone changes BEGIN
    @Override
    protected EmbedConnection getNewEmbedConnection(String url, Properties info,
        long id, boolean isRemote, long incomingId)
    throws SQLException {
        return new EmbedConnection40(this, url, info, id, incomingId, isRemote);
    }
// GemStone changes END

//  GemStone changes BEGIN
    /**
     * returns a new EmbedStatement
     * @param  conn                 the EmbedConnection class associated with  
     *                              this statement object
     * @param  forMetaData          boolean
     * @param  resultSetType        int
     * @param  resultSetConcurrency int
     * @param  resultSetHoldability int
     * @param id long uniquely identifying the Statement
     * @return Statement            a new java.sql.Statement implementation
     * 
     */
    @Override
    public java.sql.Statement newEmbedStatement(
				EmbedConnection conn,
				boolean forMetaData,
				int resultSetType,
				int resultSetConcurrency,
				int resultSetHoldability, long id)
				    throws SQLException
	{
		return new EmbedStatement40(conn, forMetaData, resultSetType, resultSetConcurrency,
		resultSetHoldability, id);
	}
//  GemStone changes END

//  GemStone changes BEGIN
    @Override
    public PreparedStatement
        newEmbedPreparedStatement(
        EmbedConnection conn,
        String stmt,
        boolean forMetaData,
        int resultSetType,
        int resultSetConcurrency,
        int resultSetHoldability,
        int autoGeneratedKeys,
        int[] columnIndexes,
        String[] columnNames,
        long id, short execFlags, THashMap ncjMetaData, long rootID, int stmtLevel)
        throws SQLException {
        return new EmbedPreparedStatement40(conn,
            stmt,
            forMetaData,
            resultSetType,
            resultSetConcurrency,
            resultSetHoldability,
            autoGeneratedKeys,
            columnIndexes,
            columnNames, id, execFlags, ncjMetaData, rootID,
                   stmtLevel);
    }

    @Override
    public CallableStatement newEmbedCallableStatement(
        EmbedConnection conn,
        String stmt,
        int resultSetType,
        int resultSetConcurrency,
        int resultSetHoldability, long id, short execFlags)
        throws SQLException {
        return new EmbedCallableStatement40(conn,
            stmt,
            resultSetType,
            resultSetConcurrency,
            resultSetHoldability,id,execFlags);
    }
//  GemStone changes END
    @Override
    public BrokeredConnection newBrokeredConnection(BrokeredConnectionControl control) {
        
        return new BrokeredConnection40(control);
    }
    
    @Override
    public EmbedResultSet newEmbedResultSet(EmbedConnection conn, ResultSet results, boolean forMetaData, com.pivotal.gemfirexd.internal.impl.jdbc.EmbedStatement statement,boolean isAtomic) throws SQLException {
        return new EmbedResultSet40(conn, results, forMetaData, statement,
            isAtomic);
    }
    
    /**
     * Overwriting the super class boot method to set exception factory
     * @see InternalDriver#boot
     */

    @Override
	public void boot(boolean create, Properties properties) 
          throws StandardException {
        Util.setExceptionFactory (new SQLExceptionFactory40 ());
        super.boot (create, properties);
    }

    @Override
    public DatabaseMetaData newEmbedDatabaseMetaData(EmbedConnection conn, String dbname) 
        throws SQLException {
		return new EmbedDatabaseMetaData40(conn,dbname);
    }
    
        /**
         * Returns a new java.sql.ResultSetMetaData for this implementation
         *
         * @param  columnInfo a ResultColumnDescriptor that stores information 
         *                    about the columns in a ResultSet
         * @return ResultSetMetaData
         */
    @Override
        public EmbedResultSetMetaData newEmbedResultSetMetaData
                             (ResultColumnDescriptor[] columnInfo) {
            return new EmbedResultSetMetaData(columnInfo);
        }

    /**
     * Create and return an EmbedPooledConnection from the received instance
     * of EmbeddedDataSource.
     */
    @Override
    protected PooledConnection getNewPooledConnection(
        EmbeddedDataSource eds, String user, String password,
        boolean requestPassword) throws SQLException
    {
        return new EmbedPooledConnection40(
            eds, user, password, requestPassword);
    }

    /**
     * Create and return an EmbedXAConnection from the received instance
     * of EmbeddedDataSource.
     */
    @Override
    protected XAConnection getNewXAConnection(
        EmbeddedDataSource eds, ResourceAdapter ra,
        String user, String password, boolean requestPassword)
        throws SQLException
    {
        return new EmbedXAConnection40(
            eds, ra, user, password, requestPassword);
    }
}
