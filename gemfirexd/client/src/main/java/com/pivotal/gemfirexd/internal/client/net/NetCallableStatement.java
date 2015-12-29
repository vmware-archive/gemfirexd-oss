/*

   Derby - Class com.pivotal.gemfirexd.internal.client.net.NetCallableStatement

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
package com.pivotal.gemfirexd.internal.client.net;

import com.pivotal.gemfirexd.internal.client.am.CallableStatement;
import com.pivotal.gemfirexd.internal.client.am.ColumnMetaData;
import com.pivotal.gemfirexd.internal.client.am.MaterialPreparedStatement;
import com.pivotal.gemfirexd.internal.client.am.Section;
import com.pivotal.gemfirexd.internal.client.am.SqlException;
import com.pivotal.gemfirexd.internal.client.am.ClientJDBCObjectFactory;
import com.pivotal.gemfirexd.internal.client.ClientPooledConnection;
import com.pivotal.gemfirexd.jdbc.ClientDRDADriver;

public class NetCallableStatement extends NetPreparedStatement
        implements MaterialPreparedStatement {

    CallableStatement callableStatement_;

    //-----------------------------state------------------------------------------

    //---------------------constructors/finalizer---------------------------------

    private void initNetCallableStatement() {
        callableStatement_ = null;
    }

    // Relay constructor for all NetCallableStatement constructors
    NetCallableStatement(CallableStatement statement,
                         NetAgent netAgent,
                         NetConnection netConnection) throws SqlException {
        super(statement, netAgent, netConnection);
        initNetCallableStatement();
        initNetCallableStatement(statement);
    }

    void resetNetCallableStatement(CallableStatement statement,
                                   NetAgent netAgent,
                                   NetConnection netConnection) throws SqlException {
        super.resetNetPreparedStatement(statement, netAgent, netConnection);
        initNetCallableStatement();
        initNetCallableStatement(statement);
    }

    private void initNetCallableStatement(CallableStatement statement) throws SqlException {
        callableStatement_ = statement;
        callableStatement_.materialCallableStatement_ = this;

    }


    // Called by abstract Connection.prepareCall().newCallableStatement()
    // for jdbc 2 callable statements with scroll attributes.
    NetCallableStatement(NetAgent netAgent,
                         NetConnection netConnection,
                         String sql,
                         int type,
                         int concurrency,
                         int holdability,
                         ClientPooledConnection cpc) throws SqlException {
        this(ClientDRDADriver.getFactory().newCallableStatement(netAgent,
                netConnection, sql, type, concurrency, holdability,cpc),
                netAgent,
                netConnection);
    }

    void resetNetCallableStatement(NetAgent netAgent,
                                   NetConnection netConnection,
                                   String sql,
                                   int type,
                                   int concurrency,
                                   int holdability) throws SqlException {
        callableStatement_.resetCallableStatement(netAgent, netConnection, sql, type, concurrency, holdability);
        resetNetCallableStatement(callableStatement_, netAgent, netConnection);
    }

    void resetNetCallableStatement(NetAgent netAgent,
                                   NetConnection netConnection,
                                   String sql,
                                   Section section) throws SqlException {
        callableStatement_.resetCallableStatement(netAgent, netConnection, sql, section);
        resetNetCallableStatement(callableStatement_, netAgent, netConnection);
    }


    void resetNetCallableStatement(NetAgent netAgent,
                                   NetConnection netConnection,
                                   String sql,
                                   Section section,
                                   ColumnMetaData parameterMetaData,
                                   ColumnMetaData resultSetMetaData) throws SqlException {
        callableStatement_.resetCallableStatement(netAgent, netConnection, sql, section, parameterMetaData, resultSetMetaData);
        resetNetCallableStatement(callableStatement_, netAgent, netConnection);
    }

    protected void finalize() throws java.lang.Throwable {
        super.finalize();
    }

}
