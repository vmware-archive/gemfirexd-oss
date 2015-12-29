/*

   Derby - Class com.pivotal.gemfirexd.internal.client.ClientXAConnection

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
package com.pivotal.gemfirexd.internal.client;

import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;

import com.pivotal.gemfirexd.internal.client.am.SqlException;
import com.pivotal.gemfirexd.internal.client.net.NetLogWriter;
import com.pivotal.gemfirexd.internal.client.net.NetXAConnection;
import com.pivotal.gemfirexd.internal.jdbc.ClientXADataSource;

// GemStone changes BEGIN
// made this class abstract so that it doesn't need to refer to
// StatementEventListener that fails compilation with JDK 1.4;
// concrete class for JDBC3 is ClientXAConnection30
abstract
// GemStone changes END
public class ClientXAConnection extends ClientPooledConnection implements XAConnection {
    private static int rmIdSeed_ = 95688932; // semi-random starting value for rmId

    private ClientXADataSource derbyds_ = null;
    private XAResource xares_ = null;
    private com.pivotal.gemfirexd.internal.client.net.NetXAResource netXares_ = null;
    private boolean fFirstGetConnection_ = true;
    private Connection logicalCon_; // logicalConnection_ is inherited from ClientPooledConnection
    // This connection is used to access the indoubt table
    private NetXAConnection controlCon_ = null;

    public ClientXAConnection(ClientXADataSource ds,
                              com.pivotal.gemfirexd.internal.client.net.NetLogWriter logWtr,
                              String userId,
                              String password) throws SQLException {
        super(ds, logWtr, userId, password, getUnigueRmId());
        derbyds_ = ds;

        // Have to instantiate a real connection here,
        // otherwise if XA function is called before the connect happens,
        // an error will be returned
        // Note: conApp will be set after this call
        logicalCon_ = super.getConnection();

        netXares_ = new com.pivotal.gemfirexd.internal.client.net.NetXAResource(this,
                rmId_, userId, password, netXAPhysicalConnection_);
        xares_ = netXares_;
    }

    public Connection getConnection() throws SQLException {
        if (fFirstGetConnection_) {
            // Since super.getConnection() has already been called once
            // in the constructor, we don't need to call it again for the
            // call of this method.
            fFirstGetConnection_ = false;
        } else {
            // A new connection object is required
            logicalCon_ = super.getConnection();
            if (this.physicalConnection_ != null) { // have a physical connection, check if a NetXAResource
                if (netXAPhysicalConnection_ != null) { // the XAResource is a NetXAResource, re-initialize it
                    netXares_.initForReuse();
                }
            }
        }
        return logicalCon_;
    }

    private static synchronized int getUnigueRmId() {
        rmIdSeed_ += 1;
        return rmIdSeed_;
    }

    public int getRmId() {
        return rmId_;
    }

    public XAResource getXAResource() throws SQLException {
        if (logWriter_ != null) {
            logWriter_.traceExit(this, "getXAResource", xares_);
        }

        return xares_;
    }

    public ClientXADataSource getDataSource() throws SqlException {
        if (logWriter_ != null) {
            logWriter_.traceExit(this, "getDataSource", derbyds_);
        }

        return derbyds_;
    }

    public NetXAConnection createControlConnection(NetLogWriter logWriter,
                                                   String user,
                                                   String password,
                                                   com.pivotal.gemfirexd.internal.jdbc.ClientDataSource dataSource,
                                                   int rmId,
                                                   boolean isXAConn) throws SQLException {
        try
        {
            controlCon_ = new NetXAConnection(logWriter,
                    user,
                    password,
                    dataSource,
                    rmId,
                    isXAConn,
                    this);
            controlCon_.getNetConnection().setTransactionIsolation(
                    Connection.TRANSACTION_READ_UNCOMMITTED);

            if (logWriter_ != null) {
                logWriter_.traceExit(this, "createControlConnection", controlCon_);
            }

            return controlCon_;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException(controlCon_ != null ? controlCon_
                .getNetConnection().agent_ : null /* GemStoneAddition */);
        }            
    }


    public synchronized void close() throws SQLException {
        super.close();
    }
}

