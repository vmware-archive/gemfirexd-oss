/*
 * Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
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
package com.gemstone.gemfire.internal.datasource;

/**
 * @author tnegi
 */
import java.sql.SQLException;
//import javax.resource.spi.ConnectionRequestInfo;
//import javax.security.auth.Subject;
import javax.sql.PooledConnection;
import javax.sql.ConnectionEventListener;
import javax.sql.XADataSource;

import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.jta.TransactionUtils;

/**
 * This class models a connection pool for transactional database connection.
 * Extends the AbstractPoolCache to inherit the pool bahavior.
 * 
 * @author tnegi
 */
public class TranxPoolCacheImpl extends AbstractPoolCache {
  private static final long serialVersionUID = 3295652525163658888L;

  private XADataSource m_xads;

  /**
   * Constructor initializes the ConnectionPoolCacheImpl properties.
   */
  public TranxPoolCacheImpl(XADataSource xads,
      ConnectionEventListener eventListner,
      ConfiguredDataSourceProperties configs) throws PoolException {
    super(eventListner, configs);
    m_xads = xads;
    initializePool();
  }

  /**
   *  
   */
  @Override
  void destroyPooledConnection(Object connectionObject) {
    try {
      ((PooledConnection) connectionObject)
          .removeConnectionEventListener((javax.sql.ConnectionEventListener) connEventListner);
      ((PooledConnection) connectionObject).close();
      connectionObject = null;
    }
    catch (Exception ex) {
      LogWriterI18n writer = TransactionUtils.getLogWriterI18n();
      if (writer.finerEnabled())
          writer
              .finer(
                  "AbstractPoolcache::destroyPooledConnection:Exception in closing the connection.Ignoring it. The exeption is "
                      + ex.toString(), ex);
    }
  }

  /**
   * Creates a new connection for the pool. This connection can participate in
   * the transactions.
   * 
   * @return the connection from the database as PooledConnection object.
   */
  @Override
  public Object getNewPoolConnection() throws PoolException {
    if (m_xads != null) {
      PooledConnection poolConn = null;
      try {
        poolConn = m_xads.getXAConnection(configProps.getUser(), configProps
            .getPassword());
      }
      catch (SQLException sqx) {
        throw new PoolException(LocalizedStrings.TranxPoolCacheImpl_TRANXPOOLCACHEIMPLGETNEWCONNECTION_EXCEPTION_IN_CREATING_NEW_TRANSACTION_POOLEDCONNECTION.toLocalizedString(), sqx);
      }
      poolConn
          .addConnectionEventListener((javax.sql.ConnectionEventListener) connEventListner);
      return poolConn;
    }
    else {
      LogWriterI18n writer = TransactionUtils.getLogWriterI18n();
      if (writer.fineEnabled()) writer.fine("TranxPoolCacheImpl::getNewConnection: ConnectionPoolCache not intialized with XADatasource");
      throw new PoolException(LocalizedStrings.TranxPoolCacheImpl_TRANXPOOLCACHEIMPLGETNEWCONNECTION_CONNECTIONPOOLCACHE_NOT_INTIALIZED_WITH_XADATASOURCE.toLocalizedString());
    }
  }
}
