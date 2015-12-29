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

package com.pivotal.gemfirexd.thrift.server;

import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import com.gemstone.gemfire.internal.cache.locks.NonReentrantLock;
import com.gemstone.gemfire.internal.shared.ClientSharedUtils;
import com.gemstone.gnu.trove.TIntArrayList;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnection;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedStatement;
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState;
import com.pivotal.gemfirexd.thrift.OpenConnectionArgs;
import com.pivotal.gemfirexd.thrift.SecurityMechanism;
import com.pivotal.gemfirexd.thrift.StatementAttrs;
import com.pivotal.gemfirexd.thrift.gfxdConstants;
import com.pivotal.gemfirexd.thrift.common.ThriftExceptionUtil;

/**
 * Holder for a connection on the server side for each open client connection.
 * 
 * @author swale
 * @since gfxd 1.1
 */
final class ConnectionHolder {
  private final EmbedConnection conn;
  private final int connId;
  private final ByteBuffer token;
  private final String clientHostName;
  private final String clientID;
  private final String userName;
  private final boolean useStringForDecimal;
  private EmbedStatement reusableStatement;
  private final ArrayList<StatementHolder> activeStatements;
  private final NonReentrantLock sync;

  ConnectionHolder(final EmbedConnection conn, final OpenConnectionArgs args,
      final int connId, final SecureRandom rnd) throws SQLException {
    this.conn = conn;
    this.connId = connId;

    // generate a unique ID for the connection; this is a secure random string
    // rather than the internal long connection ID to ensure security and is
    // checked in every client-server RPC call if the client has so requested
    if (args.getSecurity() == SecurityMechanism.PLAIN
        || args.getSecurity() == SecurityMechanism.DIFFIE_HELLMAN) {
      int tokenSize = gfxdConstants.DEFAULT_SESSION_TOKEN_SIZE;
      if (args.isSetTokenSize()) {
        if (args.getTokenSize() < tokenSize) {
          // don't accept small token sizes
          throw ThriftExceptionUtil.newSQLException(
              SQLState.NET_CONNECT_AUTH_FAILED, null,
              "specified connection token size " + args.getTokenSize()
                  + " smaller than minimum allowed of " + tokenSize);
        }
        else {
          tokenSize = args.getTokenSize();
        }
      }
      byte[] rndBytes = new byte[tokenSize];
      rnd.nextBytes(rndBytes);
      this.token = ByteBuffer.wrap(rndBytes);
    }
    else {
      // no other security mechanism supported yet
      throw ThriftExceptionUtil.newSQLException(
          SQLState.NET_CONNECT_AUTH_FAILED, null,
          "unsupported security mechanism " + args.getSecurity());
    }

    this.clientHostName = args.getClientHostName();
    this.clientID = args.getClientID();
    this.userName = args.getUserName();
    this.useStringForDecimal = args.isSetUseStringForDecimal()
        && args.useStringForDecimal;
    this.reusableStatement = (EmbedStatement)conn.createStatement();
    this.activeStatements = new ArrayList<StatementHolder>(4);
    this.sync = new NonReentrantLock(true);
  }

  final class StatementHolder {
    private final Statement stmt;
    private final StatementAttrs stmtAttrs;
    private final int stmtId;
    private final String sql;
    private int singleCursorId;
    private Object resultSets;
    private TIntArrayList cursorIds;

    private StatementHolder(Statement stmt, StatementAttrs attrs, int stmtId,
        String sql) {
      this.stmt = stmt;
      this.stmtAttrs = attrs;
      this.stmtId = stmtId;
      this.sql = sql;
      this.singleCursorId = gfxdConstants.INVALID_ID;
    }

    final ConnectionHolder getConnectionHolder() {
      return ConnectionHolder.this;
    }

    final Statement getStatement() {
      return this.stmt;
    }

    final int getStatementId() {
      return this.stmtId;
    }

    final String getSQL() {
      return this.sql;
    }

    final StatementAttrs getStatementAttrs() {
      return this.stmtAttrs;
    }

    void addResultSet(ResultSet rs, int cursorId) {
      final NonReentrantLock sync = ConnectionHolder.this.sync;
      sync.lock();
      try {
        addResultSetNoLock(rs, cursorId);
      } finally {
        sync.unlock();
      }
    }

    private void addResultSetNoLock(ResultSet rs, int cursorId) {
      if (this.resultSets == null) {
        this.resultSets = rs;
        this.singleCursorId = cursorId;
      }
      else if (this.singleCursorId != gfxdConstants.INVALID_ID) {
        assert this.resultSets instanceof ResultSet: "unexpected resultset "
            + this.resultSets;

        ArrayList<Object> results = new ArrayList<Object>(4);
        results.add(this.resultSets);
        results.add(rs);
        this.resultSets = results;
        this.cursorIds = new TIntArrayList(4);
        this.cursorIds.add(this.singleCursorId);
        this.cursorIds.add(cursorId);
        this.singleCursorId = gfxdConstants.INVALID_ID;
      }
      else {
        @SuppressWarnings("unchecked")
        ArrayList<Object> results = (ArrayList<Object>)this.resultSets;
        results.add(rs);
        this.cursorIds.add(cursorId);
      }
    }

    ResultSet findResultSet(int cursorId) {
      final TIntArrayList ids;
      final NonReentrantLock sync = ConnectionHolder.this.sync;
      sync.lock();
      try {
        if (this.singleCursorId == cursorId) {
          return (ResultSet)this.resultSets;
        }
        else if ((ids = this.cursorIds) != null) {
          ArrayList<?> results = (ArrayList<?>)this.resultSets;
          int index = ids.size();
          while (--index >= 0) {
            if (ids.getQuick(index) == cursorId) {
              return (ResultSet)results.get(index);
            }
          }
        }
      } finally {
        sync.unlock();
      }
      return null;
    }

    ResultSet removeResultSet(int cursorId) {
      ResultSet rs = null;
      final NonReentrantLock sync = ConnectionHolder.this.sync;
      sync.lock();
      try {
        final TIntArrayList ids;
        if (this.singleCursorId == cursorId) {
          rs = (ResultSet)this.resultSets;
          this.resultSets = null;
          this.singleCursorId = gfxdConstants.INVALID_ID;
        }
        else if ((ids = this.cursorIds) != null) {
          ArrayList<?> results = (ArrayList<?>)this.resultSets;
          int index = ids.size();
          while (--index >= 0) {
            if (ids.getQuick(index) == cursorId) {
              rs = (ResultSet)results.get(index);
              if (ids.size() == 2) {
                if (index == 1) {
                  this.resultSets = results.get(0);
                  this.singleCursorId = ids.getQuick(0);
                }
                else {
                  this.resultSets = results.get(1);
                  this.singleCursorId = ids.getQuick(1);
                }
                this.cursorIds = null;
              }
              else {
                results.remove(index);
                ids.remove(index);
              }
              break;
            }
          }
        }
      } finally {
        sync.unlock();
      }
      return rs;
    }

    void closeResultSet(final int cursorId, final GFXDServiceImpl service) {
      final ResultSet rs = removeResultSet(cursorId);
      if (rs != null) {
        service.resultSetMap.removePrimitive(cursorId);
        try {
          rs.close();
        } catch (Exception e) {
          // deliberately ignored
        }
      }
    }

    void closeAllResultSets(final GFXDServiceImpl service) {
      TIntArrayList ids;
      if (this.singleCursorId != gfxdConstants.INVALID_ID) {
        try {
          ((ResultSet)this.resultSets).close();
        } catch (SQLException sqle) {
          // ignore exception at this point
          GFXDServiceImpl.log("unexpected exception in ResultSet.close()",
              sqle, "error", true);
        } finally {
          service.resultSetMap.removePrimitive(this.singleCursorId);
          this.resultSets = null;
          this.singleCursorId = gfxdConstants.INVALID_ID;
        }
      }
      else if ((ids = this.cursorIds) != null) {
        ArrayList<?> results = (ArrayList<?>)this.resultSets;
        final int size = ids.size();
        for (int index = 0; index < size; index++) {
          try {
            ((ResultSet)results.get(index)).close();
          } catch (SQLException sqle) {
            // ignore exception at this point
            GFXDServiceImpl.log("unexpected exception in ResultSet.close()",
                sqle, "error", true);
          } finally {
            service.resultSetMap.removePrimitive(ids.getQuick(index));
          }
        }
        this.resultSets = null;
        this.cursorIds = null;
      }
    }
  }

  EmbedStatement createNewStatement(StatementAttrs attrs) throws SQLException {
    // Get the result type
    int resultSetType = GFXDServiceImpl.getResultType(attrs);
    // Get the resultSetConcurrency
    int resultSetConcurrency = GFXDServiceImpl.getResultSetConcurrency(attrs);
    // Get the resultSetHoldability
    int resultSetHoldability = GFXDServiceImpl.getResultSetHoldability(attrs);
    this.sync.lock();
    try {
      final EmbedStatement stmt = this.reusableStatement;
      if (stmt != null) {
        stmt.reset(resultSetType, resultSetConcurrency, resultSetHoldability);
        this.reusableStatement = null;
        return stmt;
      }
    } finally {
      this.sync.unlock();
    }
    return (EmbedStatement)this.conn.createStatement(resultSetType,
        resultSetConcurrency, resultSetHoldability);
  }

  final EmbedConnection getConnection() {
    return this.conn;
  }

  final int getConnectionId() {
    return this.connId;
  }

  final ByteBuffer getToken() {
    return this.token;
  }

  /**
   * Get connection ID (session token) as a hex string. Not very efficient
   * method so use for only exception strings and such.
   */
  final String getTokenAsString() {
    return getTokenAsString(this.token);
  }

  /**
   * Get given session token as a hex string.
   */
  static final String getTokenAsString(ByteBuffer connId) {
    if (connId != null) {
      return ClientSharedUtils.toHexString(connId);
    }
    else {
      return "NULL";
    }
  }

  final String getClientHostName() {
    return this.clientHostName;
  }

  final String getClientID() {
    return this.clientID;
  }

  final String getUserName() {
    return this.userName;
  }

  final boolean useStringForDecimal() {
    return this.useStringForDecimal;
  }

  void setStatementForReuse(EmbedStatement stmt) throws SQLException {
    this.sync.lock();
    try {
      setStatementForReuseNoLock(stmt);
    } finally {
      this.sync.unlock();
    }
  }

  private final void setStatementForReuseNoLock(final EmbedStatement stmt)
      throws SQLException {
    if (this.reusableStatement == null) {
      stmt.resetForReuse();
      this.reusableStatement = stmt;
    }
    else {
      stmt.close();
    }
  }

  StatementHolder registerStatement(Statement stmt, StatementAttrs attrs,
      int stmtId, String preparedSQL) {
    StatementHolder stmtHolder;
    this.sync.lock();
    try {
      stmtHolder = new StatementHolder(stmt, attrs, stmtId, preparedSQL);
      this.activeStatements.add(stmtHolder);
    } finally {
      this.sync.unlock();
    }
    return stmtHolder;
  }

  StatementHolder registerResultSet(Statement stmt, StatementAttrs attrs,
      int stmtId, ResultSet rs, int cursorId, String sql) {
    StatementHolder stmtHolder;
    this.sync.lock();
    try {
      stmtHolder = new StatementHolder(stmt, attrs, stmtId, sql);
      stmtHolder.addResultSetNoLock(rs, cursorId);
      this.activeStatements.add(stmtHolder);
    } finally {
      this.sync.unlock();
    }
    return stmtHolder;
  }

  void closeStatement(final StatementHolder stmtHolder,
      final GFXDServiceImpl service) {
    final int stmtId = stmtHolder.getStatementId();
    this.sync.lock();
    try {
      final Statement stmt;
      final EmbedStatement estmt;
 
      removeActiveStatementNoLock(stmtHolder);
      stmtHolder.closeAllResultSets(service);
      stmt = stmtHolder.getStatement();
      // set statement for reuse now that it is being closed on client
      if (stmt instanceof EmbedStatement
          && !(estmt = (EmbedStatement)stmt).isPrepared()) {
        setStatementForReuseNoLock(estmt);
      }
      else if (stmt != null) {
        stmt.close();
      }
    } catch (Exception e) {
      // deliberately ignored
    } finally {
      this.sync.unlock();
      service.statementMap.removePrimitive(stmtId);
    }
  }

  private final void removeActiveStatementNoLock(
      final StatementHolder stmtHolder) {
    final ArrayList<StatementHolder> statements = this.activeStatements;
    int size = statements.size();
    // usually we will find the statement faster from the back
    while (--size >= 0) {
      if (statements.get(size) == stmtHolder) {
        statements.remove(size);
        break;
      }
    }
  }

  void close(final GFXDServiceImpl service) {
    this.sync.lock();
    try {
      for (StatementHolder stmtHolder : this.activeStatements) {
        stmtHolder.closeAllResultSets(service);
        Statement stmt = stmtHolder.getStatement();
        if (stmt != null) {
          try {
            stmt.close();
          } catch (SQLException sqle) {
            // ignore exception at this point
            GFXDServiceImpl.log("unexpected exception in Statement.close()",
                sqle, "error", true);
          } finally {
            service.statementMap.removePrimitive(stmtHolder.getStatementId());
          }
        }
      }

      if (this.reusableStatement != null) {
        try {
          this.reusableStatement.close();
        } catch (SQLException sqle) {
          // ignore exception at this point
          GFXDServiceImpl.log("unexpected exception in Statement.close()",
              sqle, "error", true);
        }
      }
      try {
        this.conn.close();
      } catch (SQLException sqle) {
        // force close at this point
        this.conn.forceClose();
      }
    } finally {
      this.sync.unlock();
    }
  }

  final boolean equals(ByteBuffer otherId) {
    // if session tokens are not being used then return true
    if (this.token == null) {
      return true;
    }
    return ClientSharedUtils.equalBuffers(this.token, otherId);
  }

  @Override
  public final int hashCode() {
    return this.connId;
  }
}
