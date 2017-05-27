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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.TransactionFlag;
import com.gemstone.gemfire.internal.concurrent.ConcurrentTLongObjectHashMap;
import com.gemstone.gemfire.internal.shared.SystemProperties;
import com.gemstone.gnu.trove.TObjectProcedure;
import com.pivotal.gemfirexd.Attribute;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.sql.conn.GfxdHeapThresholdListener;
import com.pivotal.gemfirexd.internal.engine.store.GemFireStore;
import com.pivotal.gemfirexd.internal.iapi.jdbc.EngineLOB;
import com.pivotal.gemfirexd.internal.iapi.reference.Property;
import com.pivotal.gemfirexd.internal.iapi.services.i18n.MessageService;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultColumnDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.store.access.XATransactionController;
import com.pivotal.gemfirexd.internal.iapi.types.DataTypeDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.DataTypeUtilities;
import com.pivotal.gemfirexd.internal.iapi.types.TypeId;
import com.pivotal.gemfirexd.internal.iapi.util.IdUtil;
import com.pivotal.gemfirexd.internal.impl.jdbc.*;
import com.pivotal.gemfirexd.internal.jdbc.InternalDriver;
import com.pivotal.gemfirexd.internal.shared.common.SharedUtils;
import com.pivotal.gemfirexd.internal.shared.common.error.ExceptionSeverity;
import com.pivotal.gemfirexd.internal.shared.common.reference.JDBC40Translation;
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;
import com.pivotal.gemfirexd.thrift.*;
import com.pivotal.gemfirexd.thrift.RowIdLifetime;
import com.pivotal.gemfirexd.thrift.common.Converters;
import com.pivotal.gemfirexd.thrift.common.OptimizedElementArray;
import com.pivotal.gemfirexd.thrift.common.ThriftUtils;
import com.pivotal.gemfirexd.thrift.server.ConnectionHolder.StatementHolder;
import org.apache.thrift.ProcessFunction;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TType;

/**
 * Server-side implementation of thrift GFXDService (see gfxd.thrift).
 *
 * @author swale
 * @since gfxd 1.1
 */
public final class GFXDServiceImpl extends LocatorServiceImpl implements
    GFXDService.Iface {

  final GfxdHeapThresholdListener thresholdListener;
  final SecureRandom rand;
  final ConcurrentTLongObjectHashMap<ConnectionHolder> connectionMap;
  final ConcurrentTLongObjectHashMap<StatementHolder> statementMap;
  final ConcurrentTLongObjectHashMap<StatementHolder> resultSetMap;
  final AtomicInteger currentConnectionId;
  final AtomicInteger currentStatementId;
  final AtomicInteger currentCursorId;

  private static final int INVALID_ID = gfxdConstants.INVALID_ID;

  public GFXDServiceImpl(String address, int port) {
    super(address, port);
    final GemFireStore store = Misc.getMemStoreBooting();
    this.thresholdListener = store.thresholdListener();

    this.rand = new SecureRandom();
    // Force the random generator to seed itself.
    final byte[] someBytes = new byte[55];
    this.rand.nextBytes(someBytes);

    this.connectionMap = new ConcurrentTLongObjectHashMap<ConnectionHolder>();
    this.statementMap = new ConcurrentTLongObjectHashMap<StatementHolder>();
    this.resultSetMap = new ConcurrentTLongObjectHashMap<StatementHolder>();
    this.currentConnectionId = new AtomicInteger(1);
    this.currentStatementId = new AtomicInteger(1);
    this.currentCursorId = new AtomicInteger(1);
  }

  /**
   * Custom Processor implementation to handle closeConnection by closing
   * server-side connection cleanly.
   */
  public static final class Processor extends
      GFXDService.Processor<GFXDServiceImpl> {

    private final GFXDServiceImpl inst;
    private final HashMap<String, ProcessFunction<GFXDServiceImpl, ?>> fnMap;

    public Processor(GFXDServiceImpl inst) {
      super(inst);
      this.inst = inst;
      this.fnMap = new HashMap<String, ProcessFunction<GFXDServiceImpl, ?>>(
          super.getProcessMapView());
    }

    @Override
    public final boolean process(final TProtocol in, final TProtocol out)
        throws TException {
      final TMessage msg = in.readMessageBegin();
      final ProcessFunction<GFXDServiceImpl, ?> fn = this.fnMap.get(msg.name);
      if (fn != null) {
        fn.process(msg.seqid, in, out, this.inst);
        // terminate connection on receiving closeConnection
        // direct class comparison should be the fastest way
        // TODO: SW: also need to clean up connection artifacts in the case of
        // client connection failure (ConnectionListener does get a notification
        // but how to tie the socket/connectionNumber to the connectionID?)
        return fn.getClass() != GFXDService.Processor.closeConnection.class;
      }
      else {
        TProtocolUtil.skip(in, TType.STRUCT);
        in.readMessageEnd();
        TApplicationException x = new TApplicationException(
            TApplicationException.UNKNOWN_METHOD, "Invalid method name: '"
                + msg.name + "'");
        out.writeMessageBegin(new TMessage(msg.name, TMessageType.EXCEPTION,
            msg.seqid));
        x.write(out);
        out.writeMessageEnd();
        out.getTransport().flush();
        return true;
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ConnectionProperties openConnection(OpenConnectionArgs arguments)
      throws GFXDException {
    ConnectionHolder connHolder;
    try {
      Properties props = new Properties();
      String clientHost = null, clientId = null;

      if (arguments != null) {
        if (arguments.isSetUserName()) {
          props.put(Attribute.USERNAME_ATTR, arguments.getUserName());
        }
        if (arguments.isSetPassword()) {
          props.put(Attribute.PASSWORD_ATTR, arguments.getPassword());
        }
        if (arguments.isSetProperties()) {
          props.putAll(arguments.getProperties());
        }
        clientHost = arguments.getClientHostName();
        clientId = arguments.getClientID();
      }

      final String protocol;
      // default route-query to true on client connections
      if (Misc.getMemStoreBooting().isSnappyStore()) {
        if (!props.containsKey(Attribute.ROUTE_QUERY)) {
          props.setProperty(Attribute.ROUTE_QUERY, "true");
        }
        protocol = Attribute.SNAPPY_PROTOCOL;
      } else {
        protocol = Attribute.PROTOCOL;
      }
      EmbedConnection conn = (EmbedConnection)InternalDriver.activeDriver()
          .connect(protocol, props, Converters
              .getJdbcIsolation(gfxdConstants.DEFAULT_TRANSACTION_ISOLATION));
      conn.setAutoCommit(gfxdConstants.DEFAULT_AUTOCOMMIT);
      while (true) {
        final int connId = getNextId(this.currentConnectionId);
        connHolder = new ConnectionHolder(conn, arguments, connId, this.rand);
        if (this.connectionMap.putIfAbsent(connId, connHolder) == null) {
          ConnectionProperties connProps = new ConnectionProperties(connId,
              clientHost, clientId);
          connProps.setToken(connHolder.getToken());
          return connProps;
        }
      }

    } catch (Throwable t) {
      throw gfxdException(t);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void closeConnection(int connId, ByteBuffer token) {
    try {
      ConnectionHolder connHolder = this.connectionMap.getPrimitive(connId);
      if (connHolder != null) {
        if (connHolder.equals(token)) {
          connHolder.close(this);
          this.connectionMap.removePrimitive(connId);
        }
        else {
          throw tokenMismatchException(token, "closeConnection [connId="
              + connId + ']');
        }
      }
    } catch (Throwable t) {
      if (!ignoreNonFatalException(t)) {
        SanityManager.DEBUG_PRINT(SanityManager.TRACE_CLIENT_CONN,
            "Unexpected exception in closeConnection for CONNID=" + connId, t);
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void bulkClose(List<EntityId> entities) {
    if (entities == null) {
      return;
    }
    for (EntityId entity : entities) {
      try {
        int id = entity.id;
        ByteBuffer token = entity.token;
        switch (entity.type) {
          case gfxdConstants.BULK_CLOSE_RESULTSET:
            closeResultSet(id, token);
            break;
          case gfxdConstants.BULK_CLOSE_LOB:
            freeLob(entity.connId, id, token);
            break;
          case gfxdConstants.BULK_CLOSE_STATEMENT:
            closeStatement(id, token);
            break;
          case gfxdConstants.BULK_CLOSE_CONNECTION:
            closeConnection(id, token);
            break;
          default:
            // ignore; this is a oneway function
            break;
        }
      } catch (GFXDException gfxde) {
        if (SQLState.NET_CONNECT_AUTH_FAILED.substring(0, 5).equals(
            gfxde.getExceptionData().getSqlState())) {
          SanityManager.DEBUG_PRINT("warning:"
              + SanityManager.TRACE_CLIENT_STMT,
              "AUTH exception in bulkClose (continuing with other "
                  + "close operations): " + gfxde);
        }
      } catch (Throwable t) {
        if (!ignoreNonFatalException(t)) {
          SanityManager.DEBUG_PRINT(SanityManager.TRACE_CLIENT_STMT,
              "Unexpected exception in bulkClose (continuing with other "
                  + "close operations): " + t);
        }
      }
    }
  }

  /**
   * Helper method Validate the connection Id. If Id not found in the map throw
   * the connection unavailable exception.
   *
   * @param token
   * @return ConnectionHolder if found in the map.
   * @throws GFXDException
   */
  private ConnectionHolder getValidConnection(int connId, ByteBuffer token)
      throws GFXDException {
    ConnectionHolder connHolder = this.connectionMap.getPrimitive(connId);
    if (connHolder != null) {
      if (connHolder.equals(token)) {
        return connHolder;
      }
      else {
        throw tokenMismatchException(token, "getConnection [connId=" + connId
            + ']');
      }
    }
    else {
      // TODO: SW: i18 string here
      GFXDExceptionData exData = new GFXDExceptionData();
      exData.setReason("No connection with ID=0x"
          + ConnectionHolder.getTokenAsString(token));
      exData.setSqlState(SQLState.NO_CURRENT_CONNECTION);
      exData.setSeverity(ExceptionSeverity.STATEMENT_SEVERITY);
      throw new GFXDException(exData, getServerInfo());
    }
  }

  private StatementHolder getStatement(ByteBuffer token, int stmtId,
      boolean isPrepared, String op) throws GFXDException {
    StatementHolder stmtHolder;
    if ((stmtHolder = this.statementMap.getPrimitive(stmtId)) != null) {
      if (stmtHolder.getConnectionHolder().equals(token)) {
        if (!isPrepared
            || stmtHolder.getStatement() instanceof PreparedStatement) {
          return stmtHolder;
        }
        else {
          throw statementNotFoundException(stmtId, op, isPrepared);
        }
      }
      else {
        throw tokenMismatchException(token, op);
      }
    }
    else {
      throw statementNotFoundException(stmtId, op, isPrepared);
    }
  }

  private StatementHolder getStatementForResultSet(ByteBuffer token,
      int cursorId, String op) throws GFXDException {
    StatementHolder stmtHolder;
    if ((stmtHolder = this.resultSetMap.getPrimitive(cursorId)) != null) {
      if (stmtHolder.getConnectionHolder().equals(token)) {
        return stmtHolder;
      }
      else {
        throw tokenMismatchException(token, op);
      }
    }
    else {
      throw resultSetNotFoundException(cursorId, op);
    }
  }

  GFXDException tokenMismatchException(ByteBuffer token, final String op) {
    GFXDExceptionData exData = new GFXDExceptionData();
    exData.setReason(MessageService.getTextMessage(
        SQLState.NET_CONNECT_AUTH_FAILED, "connection token "
            + ConnectionHolder.getTokenAsString(token)
            + " mismatch for operation " + op));
    exData.setSqlState(SQLState.NET_CONNECT_AUTH_FAILED.substring(0, 5));
    exData.setSeverity(ExceptionSeverity.SESSION_SEVERITY);
    return new GFXDException(exData, getServerInfo());
  }

  GFXDException resultSetNotFoundException(int cursorId, String op) {
    // TODO: SW: i18 string
    GFXDExceptionData exData = new GFXDExceptionData();
    exData.setReason("No result set open with ID=" + cursorId
        + " for operation " + op);
    exData.setSqlState(SQLState.LANG_RESULT_SET_NOT_OPEN.substring(0, 5));
    exData.setSeverity(ExceptionSeverity.STATEMENT_SEVERITY);
    return new GFXDException(exData, getServerInfo());
  }

  GFXDException statementNotFoundException(int stmtId, String op,
      boolean isPrepared) {
    // TODO: SW: i18 string
    GFXDExceptionData exData = new GFXDExceptionData();
    exData.setReason("No " + (isPrepared ? "prepared " : "")
        + "statement with ID=" + stmtId + " for operation " + op);
    exData.setSqlState(SQLState.LANG_DEAD_STATEMENT);
    exData.setSeverity(ExceptionSeverity.STATEMENT_SEVERITY);
    return new GFXDException(exData, getServerInfo());
  }

  @Override
  protected String getServerInfo() {
    return "Server=" + this.hostAddress + '[' + this.hostPort + "] Thread="
        + Thread.currentThread().getName();
  }

  private static int getNextId(final AtomicInteger id) {
    while (true) {
      int currentId = id.get();
      int nextId = currentId + 1;
      if (nextId == INVALID_ID) {
        nextId++;
      }
      if (id.compareAndSet(currentId, nextId)) {
        return currentId;
      }
    }
  }

  /**
   * Returns the resultType if set if not set returns the default
   * java.sql.ResultSet.TYPE_FORWARD_ONLY
   */
  static int getResultType(StatementAttrs attrs) {
    int rsType;
    if (attrs != null && attrs.isSetResultSetType()) {
      rsType = attrs.getResultSetType();
    }
    else {
      rsType = gfxdConstants.DEFAULT_RESULTSET_TYPE;
    }
    switch (rsType) {
      case gfxdConstants.RESULTSET_TYPE_FORWARD_ONLY:
        return ResultSet.TYPE_FORWARD_ONLY;
      case gfxdConstants.RESULTSET_TYPE_INSENSITIVE:
        return ResultSet.TYPE_SCROLL_INSENSITIVE;
      case gfxdConstants.RESULTSET_TYPE_SENSITIVE:
        return ResultSet.TYPE_SCROLL_SENSITIVE;
      default:
        throw new InternalGemFireError("unknown resultSet type "
            + attrs.getResultSetType());
    }
  }

  /**
   * Returns the Concurrency associated with the statement if set the the input
   * object if not set by default returns java.sql.ResultSet.CONCUR_READ_ONLY
   *
   * @param attrs
   * @return
   */
  static int getResultSetConcurrency(StatementAttrs attrs) {
    return attrs != null && attrs.isSetUpdatable() && attrs.isUpdatable()
        ? ResultSet.CONCUR_UPDATABLE : ResultSet.CONCUR_READ_ONLY;
  }

  /**
   * Returns the Holdability associated with the statement if set the the input
   * object if not set by default returns #ResultS
   *
   * @param attrs
   * @return
   */
  static int getResultSetHoldability(StatementAttrs attrs) {
    return attrs != null && attrs.isSetHoldCursorsOverCommit()
        && attrs.isHoldCursorsOverCommit() ? ResultSet.HOLD_CURSORS_OVER_COMMIT
        : ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  private BlobChunk handleBlob(Blob blob, ConnectionHolder connHolder,
      StatementAttrs attrs) throws SQLException {
    final long length = blob.length();
    if (length > Integer.MAX_VALUE) {
      throw Util.generateCsSQLException(SQLState.BLOB_TOO_LARGE_FOR_CLIENT,
          Long.toString(length), Long.toString(Integer.MAX_VALUE));
    }
    BlobChunk chunk = new BlobChunk().setOffset(0).setTotalLength(length);
    final int chunkSize;
    if (attrs != null && attrs.isSetLobChunkSize()) {
      chunkSize = attrs.lobChunkSize;
    }
    else {
      chunkSize = gfxdConstants.DEFAULT_LOB_CHUNKSIZE;
    }
    if (chunkSize > 0 && chunkSize < length) {
      chunk.setChunk(blob.getBytes(1, chunkSize)).setLast(false);
      // need to add explicit mapping for the LOB in this case
      int lobId;
      if (blob instanceof EngineLOB) {
        lobId = ((EngineLOB)blob).getLocator();
      }
      else {
        lobId = connHolder.getConnection().addLOBMapping(blob);
      }
      chunk.setLobId(lobId);
    }
    else {
      chunk.setChunk(blob.getBytes(1, (int)length)).setLast(true);
    }
    return chunk;
  }

  private ClobChunk handleClob(Clob clob, ConnectionHolder connHolder,
      StatementAttrs attrs) throws SQLException {
    final long length = clob.length();
    if (length > Integer.MAX_VALUE) {
      throw Util.generateCsSQLException(SQLState.BLOB_TOO_LARGE_FOR_CLIENT,
          Long.toString(length), Long.toString(Integer.MAX_VALUE));
    }
    ClobChunk chunk = new ClobChunk().setOffset(0)
        .setTotalLength(length);
    final int chunkSize;
    if (attrs != null && attrs.isSetLobChunkSize()) {
      chunkSize = attrs.lobChunkSize;
    }
    else {
      chunkSize = gfxdConstants.DEFAULT_LOB_CHUNKSIZE;
    }
    if (chunkSize > 0 && chunkSize < length) {
      chunk.setChunk(clob.getSubString(1, chunkSize)).setLast(false);
      // need to add explicit mapping for the LOB in this case
      int lobId;
      if (clob instanceof EngineLOB) {
        lobId = ((EngineLOB)clob).getLocator();
      }
      else {
        lobId = connHolder.getConnection().addLOBMapping(clob);
      }
      chunk.setLobId(lobId);
    }
    else {
      chunk.setChunk(clob.getSubString(1, (int)length)).setLast(true);
    }
    return chunk;
  }

  /**
   * Set a column value in a Row. Refer to gfxd.thrift file for the definition
   * of ColumnValue structure. Remember it is Illegal to define more than one
   * value in the ColumnValue structure. We cannot make it union becuase of the
   * existing bug in the Apache Thrift.
   * https://issues.apache.org/jira/browse/THRIFT-1833
   *
   * The java version of Row overrides the thrift one to make use of
   * {@link OptimizedElementArray} to reduce overhead/objects while still
   * keeping serialization compatible with thrift Row.
   */
  private void setColumnValue(EmbedResultSet rs, GFXDType colType,
      int columnIndex, ConnectionHolder connHolder, StatementAttrs attrs,
      Row result) throws SQLException {
    final int index = columnIndex - 1;
    switch (colType) {
      case BOOLEAN:
        boolean boolValue = rs.getBoolean(columnIndex);
        if (rs.wasNull()) {
          result.setNull(index);
        }
        else {
          result.setBoolean(index, boolValue);
        }
        break;
      case TINYINT:
        byte byteValue = rs.getByte(columnIndex);
        if (rs.wasNull()) {
          result.setNull(index);
        }
        else {
          result.setByte(index, byteValue);
        }
        break;
      case SMALLINT:
        short shortValue = rs.getShort(columnIndex);
        if (rs.wasNull()) {
          result.setNull(index);
        }
        else {
          result.setShort(index, shortValue);
        }
        break;
      case INTEGER:
        int intValue = rs.getInt(columnIndex);
        if (rs.wasNull()) {
          result.setNull(index);
        }
        else {
          result.setInt(index, intValue);
        }
        break;
      case BIGINT:
        long longValue = rs.getLong(columnIndex);
        if (rs.wasNull()) {
          result.setNull(index);
        }
        else {
          result.setLong(index, longValue);
        }
        break;
      case REAL:
        float fltValue = rs.getFloat(columnIndex);
        if (rs.wasNull()) {
          result.setNull(index);
        }
        else {
          result.setFloat(index, fltValue);
        }
        break;
      case FLOAT:
      case DOUBLE:
        double dblValue = rs.getDouble(columnIndex);
        if (rs.wasNull()) {
          result.setNull(index);
        }
        else {
          result.setDouble(index, dblValue);
        }
        break;
      case CHAR:
      case VARCHAR:
      case LONGVARCHAR:
      case NCHAR:
      case NVARCHAR:
      case LONGNVARCHAR:
      case SQLXML:
        String strValue = rs.getString(columnIndex);
        if (rs.wasNull()) {
          result.setNull(index);
        }
        else {
          result.setObject(index, strValue, colType);
        }
        break;
      case BLOB:
        Blob blob = rs.getBlob(columnIndex);
        if (rs.wasNull()) {
          result.setNull(index);
        }
        else {
          result.setObject(index, handleBlob(blob, connHolder, attrs),
              GFXDType.BLOB);
        }
        break;
      case CLOB:
      case NCLOB:
        Clob clob = rs.getClob(columnIndex);
        if (rs.wasNull()) {
          result.setNull(index);
        }
        else {
          result.setObject(index, handleClob(clob, connHolder, attrs),
              GFXDType.CLOB);
        }
        break;
      case DECIMAL:
        BigDecimal bd = rs.getBigDecimal(columnIndex);
        if (rs.wasNull()) {
          result.setNull(index);
        }
        else {
          if (connHolder.useStringForDecimal()) {
            result.setObject(index, bd.toPlainString(), GFXDType.VARCHAR);
          }
          else {
            result.setObject(index, bd, GFXDType.DECIMAL);
          }
        }
        break;
      case DATE:
        Date dtVal = rs.getDate(columnIndex);
        if (rs.wasNull()) {
          result.setNull(index);
        }
        else {
          result.setObject(index, dtVal, GFXDType.DATE);
        }
        break;
      case TIME:
        Time timeVal = rs.getTime(columnIndex);
        if (rs.wasNull()) {
          result.setNull(index);
        }
        else {
          result.setObject(index, timeVal, GFXDType.TIME);
        }
        break;
      case TIMESTAMP:
        java.sql.Timestamp tsVal = rs.getTimestamp(columnIndex);
        if (rs.wasNull()) {
          result.setNull(index);
        }
        else {
          result.setObject(index, tsVal, GFXDType.TIMESTAMP);
        }
        break;
      case BINARY:
      case VARBINARY:
      case LONGVARBINARY:
        byte[] byteArray = rs.getBytes(columnIndex);
        if (rs.wasNull()) {
          result.setNull(index);
        }
        else {
          result.setObject(index, byteArray, colType);
        }
        break;
      case NULLTYPE:
        result.setNull(index);
        break;
      case JAVA_OBJECT:
        Object o = rs.getObject(columnIndex);
        if (rs.wasNull()) {
          result.setNull(index);
        }
        else if (o instanceof PDXObject) {
          result.setObject(index, o, GFXDType.PDX_OBJECT);
        }
        else if (o instanceof JSONObject) {
          result.setObject(index, o, GFXDType.JSON_OBJECT);
        }
        else {
          result.setObject(index, o, GFXDType.JAVA_OBJECT);
        }
        break;
      case PDX_OBJECT:
        o = rs.getObject(columnIndex);
        if (rs.wasNull()) {
          result.setNull(index);
        }
        else {
          result.setObject(index, o, GFXDType.PDX_OBJECT);
        }
        break;
      case JSON_OBJECT:
        o = rs.getObject(columnIndex);
        if (rs.wasNull()) {
          result.setNull(index);
        }
        else {
          result.setObject(index, o, GFXDType.JSON_OBJECT);
        }
        break;
      case DISTINCT:
      case STRUCT:
      case ARRAY:
      case REF:
      case DATALINK:
      case ROWID:
      case OTHER:
        throw Util.generateCsSQLException(SQLState.DATA_TYPE_NOT_SUPPORTED,
            Util.typeName(Converters.getJdbcType(colType)));
    }
  }

  /**
   * Set a column value in a Row. Refer to gfxd.thrift file for the definition
   * of ColumnValue structure Remember it is Illegal to define more than on
   * value in the ColumnValue structure. We cannot make it union becuase of the
   * existing bug in the Apache Thrift.
   * https://issues.apache.org/jira/browse/THRIFT-1833
   *
   * The java version of Row overrides the thrift one to make use of
   * {@link OptimizedElementArray} to reduce overhead/objects while still
   * keeping serialization compatible with thrift Row.
   */
  private void setColumnValue(ResultSet rs, ResultSetMetaData rsmd,
      int columnIndex, ConnectionHolder connHolder, StatementAttrs attrs,
      Row result) throws SQLException {
    int colType = rsmd.getColumnType(columnIndex);
    final int index = columnIndex - 1;
    switch (colType) {
      case Types.BOOLEAN:
      case Types.BIT:
        boolean boolValue = rs.getBoolean(columnIndex);
        if (rs.wasNull()) {
          result.setNull(index);
        }
        else {
          result.setBoolean(index, boolValue);
        }
        break;
      case Types.TINYINT:
        byte byteValue = rs.getByte(columnIndex);
        if (rs.wasNull()) {
          result.setNull(index);
        }
        else {
          result.setByte(index, byteValue);
        }
        break;
      case Types.SMALLINT:
        short shortValue = rs.getShort(columnIndex);
        if (rs.wasNull()) {
          result.setNull(index);
        }
        else {
          result.setShort(index, shortValue);
        }
        break;
      case Types.INTEGER:
        int intValue = rs.getInt(columnIndex);
        if (rs.wasNull()) {
          result.setNull(index);
        }
        else {
          result.setInt(index, intValue);
        }
        break;
      case Types.BIGINT:
        long longValue = rs.getLong(columnIndex);
        if (rs.wasNull()) {
          result.setNull(index);
        }
        else {
          result.setLong(index, longValue);
        }
        break;
      case Types.REAL:
        float fltValue = rs.getFloat(columnIndex);
        if (rs.wasNull()) {
          result.setNull(index);
        }
        else {
          result.setFloat(index, fltValue);
        }
        break;
      case Types.FLOAT:
      case Types.DOUBLE:
        double dblValue = rs.getDouble(columnIndex);
        if (rs.wasNull()) {
          result.setNull(index);
        }
        else {
          result.setDouble(index, dblValue);
        }
        break;
      case Types.CHAR:
      case Types.VARCHAR:
      case Types.LONGVARCHAR:
      case Types.NCHAR:
      case Types.NVARCHAR:
      case Types.LONGNVARCHAR:
      case Types.SQLXML:
        String strValue = rs.getString(columnIndex);
        if (rs.wasNull()) {
          result.setNull(index);
        }
        else {
          result.setObject(index, strValue,
              Converters.getThriftSQLType(colType));
        }
        break;
      case Types.BLOB:
        Blob blob = rs.getBlob(columnIndex);
        if (rs.wasNull()) {
          result.setNull(index);
        }
        else {
          result.setObject(index, handleBlob(blob, connHolder, attrs),
              GFXDType.BLOB);
        }
        break;
      case Types.CLOB:
      case Types.NCLOB:
        Clob clob = rs.getClob(columnIndex);
        if (rs.wasNull()) {
          result.setNull(index);
        }
        else {
          result.setObject(index, handleClob(clob, connHolder, attrs),
              GFXDType.CLOB);
        }
        break;
      case Types.NUMERIC:
      case Types.DECIMAL:
        BigDecimal bd = rs.getBigDecimal(columnIndex);
        if (rs.wasNull()) {
          result.setNull(index);
        }
        else {
          if (connHolder.useStringForDecimal()) {
            result.setObject(index, bd.toPlainString(), GFXDType.VARCHAR);
          }
          else {
            result.setObject(index, bd, GFXDType.DECIMAL);
          }
        }
        break;
      case Types.DATE:
        Date dtVal = rs.getDate(columnIndex);
        if (rs.wasNull()) {
          result.setNull(index);
        }
        else {
          result.setObject(index, dtVal, GFXDType.DATE);
        }
        break;
      case Types.TIME:
        Time timeVal = rs.getTime(columnIndex);
        if (rs.wasNull()) {
          result.setNull(index);
        }
        else {
          result.setObject(index, timeVal, GFXDType.TIME);
        }
        break;
      case Types.TIMESTAMP:
        java.sql.Timestamp tsVal = rs.getTimestamp(columnIndex);
        if (rs.wasNull()) {
          result.setNull(index);
        }
        else {
          result.setObject(index, tsVal, GFXDType.TIMESTAMP);
        }
        break;
      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
        byte[] byteArray = rs.getBytes(columnIndex);
        if (rs.wasNull()) {
          result.setNull(index);
        }
        else {
          result.setObject(index, byteArray,
              Converters.getThriftSQLType(colType));
        }
        break;
      case Types.NULL:
        result.setNull(index);
        break;
      case Types.JAVA_OBJECT:
        Object o = rs.getObject(columnIndex);
        if (rs.wasNull()) {
          result.setNull(index);
        }
        else if (o instanceof PDXObject) {
          result.setObject(index, o, GFXDType.PDX_OBJECT);
        }
        else if (o instanceof JSONObject) {
          result.setObject(index, o, GFXDType.JSON_OBJECT);
        }
        else {
          result.setObject(index, o, GFXDType.JAVA_OBJECT);
        }
        break;
      case JDBC40Translation.PDX:
        o = rs.getObject(columnIndex);
        if (rs.wasNull()) {
          result.setNull(index);
        }
        else {
          result.setObject(index, o, GFXDType.PDX_OBJECT);
        }
        break;
      case JDBC40Translation.JSON:
        o = rs.getObject(columnIndex);
        if (rs.wasNull()) {
          result.setNull(index);
        }
        else {
          result.setObject(index, o, GFXDType.JSON_OBJECT);
        }
        break;
      case Types.DISTINCT:
      case Types.STRUCT:
      case Types.ARRAY:
      case Types.REF:
      case Types.DATALINK:
      case Types.ROWID:
      case Types.OTHER:
        throw Util.generateCsSQLException(SQLState.DATA_TYPE_NOT_SUPPORTED,
            Util.typeName(colType));
    }
  }

  private ArrayList<ColumnDescriptor> getRowSetMetaData(
      final ResultSetMetaData rsmd, final int columnCount) throws SQLException {
    final ArrayList<ColumnDescriptor> descriptors =
        new ArrayList<ColumnDescriptor>(columnCount);

    if (rsmd instanceof EmbedResultSetMetaData) {
      // using somewhat more efficient methods for EmbedResultSetMetaData
      final EmbedResultSetMetaData ersmd = (EmbedResultSetMetaData)rsmd;
      final boolean tableReadOnly = ersmd.isTableReadOnly();
      String columnName, schemaName, tableName, fullTableName;
      String typeName, className;
      String prevSchemaName = null, prevTableName = null;
      String prevTypeName = null, prevClassName = null;
      int jdbcType, scale;
      GFXDType type;
      for (int colIndex = 1; colIndex <= columnCount; colIndex++) {
        ResultColumnDescriptor rcd = ersmd.getColumnDescriptor(colIndex);
        DataTypeDescriptor dtd = rcd.getType();
        TypeId typeId = dtd.getTypeId();
        jdbcType = typeId.getJDBCTypeId();
        type = Converters.getThriftSQLType(jdbcType);
        ColumnDescriptor columnDesc = new ColumnDescriptor();
        columnName = rcd.getName();
        if (columnName != null) {
          columnDesc.setName(columnName);
        }
        schemaName = rcd.getSourceSchemaName();
        tableName = rcd.getSourceTableName();
        if (colIndex > 1) {
          if ((schemaName != null && !schemaName.equals(prevSchemaName))
              || (tableName != null && !tableName.equals(prevTableName))) {
            fullTableName = schemaName != null ? (schemaName + '.' + tableName)
                : tableName;
            columnDesc.setFullTableName(fullTableName);
          }
        }
        else {
          fullTableName = schemaName != null ? (schemaName + '.' + tableName)
              : tableName;
          columnDesc.setFullTableName(fullTableName);
        }
        prevSchemaName = schemaName;
        prevTableName = tableName;

        int nullable = DataTypeUtilities.isNullable(dtd);
        short flags = 0x0;
        if (nullable == ResultSetMetaData.columnNullable) {
          flags &= gfxdConstants.COLUMN_NULLABLE;
        }
        else if (nullable == ResultSetMetaData.columnNoNulls) {
          flags &= gfxdConstants.COLUMN_NONULLS;
        }
        if (!tableReadOnly) {
          if (rcd.updatableByCursor()) {
            flags &= gfxdConstants.COLUMN_UPDATABLE;
          }
          if (ersmd.isDefiniteWritable_(colIndex)) {
            flags &= gfxdConstants.COLUMN_DEFINITELY_UPDATABLE;
          }
        }
        if (rcd.isAutoincrement()) {
          flags &= gfxdConstants.COLUMN_AUTOINC;
        }
        columnDesc.setType(type);
        columnDesc.setDescFlags(flags);
        columnDesc
            .setPrecision((short)DataTypeUtilities.getDigitPrecision(dtd));
        scale = dtd.getScale();
        if (scale != 0) {
          columnDesc.setScale((short)scale);
        }
        if (jdbcType == Types.JAVA_OBJECT) {
          typeName = typeId.getSQLTypeName();
          className = typeId.getResultSetMetaDataTypeName();
          if ((typeName != null && !typeName.equals(prevTypeName))
              || (className != null && !className.equals(prevClassName))) {
            columnDesc.setUdtTypeAndClassName(typeName
                + (className != null ? ":" + className : ""));
          }
          prevTypeName = typeName;
          prevClassName = className;
        }
        descriptors.add(columnDesc);
      }
    }
    else {
      String schemaName, tableName, fullTableName;
      String typeName, className;
      String prevSchemaName = null, prevTableName = null;
      String prevTypeName = null, prevClassName = null;
      int jdbcType, scale;
      for (int colIndex = 1; colIndex <= columnCount; colIndex++) {
        ColumnDescriptor columnDesc = new ColumnDescriptor();
        columnDesc.setName(rsmd.getColumnName(colIndex));
        schemaName = rsmd.getSchemaName(colIndex);
        tableName = rsmd.getTableName(colIndex);
        if (colIndex > 1) {
          if ((schemaName != null && !schemaName.equals(prevSchemaName))
              || (tableName != null && !tableName.equals(prevTableName))) {
            fullTableName = schemaName != null ? (schemaName + '.' + tableName)
                : tableName;
            columnDesc.setFullTableName(fullTableName);
          }
        }
        else {
          fullTableName = schemaName != null ? (schemaName + '.' + tableName)
              : tableName;
          columnDesc.setFullTableName(fullTableName);
        }
        prevSchemaName = schemaName;
        prevTableName = tableName;

        int nullable = rsmd.isNullable(colIndex);
        short flags = 0x0;
        if (nullable == ResultSetMetaData.columnNullable) {
          flags &= gfxdConstants.COLUMN_NULLABLE;
        }
        else if (nullable == ResultSetMetaData.columnNoNulls) {
          flags &= gfxdConstants.COLUMN_NONULLS;
        }
        if (rsmd.isDefinitelyWritable(colIndex)) {
          flags &= gfxdConstants.COLUMN_DEFINITELY_UPDATABLE;
        }
        if (rsmd.isWritable(colIndex)) {
          flags &= gfxdConstants.COLUMN_UPDATABLE;
        }
        if (rsmd.isAutoIncrement(colIndex)) {
          flags &= gfxdConstants.COLUMN_AUTOINC;
        }
        jdbcType = rsmd.getColumnType(colIndex);
        columnDesc.setType(Converters.getThriftSQLType(jdbcType));
        columnDesc.setDescFlags(flags);
        columnDesc.setPrecision((short)rsmd.getPrecision(colIndex));
        scale = rsmd.getScale(colIndex);
        if (scale != 0) {
          columnDesc.setScale((short)scale);
        }
        if (jdbcType == Types.JAVA_OBJECT) {
          typeName = rsmd.getColumnTypeName(colIndex);
          className = rsmd.getColumnClassName(colIndex);
          if ((typeName != null && !typeName.equals(prevTypeName))
              || (className != null && !className.equals(prevClassName))) {
            columnDesc.setUdtTypeAndClassName(typeName
                + (className != null ? ":" + className : ""));
          }
          prevTypeName = typeName;
          prevClassName = className;
        }
        descriptors.add(columnDesc);
      }
    }
    return descriptors;
  }

  /**
   * Encapsulates the ResultSet of statement execution as thrift RowSet.
   */
  private RowSet getRowSet(final Statement stmt, StatementHolder stmtHolder,
      final ResultSet rs, int cursorId, final int connId,
      final StatementAttrs attrs, final int offset,
      final boolean offsetIsAbsolute, final boolean fetchReverse,
      final int fetchSize, final ConnectionHolder connHolder,
      boolean postExecution, String sql) throws GFXDException {
    boolean isLastBatch = true;
    try {
      RowSet result = createEmptyRowSet().setConnId(connId);
      final boolean isForwardOnly = rs.getType() == ResultSet.TYPE_FORWARD_ONLY;
      // first fill in the metadata
      final ResultSetMetaData rsmd = rs.getMetaData();
      final int columnCount = rsmd.getColumnCount();
      final ArrayList<ColumnDescriptor> descriptors = getRowSetMetaData(rsmd,
          columnCount);
      if (postExecution) {
        result.setMetadata(descriptors);
      }
      // now fill in the values
      final int batchSize;
      final boolean moveForward = !fetchReverse;
      boolean hasMoreRows = false;
      long startTime = 0;
      if (fetchSize > 0) {
        batchSize = fetchSize;
      }
      else if (attrs != null && attrs.isSetBatchSize()) {
        batchSize = attrs.batchSize;
      }
      else {
        batchSize = gfxdConstants.DEFAULT_RESULTSET_BATCHSIZE;
      }

      if (offsetIsAbsolute) {
        hasMoreRows = rs.absolute(offset);
        result.setOffset(offset);
      }
      else if (offset != 0) {
        hasMoreRows = rs.relative(offset);
        result.setOffset(rs.getRow() - 1);
      }
      else if (!isForwardOnly) {
        result.setOffset(rs.getRow() - 1);
      }

      final EmbedConnection conn = connHolder.getConnection();
      final List<Row> rows = result.getRows();
      Row templateRow = null;
      int nrows = 0;
      if (rs instanceof EmbedResultSet) {
        final EmbedResultSet ers = (EmbedResultSet)rs;
        final ColumnDescriptor[] descs = descriptors
            .toArray(new ColumnDescriptor[columnCount]);
        synchronized (conn.getConnectionSynchronization()) {
          ers.setupContextStack(false);
          ers.pushStatementContext(conn.getLanguageConnection(), true);
          try {
            // skip the first move in case cursor was already positioned by an
            // explicit call to absolute or relative
            if (offset == 0) {
              hasMoreRows = moveForward ? ers.lightWeightNext() : ers
                  .lightWeightPrevious();
            }
            while (hasMoreRows) {
              Row eachRow;
              if (templateRow == null) {
                templateRow = new Row(descriptors);
                eachRow = templateRow;
              }
              else {
                eachRow = new Row(templateRow, false);
              }
              for (int colIndex = 1; colIndex <= columnCount; colIndex++) {
                setColumnValue(ers, descs[colIndex - 1].type, colIndex,
                    connHolder, attrs, eachRow);
              }
              rows.add(eachRow);
              if (((++nrows) % GemFireXDUtils.DML_SAMPLE_INTERVAL) == 0) {
                // throttle the processing and sends
                if (throttleIfCritical()) {
                  isLastBatch = false;
                  break;
                }
                getCancelCriterion().checkCancelInProgress(null);
                long currentTime = System.nanoTime();
                if (startTime > 0) {
                  if ((currentTime - startTime) >=
                      (1000000L * GemFireXDUtils.DML_MAX_CHUNK_MILLIS)) {
                    isLastBatch = false;
                    break;
                  }
                }
                else {
                  startTime = currentTime;
                }
              }
              if (nrows >= batchSize) {
                isLastBatch = false;
                break;
              }
              hasMoreRows = moveForward ? ers.lightWeightNext() : ers
                  .lightWeightPrevious();
            }
          } finally {
            ers.popStatementContext();
            ers.restoreContextStack();
          }
        }
      }
      else {
        if (offset == 0) {
          hasMoreRows = moveForward ? rs.next() : rs.previous();
        }
        while (hasMoreRows) {
          Row eachRow;
          if (templateRow == null) {
            templateRow = new Row(descriptors);
            eachRow = templateRow;
          }
          else {
            eachRow = new Row(templateRow, false);
          }
          for (int colIndex = 1; colIndex <= columnCount; colIndex++) {
            setColumnValue(rs, rsmd, colIndex, connHolder, attrs, eachRow);
          }
          rows.add(eachRow);
          if (((++nrows) % GemFireXDUtils.DML_SAMPLE_INTERVAL) == 0) {
            // throttle the processing and sends
            if (throttleIfCritical()) {
              isLastBatch = false;
              break;
            }
            getCancelCriterion().checkCancelInProgress(null);
            long currentTime = System.nanoTime();
            if (startTime > 0) {
              if ((currentTime - startTime) >=
                  (1000000L * GemFireXDUtils.DML_MAX_CHUNK_MILLIS)) {
                isLastBatch = false;
                break;
              }
            }
            else {
              startTime = currentTime;
            }
          }
          if (nrows >= batchSize) {
            isLastBatch = false;
            break;
          }
          hasMoreRows = moveForward ? rs.next() : rs.previous();
        }
      }

      /*
      // reverse the list if traversing backwards (client always expects the
      // results in forward order that it will traverse in reverse if required)
      if (!moveForward) {
        Collections.reverse(rows);
      }
      */

      byte flags = 0;
      if (isLastBatch) flags |= gfxdConstants.ROWSET_LAST_BATCH;
      // TODO: implement multiple RowSets for Callable statements
      // TODO: proper LOB support
      flags |= gfxdConstants.ROWSET_DONE_FOR_LOBS;
      result.setFlags(flags);
      // TODO: implement updatable ResultSets
      fillWarnings(result, rs);
      // send cursorId for scrollable, partial resultsets and no open LOBs
      if (isLastBatch && isForwardOnly && conn.getlobHMObj().isEmpty()) {
         if (stmtHolder == null || cursorId == INVALID_ID) {
          rs.close();
        }
        else {
          stmtHolder.closeResultSet(cursorId, this);
        }
        // setup the Statement for reuse
        EmbedStatement estmt;
        if (postExecution && stmt instanceof EmbedStatement
            && !(estmt = (EmbedStatement)stmt).isPrepared()) {
          connHolder.setStatementForReuse(estmt);
        }
        result.setCursorId(INVALID_ID);
        result.setStatementId(INVALID_ID);
      }
      else {
        if (postExecution) {
          cursorId = getNextId(this.currentCursorId);
          if (stmtHolder == null) {
            stmtHolder = registerResultSet(cursorId, rs, connHolder, stmt,
                attrs, sql);
          }
          else {
            registerResultSet(cursorId, rs, stmtHolder);
          }
        }
        result.setCursorId(cursorId);
        result.setStatementId(stmtHolder.getStatementId());
      }
      return result;
    } catch (SQLException e) {
      throw gfxdException(e);
    }
  }

  private final boolean throttleIfCritical() {
    if (this.thresholdListener.isCritical()) {
      // throttle the processing and sends
      try {
        for (int tries = 1; tries <= 5; tries++) {
          Thread.sleep(4);
          if (!this.thresholdListener.isCritical()) {
            break;
          }
        }
      } catch (InterruptedException ie) {
        getCancelCriterion().checkCancelInProgress(ie);
      }
      return true;
    }
    else {
      return false;
    }
  }

  /**
   * Create an empty row set.
   */
  private static RowSet createEmptyRowSet() {
    RowSet rs = new RowSet();
    rs.setRows(new ArrayList<Row>());
    return rs;
  }

  /**
   * Create an empty statement result.
   */
  private StatementResult createEmptyStatementResult() {
    return new StatementResult();
  }

  private StatementHolder registerResultSet(int cursorId, ResultSet rs,
      ConnectionHolder connHolder, Statement stmt, StatementAttrs attrs,
      String sql) {
    final StatementHolder stmtHolder;
    if (stmt != null) {
      final int stmtId = getNextId(this.currentStatementId);
      stmtHolder = connHolder.registerResultSet(stmt, attrs, stmtId, rs,
          cursorId, sql);
      this.statementMap.putPrimitive(stmtId, stmtHolder);
    }
    else {
      stmtHolder = connHolder.registerResultSet(null, null, INVALID_ID, rs,
          cursorId, sql);
    }
    this.resultSetMap.putPrimitive(cursorId, stmtHolder);
    return stmtHolder;
  }

  private void registerResultSet(int cursorId, ResultSet rs,
      StatementHolder stmtHolder) {
    stmtHolder.addResultSet(rs, cursorId);
    this.resultSetMap.putPrimitive(cursorId, stmtHolder);
  }

  private boolean processPendingTransactionAttributes(StatementAttrs attrs,
      EmbedConnection conn) throws GFXDException {
    if (attrs == null) {
      return false;
    }
    Map<TransactionAttribute, Boolean> pendingTXAttrs = attrs
        .getPendingTransactionAttrs();
    if (pendingTXAttrs != null && !pendingTXAttrs.isEmpty()) {
      beginOrAlterTransaction(conn, gfxdConstants.TRANSACTION_NO_CHANGE,
          pendingTXAttrs, false);
    }
    if (attrs.possibleDuplicate) {
      conn.setPossibleDuplicate(true);
      return true;
    }
    else {
      return false;
    }
  }

  private void cleanupResultSet(ResultSet rs) {
    if (rs != null) {
      try {
        rs.close();
      } catch (Exception e) {
        // ignored
      }
    }
  }

  private void cleanupStatement(EmbedStatement stmt) {
    if (stmt != null) {
      try {
        stmt.close();
      } catch (Exception e) {
        // ignored
      }
    }
  }

  private boolean ignoreNonFatalException(Throwable t) {
    final Error err;
    if (t instanceof Error && SystemFailure.isJVMFailureError(
        err = (Error)t)) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error. We're poisoned
      // now, so don't let this thread continue.
      throw err;
    }
    // Whenever you catch Error or Throwable, you must also
    // check for fatal JVM error (see above). However, there is
    // _still_ a possibility that you are dealing with a cascading
    // error condition, so you also need to check to see if the JVM
    // is still usable.
    SystemFailure.checkFailure();

    // ignore for node going down
    return GemFireXDUtils.nodeFailureException(t);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public StatementResult execute(int connId, String sql,
      Map<Integer, OutputParameter> outputParams, StatementAttrs attrs,
      ByteBuffer token) throws GFXDException {

    ConnectionHolder connHolder = null;
    EmbedConnection conn = null;
    EmbedStatement stmt = null;
    ResultSet rs = null;
    boolean posDup = false;
    try {
      connHolder = getValidConnection(connId, token);
      conn = connHolder.getConnection();

      // first process any pending TX & other flags in StatementAttrs
      posDup = processPendingTransactionAttributes(attrs, conn);

      // Check if user has provided output parameters i.e. CallableStatement
      if (outputParams != null && !outputParams.isEmpty()) {
        // TODO: implement this case by adding support for output parameters
        // to EmbedStatement itself (actually outputParams is not required
        // at all by the execution engine itself)
        // Also take care of open LOBs so avoid closing statement
        // for autocommit==true
        throw notImplementedException("CALL with output");
      }
      else { // Case : New statement Object
        stmt = connHolder.createNewStatement(attrs);
      }
      // Now we have valid statement object and valid connect id.
      // Create new empty StatementResult.
      StatementResult sr = createEmptyStatementResult();
      RowSet rowSet;

      boolean resultType = stmt.execute(sql);
      if (resultType) { // Case : result is a ResultSet
        rs = stmt.getResultSet();
        rowSet = getRowSet(stmt, null, rs, INVALID_ID, connId, attrs, 0, false,
            false, 0, connHolder, true, sql);
        sr.setResultSet(rowSet);
      }
      else { // Case : result is update count
        sr.setUpdateCount(stmt.getUpdateCount());
        rs = stmt.getGeneratedKeys();
        if (rs != null) {
          rowSet = getRowSet(stmt, null, rs, INVALID_ID, connId, attrs, 0,
              false, false, 0, connHolder, true, "getGeneratedKeys");
          sr.setGeneratedKeys(rowSet);
        }
        else {
          // setup the Statement for reuse
          connHolder.setStatementForReuse(stmt);
        }
      }
      // don't attempt stmt cleanup after this point since we are reusing it
      final EmbedStatement st = stmt;
      stmt = null;

      fillWarnings(sr, st);

      if (posDup) {
        conn.setPossibleDuplicate(false);
      }
      return sr;
    } catch (Throwable t) {
      if (posDup && conn != null) {
        conn.setPossibleDuplicate(false);
      }
      cleanupResultSet(rs);
      cleanupStatement(stmt);
      throw gfxdException(t);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public UpdateResult executeUpdate(int connId, List<String> sqls,
      StatementAttrs attrs, ByteBuffer token) throws GFXDException {

    ConnectionHolder connHolder = null;
    EmbedConnection conn = null;
    EmbedStatement stmt = null;
    ResultSet rs = null;
    UpdateResult result;
    boolean posDup = false;
    try {
      connHolder = getValidConnection(connId, token);
      conn = connHolder.getConnection();

      // first process any pending TX & other flags in StatementAttrs
      posDup = processPendingTransactionAttributes(attrs, conn);

      stmt = connHolder.createNewStatement(attrs);
      if (sqls.size() == 1) {
        int updateCount = stmt.executeUpdate(sqls.get(0));
        result = new UpdateResult();
        result.setUpdateCount(updateCount);
      }
      else {
        stmt.clearBatch();
        for (String sql : sqls) {
          stmt.addBatch(sql);
        }
        int[] batchUpdateCounts = stmt.executeBatch();
        result = new UpdateResult();
        for (int count : batchUpdateCounts) {
          result.addToBatchUpdateCounts(count);
        }
      }

      rs = stmt.getGeneratedKeys();
      if (rs == null) {
        // setup the Statement for reuse
        connHolder.setStatementForReuse(stmt);
      }
      else {
        RowSet rowSet = getRowSet(stmt, null, rs, INVALID_ID, connId, attrs, 0,
            false, false, 0, connHolder, true, "getGeneratedKeys");
        result.setGeneratedKeys(rowSet);
      }
      // don't attempt stmt cleanup after this point since we are reusing it
      final EmbedStatement st = stmt;
      stmt = null;

      fillWarnings(result, st);

      if (posDup) {
        conn.setPossibleDuplicate(false);
      }
      return result;
    } catch (Throwable t) {
      if (posDup && conn != null) {
        conn.setPossibleDuplicate(false);
      }
      cleanupResultSet(rs);
      cleanupStatement(stmt);
      throw gfxdException(t);
    } finally {
      if (stmt != null && sqls != null && sqls.size() > 1) {
        stmt.clearBatchIfPossible();
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public RowSet executeQuery(int connId, final String sql,
      StatementAttrs attrs, ByteBuffer token) throws GFXDException {

    ConnectionHolder connHolder = null;
    EmbedConnection conn = null;
    EmbedStatement stmt = null;
    ResultSet rs = null;
    boolean posDup = false;
    try {
      connHolder = getValidConnection(connId, token);
      conn = connHolder.getConnection();

      // first process any pending TX & other flags in StatementAttrs
      posDup = processPendingTransactionAttributes(attrs, conn);

      stmt = connHolder.createNewStatement(attrs);
      rs = stmt.executeQuery(sql);
      RowSet rowSet = getRowSet(stmt, null, rs, INVALID_ID, connId, attrs, 0,
          false, false, 0, connHolder, true, sql);
      // don't attempt stmt cleanup after this point since we are reusing it
      stmt = null;

      if (posDup) {
        conn.setPossibleDuplicate(false);
      }
      return rowSet;
    } catch (Throwable t) {
      if (posDup && conn != null) {
        conn.setPossibleDuplicate(false);
      }
      cleanupResultSet(rs);
      cleanupStatement(stmt);
      throw gfxdException(t);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public PrepareResult prepareStatement(int connId, String sql,
      Map<Integer, OutputParameter> outputParams, StatementAttrs attrs,
      ByteBuffer token) throws GFXDException {

    ConnectionHolder connHolder = null;
    EmbedConnection conn = null;
    PreparedStatement pstmt = null;
    StatementHolder stmtHolder = null;
    boolean posDup = false;
    try {
      connHolder = getValidConnection(connId, token);
      conn = connHolder.getConnection();

      // first process any pending TX & other flags in StatementAttrs
      posDup = processPendingTransactionAttributes(attrs, conn);

      int pstmtId;
      ArrayList<ColumnDescriptor> pmDescs;
      GFXDExceptionData sqlw = null;

      final int resultSetType = getResultType(attrs);
      final int resultSetConcurrency = getResultSetConcurrency(attrs);
      final int resultSetHoldability = getResultSetHoldability(attrs);
      if (attrs != null && attrs.isSetRequireAutoIncCols()
          && attrs.requireAutoIncCols) {
        if (outputParams != null && !outputParams.isEmpty()) {
          throw newGFXDException(SQLState.REQUIRES_CALLABLE_STATEMENT,
              "AUTOINC not valid with output parameters: " + sql);
        }
        final List<Integer> autoIncCols = attrs.autoIncColumns;
        final List<String> autoIncColNames;
        int nCols;
        if (autoIncCols != null && (nCols = autoIncCols.size()) > 0) {
          int[] aiCols = new int[nCols];
          for (int index = 0; index < nCols; index++) {
            aiCols[index] = autoIncCols.get(index).intValue();
          }
          pstmt = conn.prepareStatement(sql, resultSetType,
              resultSetConcurrency, resultSetHoldability, aiCols);
        }
        else if ((autoIncColNames = attrs.autoIncColumnNames) != null
            && (nCols = autoIncColNames.size()) > 0) {
          pstmt = conn.prepareStatement(sql, resultSetType,
              resultSetConcurrency, resultSetHoldability,
              autoIncColNames.toArray(new String[nCols]));
        }
        else {
          pstmt = conn.prepareStatement(sql, resultSetType,
              resultSetConcurrency, resultSetHoldability,
              Statement.RETURN_GENERATED_KEYS);
        }
      }
      else {
        CallableStatement cstmt;
        pstmt = cstmt = conn.prepareCall(sql, resultSetType,
            resultSetConcurrency, resultSetHoldability);
        registerOutputParameters(cstmt, outputParams);
      }
      // prepared result consists of ID + ParameterMetaData
      ParameterMetaData pmd = pstmt.getParameterMetaData();
      int numParams = pmd.getParameterCount();
      pmDescs = new ArrayList<ColumnDescriptor>(numParams);
      int pmType, mode, nullable, scale;
      GFXDType type;
      String typeName, className;
      for (int paramIndex = 1; paramIndex <= numParams; paramIndex++) {
        pmType = pmd.getParameterType(paramIndex);
        ColumnDescriptor pmDesc = new ColumnDescriptor();
        type = Converters.getThriftSQLType(pmType);
        short flags = 0;
        mode = pmd.getParameterMode(paramIndex);
        switch (mode) {
          case ParameterMetaData.parameterModeIn:
            flags &= gfxdConstants.PARAMETER_MODE_IN;
            break;
          case ParameterMetaData.parameterModeOut:
            flags &= gfxdConstants.PARAMETER_MODE_OUT;
            break;
          case ParameterMetaData.parameterModeInOut:
            flags &= gfxdConstants.PARAMETER_MODE_INOUT;
            break;
          default:
            flags &= gfxdConstants.PARAMETER_MODE_IN;
            break;
        }
        nullable = pmd.isNullable(paramIndex);
        if (nullable == ParameterMetaData.parameterNullable) {
          flags &= gfxdConstants.COLUMN_NULLABLE;
        }
        else if (nullable == ParameterMetaData.parameterNoNulls) {
          flags &= gfxdConstants.COLUMN_NONULLS;
        }
        pmDesc.setType(type);
        pmDesc.setDescFlags(flags);
        pmDesc.setPrecision((short)pmd.getPrecision(paramIndex));
        scale = pmd.getScale(paramIndex);
        if (scale != 0) {
          pmDesc.setScale((short)scale);
        }
        if (pmType == Types.JAVA_OBJECT) {
          typeName = pmd.getParameterTypeName(paramIndex);
          className = pmd.getParameterClassName(paramIndex);
          if (className != null) {
            pmDesc.setUdtTypeAndClassName(typeName + ':' + className);
          }
          else {
            pmDesc.setUdtTypeAndClassName(typeName);
          }
        }
        pmDescs.add(pmDesc);
      }
      // any warnings
      SQLWarning warnings = pstmt.getWarnings();
      if (warnings != null) {
        sqlw = gfxdWarning(warnings);
      }
      pstmtId = getNextId(this.currentStatementId);
      stmtHolder = connHolder.registerStatement(pstmt, attrs, pstmtId, sql);
      this.statementMap.putPrimitive(pstmtId, stmtHolder);

      PrepareResult result = new PrepareResult(pstmtId, pmDescs);
      if (sqlw != null) {
        result.setWarnings(sqlw);
      }
      // fill in ResultSet meta-data
      ResultSetMetaData rsmd = pstmt.getMetaData();
      if (rsmd != null) {
        result.setResultSetMetaData(getRowSetMetaData(rsmd,
            rsmd.getColumnCount()));
      }

      if (posDup) {
        conn.setPossibleDuplicate(false);
      }
      return result;
    } catch (Throwable t) {
      if (posDup && conn != null) {
        conn.setPossibleDuplicate(false);
      }
      if (pstmt != null) {
        if (stmtHolder != null) {
          connHolder.closeStatement(stmtHolder, this);
        }
        try {
          pstmt.close();
        } catch (Exception e) {
          // ignore
        }
      }
      throw gfxdException(t);
    }
  }

  private void registerOutputParameters(CallableStatement cstmt,
      Map<Integer, OutputParameter> outputParams) throws SQLException {
    if (outputParams != null && !outputParams.isEmpty()) {
      OutputParameter outParam;
      int jdbcType;
      for (Map.Entry<Integer, OutputParameter> param : outputParams
          .entrySet()) {
        outParam = param.getValue();
        jdbcType = Converters.getJdbcType(outParam.type);
        int paramIndex = param.getKey().intValue();
        if (outParam.isSetScale()) {
          cstmt.registerOutParameter(paramIndex, jdbcType, outParam.scale);
        }
        else if (outParam.isSetTypeName()) {
          cstmt.registerOutParameter(paramIndex, jdbcType, outParam.typeName);
        }
        else {
          cstmt.registerOutParameter(paramIndex, jdbcType);
        }
      }
    }
  }

  private void updateParameters(Row params, PreparedStatement pstmt,
      EmbedConnection conn) throws SQLException {
    final int numParams = params != null ? params.size() : 0;
    for (int paramIndex = 1; paramIndex <= numParams; paramIndex++) {
      final int index = paramIndex - 1;
      GFXDType paramType = params.getSQLType(index);
      Object paramVal;
      switch (paramType) {
        case BOOLEAN:
          if (params.isNull(index)) {
            pstmt.setNull(paramIndex, Types.BOOLEAN);
          }
          else {
            pstmt.setBoolean(paramIndex, params.getBoolean(index));
          }
          break;
        case TINYINT:
          if (params.isNull(index)) {
            pstmt.setNull(paramIndex, Types.TINYINT);
          }
          else {
            pstmt.setByte(paramIndex, params.getByte(index));
          }
          break;
        case SMALLINT:
          if (params.isNull(index)) {
            pstmt.setNull(paramIndex, Types.SMALLINT);
          }
          else {
            pstmt.setShort(paramIndex, params.getShort(index));
          }
          break;
        case INTEGER:
          if (params.isNull(index)) {
            pstmt.setNull(paramIndex, Types.INTEGER);
          }
          else {
            pstmt.setInt(paramIndex, params.getInt(index));
          }
          break;
        case BIGINT:
          if (params.isNull(index)) {
            pstmt.setNull(paramIndex, Types.BIGINT);
          }
          else {
            pstmt.setLong(paramIndex, params.getLong(index));
          }
          break;
        case REAL:
          if (params.isNull(index)) {
            pstmt.setNull(paramIndex, Types.REAL);
          }
          else {
            pstmt.setFloat(paramIndex, params.getFloat(index));
          }
          break;
        case FLOAT:
          if (params.isNull(index)) {
            pstmt.setNull(paramIndex, Types.FLOAT);
          }
          else {
            pstmt.setDouble(paramIndex, params.getDouble(index));
          }
          break;
        case DOUBLE:
          if (params.isNull(index)) {
            pstmt.setNull(paramIndex, Types.DOUBLE);
          }
          else {
            pstmt.setDouble(paramIndex, params.getDouble(index));
          }
          break;
        case CHAR:
        case VARCHAR:
        case LONGVARCHAR:
        case NCHAR:
        case NVARCHAR:
        case LONGNVARCHAR:
          if (params.isNull(index)) {
            pstmt.setNull(paramIndex, Converters.getJdbcType(paramType));
          }
          else {
            pstmt.setString(paramIndex, (String)params.getObject(index));
          }
          break;
        case BLOB:
          if (params.isNull(index)) {
            pstmt.setNull(paramIndex, Types.BLOB);
          }
          else if ((paramVal = params.getObject(index)) instanceof byte[]) {
            pstmt.setBytes(paramIndex, (byte[])paramVal);
          }
          else {
            BlobChunk chunk = (BlobChunk)paramVal;
            Blob blob;
            // if blob chunks were sent separately, then lookup that blob
            // else create a new one
            if (chunk.isSetLobId()) {
              Object lob = conn.getLOBMapping(chunk.lobId);
              if (lob instanceof Blob) {
                blob = (Blob)lob;
              }
              else {
                throw Util.generateCsSQLException(SQLState.LOB_LOCATOR_INVALID);
              }
            }
            else if (chunk.last) {
              // set as a normal byte[]
              pstmt.setBytes(paramIndex, chunk.getChunk());
              break;
            }
            else {
              blob = conn.createBlob();
            }
            long offset = 1;
            if (chunk.isSetOffset()) {
              offset += chunk.offset;
            }
            blob.setBytes(offset, chunk.getChunk());
            pstmt.setBlob(paramIndex, blob);
          }
          break;
        case CLOB:
        case NCLOB:
          if (params.isNull(index)) {
            pstmt.setNull(paramIndex, Types.CLOB);
          }
          else if ((paramVal = params.getObject(index)) instanceof String) {
            pstmt.setString(paramIndex, (String)paramVal);
          }
          else {
            Clob clob;
            ClobChunk chunk = (ClobChunk)paramVal;
            // if clob chunks were sent separately, then lookup that clob
            // else create a new one
            if (chunk.isSetLobId()) {
              Object lob = conn.getLOBMapping(chunk.lobId);
              if (lob instanceof Clob) {
                clob = (Clob)lob;
              }
              else {
                throw Util.generateCsSQLException(SQLState.LOB_LOCATOR_INVALID);
              }
            }
            else if (chunk.last) {
              // set as a normal String
              pstmt.setString(paramIndex, chunk.getChunk());
              break;
            }
            else {
              clob = conn.createClob();
            }
            long offset = 1;
            if (chunk.isSetOffset()) {
              offset += chunk.offset;
            }
            clob.setString(offset, chunk.getChunk());
            pstmt.setClob(paramIndex, clob);
          }
          break;
        case DECIMAL:
          if (params.isNull(index)) {
            pstmt.setNull(paramIndex, Types.DECIMAL);
          }
          else {
            pstmt
                .setBigDecimal(paramIndex, (BigDecimal)params.getObject(index));
          }
          break;
        case DATE:
          if (params.isNull(index)) {
            pstmt.setNull(paramIndex, Types.DATE);
          }
          else {
            pstmt.setDate(paramIndex, (Date)params.getObject(index));
          }
          break;
        case TIME:
          if (params.isNull(index)) {
            pstmt.setNull(paramIndex, Types.TIME);
          }
          else {
            pstmt.setTime(paramIndex, (Time)params.getObject(index));
          }
          break;
        case TIMESTAMP:
          if (params.isNull(index)) {
            pstmt.setNull(paramIndex, Types.TIMESTAMP);
          }
          else {
            pstmt.setTimestamp(paramIndex,
                (java.sql.Timestamp)params.getObject(index));
          }
          break;
        case BINARY:
        case VARBINARY:
        case LONGVARBINARY:
          if (params.isNull(index)) {
            pstmt.setNull(paramIndex, Converters.getJdbcType(paramType));
          }
          else {
            pstmt.setBytes(paramIndex, (byte[])params.getObject(index));
          }
          break;
        case NULLTYPE:
          pstmt.setNull(index, Types.NULL);
          break;
        case JAVA_OBJECT:
        case PDX_OBJECT:
        case JSON_OBJECT:
          if (params.isNull(index)) {
            pstmt.setNull(paramIndex, Converters.getJdbcType(paramType));
          }
          else {
            pstmt.setObject(paramIndex, params.getObject(index));
          }
          break;
        default:
          throw Util.generateCsSQLException(SQLState.DATA_TYPE_NOT_SUPPORTED,
              paramType.toString());
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public StatementResult executePrepared(int stmtId, final Row params,
      final Map<Integer, OutputParameter> outputParams, ByteBuffer token)
      throws GFXDException {
    PreparedStatement pstmt = null;
    ResultSet rs = null;
    try {
      // Validate the statement Id
      StatementHolder stmtHolder = getStatement(token, stmtId, true,
          "executePrepared");
      ConnectionHolder connHolder = stmtHolder.getConnectionHolder();
      final int connId = connHolder.getConnectionId();
      pstmt = (PreparedStatement)stmtHolder.getStatement();

      if (outputParams != null && !outputParams.isEmpty()) {
        if (pstmt instanceof CallableStatement) {
          registerOutputParameters((CallableStatement)pstmt, outputParams);
        }
        else {
          throw newGFXDException(SQLState.REQUIRES_CALLABLE_STATEMENT,
              stmtHolder.getSQL());
        }
      }
      StatementResult stmtResult = createEmptyStatementResult();
      // clear any existing parameters first
      pstmt.clearParameters();
      updateParameters(params, pstmt, connHolder.getConnection());

      StatementAttrs attrs = stmtHolder.getStatementAttrs();
      boolean resultType = pstmt.execute();
      if (resultType) { // Case : result is a ResultSet
        rs = pstmt.getResultSet();
        RowSet rowSet = getRowSet(pstmt, stmtHolder, rs, INVALID_ID, connId,
            attrs, 0, false, false, 0, connHolder, true, null /*already set*/);
        stmtResult.setResultSet(rowSet);
      }
      else { // Case : result is update count
        stmtResult.setUpdateCount(pstmt.getUpdateCount());
        rs = pstmt.getGeneratedKeys();
        if (rs != null) {
          RowSet rowSet = getRowSet(pstmt, stmtHolder, rs, INVALID_ID, connId,
              attrs, 0, false, false, 0, connHolder, true, "getGeneratedKeys");
          stmtResult.setGeneratedKeys(rowSet);
        }
      }
      fillWarnings(stmtResult, pstmt);
      return stmtResult;
    } catch (Throwable t) {
      cleanupResultSet(rs);
      throw gfxdException(t);
    } finally {
      if (pstmt != null) {
        try {
          pstmt.clearParameters();
        } catch (Throwable t) {
          // ignore exceptions at this point
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public UpdateResult executePreparedUpdate(int stmtId,
      final Row params, ByteBuffer token) throws GFXDException {
    PreparedStatement pstmt = null;
    ResultSet rs = null;
    try {
      StatementHolder stmtHolder = getStatement(token, stmtId, true,
          "executePreparedUpdate");
      ConnectionHolder connHolder = stmtHolder.getConnectionHolder();
      final int connId = connHolder.getConnectionId();
      pstmt = (PreparedStatement)stmtHolder.getStatement();
      // clear any existing parameters first
      pstmt.clearParameters();
      updateParameters(params, pstmt, connHolder.getConnection());

      int updateCount = pstmt.executeUpdate();
      UpdateResult result = new UpdateResult();
      result.setUpdateCount(updateCount);

      rs = pstmt.getGeneratedKeys();
      if (rs != null) {
        RowSet rowSet = getRowSet(pstmt, stmtHolder, rs, INVALID_ID, connId,
            stmtHolder.getStatementAttrs(), 0, false, false, 0, connHolder,
            true, "getGeneratedKeys");
        result.setGeneratedKeys(rowSet);
      }

      fillWarnings(result, pstmt);
      return result;
    } catch (Throwable t) {
      cleanupResultSet(rs);
      throw gfxdException(t);
    } finally {
      if (pstmt != null) {
        try {
          pstmt.clearParameters();
        } catch (Throwable t) {
          // ignore exceptions at this point
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public RowSet executePreparedQuery(int stmtId, final Row params,
      ByteBuffer token) throws GFXDException {
    PreparedStatement pstmt = null;
    ResultSet rs = null;
    try {
      StatementHolder stmtHolder = getStatement(token, stmtId, true,
          "executePreparedQuery");
      ConnectionHolder connHolder = stmtHolder.getConnectionHolder();
      final int connId = connHolder.getConnectionId();
      pstmt = (PreparedStatement)stmtHolder.getStatement();
      // clear any existing parameters first
      pstmt.clearParameters();
      updateParameters(params, pstmt, connHolder.getConnection());

      rs = pstmt.executeQuery();
      return getRowSet(pstmt, stmtHolder, rs, INVALID_ID, connId,
          stmtHolder.getStatementAttrs(), 0, false, false, 0, connHolder, true,
          null /*already set*/);
    } catch (Throwable t) {
      cleanupResultSet(rs);
      throw gfxdException(t);
    } finally {
      if (pstmt != null) {
        try {
          pstmt.clearParameters();
        } catch (Throwable t) {
          // ignore exceptions at this point
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public UpdateResult executePreparedBatch(int stmtId, List<Row> paramsBatch,
      ByteBuffer token) throws GFXDException {
    PreparedStatement pstmt = null;
    ResultSet rs = null;
    try {
      StatementHolder stmtHolder = getStatement(token, stmtId, true,
          "executePreparedBatch");
      ConnectionHolder connHolder = stmtHolder.getConnectionHolder();
      final int connId = connHolder.getConnectionId();
      pstmt = (PreparedStatement)stmtHolder.getStatement();
      EmbedConnection conn = connHolder.getConnection();
      // clear any existing parameters first
      pstmt.clearParameters();
      pstmt.clearBatch();
      for (Row params : paramsBatch) {
        updateParameters(params, pstmt, conn);
        pstmt.addBatch();
      }
      int[] batchUpdateCounts = pstmt.executeBatch();
      UpdateResult result = new UpdateResult();
      for (int count : batchUpdateCounts) {
        result.addToBatchUpdateCounts(count);
      }

      rs = pstmt.getGeneratedKeys();
      if (rs != null) {
        RowSet rowSet = getRowSet(pstmt, stmtHolder, rs, INVALID_ID, connId,
            stmtHolder.getStatementAttrs(), 0, false, false, 0, connHolder,
            true, "getGeneratedKeys");
        result.setGeneratedKeys(rowSet);
      }

      fillWarnings(result, pstmt);
      return result;
    } catch (Throwable t) {
      cleanupResultSet(rs);
      throw gfxdException(t);
    } finally {
      if (pstmt != null) {
        try {
          pstmt.clearParameters();
        } catch (Throwable t) {
          // ignore exceptions at this point
        }
        try {
          pstmt.clearBatch();
        } catch (Throwable t) {
          // ignore exceptions at this point
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public StatementResult prepareAndExecute(int connId, String sql,
      final List<Row> paramsBatch, Map<Integer, OutputParameter> outputParams,
      StatementAttrs attrs, ByteBuffer token) throws GFXDException {
    PrepareResult prepResult = prepareStatement(connId, sql, outputParams,
        attrs, token);

    ConnectionHolder connHolder = null;
    final boolean posDup = (attrs != null && attrs.possibleDuplicate);
    try {
      StatementResult sr;

      if (posDup) {
        connHolder = getValidConnection(connId, token);
        connHolder.getConnection().setPossibleDuplicate(true);
      }
      if (paramsBatch.size() == 1) {
        sr = executePrepared(prepResult.statementId, paramsBatch.get(0),
            outputParams, token);
        // also copy the single update to list of updates
        // just in case user always reads the list of updates
        if (sr.updateCount >= 0) {
          sr.setBatchUpdateCounts(Collections.singletonList(sr.updateCount));
        }
      }
      else {
        UpdateResult ur = executePreparedBatch(prepResult.statementId,
            paramsBatch, token);
        sr = new StatementResult();
        sr.setBatchUpdateCounts(ur.getBatchUpdateCounts());
        sr.setGeneratedKeys(ur.getGeneratedKeys());
        sr.setWarnings(ur.getWarnings());
      }
      sr.setPreparedResult(prepResult);
      return sr;
    } finally {
      if (posDup && connHolder != null) {
        connHolder.getConnection().setPossibleDuplicate(false);
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void beginTransaction(int connId, byte isolationLevel,
      Map<TransactionAttribute, Boolean> flags, ByteBuffer token)
      throws GFXDException {

    beginOrAlterTransaction(getValidConnection(connId, token).getConnection(),
        isolationLevel, flags, true);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setTransactionAttributes(int connId,
      Map<TransactionAttribute, Boolean> flags, ByteBuffer token)
      throws GFXDException {
    if (flags != null && !flags.isEmpty()) {
      beginOrAlterTransaction(getValidConnection(connId, token).getConnection(),
          gfxdConstants.TRANSACTION_NO_CHANGE, flags, false);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Map<TransactionAttribute, Boolean> getTransactionAttributes(
      int connId, ByteBuffer token) throws GFXDException {
    try {
      EmbedConnection conn = getValidConnection(connId, token).getConnection();

      final EnumMap<TransactionAttribute, Boolean> txAttrs = ThriftUtils
          .newTransactionFlags();
      EnumSet<TransactionFlag> txFlags = conn.getTR().getTXFlags();
      txAttrs.put(TransactionAttribute.AUTOCOMMIT, conn.getAutoCommit());
      txAttrs.put(TransactionAttribute.READ_ONLY_CONNECTION,
          conn.isReadOnly());
      if (txFlags != null) {
        txAttrs.put(TransactionAttribute.DISABLE_BATCHING,
            txFlags.contains(TransactionFlag.DISABLE_BATCHING));
        txAttrs.put(TransactionAttribute.SYNC_COMMITS,
            txFlags.contains(TransactionFlag.SYNC_COMMITS));
        txAttrs.put(TransactionAttribute.WAITING_MODE,
            txFlags.contains(TransactionFlag.WAITING_MODE));
      }
      else {
        txAttrs.put(TransactionAttribute.DISABLE_BATCHING, false);
        txAttrs.put(TransactionAttribute.SYNC_COMMITS, false);
        txAttrs.put(TransactionAttribute.WAITING_MODE, false);
      }
      return txAttrs;
    } catch (Throwable t) {
      throw gfxdException(t);
    }
  }

  private void beginOrAlterTransaction(EmbedConnection conn,
      byte isolationLevel, Map<TransactionAttribute, Boolean> flags,
      boolean commitExisting) throws GFXDException {

    try {
      EnumSet<TransactionFlag> txFlags = null;
      Boolean autoCommit = null;
      Boolean readOnly = null;
      boolean hasTXFlags = false;
      if (flags != null && flags.size() > 0) {
        txFlags = EnumSet.noneOf(TransactionFlag.class);
        for (Map.Entry<TransactionAttribute, Boolean> e : flags.entrySet()) {
          TransactionAttribute flag = e.getKey();
          switch (flag) {
            case AUTOCOMMIT:
              autoCommit = e.getValue();
              break;
            case DISABLE_BATCHING:
              if (e.getValue().booleanValue()) {
                txFlags.add(TransactionFlag.DISABLE_BATCHING);
              }
              hasTXFlags = true;
              break;
            case WAITING_MODE:
              if (e.getValue().booleanValue()) {
                txFlags.add(TransactionFlag.WAITING_MODE);
              }
              hasTXFlags = true;
              break;
            case SYNC_COMMITS:
              if (e.getValue().booleanValue()) {
                txFlags.add(TransactionFlag.SYNC_COMMITS);
              }
              hasTXFlags = true;
              break;
            case READ_ONLY_CONNECTION:
              readOnly = e.getValue();
              break;
          }
        }
        if (!hasTXFlags) {
          txFlags = null;
        }
      }
      if (readOnly != null) {
        conn.setReadOnly(readOnly.booleanValue());
      }
      if (autoCommit != null) {
        conn.setAutoCommit(autoCommit.booleanValue());
      }
      if (isolationLevel != gfxdConstants.TRANSACTION_NO_CHANGE) {
        conn.setTransactionIsolation(isolationLevel, txFlags);
      }
      else {
        if (commitExisting) {
          conn.commit();
        }
        if (txFlags != null) {
          LanguageConnectionContext lcc = conn.getLanguageConnectionContext();
          lcc.setTXFlags(txFlags);
        }
      }
    } catch (Throwable t) {
      throw gfxdException(t);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void commitTransaction(int connId, final boolean startNewTransaction,
      Map<TransactionAttribute, Boolean> flags, ByteBuffer token)
      throws GFXDException {

    try {
      EmbedConnection conn = getValidConnection(connId, token).getConnection();
      if (flags != null && !flags.isEmpty()) {
        beginOrAlterTransaction(conn, gfxdConstants.TRANSACTION_NO_CHANGE,
            flags, false);
      }
      conn.commit();
      // JDBC starts a new transaction immediately; we need to set the isolation
      // explicitly to NONE to avoid that
      if (!startNewTransaction) {
        conn.setTransactionIsolation(Connection.TRANSACTION_NONE);
      }
    } catch (Throwable t) {
      throw gfxdException(t);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void rollbackTransaction(int connId,
      final boolean startNewTransaction,
      Map<TransactionAttribute, Boolean> flags, ByteBuffer token)
      throws GFXDException {

    try {
      EmbedConnection conn = getValidConnection(connId, token).getConnection();
      if (flags != null && !flags.isEmpty()) {
        beginOrAlterTransaction(conn, gfxdConstants.TRANSACTION_NO_CHANGE,
            flags, false);
      }
      conn.rollback();
      // JDBC starts a new transaction immediately; we need to set the isolation
      // explicitly to NONE to avoid that
      if (!startNewTransaction) {
        conn.setTransactionIsolation(Connection.TRANSACTION_NONE);
      }
    } catch (Throwable t) {
      throw gfxdException(t);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean prepareCommitTransaction(int connId,
      Map<TransactionAttribute, Boolean> flags, ByteBuffer token)
      throws GFXDException {

    try {
      EmbedConnection conn = getValidConnection(connId, token).getConnection();
      if (flags != null && !flags.isEmpty()) {
        beginOrAlterTransaction(conn, gfxdConstants.TRANSACTION_NO_CHANGE,
            flags, false);
      }
      return conn.xa_prepare() == XATransactionController.XA_OK;
    } catch (Throwable t) {
      throw gfxdException(t);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public RowSet scrollCursor(int cursorId, int offset,
      boolean offsetIsAbsolute, boolean fetchReverse, int fetchSize,
      ByteBuffer token) throws GFXDException {
    try {
      StatementHolder stmtHolder = getStatementForResultSet(token,
          cursorId, "scrollCursor");
      ConnectionHolder connHolder = stmtHolder.getConnectionHolder();
      final int connId = connHolder.getConnectionId();
      ResultSet rs = stmtHolder.findResultSet(cursorId);
      if (rs != null) {
        return getRowSet(stmtHolder.getStatement(), stmtHolder, rs, cursorId,
            connId, stmtHolder.getStatementAttrs(), offset, offsetIsAbsolute,
            fetchReverse, fetchSize, connHolder, false, null /*already set*/);
      }
      else {
        throw resultSetNotFoundException(cursorId, "scrollCursor");
      }
    } catch (Throwable t) {
      throw gfxdException(t);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void executeCursorUpdate(int cursorId, List<Byte> operations,
      List<Row> changedRows, List<List<Integer>> changedColumnsList,
      List<Integer> changedRowIndexes, ByteBuffer token) throws GFXDException {
    // TODO Auto-generated method stub
    throw notImplementedException("executeCursorUpdate");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public RowSet getNextResultSet(int cursorId, byte otherResultSetBehaviour,
      ByteBuffer token) throws GFXDException {
    ResultSet rs = null;
    try {
      StatementHolder stmtHolder = getStatementForResultSet(token, cursorId,
          "getNextResultSet");
      Statement stmt = stmtHolder.getStatement();
      if (stmt == null) {
        return null;
      }
      final boolean moreResults;
      if (otherResultSetBehaviour == 0) {
        moreResults = stmt.getMoreResults();
      }
      else {
        final int current;
        switch (otherResultSetBehaviour) {
          case gfxdConstants.NEXTRS_CLOSE_CURRENT_RESULT:
            current = EmbedStatement.CLOSE_CURRENT_RESULT;
            break;
          case gfxdConstants.NEXTRS_KEEP_CURRENT_RESULT:
            current = EmbedStatement.KEEP_CURRENT_RESULT;
            break;
          default:
            current = EmbedStatement.CLOSE_ALL_RESULTS;
            break;
        }
        moreResults = stmt.getMoreResults(current);
      }
      if (moreResults) {
        ConnectionHolder connHolder = stmtHolder.getConnectionHolder();
        rs = stmt.getResultSet();
        return getRowSet(stmt, stmtHolder, rs, INVALID_ID,
            connHolder.getConnectionId(), stmtHolder.getStatementAttrs(), 0,
            false, false, 0, connHolder, true, null /*already set*/);
      }
      else {
        return null;
      }
    } catch (Throwable t) {
      cleanupResultSet(rs);
      throw gfxdException(t);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public BlobChunk getBlobChunk(int connId, int lobId, long offset,
      int chunkSize, boolean freeLobAtEnd, ByteBuffer token)
      throws GFXDException {
    try {
      EmbedConnection conn = getValidConnection(connId, token).getConnection();
      Object lob = conn.getLOBMapping(lobId);
      if (lob instanceof Blob) {
        Blob blob = (Blob)lob;
        long length = blob.length() - offset;
        if (length > Integer.MAX_VALUE) {
          throw Util.generateCsSQLException(SQLState.BLOB_TOO_LARGE_FOR_CLIENT,
              Long.toString(length), Long.toString(Integer.MAX_VALUE));
        }
        BlobChunk chunk = new BlobChunk().setLobId(lobId).setOffset(offset);
        if (chunkSize > 0 && chunkSize < length) {
          chunk.setChunk(blob.getBytes(offset + 1, chunkSize)).setLast(false);
        }
        else {
          chunk.setChunk(blob.getBytes(offset + 1, (int)length)).setLast(true);
          if (freeLobAtEnd) {
            conn.removeLOBMapping(lobId);
            blob.free();
          }
        }
        return chunk;
      }
      else {
        throw Util.generateCsSQLException(SQLState.LOB_LOCATOR_INVALID);
      }
    } catch (Throwable t) {
      throw gfxdException(t);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ClobChunk getClobChunk(int connId, int lobId, long offset,
      int chunkSize, boolean freeLobAtEnd, ByteBuffer token)
      throws GFXDException {
    try {
      EmbedConnection conn = getValidConnection(connId, token).getConnection();
      Object lob = conn.getLOBMapping(lobId);
      if (lob instanceof Clob) {
        Clob clob = (Clob)lob;
        long length = clob.length() - offset;
        if (length > Integer.MAX_VALUE) {
          throw Util.generateCsSQLException(SQLState.BLOB_TOO_LARGE_FOR_CLIENT,
              Long.toString(length), Long.toString(Integer.MAX_VALUE));
        }
        ClobChunk chunk = new ClobChunk().setLobId(lobId).setOffset(offset);
        if (chunkSize > 0 && chunkSize < length) {
          chunk.setChunk(clob.getSubString(offset + 1, chunkSize)).setLast(
              false);
        }
        else {
          chunk.setChunk(clob.getSubString(offset + 1, (int)length)).setLast(
              true);
          if (freeLobAtEnd) {
            conn.removeLOBMapping(lobId);
            clob.free();
          }
        }
        return chunk;
      }
      else {
        throw Util.generateCsSQLException(SQLState.LOB_LOCATOR_INVALID);
      }
    } catch (Throwable t) {
      throw gfxdException(t);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void closeResultSet(int cursorId, ByteBuffer token)
      throws GFXDException {
    if (cursorId == INVALID_ID) {
      return;
    }
    try {
      StatementHolder stmtHolder = getStatementForResultSet(token, cursorId,
          "closeResultSet");
      stmtHolder.closeResultSet(cursorId, this);
    } catch (Throwable t) {
      throw gfxdException(t);
    }
  }

  private void checkDBOwner(int connId, ByteBuffer token, String module)
      throws GFXDException {
    // check for valid token
    ConnectionHolder connHolder = getValidConnection(connId, token);

    String authId;
    try {
      authId = IdUtil.getUserAuthorizationId(connHolder.getUserName());
    } catch (Exception e) {
      throw gfxdException(e);
    }
    if (!Misc.getMemStore().getDatabase().getDataDictionary()
        .getAuthorizationDatabaseOwner().equals(authId)) {
      throw newGFXDException(SQLState.LOGIN_FAILED,
          "administrator access required for " + module);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<ConnectionProperties> fetchActiveConnections(int connId,
      ByteBuffer token) throws GFXDException {
    // only allow admin user
    checkDBOwner(connId, token, "fetchActiveConnections");

    final ArrayList<ConnectionProperties> activeConns = new ArrayList<>(
        this.connectionMap.size());
    this.connectionMap.forEachValue(new TObjectProcedure() {
      @Override
      public boolean execute(Object h) {
        final ConnectionHolder connHolder = (ConnectionHolder)h;
        ConnectionProperties props = new ConnectionProperties(connHolder
            .getConnectionId(), connHolder.getClientHostName(), connHolder
            .getClientID());
        props.setUserName(connHolder.getUserName());
        activeConns.add(props);
        return true;
      }
    });
    return activeConns;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Map<Integer, String> fetchActiveStatements(int connId, ByteBuffer token)
      throws GFXDException {
    // only allow admin user
    checkDBOwner(connId, token, "fetchActiveStatements");

    final HashMap<Integer, String> activeStmts = new HashMap<>(
        this.statementMap.size());
    this.statementMap.forEachValue(new TObjectProcedure() {
      @Override
      public boolean execute(Object h) {
        final StatementHolder stmtHolder = (StatementHolder)h;
        activeStmts.put(stmtHolder.getStatementId(), stmtHolder.getSQL());
        return true;
      }
    });
    return activeStmts;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void cancelStatement(int stmtId, ByteBuffer token)
      throws GFXDException {
    try {
      StatementHolder stmtHolder = getStatement(token, stmtId, false,
          "cancelStatement");
      stmtHolder.getStatement().cancel();
    } catch (Throwable t) {
      throw gfxdException(t);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void closeStatement(int stmtId, ByteBuffer token)
      throws GFXDException {
    try {
      StatementHolder stmtHolder = getStatement(token, stmtId, false,
          "closeStatement");
      stmtHolder.getConnectionHolder().closeStatement(stmtHolder, this);
    } catch (Throwable t) {
      throw gfxdException(t);
    }
  }

  private GFXDExceptionData gfxdWarning(SQLWarning warnings)
      throws SQLException {
    GFXDExceptionData warningData = new GFXDExceptionData(
        warnings.getMessage(), warnings.getSQLState(), warnings.getErrorCode());
    ArrayList<GFXDExceptionData> nextWarnings = null;
    SQLWarning next = warnings.getNextWarning();
    if (next != null) {
      nextWarnings = new ArrayList<GFXDExceptionData>();
      do {
        nextWarnings.add(new GFXDExceptionData(next.getMessage(), next
            .getSQLState(), next.getErrorCode()));
      } while ((next = next.getNextWarning()) != null);
    }
    //GFXDExceptionData sqlw = new GFXDExceptionData(warningData);
    //sqlw.setNextWarnings(nextWarnings);
    if (nextWarnings != null) {
      warningData.setReason(warningData.getReason() + nextWarnings.toString());
    }
    return warningData;
  }

  private final void fillWarnings(StatementResult sr, Statement stmt)
      throws SQLException {
    SQLWarning warnings = stmt.getWarnings();
    if (warnings != null) {
      sr.setWarnings(gfxdWarning(warnings));
    }
  }

  private final void fillWarnings(UpdateResult ur, Statement stmt)
      throws SQLException {
    SQLWarning warnings = stmt.getWarnings();
    if (warnings != null) {
      ur.setWarnings(gfxdWarning(warnings));
    }
  }

  private final void fillWarnings(RowSet rs, ResultSet resultSet)
      throws SQLException {
    SQLWarning warnings = resultSet.getWarnings();
    if (warnings != null) {
      rs.setWarnings(gfxdWarning(warnings));
    }
  }

  private GFXDException internalException(String message) {
    GFXDExceptionData exData = new GFXDExceptionData();
    exData.setReason(message);
    exData.setSqlState(SQLState.JAVA_EXCEPTION);
    exData.setSeverity(ExceptionSeverity.NO_APPLICABLE_SEVERITY);
    return new GFXDException(exData, getServerInfo());
  }

  private GFXDException notImplementedException(String method) {
    GFXDExceptionData exData = new GFXDExceptionData();
    exData.setReason("ASSERT: " + method + "() not implemented");
    exData.setSqlState(SQLState.JDBC_METHOD_NOT_SUPPORTED_BY_SERVER);
    exData.setSeverity(ExceptionSeverity.STATEMENT_SEVERITY);
    return new GFXDException(exData, getServerInfo());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int sendBlobChunk(BlobChunk chunk, int connId, ByteBuffer token)
      throws GFXDException {
    try {
      EmbedConnection conn = getValidConnection(connId, token).getConnection();
      int lobId;
      Blob blob;
      if (chunk.isSetLobId()) {
        lobId = chunk.lobId;
        Object lob = conn.getLOBMapping(lobId);
        if (lob instanceof Blob) {
          blob = (Blob)lob;
        }
        else {
          throw Util.generateCsSQLException(SQLState.LOB_LOCATOR_INVALID);
        }
      }
      else {
        EmbedBlob eblob = conn.createBlob();
        lobId = eblob.getLocator();
        blob = eblob;
      }
      long offset = 1;
      if (chunk.isSetOffset()) {
        offset += chunk.offset;
      }
      blob.setBytes(offset, chunk.getChunk());
      return lobId;
    } catch (Throwable t) {
      throw gfxdException(t);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int sendClobChunk(ClobChunk chunk, int connId, ByteBuffer token)
      throws GFXDException {
    try {
      EmbedConnection conn = getValidConnection(connId, token).getConnection();
      int lobId;
      Clob clob;
      if (chunk.isSetLobId()) {
        lobId = chunk.lobId;
        Object lob = conn.getLOBMapping(lobId);
        if (lob instanceof Clob) {
          clob = (Clob)lob;
        }
        else {
          throw Util.generateCsSQLException(SQLState.LOB_LOCATOR_INVALID);
        }
      }
      else {
        EmbedClob eclob = conn.createClob();
        lobId = eclob.getLocator();
        clob = eclob;
      }
      long offset = 1;
      if (chunk.isSetOffset()) {
        offset += chunk.offset;
      }
      clob.setString(offset, chunk.getChunk());
      return lobId;
    } catch (Throwable t) {
      throw gfxdException(t);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void freeLob(int connId, int lobId, ByteBuffer token)
      throws GFXDException {
    try {
      EmbedConnection conn = getValidConnection(connId, token).getConnection();
      Object lob = conn.getLOBMapping(lobId);
      if (lob instanceof EngineLOB) {
        ((EngineLOB)lob).free();
        conn.removeLOBMapping(lobId);
      }
      else {
        throw Util.generateCsSQLException(SQLState.LOB_LOCATOR_INVALID);
      }
    } catch (Throwable t) {
      throw gfxdException(t);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ServiceMetaData getServiceMetaData(int connId, ByteBuffer token)
      throws GFXDException {

    try {
      final ConnectionHolder connHolder = getValidConnection(connId, token);
      EmbedDatabaseMetaData dmd = (EmbedDatabaseMetaData)connHolder
          .getConnection().getMetaData();
      ServiceMetaData metadata = new ServiceMetaData()
          .setCatalogSeparator(dmd.getCatalogSeparator())
          .setCatalogTerm(dmd.getCatalogTerm())
          .setDateTimeFunctions(toList(dmd.getTimeDateFunctions()))
          .setDefaultResultSetHoldabilityHoldCursorsOverCommit(dmd
              .getResultSetHoldability() == ResultSet.HOLD_CURSORS_OVER_COMMIT)
          .setDefaultResultSetType(gfxdConstants.RESULTSET_TYPE_FORWARD_ONLY)
          .setDefaultTransactionIsolation(
              gfxdConstants.DEFAULT_TRANSACTION_ISOLATION)
          .setExtraNameCharacters(dmd.getExtraNameCharacters())
          .setCatalogAtStart(dmd.isCatalogAtStart())
          .setIdentifierQuote(dmd.getIdentifierQuoteString())
          .setJdbcMajorVersion(dmd.getJDBCMajorVersion())
          .setJdbcMinorVersion(dmd.getJDBCMinorVersion())
          .setMaxBinaryLiteralLength(dmd.getMaxBinaryLiteralLength())
          .setMaxCatalogNameLength(dmd.getMaxCatalogNameLength())
          .setMaxCharLiteralLength(dmd.getMaxCharLiteralLength())
          .setMaxColumnNameLength(dmd.getMaxColumnNameLength())
          .setMaxColumnsInGroupBy(dmd.getMaxColumnsInGroupBy())
          .setMaxColumnsInIndex(dmd.getMaxColumnsInIndex())
          .setMaxColumnsInOrderBy(dmd.getMaxColumnsInOrderBy())
          .setMaxColumnsInSelect(dmd.getMaxColumnsInSelect())
          .setMaxColumnsInTable(dmd.getMaxColumnsInTable())
          .setMaxConnections(dmd.getMaxConnections())
          .setMaxCursorNameLength(dmd.getMaxCursorNameLength())
          .setMaxIndexLength(dmd.getMaxIndexLength())
          .setMaxOpenStatements(dmd.getMaxStatements())
          .setMaxProcedureNameLength(dmd.getMaxProcedureNameLength())
          .setMaxRowSize(dmd.getMaxRowSize())
          .setMaxSchemaNameLength(dmd.getMaxSchemaNameLength())
          .setMaxStatementLength(dmd.getMaxStatementLength())
          .setMaxTableNameLength(dmd.getMaxTableNameLength())
          .setMaxTableNamesInSelect(dmd.getMaxTablesInSelect())
          .setMaxUserNameLength(dmd.getMaxUserNameLength())
          .setNumericFunctions(toList(dmd.getNumericFunctions()))
          .setProcedureTerm(dmd.getProcedureTerm())
          .setProductMajorVersion(dmd.getDatabaseMajorVersion())
          .setProductMinorVersion(dmd.getDatabaseMinorVersion())
          .setProductName(dmd.getDatabaseProductName())
          .setProductVersion(dmd.getDatabaseProductVersion())
          .setSchemaTerm(dmd.getSchemaTerm())
          .setSearchStringEscape(dmd.getSearchStringEscape())
          .setSqlKeywords(toList(dmd.getSQLKeywords()))
          .setSqlStateIsXOpen(
              dmd.getSQLStateType() == DatabaseMetaData.sqlStateXOpen)
          .setStringFunctions(toList(dmd.getStringFunctions()))
          .setSystemFunctions(toList(dmd.getSystemFunctions()));

      // populate the TransactionAttribute defaults
      final EnumMap<TransactionAttribute, Boolean> txDefaults = ThriftUtils
          .newTransactionFlags();

      final SystemProperties sysProps = SystemProperties.getServerInstance();
      txDefaults.put(TransactionAttribute.AUTOCOMMIT,
          gfxdConstants.DEFAULT_AUTOCOMMIT);
      txDefaults.put(TransactionAttribute.DISABLE_BATCHING, sysProps
          .getBoolean(Property.GFXD_DISABLE_TX_BATCHING,
              !GfxdConstants.GFXD_TX_BATCHING_DEFAULT));
      txDefaults.put(TransactionAttribute.READ_ONLY_CONNECTION,
          dmd.isReadOnly());
      txDefaults.put(TransactionAttribute.SYNC_COMMITS, sysProps.getBoolean(
          Property.GFXD_TX_SYNC_COMMITS,
          GfxdConstants.GFXD_TX_SYNC_COMMITS_DEFAULT));
      txDefaults.put(TransactionAttribute.WAITING_MODE, sysProps.getBoolean(
          Property.GFXD_ENABLE_TX_WAIT_MODE,
          GfxdConstants.GFXD_TX_WAIT_MODE_DEFAULT));
      metadata.setTransactionDefaults(txDefaults);

      RowIdLifetime rowIdLifeTime;
      switch (dmd.getRowIdLifetime()) {
        case ROWID_VALID_OTHER:
          rowIdLifeTime = RowIdLifetime.ROWID_VALID_OTHER;
          break;
        case ROWID_VALID_FOREVER:
          rowIdLifeTime = RowIdLifetime.ROWID_VALID_FOREVER;
          break;
        case ROWID_VALID_SESSION:
          rowIdLifeTime = RowIdLifetime.ROWID_VALID_SESSION;
          break;
        case ROWID_VALID_TRANSACTION:
          rowIdLifeTime = RowIdLifetime.ROWID_VALID_TRANSACTION;
          break;
        case ROWID_UNSUPPORTED:
        default:
          rowIdLifeTime = RowIdLifetime.ROWID_UNSUPPORTED;
          break;
      }
      metadata.setRowIdLifeTime(rowIdLifeTime);

      // supported features
      HashSet<ServiceFeature> supportedFeatures = new HashSet<ServiceFeature>();
      if (dmd.supportsAlterTableWithAddColumn()) {
        supportedFeatures.add(ServiceFeature.ALTER_TABLE_ADD_COLUMN);
      }
      if (dmd.supportsAlterTableWithDropColumn()) {
        supportedFeatures.add(ServiceFeature.ALTER_TABLE_DROP_COLUMN);
      }
      if (dmd.supportsANSI92EntryLevelSQL()) {
        supportedFeatures.add(ServiceFeature.SQL_GRAMMAR_ANSI92_ENTRY);
      }
      if (dmd.supportsANSI92FullSQL()) {
        supportedFeatures.add(ServiceFeature.SQL_GRAMMAR_ANSI92_FULL);
      }
      if (dmd.supportsANSI92IntermediateSQL()) {
        supportedFeatures.add(ServiceFeature.SQL_GRAMMAR_ANSI92_INTERMEDIATE);
      }
      if (dmd.supportsBatchUpdates()) {
        supportedFeatures.add(ServiceFeature.BATCH_UPDATES);
      }
      if (dmd.supportsCatalogsInDataManipulation()) {
        supportedFeatures.add(ServiceFeature.CATALOGS_IN_DMLS);
      }
      if (dmd.supportsCatalogsInIndexDefinitions()) {
        supportedFeatures.add(ServiceFeature.CATALOGS_IN_INDEX_DEFS);
      }
      if (dmd.supportsCatalogsInPrivilegeDefinitions()) {
        supportedFeatures.add(ServiceFeature.CATALOGS_IN_PRIVILEGE_DEFS);
      }
      if (dmd.supportsCatalogsInProcedureCalls()) {
        supportedFeatures.add(ServiceFeature.CATALOGS_IN_PROCEDURE_CALLS);
      }
      if (dmd.supportsCatalogsInTableDefinitions()) {
        supportedFeatures.add(ServiceFeature.CATALOGS_IN_TABLE_DEFS);
      }
      if (dmd.supportsColumnAliasing()) {
        supportedFeatures.add(ServiceFeature.COLUMN_ALIASING);
      }
      if (dmd.supportsConvert()) {
        supportedFeatures.add(ServiceFeature.CONVERT);
      }
      if (dmd.supportsCoreSQLGrammar()) {
        supportedFeatures.add(ServiceFeature.SQL_GRAMMAR_CORE);
      }
      if (dmd.supportsCorrelatedSubqueries()) {
        supportedFeatures.add(ServiceFeature.SUBQUERIES_CORRELATED);
      }
      if (dmd.supportsDataDefinitionAndDataManipulationTransactions()) {
        supportedFeatures.add(ServiceFeature.TRANSACTIONS_BOTH_DMLS_AND_DDLS);
      }
      if (dmd.supportsDataManipulationTransactionsOnly()) {
        supportedFeatures.add(ServiceFeature.TRANSACTIONS_DMLS_ONLY);
      }
      if (dmd.supportsDifferentTableCorrelationNames()) {
        supportedFeatures.add(ServiceFeature.TABLE_CORRELATION_NAMES_DIFFERENT);
      }
      if (dmd.supportsExpressionsInOrderBy()) {
        supportedFeatures.add(ServiceFeature.ORDER_BY_EXPRESSIONS);
      }
      if (dmd.supportsExtendedSQLGrammar()) {
        supportedFeatures.add(ServiceFeature.SQL_GRAMMAR_EXTENDED);
      }
      if (dmd.supportsFullOuterJoins()) {
        supportedFeatures.add(ServiceFeature.OUTER_JOINS_FULL);
      }
      if (dmd.supportsGetGeneratedKeys()) {
        supportedFeatures.add(ServiceFeature.GENERATED_KEYS_RETRIEVAL);
      }
      if (dmd.supportsGroupBy()) {
        supportedFeatures.add(ServiceFeature.GROUP_BY);
      }
      if (dmd.supportsGroupByBeyondSelect()) {
        supportedFeatures.add(ServiceFeature.GROUP_BY_BEYOND_SELECT);
      }
      if (dmd.supportsGroupByUnrelated()) {
        supportedFeatures.add(ServiceFeature.GROUP_BY_UNRELATED);
      }
      if (dmd.supportsIntegrityEnhancementFacility()) {
        supportedFeatures.add(ServiceFeature.INTEGRITY_ENHANCEMENT);
      }
      if (dmd.supportsLikeEscapeClause()) {
        supportedFeatures.add(ServiceFeature.LIKE_ESCAPE);
      }
      if (dmd.supportsLimitedOuterJoins()) {
        supportedFeatures.add(ServiceFeature.OUTER_JOINS_LIMITED);
      }
      if (dmd.supportsMinimumSQLGrammar()) {
        supportedFeatures.add(ServiceFeature.SQL_GRAMMAR_MINIMUM);
      }
      if (dmd.supportsMixedCaseIdentifiers()) {
        supportedFeatures.add(ServiceFeature.MIXEDCASE_IDENTIFIERS);
      }
      if (dmd.supportsMixedCaseQuotedIdentifiers()) {
        supportedFeatures.add(ServiceFeature.MIXEDCASE_QUOTED_IDENTIFIERS);
      }
      if (dmd.supportsMultipleOpenResults()) {
        supportedFeatures.add(ServiceFeature.MULTIPLE_RESULTSETS);
      }
      if (dmd.supportsMultipleTransactions()) {
        supportedFeatures.add(ServiceFeature.MULTIPLE_TRANSACTIONS);
      }
      if (dmd.supportsNamedParameters()) {
        supportedFeatures.add(ServiceFeature.CALLABLE_NAMED_PARAMETERS);
      }
      if (dmd.supportsNonNullableColumns()) {
        supportedFeatures.add(ServiceFeature.NON_NULLABLE_COLUMNS);
      }
      if (dmd.supportsOpenCursorsAcrossCommit()) {
        supportedFeatures.add(ServiceFeature.OPEN_CURSORS_ACROSS_COMMIT);
      }
      if (dmd.supportsOpenCursorsAcrossRollback()) {
        supportedFeatures.add(ServiceFeature.OPEN_CURSORS_ACROSS_ROLLBACK);
      }
      if (dmd.supportsOpenStatementsAcrossCommit()) {
        supportedFeatures.add(ServiceFeature.OPEN_STATEMENTS_ACROSS_COMMIT);
      }
      if (dmd.supportsOpenStatementsAcrossRollback()) {
        supportedFeatures.add(ServiceFeature.OPEN_STATEMENTS_ACROSS_ROLLBACK);
      }
      if (dmd.supportsOrderByUnrelated()) {
        supportedFeatures.add(ServiceFeature.ORDER_BY_UNRELATED);
      }
      if (dmd.supportsOuterJoins()) {
        supportedFeatures.add(ServiceFeature.OUTER_JOINS);
      }
      if (dmd.supportsPositionedDelete()) {
        supportedFeatures.add(ServiceFeature.POSITIONED_DELETE);
      }
      if (dmd.supportsPositionedUpdate()) {
        supportedFeatures.add(ServiceFeature.POSITIONED_UPDATE);
      }
      if (dmd.supportsResultSetHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT)) {
        supportedFeatures
            .add(ServiceFeature.RESULTSET_HOLDABILITY_CLOSE_CURSORS_AT_COMMIT);
      }
      if (dmd.supportsResultSetHoldability(
          ResultSet.HOLD_CURSORS_OVER_COMMIT)) {
        supportedFeatures
            .add(ServiceFeature.RESULTSET_HOLDABILITY_HOLD_CURSORS_OVER_COMMIT);
      }
      if (dmd.supportsResultSetType(ResultSet.TYPE_FORWARD_ONLY)) {
        supportedFeatures.add(ServiceFeature.RESULTSET_FORWARD_ONLY);
      }
      if (dmd.supportsResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE)) {
        supportedFeatures.add(ServiceFeature.RESULTSET_SCROLL_INSENSITIVE);
      }
      if (dmd.supportsResultSetType(ResultSet.TYPE_SCROLL_SENSITIVE)) {
        supportedFeatures.add(ServiceFeature.RESULTSET_SCROLL_SENSITIVE);
      }
      if (dmd.supportsSavepoints()) {
        supportedFeatures.add(ServiceFeature.TRANSACTIONS_SAVEPOINTS);
      }
      if (dmd.supportsSchemasInDataManipulation()) {
        supportedFeatures.add(ServiceFeature.SCHEMAS_IN_DMLS);
      }
      if (dmd.supportsSchemasInIndexDefinitions()) {
        supportedFeatures.add(ServiceFeature.SCHEMAS_IN_INDEX_DEFS);
      }
      if (dmd.supportsSchemasInPrivilegeDefinitions()) {
        supportedFeatures.add(ServiceFeature.SCHEMAS_IN_PRIVILEGE_DEFS);
      }
      if (dmd.supportsSchemasInProcedureCalls()) {
        supportedFeatures.add(ServiceFeature.SCHEMAS_IN_PROCEDURE_CALLS);
      }
      if (dmd.supportsSchemasInTableDefinitions()) {
        supportedFeatures.add(ServiceFeature.SCHEMAS_IN_TABLE_DEFS);
      }
      if (dmd.supportsSelectForUpdate()) {
        supportedFeatures.add(ServiceFeature.SELECT_FOR_UPDATE);
      }
      if (dmd.supportsStatementPooling()) {
        supportedFeatures.add(ServiceFeature.STATEMENT_POOLING);
      }
      if (dmd.supportsStoredFunctionsUsingCallSyntax()) {
        supportedFeatures.add(ServiceFeature.STORED_FUNCTIONS_USING_CALL);
      }
      if (dmd.supportsStoredProcedures()) {
        supportedFeatures.add(ServiceFeature.STORED_PROCEDURES);
      }
      if (dmd.supportsSubqueriesInComparisons()) {
        supportedFeatures.add(ServiceFeature.SUBQUERIES_IN_COMPARISONS);
      }
      if (dmd.supportsSubqueriesInExists()) {
        supportedFeatures.add(ServiceFeature.SUBQUERIES_IN_EXISTS);
      }
      if (dmd.supportsSubqueriesInIns()) {
        supportedFeatures.add(ServiceFeature.SUBQUERIES_IN_INS);
      }
      if (dmd.supportsSubqueriesInQuantifieds()) {
        supportedFeatures.add(ServiceFeature.SUBQUERIES_IN_QUANTIFIEDS);
      }
      if (dmd.supportsTableCorrelationNames()) {
        supportedFeatures.add(ServiceFeature.TABLE_CORRELATION_NAMES);
      }
      if (dmd.supportsTransactions()) {
        supportedFeatures.add(ServiceFeature.TRANSACTIONS);
      }
      if (dmd.supportsUnion()) {
        supportedFeatures.add(ServiceFeature.UNION);
      }
      if (dmd.supportsUnionAll()) {
        supportedFeatures.add(ServiceFeature.UNION_ALL);
      }
      if (dmd.allProceduresAreCallable()) {
        supportedFeatures.add(ServiceFeature.ALL_PROCEDURES_CALLABLE);
      }
      if (dmd.allTablesAreSelectable()) {
        supportedFeatures.add(ServiceFeature.ALL_TABLES_SELECTABLE);
      }
      if (dmd.autoCommitFailureClosesAllResultSets()) {
        supportedFeatures
            .add(ServiceFeature.AUTOCOMMIT_FAILURE_CLOSES_ALL_RESULTSETS);
      }
      if (dmd.dataDefinitionCausesTransactionCommit()) {
        supportedFeatures.add(ServiceFeature.TRANSACTIONS_DDLS_IMPLICIT_COMMIT);
      }
      if (dmd.dataDefinitionIgnoredInTransactions()) {
        supportedFeatures.add(ServiceFeature.TRANSACTIONS_DDLS_IGNORED);
      }
      if (dmd.doesMaxRowSizeIncludeBlobs()) {
        supportedFeatures.add(ServiceFeature.MAX_ROWSIZE_INCLUDES_BLOBSIZE);
      }
      if (dmd.generatedKeyAlwaysReturned()) {
        supportedFeatures.add(ServiceFeature.GENERATED_KEYS_ALWAYS_RETURNED);
      }
      if (dmd.locatorsUpdateCopy()) {
        supportedFeatures.add(ServiceFeature.LOB_UPDATES_COPY);
      }
      if (dmd.nullPlusNonNullIsNull()) {
        supportedFeatures.add(ServiceFeature.NULL_CONCAT_NON_NULL_IS_NULL);
      }
      if (dmd.nullsAreSortedAtEnd()) {
        supportedFeatures.add(ServiceFeature.NULLS_SORTED_END);
      }
      if (dmd.nullsAreSortedAtStart()) {
        supportedFeatures.add(ServiceFeature.NULLS_SORTED_START);
      }
      if (dmd.nullsAreSortedHigh()) {
        supportedFeatures.add(ServiceFeature.NULLS_SORTED_HIGH);
      }
      if (dmd.nullsAreSortedLow()) {
        supportedFeatures.add(ServiceFeature.NULLS_SORTED_LOW);
      }
      if (dmd.storesLowerCaseIdentifiers()) {
        supportedFeatures.add(ServiceFeature.STORES_LOWERCASE_IDENTIFIERS);
      }
      if (dmd.storesLowerCaseQuotedIdentifiers()) {
        supportedFeatures
            .add(ServiceFeature.STORES_LOWERCASE_QUOTED_IDENTIFIERS);
      }
      if (dmd.storesMixedCaseIdentifiers()) {
        supportedFeatures.add(ServiceFeature.STORES_MIXEDCASE_IDENTIFIERS);
      }
      if (dmd.storesMixedCaseQuotedIdentifiers()) {
        supportedFeatures
            .add(ServiceFeature.STORES_MIXEDCASE_QUOTED_IDENTIFIERS);
      }
      if (dmd.storesUpperCaseIdentifiers()) {
        supportedFeatures.add(ServiceFeature.STORES_UPPERCASE_IDENTIFIERS);
      }
      if (dmd.storesUpperCaseQuotedIdentifiers()) {
        supportedFeatures
            .add(ServiceFeature.STORES_UPPERCASE_QUOTED_IDENTIFIERS);
      }
      if (dmd.usesLocalFilePerTable()) {
        supportedFeatures.add(ServiceFeature.USES_LOCAL_FILE_PER_TABLE);
      }
      if (dmd.usesLocalFiles()) {
        supportedFeatures.add(ServiceFeature.USES_LOCAL_FILES);
      }
      metadata.setSupportedFeatures(supportedFeatures);

      // CONVERT support
      final int[] allTypes = new int[] { Types.ARRAY, Types.BIGINT,
          Types.BINARY, Types.BIT, Types.BLOB, Types.BOOLEAN, Types.CHAR,
          Types.CLOB, Types.DATALINK, Types.DATE, Types.DECIMAL,
          Types.DISTINCT, Types.DOUBLE, Types.FLOAT, Types.INTEGER,
          Types.JAVA_OBJECT, Types.LONGNVARCHAR, Types.LONGVARBINARY,
          Types.LONGVARCHAR, Types.NCHAR, Types.NCLOB, Types.NULL,
          Types.NUMERIC, Types.NVARCHAR, Types.OTHER, Types.REAL, Types.REF,
          Types.ROWID, Types.SMALLINT, Types.SQLXML, Types.STRUCT, Types.TIME,
          Types.TIMESTAMP, Types.TINYINT, Types.VARBINARY, Types.VARCHAR,
          JDBC40Translation.PDX, JDBC40Translation.JSON };
      Map<GFXDType, Set<GFXDType>> convertMap =
          new HashMap<GFXDType, Set<GFXDType>>();
      for (int fromType : allTypes) {
        HashSet<GFXDType> supportedConverts = new HashSet<GFXDType>();
        for (int toType : allTypes) {
          if (dmd.supportsConvert(fromType, toType)) {
            supportedConverts.add(Converters.getThriftSQLType(toType));
          }
        }
        if (!supportedConverts.isEmpty()) {
          convertMap.put(Converters.getThriftSQLType(fromType),
              supportedConverts);
        }
      }
      metadata.setSupportedCONVERT(convertMap);

      // lastly features with parameters
      HashMap<ServiceFeatureParameterized, List<Integer>> featureParameters =
          new HashMap<ServiceFeatureParameterized, List<Integer>>();
      ArrayList<Integer> supportedValues = new ArrayList<Integer>(4);
      final int[] isolationLevels = new int[] { Connection.TRANSACTION_NONE,
          Connection.TRANSACTION_READ_UNCOMMITTED,
          Connection.TRANSACTION_READ_COMMITTED,
          Connection.TRANSACTION_REPEATABLE_READ,
          Connection.TRANSACTION_SERIALIZABLE };
      for (int isolationLevel : isolationLevels) {
        if (dmd.supportsTransactionIsolationLevel(isolationLevel)) {
          supportedValues.add(Integer.valueOf(Converters
              .getThriftTransactionIsolation(isolationLevel)));
        }
      }
      if (!supportedValues.isEmpty()) {
        featureParameters.put(
            ServiceFeatureParameterized.TRANSACTIONS_SUPPORT_ISOLATION,
            supportedValues);
        supportedValues = new ArrayList<Integer>(4);
      }

      final int[] rsTypes = new int[] { ResultSet.TYPE_FORWARD_ONLY,
          ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.TYPE_SCROLL_SENSITIVE };
      for (int rsType : rsTypes) {
        if (dmd.supportsResultSetType(rsType)) {
          supportedValues.add(Integer.valueOf(Converters
              .getThriftResultSetType(rsType)));
        }
      }
      if (!supportedValues.isEmpty()) {
        featureParameters.put(
            ServiceFeatureParameterized.RESULTSET_TYPE, supportedValues);
        supportedValues = new ArrayList<Integer>(4);
      }

      final int[] concurTypes = new int[] { ResultSet.CONCUR_READ_ONLY,
          ResultSet.CONCUR_UPDATABLE };
      for (int concurType : concurTypes) {
        ServiceFeatureParameterized thriftConcurType =
          (concurType == ResultSet.CONCUR_READ_ONLY
              ? ServiceFeatureParameterized.RESULTSET_CONCURRENCY_READ_ONLY
              : ServiceFeatureParameterized.RESULTSET_CONCURRENCY_UPDATABLE);
        for (int rsType : rsTypes) {
          if (dmd.supportsResultSetConcurrency(rsType, concurType)) {
            supportedValues.add(Integer.valueOf(Converters
                .getThriftResultSetType(rsType)));
          }
        }
        if (!supportedValues.isEmpty()) {
          featureParameters.put(thriftConcurType, supportedValues);
          supportedValues = new ArrayList<Integer>(4);
        }
      }

      for (int rsType : rsTypes) {
        if (dmd.ownUpdatesAreVisible(rsType)) {
          supportedValues.add(Integer.valueOf(Converters
              .getThriftResultSetType(rsType)));
        }
      }
      if (!supportedValues.isEmpty()) {
        featureParameters.put(ServiceFeatureParameterized
            .RESULTSET_OWN_UPDATES_VISIBLE, supportedValues);
        supportedValues = new ArrayList<Integer>(4);
      }

      for (int rsType : rsTypes) {
        if (dmd.ownDeletesAreVisible(rsType)) {
          supportedValues.add(Integer.valueOf(Converters
              .getThriftResultSetType(rsType)));
        }
      }
      if (!supportedValues.isEmpty()) {
        featureParameters.put(ServiceFeatureParameterized
            .RESULTSET_OWN_DELETES_VISIBLE, supportedValues);
        supportedValues = new ArrayList<Integer>(4);
      }

      for (int rsType : rsTypes) {
        if (dmd.ownInsertsAreVisible(rsType)) {
          supportedValues.add(Integer.valueOf(Converters
              .getThriftResultSetType(rsType)));
        }
      }
      if (!supportedValues.isEmpty()) {
        featureParameters.put(ServiceFeatureParameterized
            .RESULTSET_OWN_INSERTS_VISIBLE, supportedValues);
        supportedValues = new ArrayList<Integer>(4);
      }


      for (int rsType : rsTypes) {
        if (dmd.othersUpdatesAreVisible(rsType)) {
          supportedValues.add(Integer.valueOf(Converters
              .getThriftResultSetType(rsType)));
        }
      }
      if (!supportedValues.isEmpty()) {
        featureParameters.put(ServiceFeatureParameterized
            .RESULTSET_OTHERS_UPDATES_VISIBLE, supportedValues);
        supportedValues = new ArrayList<Integer>(4);
      }

      for (int rsType : rsTypes) {
        if (dmd.othersDeletesAreVisible(rsType)) {
          supportedValues.add(Integer.valueOf(Converters
              .getThriftResultSetType(rsType)));
        }
      }
      if (!supportedValues.isEmpty()) {
        featureParameters.put(ServiceFeatureParameterized
            .RESULTSET_OTHERS_DELETES_VISIBLE, supportedValues);
        supportedValues = new ArrayList<Integer>(4);
      }

      for (int rsType : rsTypes) {
        if (dmd.othersInsertsAreVisible(rsType)) {
          supportedValues.add(Integer.valueOf(Converters
              .getThriftResultSetType(rsType)));
        }
      }
      if (!supportedValues.isEmpty()) {
        featureParameters.put(ServiceFeatureParameterized
            .RESULTSET_OTHERS_INSERTS_VISIBLE, supportedValues);
        supportedValues = new ArrayList<Integer>(4);
      }

      for (int rsType : rsTypes) {
        if (dmd.updatesAreDetected(rsType)) {
          supportedValues.add(Integer.valueOf(Converters
              .getThriftResultSetType(rsType)));
        }
      }
      if (!supportedValues.isEmpty()) {
        featureParameters.put(ServiceFeatureParameterized
            .RESULTSET_UPDATES_DETECTED, supportedValues);
        supportedValues = new ArrayList<Integer>(4);
      }

      for (int rsType : rsTypes) {
        if (dmd.deletesAreDetected(rsType)) {
          supportedValues.add(Integer.valueOf(Converters
              .getThriftResultSetType(rsType)));
        }
      }
      if (!supportedValues.isEmpty()) {
        featureParameters.put(ServiceFeatureParameterized
            .RESULTSET_DELETES_DETECTED, supportedValues);
        supportedValues = new ArrayList<Integer>(4);
      }

      for (int rsType : rsTypes) {
        if (dmd.insertsAreDetected(rsType)) {
          supportedValues.add(Integer.valueOf(Converters
              .getThriftResultSetType(rsType)));
        }
      }
      if (!supportedValues.isEmpty()) {
        featureParameters.put(ServiceFeatureParameterized
            .RESULTSET_INSERTS_DETECTED, supportedValues);
        supportedValues = new ArrayList<Integer>(4);
      }

      metadata.setFeaturesWithParams(featureParameters);

      return metadata;
    } catch (Throwable t) {
      throw gfxdException(t);
    }
  }

  private ArrayList<String> toList(String csv) {
    final ArrayList<String> strings = new ArrayList<String>();
    SharedUtils.splitCSV(csv, SharedUtils.stringAggregator, strings,
        Boolean.TRUE);
    return strings;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public RowSet getSchemaMetaData(ServiceMetaDataCall schemaCall,
      ServiceMetaDataArgs args) throws GFXDException {
    ResultSet rs = null;
    try {
      final ConnectionHolder connHolder = getValidConnection(args.connId,
          args.token);
      EmbedDatabaseMetaData dmd = (EmbedDatabaseMetaData)connHolder
          .getConnection().getMetaData();
      final boolean isODBC = args.getDriverType() == gfxdConstants.DRIVER_ODBC;
      switch (schemaCall) {
        case ATTRIBUTES:
          rs = dmd.getAttributes(null, args.getSchema(), args.getTypeName(),
              args.getAttributeName());
          break;
        case CATALOGS:
          rs = dmd.getCatalogs();
          break;
        case CLIENTINFOPROPS:
          rs = dmd.getClientInfoProperties();
          break;
        case COLUMNPRIVILEGES:
          rs = dmd.getColumnPrivileges(null, args.getSchema(), args.getTable(),
              args.getColumnName());
          break;
        case COLUMNS:
          if (isODBC) {
            rs = dmd.getColumnsForODBC(null, args.getSchema(), args.getTable(),
                args.getColumnName());
          }
          else {
            rs = dmd.getColumns(null, args.getSchema(), args.getTable(),
                args.getColumnName());
          }
          break;
        case CROSSREFERENCE:
          if (isODBC) {
            rs = dmd.getCrossReferenceForODBC(null, args.getSchema(),
                args.getTable(), null, args.getForeignSchema(),
                args.getForeignTable());
          }
          else {
            rs = dmd.getCrossReference(null, args.getSchema(), args.getTable(),
                null, args.getForeignSchema(), args.getForeignTable());
          }
          break;
        case EXPORTEDKEYS:
          rs = dmd.getExportedKeys(null, args.getSchema(), args.getTable());
          break;
        case FUNCTIONCOLUMNS:
          rs = dmd.getFunctionColumns(null, args.getSchema(),
              args.getFunctionName(), args.getColumnName());
          break;
        case FUNCTIONS:
          rs = dmd.getFunctions(null, args.getSchema(), args.getFunctionName());
          break;
        case IMPORTEDKEYS:
          rs = dmd.getImportedKeys(null, args.getSchema(), args.getTable());
          break;
        case PRIMARYKEYS:
          rs = dmd.getPrimaryKeys(null, args.getSchema(), args.getTable());
          break;
        case PROCEDURECOLUMNS:
          if (isODBC) {
            rs = dmd.getProcedureColumnsForODBC(null, args.getSchema(),
                args.getProcedureName(), args.getColumnName());
          }
          else {
            rs = dmd.getProcedureColumns(null, args.getSchema(),
                args.getProcedureName(), args.getColumnName());
          }
          break;
        case PROCEDURES:
          if (isODBC) {
            rs = dmd.getProceduresForODBC(null, args.getSchema(),
                args.getProcedureName());
          }
          else {
            rs = dmd.getProcedures(null, args.getSchema(),
                args.getProcedureName());
          }
          break;
        case PSEUDOCOLUMNS:
          rs = dmd.getPseudoColumns(null, args.getSchema(), args.getTable(),
              args.getColumnName());
          break;
        case SCHEMAS:
          if (args.getSchema() != null) {
            rs = dmd.getSchemas(null, args.getSchema());
          }
          else {
            rs = dmd.getSchemas();
          }
          break;
        case SUPERTABLES:
          rs = dmd.getSuperTables(null, args.getSchema(), args.getTypeName());
          break;
        case SUPERTYPES:
          rs = dmd.getSuperTypes(null, args.getSchema(), args.getTypeName());
          break;
        case TABLEPRIVILEGES:
          rs = dmd.getTablePrivileges(null, args.getSchema(), args.getTable());
          break;
        case TABLES:
          List<String> tableTypes = args.getTableTypes();
          String[] types = null;
          if (tableTypes != null && !tableTypes.isEmpty()) {
            types = tableTypes.toArray(new String[tableTypes.size()]);
          }
          rs = dmd.getTables(null, args.getSchema(), args.getTable(), types);
          break;
        case TABLETYPES:
          rs = dmd.getTableTypes();
          break;
        case TYPEINFO:
          if (isODBC) {
            rs = dmd.getTypeInfoForODBC((short)(args.isSetTypeId() ? Converters
                .getJdbcType(args.getTypeId()) : 0));
          }
          else {
            rs = dmd.getTypeInfo();
          }
          break;
        case VERSIONCOLUMNS:
          if (isODBC) {
            rs = dmd.getVersionColumnsForODBC(null, args.getSchema(),
                args.getTable());
          }
          else {
            rs = dmd.getVersionColumns(null, args.getSchema(), args.getTable());
          }
          break;
        default:
          throw internalException("unexpected metadata call: " + schemaCall);
      }
      return getRowSet(null, null, rs, INVALID_ID, args.connId, null, 0, false,
          false, 0, connHolder, true, "getSchemaMetaData");
    } catch (Throwable t) {
      cleanupResultSet(rs);
      throw gfxdException(t);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public RowSet getIndexInfo(ServiceMetaDataArgs args, boolean unique,
      boolean approximate) throws GFXDException {
    ResultSet rs = null;
    try {
      final ConnectionHolder connHolder = getValidConnection(args.connId,
          args.token);
      EmbedDatabaseMetaData dmd = (EmbedDatabaseMetaData)connHolder
          .getConnection().getMetaData();
      final boolean isODBC = args.getDriverType() == gfxdConstants.DRIVER_ODBC;
      rs = isODBC ? dmd.getIndexInfoForODBC(null, args.getSchema(),
          args.getTable(), unique, approximate) : dmd.getIndexInfo(null,
          args.getSchema(), args.getTable(), unique, approximate);
      return getRowSet(null, null, rs, INVALID_ID, args.connId, null, 0, false,
          false, 0, connHolder, true, "getIndexInfo");
    } catch (Throwable t) {
      cleanupResultSet(rs);
      throw gfxdException(t);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public RowSet getUDTs(ServiceMetaDataArgs args, List<GFXDType> types)
      throws GFXDException {
    ResultSet rs = null;
    try {
      final ConnectionHolder connHolder = getValidConnection(args.connId,
          args.token);
      EmbedDatabaseMetaData dmd = (EmbedDatabaseMetaData)connHolder
          .getConnection().getMetaData();
      int[] sqlTypes = null;
      if (types != null && !types.isEmpty()) {
        sqlTypes = new int[types.size()];
        for (int index = 0; index < types.size(); index++) {
          sqlTypes[index] = Converters.getJdbcType(types.get(index));
        }
      }
      rs = dmd.getUDTs(null, args.getSchema(), args.getTypeName(), sqlTypes);
      return getRowSet(null, null, rs, INVALID_ID, args.connId, null, 0, false,
          false, 0, connHolder, true, "getUDTs");
    } catch (Throwable t) {
      cleanupResultSet(rs);
      throw gfxdException(t);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public RowSet getBestRowIdentifier(ServiceMetaDataArgs args, int scope,
      boolean nullable) throws GFXDException {
    ResultSet rs = null;
    try {
      final ConnectionHolder connHolder = getValidConnection(args.connId,
          args.token);
      EmbedDatabaseMetaData dmd = (EmbedDatabaseMetaData)connHolder
          .getConnection().getMetaData();
      final boolean isODBC = args.getDriverType() == gfxdConstants.DRIVER_ODBC;
      rs = isODBC ? dmd.getBestRowIdentifierForODBC(null, args.getSchema(),
          args.getTable(), scope, nullable) : dmd.getBestRowIdentifier(null,
          args.getSchema(), args.getTable(), scope, nullable);
      return getRowSet(null, null, rs, INVALID_ID, args.connId, null, 0, false,
          false, 0, connHolder, true, "getBestRowIdentifier");
    } catch (Throwable t) {
      cleanupResultSet(rs);
      throw gfxdException(t);
    }
  }
}
