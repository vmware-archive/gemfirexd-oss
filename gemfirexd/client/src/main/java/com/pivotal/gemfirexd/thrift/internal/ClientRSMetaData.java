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

package com.pivotal.gemfirexd.thrift.internal;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

import com.pivotal.gemfirexd.internal.iapi.reference.JDBC30Translation;
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState;
import com.pivotal.gemfirexd.thrift.ColumnDescriptor;
import com.pivotal.gemfirexd.thrift.GFXDType;
import com.pivotal.gemfirexd.thrift.gfxdConstants;
import com.pivotal.gemfirexd.thrift.common.Converters;
import com.pivotal.gemfirexd.thrift.common.ThriftExceptionUtil;

/**
 * @author swale
 * @since gfxd 1.1
 */
public final class ClientRSMetaData implements ResultSetMetaData {

  private final List<ColumnDescriptor> descriptors;

  ClientRSMetaData(List<ColumnDescriptor> metadata) {
    this.descriptors = metadata;
  }

  /** check if 1-based index of column is valid one */
  final void checkForValidColumn(int column) throws SQLException {
    final int numColumns = this.descriptors.size();
    if (column < 1 || column > numColumns) {
      ThriftExceptionUtil.newSQLException(
          SQLState.LANG_INVALID_COLUMN_POSITION, null, Integer.valueOf(column),
          Integer.valueOf(numColumns));
    }
  }

  final ColumnDescriptor getDescriptor(int column) throws SQLException {
    checkForValidColumn(column);
    return this.descriptors.get(column - 1);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getColumnCount() throws SQLException {
    return this.descriptors.size();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isAutoIncrement(int column) throws SQLException {
    ColumnDescriptor desc = getDescriptor(column);
    if (desc.isSetDescFlags()) {
      short flags = desc.getDescFlags();
      return ((flags & gfxdConstants.COLUMN_AUTOINC) != 0);
    }
    else {
      return false;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isCaseSensitive(int column) throws SQLException {
    switch (getDescriptor(column).getType()) {
      case CHAR:
      case VARCHAR:
      case LONGVARCHAR:
      case CLOB:
      case SQLXML:
      case NCHAR:
      case NVARCHAR:
      case LONGNVARCHAR:
      case NCLOB:
        return true;
      default:
        return false;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isSearchable(int column) throws SQLException {
    checkForValidColumn(column);
    // we have no restrictions yet, so this is always true
    return true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isCurrency(int column) throws SQLException {
    return getDescriptor(column).getType() == GFXDType.DECIMAL;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int isNullable(int column) throws SQLException {
    ColumnDescriptor desc = getDescriptor(column);
    if (desc.isSetDescFlags()) {
      short flags = desc.getDescFlags();
      if ((flags & gfxdConstants.COLUMN_NULLABLE) != 0) {
        return columnNullable;
      }
      else if ((flags & gfxdConstants.COLUMN_NONULLS) != 0) {
        return columnNoNulls;
      }
    }
    return columnNullableUnknown;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isSigned(int column) throws SQLException {
    switch (getDescriptor(column).getType()) {
      case INTEGER:
      case FLOAT:
      case DECIMAL:
      case SMALLINT:
      case BIGINT:
      case TINYINT:
      case REAL:
      case DOUBLE:
        return true;
      default:
        return false;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getColumnDisplaySize(int column) throws SQLException {
    ColumnDescriptor desc = getDescriptor(column);
    int sz;
    switch (desc.getType()) {
      case TIMESTAMP:
        return 26;
      case DATE:
        return 10;
      case TIME:
        return 8;
      case INTEGER:
        return 11;
      case SMALLINT:
        return 6;
      case REAL:
      case FLOAT:
        return 13;
      case DOUBLE:
        return 22;
      case TINYINT:
        return 15;

      case BIGINT:
        return 20;
      case BOOLEAN:
        // 5 chars for 'false'
        return 5;
      case BINARY:
      case VARBINARY:
      case LONGVARBINARY:
      case BLOB:
        sz = (2 * desc.getPrecision());
        return (sz > 0 ? sz
            : (2 * JDBC30Translation.DEFAULT_COLUMN_DISPLAY_SIZE));
      default:
        // MaximumWidth is -1 when it is unknown.
        sz = desc.getPrecision();
        return (sz > 0 ? sz : JDBC30Translation.DEFAULT_COLUMN_DISPLAY_SIZE);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getColumnLabel(int column) throws SQLException {
    String name = getDescriptor(column).getName();
    return (name != null ? name : ("Column" + Integer.toString(column)));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getColumnName(int column) throws SQLException {
    String name = getDescriptor(column).getName();
    return (name != null ? name : "");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getSchemaName(int column) throws SQLException {
    String tableName = getDescriptor(column).getFullTableName();
    if (tableName != null) {
      int dotIndex = tableName.indexOf('.');
      if (dotIndex > 0) {
        return tableName.substring(0, dotIndex);
      }
      else {
        return "";
      }
    }
    else {
      return "";
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getPrecision(int column) throws SQLException {
    return getDescriptor(column).getPrecision();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getScale(int column) throws SQLException {
    ColumnDescriptor desc = getDescriptor(column);
    if (desc.isSetScale()) {
      return desc.getScale();
    }
    else {
      switch (desc.getType()) {
        case BOOLEAN:
        case TINYINT:
        case SMALLINT:
        case INTEGER:
        case BIGINT:
        case FLOAT:
        case REAL:
        case DOUBLE:
        case DATE:
        case TIME:
          return 0;
        case TIMESTAMP:
          return 6;
        default:
          return gfxdConstants.COLUMN_SCALE_UNKNOWN;
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getTableName(int column) throws SQLException {
    String tableName = getDescriptor(column).getFullTableName();
    if (tableName != null) {
      int dotIndex = tableName.indexOf('.');
      if (dotIndex >= 0) {
        return tableName.substring(dotIndex + 1);
      }
      else {
        return tableName;
      }
    }
    else {
      return "";
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getCatalogName(int column) throws SQLException {
    checkForValidColumn(column);
    return "";
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getColumnType(int column) throws SQLException {
    return Converters.getJdbcType(getDescriptor(column).type);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getColumnTypeName(int column) throws SQLException {
    ColumnDescriptor desc = getDescriptor(column);
    String typeAndClassName = desc.getUdtTypeAndClassName();
    if (typeAndClassName != null) {
      int colonIndex = typeAndClassName.indexOf(':');
      if (colonIndex >= 0) {
        return typeAndClassName.substring(0, colonIndex);
      }
      else {
        return typeAndClassName;
      }
    }
    else {
      switch (desc.getType()) {
        case TINYINT:
          return "TINYINT";
        case SMALLINT:
          return "SMALLINT";
        case INTEGER:
          return "INTEGER";
        case BIGINT:
          return "BIGINT";
        case FLOAT:
          return "FLOAT";
        case REAL:
          return "REAL";
        case DOUBLE:
          return "DOUBLE";
        case DECIMAL:
          return "DECIMAL";
        case CHAR:
          return "CHAR";
        case VARCHAR:
          return "VARCHAR";
        case LONGVARCHAR:
          return "LONG VARCHAR";
        case DATE:
          return "DATE";
        case TIME:
          return "TIME";
        case TIMESTAMP:
          return "TIMESTAMP";
        case BINARY:
          return "CHAR FOR BIT DATA";
        case VARBINARY:
          return "VARCHAR FOR BIT DATA";
        case LONGVARBINARY:
          return "LONG VARCHAR FOR BIT DATA";
        case JAVA_OBJECT:
          return "JAVA";
        case BLOB:
          return "BLOB";
        case CLOB:
          return "CLOB";
        case BOOLEAN:
          return "BOOLEAN";
        case SQLXML:
          return "XML";
        case PDX_OBJECT:
          return "PDX";
        case JSON_OBJECT:
          return "JSON";
        default:
          return "UNKNOWN";
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isReadOnly(int column) throws SQLException {
    ColumnDescriptor desc = getDescriptor(column);
    if (desc.isSetDescFlags()) {
      short flags = getDescriptor(column).getDescFlags();
      return ((flags & gfxdConstants.COLUMN_UPDATABLE) == 0)
          && ((flags & gfxdConstants.COLUMN_DEFINITELY_UPDATABLE) == 0);
    }
    else {
      return true;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isWritable(int column) throws SQLException {
    ColumnDescriptor desc = getDescriptor(column);
    if (desc.isSetDescFlags()) {
      short flags = getDescriptor(column).getDescFlags();
      return ((flags & gfxdConstants.COLUMN_UPDATABLE) != 0)
          || ((flags & gfxdConstants.COLUMN_DEFINITELY_UPDATABLE) != 0);
    }
    else {
      return false;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isDefinitelyWritable(int column) throws SQLException {
    ColumnDescriptor desc = getDescriptor(column);
    if (desc.isSetDescFlags()) {
      short flags = getDescriptor(column).getDescFlags();
      return ((flags & gfxdConstants.COLUMN_DEFINITELY_UPDATABLE) != 0);
    }
    else {
      return false;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getColumnClassName(int column) throws SQLException {
    ColumnDescriptor desc = getDescriptor(column);
    String typeAndClassName = desc.getUdtTypeAndClassName();
    if (typeAndClassName != null) {
      int colonIndex = typeAndClassName.indexOf(':');
      if (colonIndex >= 0) {
        return typeAndClassName.substring(colonIndex + 1);
      }
    }
    switch (desc.getType()) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
        return "java.lang.Integer";
      case BIGINT:
        return "java.lang.Long";
      case FLOAT:
      case REAL:
        return "java.lang.Float";
      case DOUBLE:
        return "java.lang.Double";
      case DECIMAL:
        return "java.math.BigDecimal";
      case CHAR:
      case VARCHAR:
      case LONGVARCHAR:
      case NCHAR:
      case NVARCHAR:
      case LONGNVARCHAR:
        return "java.lang.String";
      case BOOLEAN:
        return "java.lang.Boolean";
      case DATE:
        return "java.sql.Date";
      case TIME:
        return "java.sql.Time";
      case TIMESTAMP:
        return "java.sql.Timestamp";
      case BINARY:
      case VARBINARY:
      case LONGVARBINARY:
        return "byte[]";
      case JAVA_OBJECT:
        return "java.lang.Object";
      case BLOB:
        return "java.sql.Blob";
      case CLOB:
        return "java.sql.Clob";
      case NCLOB:
        return "java.sql.NClob";
      case SQLXML:
        return "java.sql.SQLXML";
      case PDX_OBJECT:
        return "com.pivotal.gemfirexd.thrift.generated.PDXObject";
      case JSON_OBJECT:
        return "com.pivotal.gemfirexd.thrift.generated.JSONObject";
      case STRUCT:
        return "java.sql.Struct";
      case ROWID:
        return "java.sql.RowId";
      case REF:
        return "java.sql.Ref";
      case ARRAY:
        return "java.sql.Array";
      default:
        return "java.lang.Object";
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    try {
      return iface.cast(this);
    } catch (ClassCastException cce) {
      throw ThriftExceptionUtil.newSQLException(SQLState.UNABLE_TO_UNWRAP,
          cce, iface);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface.isInstance(this);
  }
}
