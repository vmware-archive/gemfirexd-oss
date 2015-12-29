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
package sql.poc.useCase2.listAggNewImpl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;

/**
 * Meta-data for a column returned in result. A list of {@link ColumnDef}s is
 * sent once by each node to the processor to enable regrouping and reordering.
 */
public final class ColumnDef implements DataSerializable {

  private static final long serialVersionUID = 7402578193767651032L;

  static final int ASC = 1;
  static final int DESC = 2;
  static final int NONE = 0;

  static final int GROUPBY = 0;
  static final int LISTAGG = 1;
  static final int GROUPBY_AND_LISTAGG = 2;

  String columnName;
  String delimiter = null;
  int sortOrder = NONE;
  int colNumber = -1;
  int aggType;

  public ColumnDef() {
  }

  public ColumnDef(String colName, int c) {
    int splitIndex = colName.lastIndexOf(' ');
    if (splitIndex > 0) {
      columnName = colName.substring(0, splitIndex).trim();
      String orderDef = colName.substring(splitIndex + 1);
      if ("DESC".equalsIgnoreCase(orderDef)) {
        sortOrder = ColumnDef.DESC;
      }
      else {
        sortOrder = ColumnDef.ASC;
      }
    }
    else {
      columnName = colName;
      sortOrder = ASC;
    }

    aggType = GROUPBY;
    delimiter = null;
    colNumber = c;
  }

  public ColumnDef(String colName, int c, String d) {
    int splitIndex = colName.lastIndexOf(' ');
    if (splitIndex > 0) {
      columnName = colName.substring(0, splitIndex).trim();
      String orderDef = colName.substring(splitIndex + 1);
      if ("DESC".equalsIgnoreCase(orderDef)) {
        sortOrder = ColumnDef.DESC;
      }
      else if ("ASC".equalsIgnoreCase(orderDef)) {
        sortOrder = ColumnDef.ASC;
      }
      else {
        sortOrder = NONE;
      }
    }
    else {
      columnName = colName;
      sortOrder = NONE;
    }

    aggType = LISTAGG;
    delimiter = d;
    colNumber = c;
  }

  @Override
  public String toString() {
    return "ColumnDef: columnName=" + columnName + " number=" + colNumber
        + " aggregate=" + aggType + " delim=" + delimiter + " sortOrder="
        + sortOrder;
  }

  public String getColumnName() {
    return columnName;
  }

  public boolean isListAggCol() {
    return (aggType != GROUPBY);
  }

  public boolean isGroupByCol() {
    return (aggType != LISTAGG);
  }

  public void setListAggCol() {
    if (aggType == GROUPBY) {
      aggType = GROUPBY_AND_LISTAGG;
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result
        + ((columnName == null) ? 0 : columnName.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    ColumnDef other = (ColumnDef)obj;
    if (columnName == null) {
      if (other.columnName != null) return false;
    }
    else if (!columnName.equals(other.columnName)) return false;
    return true;
  }

  public boolean getOrderBy(StringBuilder result) {
    if (sortOrder != NONE) {
      result.append(columnName);
      if (sortOrder == DESC) {
        result.append(" DESC");
      }
      else {
        result.append(" ASC");
      }
      return true;
    }
    else {
      return false;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(columnName, out);
    DataSerializer.writeString(delimiter, out);
    out.writeByte(sortOrder);
    out.writeByte(aggType);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void fromData(final DataInput in) throws IOException,
      ClassNotFoundException {
    this.columnName = DataSerializer.readString(in);
    this.delimiter = DataSerializer.readString(in);
    this.sortOrder = in.readByte();
    this.aggType = in.readByte();
  }
}
