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
package sql.generic.dmlstatements;

import hydra.Log;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.Time;
import java.sql.Timestamp;

import util.TestException;
import util.TestHelper;

/**
 * PrepareStatementSetter
 * 
 * @author Namrata Thanvi
 */

public class PrepareStatementSetter {
 
  protected PreparedStatement stmt;
  int columnIndex;
  
  PrepareStatementSetter(PreparedStatement stmt){
    this.stmt = stmt;
    columnIndex=1;
  }
  
  PrepareStatementSetter(PreparedStatement stmt, int columnIndex){
    this.stmt = stmt;
    this.columnIndex=columnIndex;
  }
  
  public void setValues(Object value, int columnType)  {
try {
      switch (columnType) {
      case java.sql.Types.ARRAY:
              stmt.setArray(columnIndex++, (Array) value);
              break;
      case java.sql.Types.BIGINT:              
              stmt.setLong(columnIndex++, (Long) value);
              break;
      case java.sql.Types.BINARY:
              // todo
              break;
      case java.sql.Types.BIT:
              stmt.setBoolean(columnIndex++, (Boolean) value);
              break;
      case java.sql.Types.BLOB:
              stmt.setBlob(columnIndex++, (Blob) value);
              break;
      case java.sql.Types.BOOLEAN:
              stmt.setBoolean(columnIndex++, (Boolean) value);
              break;
      case java.sql.Types.CHAR:
              stmt.setString(columnIndex++, (String) value);
              break;
      case java.sql.Types.CLOB:
              stmt.setClob(columnIndex++, (Clob) value);
              break;
      case java.sql.Types.DATE:
              stmt.setDate(columnIndex++, (Date) value);
              break;
      case java.sql.Types.DECIMAL:
              stmt.setBigDecimal(columnIndex++, (BigDecimal) value);
              break;
      case java.sql.Types.DOUBLE:
              stmt.setDouble(columnIndex++, (Double) value);
              break;
      case java.sql.Types.FLOAT:
              stmt.setFloat(columnIndex++, (Float) value);
              break;
      case java.sql.Types.INTEGER:
              stmt.setInt(columnIndex++, (Integer) value);
              break;
      case java.sql.Types.LONGNVARCHAR:
              stmt.setString(columnIndex++, (String) value);
              break;
      case java.sql.Types.LONGVARBINARY:
              stmt.setBytes(columnIndex++, (byte[]) value);
              break;
      case java.sql.Types.LONGVARCHAR:
              stmt.setString(columnIndex++, (String) value);
              break;
      case java.sql.Types.NUMERIC:
              stmt.setBigDecimal(columnIndex++, (BigDecimal) value);
              break;
      case java.sql.Types.NVARCHAR:
      case java.sql.Types.REAL:
              stmt.setFloat(columnIndex++, (Float) value);
              break;
      case java.sql.Types.SMALLINT:
              stmt.setShort(columnIndex++, (Short) value);
              break;
      case java.sql.Types.TIME:
              stmt.setTime(columnIndex++, (Time) value);
              break;
      case java.sql.Types.TIMESTAMP:
              stmt.setTimestamp(columnIndex++, (Timestamp) value);
              break;
      case java.sql.Types.VARBINARY:
              stmt.setBytes(columnIndex++, (byte[]) value);
              break;
      case java.sql.Types.VARCHAR:
              stmt.setString(columnIndex++, (String) value);
              break;
      // need to add support for UDT
      default:
              Log.getLogWriter().info(
                              "This dataType is yet not supported : " + columnType);
      }
} catch (Exception e) {
      throw new TestException(
                      "Error occurred while trying to set data type to Statement, Data Type : "
                                      + columnType + " at columnIndex : " + columnIndex + " " + e.getMessage() + TestHelper.getStackTrace(e)
                                      );
}
}
  
  
  public PreparedStatement getStatement(){
    return stmt;
  }
}
