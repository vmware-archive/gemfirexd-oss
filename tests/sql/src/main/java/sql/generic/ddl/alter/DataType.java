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
package sql.generic.ddl.alter;

import sql.generic.SQLOldTest;

public enum DataType {
  BIGINT(java.sql.Types.BIGINT), 
  BLOB(java.sql.Types.BLOB), 
  BOOLEAN(java.sql.Types.BOOLEAN), 
  CHAR(java.sql.Types.CHAR), 
  CLOB(java.sql.Types.CLOB), 
  DATE(java.sql.Types.DATE), 
  DECIMAL(java.sql.Types.DECIMAL), 
  DOUBLE(java.sql.Types.DOUBLE), 
  FLOAT(java.sql.Types.FLOAT), 
  INTEGER(java.sql.Types.INTEGER), 
  LONGNVARCHAR(java.sql.Types.LONGNVARCHAR), 
  LONGVARCHAR(java.sql.Types.LONGVARCHAR), 
  NUMERIC(java.sql.Types.NUMERIC), 
  REAL(java.sql.Types.REAL),
  SMALLINT(java.sql.Types.SMALLINT), 
  TIME(java.sql.Types.TIME), 
  TIMESTAMP(java.sql.Types.TIMESTAMP), 
  VARCHAR(java.sql.Types.VARCHAR);

  int type;

  DataType(int type) {
    this.type = type;
  }

  public int getDataType() {
    return type;
  }

  public static DataType getRandomDataType() {
    int randomType = SQLOldTest.random.nextInt(DataType.values().length);
    return DataType.values()[randomType];
  }
}
