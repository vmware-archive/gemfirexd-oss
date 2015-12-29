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
package sql.schemas;

import hydra.Log;

import java.sql.*;

import sql.sqlutil.SQLStmt;

public class Transactions {
  protected final boolean logDML = SchemaTest.logDML;
  
  public final PreparedStatement getPreparedStatement(Connection conn, SQLStmt stmt, Object...params) throws SQLException {
    String sql = stmt.getSQL();
    StringBuilder sb = new StringBuilder();
    sb.append(sql + " setting");
    PreparedStatement pStmt = conn.prepareStatement(sql);
    for (int i = 0; i < params.length; i++) {
      if(logDML) sb.append(" param" + i + ":" + params[i] + ",");
      pStmt.setObject(i+1, params[i]);
    } 
    if(logDML) Log.getLogWriter().info("executing " + sb.toString());
    return pStmt;
  }
  
}
