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
package sql.generic.ddl.procedures;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import sql.SQLHelper;

public class DerbyDAPTest {
  private static String selectBuyOrderByTidListSql = "select * "
      + "from trade.buyorders where oid <? and tid= ?";

  protected static Connection getDerbyDefaultConnection() {
    Connection conn = null;
    try {
      conn = DriverManager.getConnection("jdbc:default:connection");
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
    return conn;
  }

  public static void selectDerbyBuyordersByTidList(int oid, int tid,
      ResultSet[] rs) throws SQLException {
    Connection conn = getDerbyDefaultConnection();
    PreparedStatement ps1 = conn.prepareStatement(selectBuyOrderByTidListSql);
    ps1.setInt(1, oid);
    ps1.setInt(2, tid);

    rs[0] = ps1.executeQuery();

    conn.close();
  }

}
