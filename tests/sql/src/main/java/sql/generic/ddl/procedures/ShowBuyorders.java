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

import hydra.Log;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import sql.generic.SqlUtilityHelper;

public class ShowBuyorders extends DAProcedureTest {

  static int oid = 397;

  @Override
  protected String getDerbyDdlStmt() {
    // TODO Auto-generated method stub
    return "create procedure trade.showDerbyBuyorders(IN DP1 Integer, "
        + "IN DP2 Integer) "
        + "PARAMETER STYLE JAVA "
        + "LANGUAGE JAVA "
        + "READS SQL DATA "
        + "DYNAMIC RESULT SETS 1 "
        + "EXTERNAL NAME 'sql.generic.ddl.procedures.DerbyDAPTest.selectDerbyBuyordersByTidList'";
  }

  @Override
  protected boolean procCalledWithSG() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  protected CallableStatement getCallableStatement(Connection conn)
      throws SQLException {
    String sql = "call " + getProcName() + "(?, ?)"
        + " ON Table trade.buyorders where oid < " + oid + " and tid= "
        + SqlUtilityHelper.tid();
    Log.getLogWriter().info(
        "call gfxd procedure " + sql + ", myTid is " + SqlUtilityHelper.tid());
    CallableStatement cs = conn.prepareCall(sql);
    cs.setInt(1, 154);
    cs.setInt(2, SqlUtilityHelper.tid());
    return cs;

  }

  @Override
  protected String getDdlStmt() {
    // TODO Auto-generated method stub
    return "create procedure trade.showBuyorders(IN DP1 Integer, "
        + "IN DP2 Integer) "
        + "PARAMETER STYLE JAVA "
        + "LANGUAGE JAVA "
        + "READS SQL DATA "
        + "DYNAMIC RESULT SETS 1 "
        + "EXTERNAL NAME 'sql.generic.ddl.procedures.DAPTest.selectGfxdBuyordersByTidList'";
  }

  @Override
  protected CallableStatement getDerbyCallableStatement(Connection conn)
      throws SQLException {

    String sql = "{call " + getDerbyProcName() + "(?, ?)}";
    Log.getLogWriter().info(
        "call Derby " + sql + ", myTid is " + SqlUtilityHelper.tid());
    CallableStatement cs = conn.prepareCall(sql);
    cs.setInt(1, 154);
    cs.setInt(2, SqlUtilityHelper.tid());
    return cs;
  }

  @Override
  protected String getDerbyProcName() {
    // TODO Auto-generated method stub
    return "trade.showDerbyBuyorders";
  }

  @Override
  protected int getOutputResultSetCount() {
    // TODO Auto-generated method stub
    return 1;
  }

  @Override
  protected ResultSet[] getOutputValues(CallableStatement cs, Object[] inOut)
      throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  protected String getProcName() {
    // TODO Auto-generated method stub
    return "trade.showBuyorders";
  }

  @Override
  protected void verifyInOutParameters(Object[] derby, Object[] gemxd) {
    // TODO Auto-generated method stub

  }

  @Override
  protected void populateCalledFunctionInfoInBB() {
    // TODO Auto-generated method stub

  }

}
