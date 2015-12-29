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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import sql.generic.SqlUtilityHelper;
import sql.generic.ddl.ColumnInfo;
import sql.generic.ddl.TableInfo;

public class ShowCustomerProcedure extends ProcedureTest {

  public ShowCustomerProcedure() {
    super();
  }

  @Override
  protected String getDdlStmt() {
    return "create procedure trade.show_customers (DP1 Integer) "
        + "PARAMETER STYLE JAVA "
        + "LANGUAGE JAVA "
        + "READS SQL DATA "
        + "DYNAMIC RESULT SETS 1 "
        + "EXTERNAL NAME 'sql.generic.ddl.procedures.ProcedureBody.selectCustomers'";

    // create procedure for eachWanSite

  }

  @Override
  protected String getProcName() {
    // TODO Auto-generated method stub
    return "trade.show_customers";
  }

  @Override
  protected CallableStatement getCallableStatement(Connection conn)
      throws SQLException {
    CallableStatement cs = conn.prepareCall(" call " + procName + " (?) ");
    cs.setInt(1, SqlUtilityHelper.tid());
    return cs;
  }

  @Override
  protected ResultSet[] getOutputValues(CallableStatement cs, Object[] inOut)
      throws SQLException {
    ResultSet[] rs = getOutputResultset(cs, getOutputResultSetCount());
    return rs;
    // TODO Auto-generated method stub
  }

  @Override
  protected int getOutputResultSetCount() {
    // TODO Auto-generated method stub
    return 1;
  }

  @Override
  protected void verifyInOutParameters(Object[] derby, Object[] gemxd) {
    // TODO Auto-generated method stub
    // not required

  }

  @Override
  protected HashMap<TableInfo, ColumnInfo> getModifiedColumns() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  protected void populateCalledFunctionInfoInBB() {
    // TODO Auto-generated method stub

  }

}
