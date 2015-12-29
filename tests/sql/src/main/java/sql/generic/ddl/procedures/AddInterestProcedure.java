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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import sql.SQLBB;
import sql.SQLPrms;
import sql.generic.GenericBBHelper;
import sql.generic.SQLGenericBB;
import sql.generic.SQLGenericTest;
import sql.generic.SQLOldTest;
import sql.generic.SqlUtilityHelper;
import sql.generic.ddl.ColumnInfo;
import sql.generic.ddl.TableInfo;

public class AddInterestProcedure extends ProcedureTest {

  @Override
  protected CallableStatement getCallableStatement(Connection conn)
      throws SQLException {
    // TODO Auto-generated method stub
    CallableStatement cs = conn.prepareCall(" call " + procName + " (?) ");
    cs.setInt(1, SqlUtilityHelper.tid());
    return cs;
  }

  @Override
  protected String getDdlStmt() {
    // TODO Auto-generated method stub
    return "create procedure trade.addInterest(DP1 Integer) "
        + "PARAMETER STYLE JAVA "
        + "LANGUAGE JAVA "
        + "MODIFIES SQL DATA "
        + "EXTERNAL NAME 'sql.generic.ddl.procedures.ProcedureBody.addInterest'";
  }

  @Override
  protected int getOutputResultSetCount() {
    // TODO Auto-generated method stub
    return 0;
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
    return "trade.addInterest";
  }

  @Override
  protected void verifyInOutParameters(Object[] derby, Object[] gemxd) {
    // TODO Auto-generated method stub

  }

  @Override
  protected HashMap<TableInfo, ColumnInfo> getModifiedColumns() {
    // TODO Auto-generated method stub
    HashMap<TableInfo, ColumnInfo> columns = new HashMap<TableInfo, ColumnInfo>();
    columns.put(GenericBBHelper.getTableInfo("trade.networth"), GenericBBHelper
        .getTableInfo("trade.networth").getColumn("cash"));
    return columns;
  }

  @Override
  protected void populateCalledFunctionInfoInBB() {
    // TODO Auto-generated method stub

    Map<String, ArrayList<String>> functionProcMap = (Map<String, ArrayList<String>>)SQLGenericBB
        .getBB().getSharedMap().get(SQLOldTest.FuncProcMap);
    ArrayList<String> procList = functionProcMap.get("trade.multiply"
        .toUpperCase());
    if (procList == null) {
      procList = new ArrayList<String>();
    }

    procList.add(this.procName);

    SQLGenericBB.getBB().getSharedMap().put(SQLOldTest.FuncProcMap,
        functionProcMap);

  }

}
