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

import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;

import sql.generic.SqlUtilityHelper;
import sql.generic.ddl.ColumnInfo;
import sql.generic.ddl.TableInfo;
import util.TestException;

public class TestInOutParamsProcedure extends ProcedureTest {

  @Override
  protected CallableStatement getCallableStatement(Connection conn)
      throws SQLException {
    // TODO Auto-generated method stub
    CallableStatement cs = conn
        .prepareCall("{call " + procName + " (?, ?, ?)}");
    cs.registerOutParameter(2, Types.DECIMAL);
    cs.registerOutParameter(3, Types.INTEGER);
    cs.setInt(1, SqlUtilityHelper.tid());
    cs.setInt(3, SqlUtilityHelper.tid());
    return cs;
  }

  @Override
  protected String getDdlStmt() {
    // TODO Auto-generated method stub
    return "create procedure trade.testInOutParam(DP1 Integer, "
        + "OUT DP2 DECIMAL (30, 20), INOUT DP3 Integer) "
        + "PARAMETER STYLE JAVA "
        + "LANGUAGE JAVA "
        + "READS SQL DATA "
        + "DYNAMIC RESULT SETS 2 "
        + "EXTERNAL NAME 'sql.generic.ddl.procedures.ProcedureBody.testInOutParam'";
  }

  @Override
  protected int getOutputResultSetCount() {
    // TODO Auto-generated method stub
    return 2;
  }

  @Override
  protected ResultSet[] getOutputValues(CallableStatement cs, Object[] inOut)
      throws SQLException {
    // TODO Auto-generated method stub
    
    ResultSet[] rs = getOutputResultset(cs, getOutputResultSetCount());
    inOut[0] = cs.getBigDecimal(2);
    inOut[1] = new Integer(cs.getInt(3));
    return rs;
  }

  @Override
  protected String getProcName() {
    // TODO Auto-generated method stub
    return "trade.testInOutParam";
  }

  @Override
  protected void verifyInOutParameters(Object[] inOut, Object[] gfxdInOut) {

    if (gfxdInOut[0] == null && inOut[0] == null) {
      Log.getLogWriter().info(
          "No max cash found, possible no data "
              + "inserted by this thread or data deleted");
    }
    else if (gfxdInOut[0] != null && inOut[0] != null) {
      if (!gfxdInOut[0].getClass().equals(BigDecimal.class)) {
        throw new TestException("supposed to get BigDecimal but got "
            + gfxdInOut[0].getClass());
      }
      else if (((BigDecimal)inOut[0]).subtract(((BigDecimal)gfxdInOut[0]))
          .longValue() != 0) {
        throw new TestException("Got different out parameter value, derby is "
            + ((BigDecimal)inOut[0]).longValue() + " gfxd is "
            + ((BigDecimal)gfxdInOut[0]).longValue());
      }
      else {
        Log.getLogWriter().info(
            "maxCash is " + ((BigDecimal)inOut[0]).longValue());
      }
    }
    else {
      throw new TestException("Only one outparam has data, derby maxCash is "
          + inOut[0] + "gemfirexd maxCash is " + gfxdInOut[0]);
    }

    if (!gfxdInOut[1].getClass().equals(Integer.class)) {
      throw new TestException("supposed to get Integer but got "
          + gfxdInOut[1].getClass());
    }

    if (((Integer)inOut[1]).intValue() != ((Integer)gfxdInOut[1]).intValue()) {
      throw new TestException("Got different out parameter value, derby is "
          + ((Integer)inOut[1]).intValue() + " gfxd is "
          + ((Integer)gfxdInOut[1]).intValue());
    }

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
