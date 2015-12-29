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
import hydra.RemoteTestModule;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import com.gemstone.gemfire.cache.query.Struct;
import com.pivotal.gemfirexd.procedure.OutgoingResultSet;
import com.pivotal.gemfirexd.procedure.ProcedureExecutionContext;

import sql.sqlutil.ResultSetHelper;
import sql.SQLBB;
import sql.SQLHelper;
import sql.SQLTest;
import util.TestException;
import util.TestHelper;

public class DAPTest {
  static final String LOCAL = "<local> ";

  private static String selectBuyOrderByTidListSql = "select * "
      + "from trade.buyorders where oid <? and tid= ?";

  public static void selectGfxdBuyordersByTidList(int oid, int tid,
      ResultSet[] rs, ProcedureExecutionContext context) throws SQLException {
    // public static void selectGfxdBuyordersByTidList(int oid, int tid,
    // ResultSet[] rs,
    // ProcedureExecutionContext c) throws SQLException {
    // context = c;
    // to reproduce
    Connection conn = context.getConnection();

    if (context.getTableName() == null) {
      throw new TestException("ProcedureExecutionContext.getTableName() is : "
          + context.getTableName());
    }
    if (!selectBuyOrderByTidListSql.contains(context.getTableName())) {
      throw new TestException(
          "ProcedureExecutionContext.getTableName() is incorrect: "
              + context.getTableName());
    } // #42836 is fixed
    if (!SQLTest.isHATest && context.isPossibleDuplicate()) {
      throw new TestException(
          "possible duplicate is true and this is not a HA test.");
    }

    /*
    boolean isPortfolioPartitioned = ((ArrayList<String> )SQLBB.getBB().getSharedMap().
        get("buyordersPartition")).size() !=0;
    if (context.isPartitioned(context.getTableName()) != isPortfolioPartitioned) {
      throw new TestException("context reports that the table partition is " + 
          context.isPartitioned(context.getTableName()) + ", but it is " + isPortfolioPartitioned);
    }  //work around #43039select cid, sid, subTotal "
    */
    // test only with invocation with where clause so the <local> is optional
    // here
    // String withLocal = (oid/2==0)? LOCAL : "" ;
    String withLocal = LOCAL;
    String sql = withLocal + selectBuyOrderByTidListSql;
    PreparedStatement ps1 = conn.prepareStatement(sql);
    ps1.setInt(1, oid);
    ps1.setInt(2, tid);

    rs[0] = ps1.executeQuery();

    // conn.close();
  }

}
