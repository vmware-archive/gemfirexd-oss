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
package tests;

import java.sql.*;


import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserver;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverAdapter;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;

public class TestProcedures {
  public static void multipleResultSets(ResultSet[] rs1, ResultSet[] rs2)
      throws Exception {
    Connection c = DriverManager.getConnection("jdbc:default:connection");

    rs1[0] = c.createStatement().executeQuery("select * from employee_table");

    rs2[0] = c.createStatement().executeQuery("select * from employee_table");
  }

  public static void inoutParamProc(String[] empname, int[] counts)
      throws Exception {
    empname[0] = empname[0] + "-Modified";
    counts[0] = 10;
  }

  public static void MoreResult_ProcedureInsertMethod(ResultSet[] rs1,
      ResultSet[] rs2) throws Exception {
    Connection c = DriverManager.getConnection("jdbc:default:connection");

    rs1[0] = c.createStatement().executeQuery(
        "select * from MoreResultInsert_table order by empid desc");

    c.createStatement()
        .executeUpdate(
            "insert into MoreResultInsert_table values(5555,'P1','3.5',12,'2009-12-12');");

    rs2[0] = c.createStatement().executeQuery(
        "select count(*) from MoreResultInsert_table where empname='P1'");
  }

  public static void MoreResult_ProcedureSelectMethod(ResultSet[] rs1,
      ResultSet[] rs2) throws Exception {
    Connection c = DriverManager.getConnection("jdbc:default:connection");

    rs1[0] = c.createStatement().executeQuery(
        "select * from MoreResultSelect_table order by empid desc");

    rs2[0] = c.createStatement().executeQuery(
        "select * from MoreResultSelect_table order by empid desc");
  }

  public static void MoreResult_ProcedureUpdateMethod(ResultSet[] rs1,
      ResultSet[] rs2) throws Exception {
    Connection c = DriverManager.getConnection("jdbc:default:connection");

    rs1[0] = c.createStatement().executeQuery(
        "select * from MoreResultUpdate_table order by empid desc");

    c.createStatement().executeUpdate(
        "update MoreResultUpdate_table set empname='X1' where empid=5555");

    rs2[0] = c.createStatement().executeQuery(
        "select count(*) from MoreResultUpdate_table where empid=5555");
  }

  public static void addQueryObserver(int sleepTime) throws Exception {
    GemFireXDQueryObserverHolder.setInstance(new ODBCQueryObserver(sleepTime));
  }

  public static void removeQueryObserver() throws Exception {
    GemFireXDQueryObserverHolder.removeObserver(ODBCQueryObserver.class);
  }

  /*
   * public static void inoutParamProc(ResultSet [] rs1,ResultSet [] rs2) throws
   * Exception { Connection c =
   * DriverManager.getConnection("jdbc:default:connection");
   * 
   * rs1[0] = c.createStatement().executeQuery("select * from employee_table");
   * 
   * rs2[0] = c.createStatement().executeQuery("select * from employee_table");
   * }
   */

  /*
   * public static void singleResultSet(ResultSet [] rs1,ResultSet [] rs2)
   * throws Exception { Connection c =
   * DriverManager.getConnection("jdbc:default:connection");
   * 
   * rs1[0] = c.createStatement().executeQuery("select * from employee_table");
   * 
   * rs2[0] = c.createStatement().executeQuery("select * from employee_table");
   * }
   */

  /*
   * public static void noResultSet(ResultSet [] rs1,ResultSet [] rs2) throws
   * Exception { Connection c =
   * DriverManager.getConnection("jdbc:default:connection");
   * 
   * rs1[0] = c.createStatement().executeQuery("select * from employee_table");
   * 
   * rs2[0] = c.createStatement().executeQuery("select * from employee_table");
   * }
   */
}
