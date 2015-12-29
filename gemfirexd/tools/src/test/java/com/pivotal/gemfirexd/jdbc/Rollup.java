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
package com.pivotal.gemfirexd.jdbc;

import com.pivotal.gemfirexd.*;
import com.pivotal.gemfirexd.procedure.*;
import java.sql.*;
import java.util.ArrayList;

/*
 * Rollup a set of time series data. Turns 24 hourly observations into
 * a daily observation.
 *
 * Define as follows:
 * CREATE PROCEDURE ROLLUP() LANGUAGE JAVA PARAMETER STYLE JAVA MODIFIES SQL DATA DYNAMIC RESULT SETS 1 EXTERNAL NAME 'Rollup.Rollup';
 *
 * Test procedure as follows:
 * CREATE PROCEDURE ROLLUP() LANGUAGE JAVA PARAMETER STYLE JAVA MODIFIES SQL DATA DYNAMIC RESULT SETS 1 EXTERNAL NAME 'Rollup.Test';
 */

public class Rollup {
  public static void Test1(ResultSet[] outResults,
      ProcedureExecutionContext context) throws SQLException {
    // This query works.
    // String queryString =
    // "<local> SELECT count(date) from hourly having count(date) > 24";

    // This query works.
    // String queryString =
    // "<local> SELECT name, year(date), month(date), day(date), avg(value), " +
    // "max(value), min(value), count(date) FROM hourly group by name, year(date), "
    // +
    // "month(date), day(date)";

    // This one fails with this exception:
    // java.lang.ClassCastException:
    // com.pivotal.gemfirexd.internal.iapi.types.SQLTimestamp cannot be
    // cast to com.pivotal.gemfirexd.internal.iapi.types.NumberDataValue
    // String queryString =
    // "<local> SELECT name, year(date), month(date), day(date), avg(value), " +
    // "max(value), min(value), count(date) FROM hourly group by name, year(date), "
    // +
    // "month(date), day(date) having count(date) = 24";

    // Note: If I run this in gfxd without the <local> part it works.
    // Last try: Remove the <local> bit and see what happens.
    // Success this time.
    String queryString = "<local> SELECT name, year(date), month(date), day(date), avg(value), "
        + "max(value), min(value), count(date) FROM hourly group by name, year(date), "
        + "month(date), day(date) having count(date) = 24";

    // Query for this column's values.
    Connection cxn = context.getConnection();
    Statement stmt = cxn.createStatement();
    ResultSet rs = stmt.executeQuery(queryString);

    OutgoingResultSet rs1 = context.getOutgoingResultSet(1);
    rs1.addColumn("result");
    ArrayList al = new ArrayList();
    al.add(new Integer(1));
    rs1.addRow(al);

    return;
  }


  public static void Test2(ResultSet[] outResults,
      ProcedureExecutionContext context) throws SQLException {

     String queryString = "SELECT count(date) FROM " + context.getTableName();
    
    // Projections seem to be broken. This method should help decide.
    // Very strange results, extra columns are present and avg seems to be sum
    // instead.
//    String queryString = "SELECT name, year(date), month(date), day(date), avg(value), " +
//    "max(value), min(value), count(date) FROM hourly group by name, year(date), " +
//    "month(date), day(date) having count(date) = 24";

    Connection cxn = context.getConnection();
    Statement stmt = cxn.createStatement();
    ResultSet rs = stmt.executeQuery(queryString);
    outResults[0] = rs;
  }

  public static void Test3(ResultSet[] outResults,
      ProcedureExecutionContext context) throws SQLException {
    // This one fails for a different reason:
    // ERROR 42X04: Column 'C' is either not in any table in the FROM list or
    // appears within a join
    // specification and is outside the scope of the join specification or
    // appears in a HAVING clause
    // and is not in the GROUP BY list.
    // ^^^ That's not true as you can see below. This one also fails when I run
    // it in gfxd.
    // String queryString =
    // "<local> SELECT name, year(date), month(date), day(date), avg(value), " +
    // "max(value), min(value), count(date) AS c FROM hourly group by name, year(date), "
    // +
    // "month(date), day(date) having c = 24";
    String queryString = "SELECT name, year(date), month(date), day(date), avg(value), "
        + "max(value), min(value), count(date) FROM hourly"
        + " group by name, year(date), "
        + "month(date), day(date) having count(date) = 24";

    String INSERTER = "insert into daily (name, date, value, maxv, minv) values (?, ?, ?, ?, ?)";

    Connection cxn = context.getConnection();
    Statement stmt = cxn.createStatement();
    ResultSet rs = stmt.executeQuery(queryString);

    // Prepare the insert statement.
    PreparedStatement preppedStmt = cxn.prepareStatement(INSERTER);

    // Iterate over the resultset, make a batch of data to put in daily.
    boolean hasNext;
    hasNext = rs.next();
    while (hasNext) {
      preppedStmt.setString(1, rs.getString(1));
      // XXX: something's not working.
      Timestamp timestamp = new Timestamp(1000000);
      preppedStmt.setTimestamp(2, timestamp);
      preppedStmt.setFloat(3, rs.getFloat(5));
      preppedStmt.setFloat(4, rs.getFloat(6));
      preppedStmt.setFloat(5, rs.getFloat(7));
      preppedStmt.addBatch();
      hasNext = rs.next();
    }
    ;

    // Run the batch.
    preppedStmt.executeBatch();

    // Delete the data out of hourly.
    // XXX

    OutgoingResultSet rs1 = context.getOutgoingResultSet(1);
    rs1.addColumn("result");
    ArrayList al = new ArrayList();
    al.add(new Integer(1));
    rs1.addRow(al);

    return;
  }
  
  public static void queryTable(ResultSet[] outResults,
      ProcedureExecutionContext context) throws SQLException {

    String queryString = "select * from t1";

    Connection cxn = context.getConnection();
    
    Statement stmt = cxn.createStatement();
    
    ResultSet rs = stmt.executeQuery(queryString);

    outResults[0] = rs;

    return;
  }

}
