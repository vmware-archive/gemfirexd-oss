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
package gfxdperf.tpch.gfxd;

import gfxdperf.tpch.DB;
import gfxdperf.tpch.DBException;
import gfxdperf.tpch.Query;
import gfxdperf.tpch.TPCHPrms;
import gfxdperf.tpch.TPCHPrms.TableName;
import gfxdperf.tpch.TPCHStats;
import gfxdperf.tpch.gfxd.GFXDPrms.ConnectionType;
import hydra.DistributedSystemHelper;
import hydra.Log;
import hydra.gemfirexd.FabricServerHelper;
import hydra.gemfirexd.LonerHelper;

import java.io.File;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import util.TestHelper;

import com.gemstone.gemfire.distributed.DistributedSystem;

/**
 * DB for measuring TPCH performance with GemFireXD.
 */
public class GFXDDB extends DB {

  public static final boolean logDML = TPCHPrms.logDML();
  public static final boolean logDMLResults = TPCHPrms.logDMLResults();

  protected Connection connection;
  private DistributedSystem distributedSystem;
  private TPCHStats statistics;
  protected long lastQueryPlanTime;
  protected Query q1;
  protected Query q2;
  protected Query q3;
  protected Query q4;
  protected Query q5;
  protected Query q6;
  protected Query q7;
  protected Query q8;
  protected Query q9;
  protected Query q10;
  protected Query q11;
  protected Query q12;
  protected Query q13;
  protected Query q14;
  protected Query q15;
  protected Query q16;
  protected Query q17;
  protected Query q18;
  protected Query q19;
  protected Query q20;
  protected Query q21;
  protected Query q22;

  @Override
  public void init(Random random) throws DBException {
    super.init(random);
    ConnectionType type = GFXDPrms.getConnectionType();
    if (type == ConnectionType.thin) {
      this.connection = initConnection(type);
      this.distributedSystem = LonerHelper.connect(); // for statistics
    } else {
      FabricServerHelper.startFabricServer();
      this.connection = initConnection(type);
      this.distributedSystem = DistributedSystemHelper.getDistributedSystem();
    }
    this.statistics = TPCHStats.getInstance();
  }

  private Connection initConnection(ConnectionType type) throws DBException {
    try {
      return GFXDUtil.openConnection(type);
    } catch (SQLException e) {
      String s = "Unable to create " + type + " client connection";
      throw new DBException(s, e);
    }
  }

  public Connection getConnection() {
    return this.connection;
  }

  @Override
  public void cleanup() throws DBException {
    Log.getLogWriter().info("Closing connection " + this.connection);
    if (this.connection == null) {
      Log.getLogWriter().info("Connection already closed");
    } else {
      try {
        this.connection.close();
        this.connection = null;
        Log.getLogWriter().info("Closed connection");
      } catch (SQLException e) {
        if (e.getSQLState().equalsIgnoreCase("X0Z01") && GFXDPrms.isHA()) {
          Log.getLogWriter().info("Connection already closed at server end");
        } else {
          throw new DBException("Problem closing connection", e);
        }
      }
    }
    if (this.statistics != null) {
      this.statistics.close();
      this.statistics = null;
    }
    ConnectionType type = GFXDPrms.getConnectionType();
    if (type == ConnectionType.peer) {
      FabricServerHelper.stopFabricServer();
    }
    if (this.distributedSystem != null) {
      this.distributedSystem.disconnect();
      this.distributedSystem = null;
    }
  }

//------------------------------------------------------------------------------
// DB

  @Override
  public void importTable(String fn, TableName tableName)
  throws DBException {
    String errorFile = System.getProperty("user.dir") + File.separator + tableName + ".err";
    String call = "call SYSCS_UTIL.IMPORT_TABLE_EX('APP', '" + tableName
        + "', '" + fn + "', '|', null, null, 0, 0, 6, 0, null, '" + errorFile + "')";
    Log.getLogWriter().info("Importing table from " + fn + " with: " + call);
    try {
      executeCall(call);
    } catch (SQLException e) {
      String s = "Problem executing call: " + call;
      throw new DBException(s, e);
    }
    Log.getLogWriter().info("Imported table from " + fn);
  }

  private void executeCall(String call) throws SQLException {
    CallableStatement cs = null;
    try {
      cs = this.connection.prepareCall(call);
      cs.execute();
    } finally {
      if (cs != null) {
        cs.close();
      }
    }
  }

  @Override
  public void createIndex(String index) throws DBException {
    Statement stmt = null;
    try {
      stmt = this.connection.createStatement();
      stmt.executeUpdate(index);
      stmt.close();
    } catch (SQLException e) {
      String s = "Problem creating index: " + index;
      throw new DBException(s, e);
    }
  }

  @Override
  public void validateQueries() throws DBException {
    StringBuilder errStr = new StringBuilder();
    try {
      errStr.append(getQ1().validateQuery());
    } catch (Exception e) {
      errStr.append("For Q1: " + TestHelper.getStackTrace(e) + "\n");
    }

    try {
      errStr.append(getQ2().validateQuery());
    } catch (Exception e) {
      errStr.append("For Q2: " + TestHelper.getStackTrace(e) + "\n");
    }

    try {
      errStr.append(getQ3().validateQuery());
    } catch (Exception e) {
      errStr.append("For Q3: " + TestHelper.getStackTrace(e) + "\n");
    }

    try {
      errStr.append(getQ4().validateQuery());
    } catch (Exception e) {
      errStr.append("For Q4: " + TestHelper.getStackTrace(e) + "\n");
    }

    try {
      errStr.append(getQ5().validateQuery());
    } catch (Exception e) {
      errStr.append("For Q5: " + TestHelper.getStackTrace(e) + "\n");
    }

    try {
      errStr.append(getQ6().validateQuery());
    } catch (Exception e) {
      errStr.append("For Q6: " + TestHelper.getStackTrace(e) + "\n");
    }

    try {
      errStr.append(getQ7().validateQuery());
    } catch (Exception e) {
      errStr.append("For Q7: " + TestHelper.getStackTrace(e) + "\n");
    }

    try {
      errStr.append(getQ8().validateQuery());
    } catch (Exception e) {
      errStr.append("For Q8: " + TestHelper.getStackTrace(e) + "\n");
    }

    try {
      errStr.append(getQ9().validateQuery());
    } catch (Exception e) {
      errStr.append("For Q9: " + TestHelper.getStackTrace(e) + "\n");
    }

    try {
      errStr.append(getQ10().validateQuery());
    } catch (Exception e) {
      errStr.append("For Q10: " + TestHelper.getStackTrace(e) + "\n");
    }

    try {
      errStr.append(getQ11().validateQuery());
    } catch (Exception e) {
      errStr.append("For Q11: " + TestHelper.getStackTrace(e) + "\n");
    }

    try {
      errStr.append(getQ12().validateQuery());
    } catch (Exception e) {
      errStr.append("For Q12: " + TestHelper.getStackTrace(e) + "\n");
    }

    try {
      errStr.append(getQ13().validateQuery());
    } catch (Exception e) {
      errStr.append("For Q13: " + TestHelper.getStackTrace(e) + "\n");
    }

    try {
      errStr.append(getQ14().validateQuery());
    } catch (Exception e) {
      errStr.append("For Q14: " + TestHelper.getStackTrace(e) + "\n");
    }

    try {
      errStr.append(getQ15().validateQuery());
    } catch (Exception e) {
      errStr.append("For Q15: " + TestHelper.getStackTrace(e) + "\n");
    }

    try {
      errStr.append(getQ16().validateQuery());
    } catch (Exception e) {
      errStr.append("For Q16: " + TestHelper.getStackTrace(e) + "\n");
    }

    try {
      errStr.append(getQ17().validateQuery());
    } catch (Exception e) {
      errStr.append("For Q17: " + TestHelper.getStackTrace(e) + "\n");
    }

    try {
      errStr.append(getQ18().validateQuery());
    } catch (Exception e) {
      errStr.append("For Q18: " + TestHelper.getStackTrace(e) + "\n");
    }

    try {
      errStr.append(getQ19().validateQuery());
    } catch (Exception e) {
      errStr.append("For Q19: " + TestHelper.getStackTrace(e) + "\n");
    }

    try {
      errStr.append(getQ20().validateQuery());
    } catch (Exception e) {
      errStr.append("For Q20: " + TestHelper.getStackTrace(e) + "\n");
    }

    try {
      errStr.append(getQ21().validateQuery());
    } catch (Exception e) {
      errStr.append("For Q21: " + TestHelper.getStackTrace(e) + "\n");
    }

    try {
      errStr.append(getQ22().validateQuery());
    } catch (Exception e) {
      errStr.append("For Q22: " + TestHelper.getStackTrace(e) + "\n");
    }

    if (errStr.length() > 0) {
      throw new DBException(errStr.toString());
    }
  }

  @Override
  public void executeQuery(int queryNum) throws DBException {
    long start = this.statistics.startQuery();
    int results = 0;
    try {
      switch (queryNum) {
        case 1:
          results = getQ1().executeQuery();
          break;
        case 2:
          results = getQ2().executeQuery();
          break;
        case 3:
          results = getQ3().executeQuery();
          break;
        case 4:
          results = getQ4().executeQuery();
          break;
        case 5:
          results = getQ5().executeQuery();
          break;
        case 6:
          results = getQ6().executeQuery();
          break;
        case 7:
          results = getQ7().executeQuery();
          break;
        case 8:
          results = getQ8().executeQuery();
          break;
        case 9:
          results = getQ9().executeQuery();
          break;
        case 10:
          results = getQ10().executeQuery();
          break;
        case 11:
          results = getQ11().executeQuery();
          break;
        case 12:
          results = getQ12().executeQuery();
          break;
        case 13:
          results = getQ13().executeQuery();
          break;
        case 14:
          results = getQ14().executeQuery();
          break;
        case 15:
          results = getQ15().executeQuery();
          break;
        case 16:
          results = getQ16().executeQuery();
          break;
        case 17:
          results = getQ17().executeQuery();
          break;
        case 18:
          results = getQ18().executeQuery();
          break;
        case 19:
          results = getQ19().executeQuery();
          break;
        case 20:
          results = getQ20().executeQuery();
          break;
        case 21:
          results = getQ21().executeQuery();
          break;
        case 22:
          results = getQ22().executeQuery();
          break;
        default:
          String s = "Unsupported query number: " + queryNum;
          throw new UnsupportedOperationException(s);
      }
    } catch (SQLException e) {
      String s = "Problem executing query number:" + queryNum;
      throw new DBException(s, e);
    }
    this.statistics.endQuery(queryNum, start, results);
  }
  
  @Override
  public void executeQueryPlan(int queryNum) throws DBException {
    try {
      switch (queryNum) {
        case 1:
          getQ1().executeQueryPlan();
          break;
        case 2:
          getQ2().executeQueryPlan();
          break;
        case 3:
          getQ3().executeQueryPlan();
          break;
        case 4:
          getQ4().executeQueryPlan();
          break;
        case 5:
          getQ5().executeQueryPlan();
          break;
        case 6:
          getQ6().executeQueryPlan();
          break;
        case 7:
          getQ7().executeQueryPlan();
          break;
        case 8:
          getQ8().executeQueryPlan();
          break;
        case 9:
          getQ9().executeQueryPlan();
          break;
        case 10:
          getQ10().executeQueryPlan();
          break;
        case 11:
          getQ11().executeQueryPlan();
          break;
        case 12:
          getQ12().executeQueryPlan();
          break;
        case 13:
          getQ13().executeQueryPlan();
          break;
        case 14:
          getQ14().executeQueryPlan();
          break;
        case 15:
          getQ15().executeQueryPlan();
          break;
        case 16:
          getQ16().executeQueryPlan();
          break;
        case 17:
          getQ17().executeQueryPlan();
          break;
        case 18:
          getQ18().executeQueryPlan();
          break;
        case 19:
          getQ19().executeQueryPlan();
          break;
        case 20:
          getQ20().executeQueryPlan();
          break;
        case 21:
          getQ21().executeQueryPlan();
          break;
        case 22:
          getQ22().executeQueryPlan();
          break;
        default:
          String s = "Unsupported query number: " + queryNum;
          throw new UnsupportedOperationException(s);
      }
    } catch (SQLException e) {
      String s = "Problem executing query number:" + queryNum;
      throw new DBException(s, e);
    }
  }
  
//------------------------------------------------------------------------------
// queries

  protected Query getQ1() {
    if (q1 == null) {
      q1 = new Q1(this.connection, this.rng);
    }
    return q1;
  }

  protected Query getQ2() {
    if (q2 == null) {
      q2 = new Q2(this.connection, this.rng);
    }
    return q2;
  }

  protected Query getQ3() {
    if (q3 == null) {
      q3 = new Q3(this.connection, this.rng);
    }
    return q3;
  }

  protected Query getQ4() {
    if (q4 == null) {
      q4 = new Q4(this.connection, this.rng);
    }
    return q4;
  }

  protected Query getQ5() {
    if (q5 == null) {
      q5 = new Q5(this.connection, this.rng);
    }
    return q5;
  }

  protected Query getQ6() {
    if (q6 == null) {
      q6 = new Q6(this.connection, this.rng);
    }
    return q6;
  }

  protected Query getQ7() {
    if (q7 == null) {
      q7 = new Q7(this.connection, this.rng);
    }
    return q7;
  }

  protected Query getQ8() {
    if (q8 == null) {
      q8 = new Q8(this.connection, this.rng);
    }
    return q8;
  }

  protected Query getQ9() {
    if (q9 == null) {
      q9 = new Q9(this.connection, this.rng);
    }
    return q9;
  }

  protected Query getQ10() {
    if (q10 == null) {
      q10 = new Q10(this.connection, this.rng);
    }
    return q10;
  }

  protected Query getQ11() {
    if (q11 == null) {
      q11 = new Q11(this.connection, this.rng);
    }
    return q11;
  }

  protected Query getQ12() {
    if (q12 == null) {
      q12 = new Q12(this.connection, this.rng);
    }
    return q12;
  }

  protected Query getQ13() {
    if (q13 == null) {
      q13 = new Q13(this.connection, this.rng);
    }
    return q13;
  }

  protected Query getQ14() {
    if (q14 == null) {
      q14 = new Q14(this.connection, this.rng);
    }
    return q14;
  }

  protected Query getQ15() {
    if (q15 == null) {
      q15 = new Q15(this.connection, this.rng);
    }
    return q15;
  }

  protected Query getQ16() {
    if (q16 == null) {
      q16 = new Q16(this.connection, this.rng);
    }
    return q16;
  }

  protected Query getQ17() {
    if (q17 == null) {
      q17 = new Q17(this.connection, this.rng);
    }
    return q17;
  }

  protected Query getQ18() {
    if (q18 == null) {
      q18 = new Q18(this.connection, this.rng);
    }
    return q18;
  }

  protected Query getQ19() {
    if (q19 == null) {
      q19 = new Q19(this.connection, this.rng);
    }
    return q19;
  }

  protected Query getQ20() {
    if (q20 == null) {
      q20 = new Q20(this.connection, this.rng);
    }
    return q20;
  }

  protected Query getQ21() {
    if (q21 == null) {
      q21 = new Q21(this.connection, this.rng);
    }
    return q21;
  }

  protected Query getQ22() {
    if (q22 == null) {
      q22 = new Q22(this.connection, this.rng);
    }
    return q22;
  }
}
