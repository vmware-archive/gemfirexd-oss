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
package sql.alterTable;

import hydra.Log;
import hydra.Prms;
import hydra.TestConfig;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Random;
import java.util.Vector;


import sql.sqlutil.ResultSetHelper;
import java.sql.ResultSet;
import sql.SQLHelper;
import sql.SQLTest;
import util.TestException;

public class AlterTableTest extends SQLTest {
  protected static AlterTableTest alterTableTest;
  protected static final Random rand = new Random();

  public static synchronized void HydraTask_initialize() {
    if (alterTableTest == null) {
      alterTableTest = new AlterTableTest();
      alterTableTest.initialize();
    }
  }

  protected void initialize() {
    // Load several parameters, such as hasDerbyServer
    super.initialize();
  }

  @SuppressWarnings("unchecked")
  public static void HydraTask_runSQLCommands() {
    if (alterTableTest == null) {
      alterTableTest = new AlterTableTest();
    }
    Vector<String> alterCmds = TestConfig.tab().vecAt(AlterTablePrms.sqlCmds);
    
    //handle unsupported exception for now
    if (alterCmds != null) {
      for (int i=0; i< alterCmds.size(); i++) {
        String sqlCmd = alterCmds.get(i);
        if (sqlCmd.contains("alter column") && !sqlCmd.contains("always")) {
          if (i==0) {
            try {
              runOneSQLCmd(alterTableTest.getGFEConnection(), sqlCmd, false, true);
            } catch (SQLException se) {
              if (se.getSQLState().equalsIgnoreCase("0A000")) {
                Log.getLogWriter().info("got expected 'Feature not implemented: " +
                		"Column modification not yet supported', continue testing");
                return;
              } 
            }
              //should get unsupported exception for alter column command
            throw new TestException("did not get unsupported exception, need to check and " +
                  "see if product supports alter column now");
          } else {
            Log.getLogWriter().info("does not support alter column yet");
            return;
          }
            
        } else if (sqlCmd.contains("alter table") && sqlCmd.contains("eviction by")) {
          try {
            runOneSQLCmd(alterTableTest.getGFEConnection(), sqlCmd, false, true);
          } catch (SQLException se) {
            if (se.getSQLState().equalsIgnoreCase("42X01")) {
              Log.getLogWriter().info("got expected 'syntax error exception' for alter " +
              		"eviction setting, continue testing");
              return;
            } 
          }
          throw new TestException("did not get syntax error exception, need to check and " +
                "see if product supports alter eviction now");
        } //handles #42505 alter eviction
        else if (sqlCmd.contains("alter table") && sqlCmd.contains("expire")) {
          try {
            runOneSQLCmd(alterTableTest.getGFEConnection(), sqlCmd, false, true);
          } catch (SQLException se) {
            if (se.getSQLState().equalsIgnoreCase("42X01")) {
              Log.getLogWriter().info("got expected 'syntax error exception' for alter " +
                  "expiration setting, continue testing");
              return;
            } 
          }
          throw new TestException("did not get syntax error exception, need to check and " +
                "see if product supports alter expiration now");
        } //handles #42505 alter expiration
        

      }
    }
     
    Log.getLogWriter().info("manageDerbyServer is "+Prms.tab().booleanAt(Prms.manageDerbyServer));
    if (Prms.tab().booleanAt(Prms.manageDerbyServer) == true) {
      runSQLCmdsDerby(alterCmds);
    }
    runSQLCmdsGFE(alterCmds);
    if (TestConfig.tab().get(AlterTablePrms.sqlNegativeCmds) != null) {
      Vector<String> negativeCmds = TestConfig.tab().vecAt(
          AlterTablePrms.sqlNegativeCmds);
      /* Run negative tests. */
      for (String negativeCmd : negativeCmds) {
        Log.getLogWriter().info(negativeCmd);
        try {
          runNegativeSQLCmdGFE(negativeCmd);
          throw new TestException("Expected exception did not occur on "
              + negativeCmd);
        } catch (SQLException sqle) {
          if (sqle.getSQLState().equalsIgnoreCase("23503") || sqle.getSQLState().equalsIgnoreCase("23513"))
            //add 23513 for adding check constraint
            Log.getLogWriter()
              .info("Got expected SQLException on " + negativeCmd + ", error code is "+sqle.getErrorCode()+", exception text is "+sqle.getMessage());
          else SQLHelper.handleSQLException(sqle);
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  public static void HydraTask_runSQLCommandsOnPopulatedDB() {
    if (alterTableTest == null) {
      alterTableTest = new AlterTableTest();
    }
    // THIS CODE FOR FUTURE SUPPORT of altering populated tables...
    Vector<String> alterCmds = AlterTablePrms.getSqlCmdsForPopulatedDB();
    
    //handle unsupported exception for now
    if (alterCmds != null) {
      for (int i=0; i< alterCmds.size(); i++) {
        String sqlCmd = alterCmds.get(i);
        if (sqlCmd.contains("alter column") && !sqlCmd.contains("always")) {
          if (i==0) {
            try {
              runOneSQLCmd(alterTableTest.getGFEConnection(), sqlCmd, false, true);
            } catch (SQLException se) {
              if (se.getSQLState().equalsIgnoreCase("0A000")) {
                Log.getLogWriter().info("got expected 'Feature not implemented: " +
                    "Column modification not yet supported', continue testing");
                return;
              } 
            }
              //should get unsupported exception for alter column command
            throw new TestException("did not get unsupported exception, need to check and " +
                  "see if product supports alter column now");
          } else {
            Log.getLogWriter().info("does not support alter column yet");
            return;
          }
            
        }
      }
    }
     
    if (alterCmds != null) {
      if (Prms.tab().booleanAt(Prms.manageDerbyServer) == true) {
        runSQLCmdsDerby(alterCmds);
      }
      runSQLCmdsGFE(alterCmds);
    }
    Vector<String> negativeCmds = AlterTablePrms.getSqlNegativeCmdsForPopulatedDB();
    if (negativeCmds != null) {
      /* Run negative tests. */
      for (String negativeCmd : negativeCmds) {
        try {
          Log.getLogWriter().info(negativeCmd);
          runNegativeSQLCmdGFE(negativeCmd);
          throw new TestException("Expected exception did not occur on "
              + negativeCmd);
        } catch (SQLException sqle) {
          Log.getLogWriter()
          .info("Got expected SQLException on " + negativeCmd + ", error code is "+sqle.getErrorCode()+", exception text is "+sqle.getMessage());
        }
      }
    }
  }

  /*
   * Run a SQL command on an already-open connection
   */
  public static void runOneSQLCmd(Connection conn, String sqlCmd,
      boolean isDerby, boolean logQueryResults) throws SQLException {
    // IMPROVE_ME: this is a hack to see if this is a query...
    if (sqlCmd.trim().toLowerCase().startsWith("select")) {
      Log.getLogWriter().info("executing query: " + sqlCmd);
      ResultSet rs = conn.createStatement().executeQuery(sqlCmd);
      if (rs != null && logQueryResults) {
        ResultSetHelper.logResultSet(Log.getLogWriter(), rs);
      }
    } else {
      SQLHelper.executeSQL(conn, sqlCmd, true);
    }
  }

  public static void runSQLCmdsBothConnections(Vector<String> sqlCmds) {
    runSQLCmdsDerby(sqlCmds);
    runSQLCmdsGFE(sqlCmds);
  }

  /*
   * Runs a SQL Command we expect to fail. No TestException is thrown - we just
   * throw the SQLException.
   */
  public static void runNegativeSQLCmdGFE(String sqlCmd) throws SQLException {
    Connection conn = alterTableTest.getGFEConnection();
    conn.setAutoCommit(true);
    try {
      conn.createStatement().execute(sqlCmd);
    } finally {
      alterTableTest.closeGFEConnection(conn);
    }
  }

  /*
   * Creates a new GFE connection, runs SQL command, closes connection.
   */
  public static void runSQLCmdGFE(String sqlCmd) {
    Vector<String> sqlCmds = new Vector<String>();
    sqlCmds.add(sqlCmd);
    runSQLCmdsGFE(sqlCmds);
  }

  /*
   * Creates a new GFE connection, runs SQL commands, closes connection.
   * SQLExceptions converted to TestExceptions.
   */
  public static void runSQLCmdsGFE(Vector<String> sqlCmds) {
    try {
      Connection conn = alterTableTest.getGFEConnection();
      conn.setAutoCommit(true);
      for (String sqlCmd : sqlCmds) {
        Log.getLogWriter().info("Running " + sqlCmd + " on GFE");
        runOneSQLCmd(conn, sqlCmd, false, true);
      }
      alterTableTest.closeGFEConnection(conn);
    } catch (SQLException sqle) {
      throw new TestException("gfe SQL connection exception", sqle);
    }
  }

  /*
   * Creates a new Derby connection, runs SQL commands, closes connection.
   * SQLExceptions converted to TestExceptions.
   */
  public static void runSQLCmdsDerby(Vector<String> sqlCmds) {
    try {
      Connection conn = alterTableTest.getDiscConnection();
      conn.setAutoCommit(true);
      for (String sqlCmd : sqlCmds) {
        Log.getLogWriter().info("Running " + sqlCmd + " on Derby");
        runOneSQLCmd(conn, sqlCmd, true, true);
      }
      alterTableTest.closeDiscConnection(conn);
    } catch (SQLException sqle) {
      throw new TestException("derby SQL connection exception", sqle);
    }
  }

}
