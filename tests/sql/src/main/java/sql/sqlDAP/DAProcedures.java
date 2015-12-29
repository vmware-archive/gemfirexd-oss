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
package sql.sqlDAP;

import hydra.Log;
import hydra.MasterController;
import hydra.RemoteTestModule;
import hydra.TestConfig;

import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.internal.types.ObjectTypeImpl;
import com.gemstone.gemfire.cache.query.types.ObjectType;

import sql.SQLBB;
import sql.SQLHelper;
import sql.SQLPrms;
import sql.SQLTest;
import sql.sqlutil.ResultSetHelper;
import sql.ddlStatements.*;
import util.TestException;
import util.TestHelper;

public class DAProcedures {
  static int maxNumOfTries = 2;
  static boolean tidByList = SQLDAPTest.tidByList;
  static boolean cidByRange = SQLDAPTest.cidByRange;
  static boolean concurrentDropDAPOp = SQLDAPTest.concurrentDropDAPOp;
  //when concurrent create/drop DAP procedure with call procedures is true
  //when only multiple threads call procedures, it is set to false
  public static boolean testServerGroupsInheritence = TestConfig.tab().booleanAt(SQLPrms.testServerGroupsInheritence,false);
  public static boolean isHATest = SQLTest.isHATest;
  private static ArrayList<String> portfolioPartitionKeys = null;
  private static ArrayList<String> sellordersPartitionKeys = null;
  boolean updateWrongSG = DAPDDLStmt.updateWrongSG;
  boolean testCustomProcessor = DAPDDLStmt.testCustomProcessor;
  public static boolean toReproduce46311 = TestConfig.tab().booleanAt(SQLPrms.toReproduce46311, false);
  
  public void callProcedures(Connection dConn, Connection gConn) {
    if (testServerGroupsInheritence & SQLTest.random.nextBoolean()) callProceduresWithServerGroups(dConn, gConn);
    else callProceduresWithoutServerGroups(dConn, gConn);
  }
  
  protected void callProceduresWithoutServerGroups(Connection dConn, Connection gConn) {
    int num = 3;
    if (SQLTest.random.nextInt(num) == 0 && cidByRange) {
     if (SQLTest.random.nextBoolean()) callProcedureByCidRangePortfolio(dConn, gConn);
     else callProcedureByCidRangePortfolioUpdate(dConn, gConn);
    }
    else if (SQLTest.random.nextInt(num) == 1 && testCustomProcessor) {
      callCustomProcedure(dConn, gConn);
    }
    else callProcedureByTidList(dConn, gConn);
  }
  
  protected void callProceduresWithServerGroups(Connection dConn, Connection gConn) {
    if (updateWrongSG) callProcedureUpdateGfxdPortfolioWrongSG(dConn, gConn);
    else callProcedureUpdateSellordersSG(dConn, gConn);
  }
  
  protected void callProcedureByTidList(Connection dConn, Connection gConn) {
    List<SQLException> exList = new ArrayList<SQLException>();
    String derbyProc = DAPDDLStmt.SHOWDERBYBUYORDERS;
    String gfxdProc = DAPDDLStmt.SHOWGFXDBUYORDERS;
    String derbyListStructProc = DAPDDLStmt.GETLISTOFDERBYBUYORDERS;
    String gfxdListStructProc = DAPDDLStmt.GETLISTOFGFXDBUYORDERS;
    
    if (dConn!=null) {
      if (concurrentDropDAPOp) {//only one thread is allowed to perform call procedure
        if (!doOp(gfxdProc)) {
          Log.getLogWriter().info("Other threads are performing op on the procedure " 
              + gfxdProc + ", abort this operation");
          return; //other threads performing operation on the procedure, abort      
        }
      }
      
      if (SQLTest.random.nextBoolean())
        callProceduresByTidList(dConn, gConn, derbyProc, gfxdProc, exList);
      //else
        //callListStructProceduresByTidList(dConn, gConn, derbyListStructProc, gfxdListStructProc, exList);
      
      if (concurrentDropDAPOp){ 
        zeroCounter(gfxdProc);
      } //zero the counter after op is finished
    }    
    else {
      if (SQLTest.random.nextBoolean())
        callProceduresByTidList(gConn, gfxdProc);
      //else
        //callListStructProceduresByTidList(gConn, gfxdListStructProc);
    } //no verification
  }
  
  protected void callProceduresByTidList(Connection dConn, Connection gConn, 
      String derbyProc, String gfxdProc, List<SQLException> exList) {
    int oid = SQLTest.random.nextInt((int) SQLBB.getBB().getSharedCounters().read(SQLBB.tradeBuyOrdersPrimary));
    int tid = getMyTid();

    ResultSet derbyRS = callDerbyProceduresByTidList(dConn,derbyProc, oid, tid, exList);
    if (derbyRS == null && exList.size()==0) {
      return;  //could not get rs from derby, therefore can not verify  
    }
    
    boolean[] successForHA = new boolean[1];
    ResultSet gfxdRS = callGfxdProceduresByTidList(gConn,gfxdProc, oid, tid, exList, successForHA);
    while (!successForHA[0]) {
      Log.getLogWriter().info("failed to get result set from gfxd");
      gfxdRS = callGfxdProceduresByTidList(gConn,gfxdProc, oid, tid, exList, successForHA); //retry
    }
    SQLHelper.handleMissedSQLException(exList); 
    if (gfxdRS == null && concurrentDropDAPOp) return; 
    ResultSetHelper.compareResultSets(derbyRS, gfxdRS);
  }
  
  @SuppressWarnings("unchecked")
  protected void callListStructProceduresByTidList(Connection dConn, Connection gConn, 
      String derbyProc, String gfxdProc, List<SQLException> exList) {
    int oid = SQLTest.random.nextInt((int) SQLBB.getBB().getSharedCounters().read(SQLBB.tradeBuyOrdersPrimary));
    int tid = getMyTid();

    ResultSet derbyRS = callDerbyProceduresByTidList(dConn,derbyProc, oid, tid, exList);
    if (derbyRS == null && exList.size()==0) {
      return;  //could not get rs from derby, therefore can not verify  
    }
    List<Struct> derbyList = ResultSetHelper.asList(derbyRS, true);
    if (derbyList == null) return;
    
    boolean[] successForHA = new boolean[1];
    Object[] gfxdRS = callGfxdListStructProceduresByTidList(gConn,gfxdProc, oid, tid, exList, successForHA);
    while (!successForHA[0]) {
      Log.getLogWriter().info("failed to get result set from gfxd");
      gfxdRS = callGfxdListStructProceduresByTidList(gConn,gfxdProc, oid, tid, exList, successForHA); //retry
    }
    if (gfxdRS.length > 1 ) {
      StringBuilder aStr = new StringBuilder();
      for (int i=0; i<gfxdRS.length; i++) {
       aStr.append(" get the following results " + i + ": " + 
            ((List<Struct>)gfxdRS[i]).toString());
      }
      throw new TestException("should get only one Object[] but gets " + gfxdRS.length +
          aStr.toString());
    }
    SQLHelper.handleMissedSQLException(exList); 
    if (gfxdRS == null && concurrentDropDAPOp) return;
    ResultSetHelper.compareResultSets(derbyList, (List<Struct>)gfxdRS[0]);
  }
  
  protected static ResultSet callDerbyProceduresByTidList(Connection conn, 
      String proc, int oid, int tid, List<SQLException> exList) {
    ResultSet rs = null;
    String sql = "{call " + proc + "(?, ?)}";
    Log.getLogWriter().info("call Derby " + sql + ", myTid is " + tid);
    try {
      rs = callProcedureByTidList(conn, sql, oid, tid);
      //printMetaData(rs);
    } catch (SQLException se) {
      if (!SQLHelper.checkDerbyException(conn, se))     return null;
      else    SQLHelper.handleDerbySQLException(se, exList);
    }   
    return rs;
  }
  
  protected static Object[] callGfxdListStructProceduresByTidList(Connection conn, 
      String whichProc, int oid, int tid, List<SQLException> exList, boolean[] success) {
    Object rs[] = null;
    String sql = "{call " + whichProc + "(?, ?)" +
        " ON Table trade.buyorders where oid < " + oid + " and tid= " + tid + "}";
    Log.getLogWriter().info("call gfxd " + sql + ", myTid is " + tid);
    success[0] = true;
    try {
      rs = callListStructProcedureByTidList(conn, sql, oid, tid);
    } catch (SQLException se) {
      if (se.getSQLState().equals("X0Z01")) {
        //node went down during procedure call
        Log.getLogWriter().info("got expected exception X0Z01, continuing test");
        success[0] = false; //whether process the resultSet or not
      } else {
        SQLHelper.handleGFGFXDException(se, exList);
      }
    }  
    return rs; 
  }
  
  protected static ResultSet callProcedureByTidList(Connection conn, String sql, 
      int oid, int tid) throws SQLException { 
    CallableStatement cs = null;
    cs = conn.prepareCall(sql);
    Log.getLogWriter().info(sql + " with oid: " + oid + " and with tid: " + tid );
    cs.setInt(1, oid);
    cs.setInt(2, tid);
    
    cs.execute();
    ResultSet rs = cs.getResultSet();
    if (rs == null) Log.getLogWriter().info("could not get result set");
    
    SQLWarning warning = cs.getWarnings(); //test to see there is a warning
    if (warning != null) {
      SQLHelper.printSQLWarning(warning);
    } 
    return rs;
  }
  
  protected static Object[] callListStructProcedureByTidList(Connection conn, String proc, 
      int oid, int tid) throws SQLException { 
    CallableStatement cs = null;
    cs = conn.prepareCall("{call " + proc + "(?, ?, ?)}");
    cs.registerOutParameter(3, Types.JAVA_OBJECT);
    cs.setInt(1, oid);
    cs.setInt(2, tid);
    
    cs.execute();
    Object[] rs = (Object[])cs.getObject(3);
    
    SQLWarning warning = cs.getWarnings(); //test to see there is a warning
    if (warning != null) {
      SQLHelper.printSQLWarning(warning);
    } 
    return rs;
  }
  
  protected static ResultSet callGfxdProceduresByTidList(Connection conn, 
      String whichProc, int oid, int tid, List<SQLException> exList, boolean[] success) {
    ResultSet rs = null;
    //String sql = "{call " + whichProc + "(?, ?)" +
    //" ON Table trade.buyorders where oid < " + oid + " and tid= " + tid + "}"; // //comment out due to #42766
    String sql = "call " + whichProc + "(?, ?)" +
      " ON Table trade.buyorders where oid < " + oid + " and tid= " + tid;
    Log.getLogWriter().info("call gfxd procedure " + sql + ", myTid is " + tid);
    success[0] = true;
    try {
      rs = callProcedureByTidList(conn, sql, oid, tid);
    } catch (SQLException se) {
      if (se.getSQLState().equals("X0Z01")) {
        //node went down during procedure call
        Log.getLogWriter().info("got expected exception, continuing test");
        success[0] = false; //whether process the resultSet or not
      } else {
        SQLHelper.handleGFGFXDException(se, exList);
      }
    }  
    return rs; 
  }
  
  protected void callProcedureByCidRangePortfolio(Connection dConn, Connection gConn) {
    List<SQLException> exList = new ArrayList<SQLException>();
    String derbyProc = DAPDDLStmt.SHOWDERBYPORTFOLIO;
    String gfxdProc = DAPDDLStmt.SHOWGFXDPORTFOLIO;
    
    if (dConn!=null) {
      if (concurrentDropDAPOp) {//only one thread is allowed to perform call procedure
        if (!doOp(gfxdProc)) {
          Log.getLogWriter().info("Other threads are performing op on the procedure " 
              + gfxdProc + ", abort this operation");
          return; //other threads performing operation on the procedure, abort      
        }
      }
      
      callProceduresByCidRangePortfolio(dConn, gConn, derbyProc, gfxdProc, exList);
     
      if (concurrentDropDAPOp){ 
        zeroCounter(gfxdProc);
      } //zero the counter after op is finished
    }    
    else {
      callProceduresByCidRangePortfolio(gConn, gfxdProc);
    } //no verification
  }
  
  protected void callProceduresByCidRangePortfolio(Connection dConn, Connection gConn, 
      String derbyProc, String gfxdProc, List<SQLException> exList) {
    int[] rangeEnds = DAPHelper.getRangeEnds("portfolio", SQLTest.random.nextInt((int) 
        SQLBB.getBB().getSharedCounters().read(SQLBB.tradeCustomersPrimary)));
    int midPoint = (rangeEnds[0] + rangeEnds[1])/2;
    int cid1 = SQLTest.random.nextInt(midPoint-rangeEnds[0]) + rangeEnds[0];
    //if (cid1 == 0) cid1 = 1; //work around #42950
    int cid2 = SQLTest.random.nextInt(rangeEnds[1]-midPoint) + midPoint;
    int sid = SQLTest.random.nextInt((int) SQLBB.getBB().getSharedCounters().read(SQLBB.tradeSecuritiesPrimary));
    int tid = getMyTid();
    int[] derbyData = new int[1];
    int[] gfxdData = new int[1];

    ResultSet[] derbyRS = callDerbyProceduresByCidRangePortfolio(dConn,derbyProc, 
       cid1, cid2, sid, tid, derbyData, exList);
    if (derbyRS == null && exList.size()==0) {
      return;  //could not get rs from derby, therefore can not verify  
    }
    
    boolean[] successForHA = new boolean[1];
    ResultSet[] gfxdRS = callGfxdProceduresByCidRangePortfolio(gConn,gfxdProc, 
        cid1, cid2, sid, tid, gfxdData, exList, successForHA);
    while (!successForHA[0]) {
      Log.getLogWriter().info("failed to get result set from gfxd");
      gfxdRS = callGfxdProceduresByCidRangePortfolio(gConn, gfxdProc, 
          cid1, cid2, sid, tid, gfxdData, exList, successForHA); //retry
    }
    SQLHelper.handleMissedSQLException(exList); 
    if (gfxdRS == null && concurrentDropDAPOp) return; //handle no procedure exist case
    
    //verification
    if (derbyData[0] != gfxdData[0]) 
      throw new TestException("the out data from derby and gfxd are not equal. derby gets " 
          + derbyData[0] + " but gfxd gets " + gfxdData[0]);
    
    //verify result set
    List<Struct> gfxdList = null;
    List<Struct> derbyList = null;
    for (int i =0; i<2; i++) {
      if (toReproduce46311) {
        derbyList = ResultSetHelper.asList(derbyRS[i], true);
        gfxdList = ResultSetHelper.asList(gfxdRS[i], false);
      } else {
        derbyList = ResultSetHelper.asList(derbyRS[i], ResultSetHelper.getStructType(derbyRS[i]), true);
        gfxdList = ResultSetHelper.asList(gfxdRS[i], ResultSetHelper.getStructType(derbyRS[i]), false); //use the same derby type to avoid #46311
      }
      if (derbyList !=null && gfxdList != null) {
        ResultSetHelper.compareResultSets(derbyList, gfxdList);
        Log.getLogWriter().info("compares results set " + (i+1) + " is successful");
      }
    }
    
    List<Struct> gfxdList2 = null;
    //verify 2 result sets from gfxd of same query but one with outgoing result set
    if (toReproduce46311) {
      gfxdList2 = ResultSetHelper.asList(gfxdRS[2], false); 
    } else {
      gfxdList2 = ResultSetHelper.asList(gfxdRS[2], ResultSetHelper.getStructType(derbyRS[1]), false); 
    }
    if (gfxdList2 == null) {
      if (SQLTest.isHATest) {
        Log.getLogWriter().info("could not get results after a few retries");
        return;
      } else {
        throw new TestException("non HA test and gemfirexd query result is " + gfxdList);
      }
    } else {
      if (gfxdList != null) {
        ResultSetHelper.compareResultSets(gfxdList, gfxdList2, 
            "second gfxd result", "third gfxd result");
        //with derby either serial or testUniqueKeys, both should be verified
      }
    }
    
    List<Struct> gfxdList3 = ResultSetHelper.asList(gfxdRS[3], false);
    if (gfxdList3 == null) {
      if (SQLTest.isHATest) {
        Log.getLogWriter().info("could not get results after a few retries");
        return;
      } else {
        throw new TestException("non HA test and gemfirexd query result is " + gfxdList3);
      }
    
    } else {
      if (gfxdList3.size() !=1 ) {
        throw new TestException("DAP on portfolio by cid range does not invoked on only one node" 
            + " got gfxd DAP added result set of " + gfxdList3.toString());
      } else Log.getLogWriter().info("gets the constructed result set");
    }
    
  }
  
  public static boolean doOp(String procName) {
    int doOp = getCounter(procName);
    int count =0;
    while (doOp != 1) {
      if (count >maxNumOfTries) return false;  //try to operation, if not do not operate
      count ++;
      MasterController.sleepForMs(100 * SQLTest.random.nextInt(30)); //sleep from 0 - 2900 ms
      doOp = getCounter(procName);
    }  
    return true;
  }
  
  protected static ResultSet[] callDerbyProceduresByCidRangePortfolio(Connection conn, 
      String proc, int cid1, int cid2, int sid, int tid, int[] data, 
      List<SQLException> exList) {
    ResultSet[] rs = null;
    String sql = "{call " + proc + "(?, ?, ?, ?, ?)}";
    Log.getLogWriter().info("call Derby " + sql + ", myTid is " + tid);
    try {
      rs = callProcedureByCidRangePortfolio(conn, sql, cid1, cid2, sid, tid, data);
    } catch (SQLException se) {
      if (!SQLHelper.checkDerbyException(conn, se))     return null;
      else    SQLHelper.handleDerbySQLException(se, exList);
    }   
    return rs;
  }
  
  protected static ResultSet[] callGfxdProceduresByCidRangePortfolio(Connection conn, 
      String proc, int cid1, int cid2, int sid, int tid, int[] data, 
      List<SQLException> exList, boolean[] success) {
    ResultSet[] rs = null;
    //String sql = "{call " + whichProc + "(?, ?)" +
    //" ON Table trade.buyorders where cid > " + cid1 + " and cid< " + cid2 + " and tid=" + tid + "}"; // //comment out due to #42766
    String sql = "{call " + proc + "(?, ?, ?, ?, ?)" +
      " ON Table trade.portfolio where cid > " + cid1 + " and cid< " + cid2 + " and tid=" + tid + "}";
    Log.getLogWriter().info("call gfxd procedure " + sql + ", myTid is " + tid);
    success[0] = true;
    try {
      rs = callProcedureByCidRangePortfolio(conn, sql, cid1, cid2, sid, tid, data);
    } catch (SQLException se) {
      if (se.getSQLState().equals("X0Z01")) {
        //node went down during procedure call
        Log.getLogWriter().info("got expected exception, continuing test");
        success[0] = false; //whether process the resultSet or not
      } else {
        SQLHelper.handleGFGFXDException(se, exList);
      }
    }  
    return rs; 
  }
  
  protected static ResultSet[] callProcedureByCidRangePortfolio(Connection conn, String sql, 
      int cid1, int cid2, int sid, int tid, int[] data) throws SQLException { 
    ResultSet[] rs = new ResultSet[4];
    CallableStatement cs = null;
    cs = conn.prepareCall(sql);
    Log.getLogWriter().info(sql + " with cid1: " + cid1 + " and with cid2: " + cid2  +
        " with sid: " + sid + " and with tid: " + tid );
    cs.setInt(1, cid1);
    cs.setInt(2, cid2);
    cs.setInt(3, sid);
    cs.setInt(4, tid);
    cs.registerOutParameter(5, Types.INTEGER);
    cs.execute();
    data[0] = new Integer(cs.getInt(5));

    rs[0] = cs.getResultSet();
    int i=1;
    while (cs.getMoreResults(Statement.KEEP_CURRENT_RESULT)) {
      Log.getLogWriter().info("has more results");
      rs[i] = cs.getResultSet();
      i++;
    }
    if (rs == null) Log.getLogWriter().info("could not get result sets in callProcedureByCidRangePortfolio");
    
    SQLWarning warning = cs.getWarnings(); //test to see there is a warning
    if (warning != null) {
      SQLHelper.printSQLWarning(warning);
    } 
    return rs;
  }
  
  //get counter to see if there is thread operation on the procedure
  public static int getCounter(String procName) {
    if (procName.startsWith("trade.showGfxdBuyorders")) {
      int counter = (int)SQLDAPBB.getBB().getSharedCounters().incrementAndRead(SQLDAPBB.showGfxdBuyorders);
      Log.getLogWriter().info(procName + " counter is " + counter + " ");
      return counter;
    } else if (procName.startsWith("trade.getListOfGfxdbuyorders")) {
      int counter = (int)SQLDAPBB.getBB().getSharedCounters().incrementAndRead(SQLDAPBB.getListOfGfxdBuyorders);
      Log.getLogWriter().info(procName + " counter is " + counter + " ");
      return counter;
    } else if (procName.startsWith("trade.updateGfxdPortfolioWrongSG")) {
      int counter = (int)SQLDAPBB.getBB().getSharedCounters().incrementAndRead(SQLDAPBB.updateGfxdPortfolioWrongSG);
      Log.getLogWriter().info(procName + " counter is " + counter + " ");
      return counter;
    } else if (procName.startsWith("trade.updateGfxdPortfolioByCidRange")) {
      int counter = (int)SQLDAPBB.getBB().getSharedCounters().incrementAndRead(SQLDAPBB.updateGfxdPortfolioByCidRange);
      Log.getLogWriter().info(procName + " counter is " + counter + " ");
      return counter;
    } else if (procName.startsWith("trade.updateGfxdSellordersSG")) {
      int counter = (int)SQLDAPBB.getBB().getSharedCounters().incrementAndRead(SQLDAPBB.updateGfxdSellordersSG);
      Log.getLogWriter().info(procName + " counter is " + counter + " ");
      return counter;
    } else if (procName.startsWith("trade.customGfxdProc")) {
      int counter = (int)SQLDAPBB.getBB().getSharedCounters().incrementAndRead(SQLDAPBB.customGfxdProc);
      Log.getLogWriter().info(procName + " counter is " + counter + " ");
      return counter;
    } else 
      return -1;
  }
  
  public static void zeroCounter(String procName) {
    if (procName.startsWith("trade.showGfxdBuyorders")) {
      SQLDAPBB.getBB().getSharedCounters().zero(SQLDAPBB.showGfxdBuyorders);
      Log.getLogWriter().info("zeros counter SQLDAPBB.showGfxdBuyorders");
    } else if (procName.startsWith("trade.getListOfGfxdbuyorders")) {
      SQLDAPBB.getBB().getSharedCounters().zero(SQLDAPBB.getListOfGfxdBuyorders);
      Log.getLogWriter().info("zeros counter SQLDAPBB.getListOfGfxdBuyorders");
    } else if (procName.startsWith("trade.updateGfxdPortfolioWrongSG")) {
      SQLDAPBB.getBB().getSharedCounters().zero(SQLDAPBB.updateGfxdPortfolioWrongSG);
      Log.getLogWriter().info("zeros counter SQLDAPBB.updateGfxdPortfolioWrongSG");
    } else if (procName.startsWith("trade.updateGfxdPortfolioWrongSG")) {
      SQLDAPBB.getBB().getSharedCounters().zero(SQLDAPBB.updateGfxdPortfolioByCidRange);
      Log.getLogWriter().info("zeros counter SQLDAPBB.updateGfxdPortfolioByCidRange");
    } else if (procName.startsWith("trade.updateGfxdSellordersSG")) {
      SQLDAPBB.getBB().getSharedCounters().zero(SQLDAPBB.updateGfxdSellordersSG);
      Log.getLogWriter().info("zeros counter SQLDAPBB.updateGfxdSellordersSG");
    } else if (procName.startsWith("trade.customGfxdProc")) {
      SQLDAPBB.getBB().getSharedCounters().zero(SQLDAPBB.customGfxdProc);
      Log.getLogWriter().info("zeros counter SQLDAPBB.customGfxdProc");
    }
  }
  
  protected int getMyTid() {
    int myTid = RemoteTestModule.getCurrentThread().getThreadId();
    return myTid;
  }

  protected void callProcedureUpdateGfxdPortfolioWrongSG(Connection dConn, Connection gConn) {
    String gfxdProc = DAPDDLStmt.UPDATGFXDFPORTFOLIOWRONGSG;
    Log.getLogWriter().info("gfxdProc is " + gfxdProc);
    
    if (dConn!=null) {
      if (concurrentDropDAPOp) {//only one thread is allowed to perform call procedure
        if (!doOp(gfxdProc)) {
          Log.getLogWriter().info("Other threads are performing op on the procedure " 
              + gfxdProc + ", abort this operation");
          return; //other threads performing operation on the procedure, abort      
        }
      }
      
      callProceduresUpdateGfxdPortfolioWrongSG(gConn, gfxdProc);
      
      if (concurrentDropDAPOp){ 
        zeroCounter(gfxdProc);
      } //zero the counter after op is finished
    }    
    else {
      callProceduresUpdateGfxdPortfolioWrongSG(gConn, gfxdProc);
    } //no verification
  }
  
  protected void callProceduresUpdateGfxdPortfolioWrongSG(Connection conn, 
      String proc) {
    //String sql = update trade.portfolio set subTotal=? where qty > ?
    String wrongSG = SQLDAPTest.getWrongServerGroups("portfolio");
    // wrong SG is null only occurs when schema created on default servers
    // and the table inherits from schema server groups
    if (wrongSG == null) {
      Log.getLogWriter().info("Not able to perform, as porfolio is created on default server groups");
      return; 
    }
    int qty = SQLTest.random.nextInt(2000);
    BigDecimal subTotal = new BigDecimal (Double.toString(( SQLTest.random.nextInt(10000)+1) 
        * .01)).multiply(new BigDecimal(String.valueOf(qty)));
    String sql = "{call " + proc + "(?, ?)" +
      " ON SERVER GROUPS (" + wrongSG + ")}";
    Log.getLogWriter().info("call gfxd procedure " + sql + " with subtotal: " + subTotal +
       " and qty:" + qty);
    try {
      CallableStatement cs = null;
      cs = conn.prepareCall(sql);
      cs.setBigDecimal(1, subTotal);
      cs.setInt(2, qty);
    } catch (SQLException se) {
      checkGFGFXDException(se);
    }  
  }
  
  protected void callProcedureByCidRangePortfolioUpdate(Connection dConn, Connection gConn) {
    List<SQLException> exList = new ArrayList<SQLException>();
    String derbyProc = DAPDDLStmt.UPDATEDERBYPORTFOLIO;
    String gfxdProc = DAPDDLStmt.UPDATGFXDFPORTFOLIO;
    
    if (dConn!=null) {
      if (concurrentDropDAPOp) {//only one thread is allowed to perform call procedure
        if (!doOp(gfxdProc)) {
          Log.getLogWriter().info("Other threads are performing op on the procedure " 
              + gfxdProc + ", abort this operation");
          return; //other threads performing operation on the procedure, abort      
        }
      }
      
      callProceduresByCidRangePortfolioUpdate(dConn, gConn, derbyProc, gfxdProc, exList);
     
      if (concurrentDropDAPOp){ 
        zeroCounter(gfxdProc);
      } //zero the counter after op is finished
    }    
    else {
      callProceduresByCidRangePortfolioUpdate(gConn, gfxdProc);
    } //no verification
  }
  
  
  //fire and forget
  protected void callProceduresByCidRangePortfolioUpdate(Connection dConn, Connection gConn, 
      String derbyProc, String gfxdProc, List<SQLException> exList) {
    if (updatePortfolioOnSubtotal()) return; //do not call update if on partitioned key
    
    int[] rangeEnds = DAPHelper.getRangeEnds("portfolio", SQLTest.random.nextInt((int) 
        SQLBB.getBB().getSharedCounters().read(SQLBB.tradeCustomersPrimary)));
    int midPoint = (rangeEnds[0] + rangeEnds[1])/2;
    int cid1 = SQLTest.random.nextInt(midPoint-rangeEnds[0]) + rangeEnds[0];
    //if (cid1 == 0) cid1 = 1; //work around #42950
    int cid2 = SQLTest.random.nextInt(rangeEnds[1]-midPoint) + rangeEnds[1]; //for two buckets
    int tid = getMyTid();
    int qty = SQLTest.random.nextInt(2000);
    BigDecimal subTotal = new BigDecimal (Double.toString(( SQLTest.random.nextInt(10000)+1) 
        * .01)).multiply(new BigDecimal(String.valueOf(qty)));

    boolean success = callDerbyProceduresByCidRangePortfolioUpdate(dConn,derbyProc, 
       cid1, cid2, subTotal, tid, exList);
    if (!success) return;
    
    boolean[] successForHA = new boolean[1];
    callGfxdProceduresByCidRangePortfolioUpdate(gConn,gfxdProc, 
        cid1, cid2, subTotal, tid, exList, successForHA);
    while (!successForHA[0]) {
      Log.getLogWriter().info("failed to get result set from gfxd");
      callGfxdProceduresByCidRangePortfolioUpdate(gConn,gfxdProc, 
          cid1, cid2, subTotal, tid, exList, successForHA); //retry
    }
    SQLHelper.handleMissedSQLException(exList); 
    
  }
  
  protected static boolean callDerbyProceduresByCidRangePortfolioUpdate(Connection conn, 
      String proc, int cid1, int cid2, BigDecimal subTotal, int tid, List<SQLException> exList) {
    String sql = "{call " + proc + "(?, ?, ?, ?)}";
    Log.getLogWriter().info("call Derby " + sql + ", myTid is " + tid);
    try {
      callProcedureByCidRangePortfolioUpdate(conn, sql, cid1, cid2, subTotal, tid);
    } catch (SQLException se) {
      if (!SQLHelper.checkDerbyException(conn, se))     return false;
      else    SQLHelper.handleDerbySQLException(se, exList);
    }   
    return true;
  }
  
  protected static void callGfxdProceduresByCidRangePortfolioUpdate(Connection conn, 
      String proc, int cid1, int cid2, BigDecimal subTotal, int tid, List<SQLException> exList,
      boolean[] success) {
    String sql = "{call " + proc + "(?, ?, ?, ?)" + 
      " ON Table trade.portfolio where cid > " + cid1 + " and cid< " + cid2 + " and tid=" + tid
      + "}";
    Log.getLogWriter().info("call gfxd procedure " + sql + ", myTid is " + tid);
    success[0] = true;
    try {
      callProcedureByCidRangePortfolioUpdate(conn, sql, cid1, cid2, subTotal, tid);
    } catch (SQLException se) {
      if (se.getSQLState().equals("X0Z01") && isHATest) {
        //node went down during procedure call
        Log.getLogWriter().info("got expected exception X0Z01, continuing test");
        success[0] = false; //whether process the resultSet or not
      } else {
        SQLHelper.handleGFGFXDException(se, exList);
      }
    }  
  }
  
  protected static void callProcedureByCidRangePortfolioUpdate(Connection conn, String sql, 
      int cid1, int cid2, BigDecimal subTotal, int tid) throws SQLException { 
    CallableStatement cs = null;
    cs = conn.prepareCall(sql);
    Log.getLogWriter().info(sql + " with cid1: " + cid1 + " and with cid2: " + cid2  +
        " with subTotal: " + subTotal + " and with tid: " + tid );
    cs.setInt(1, cid1);
    cs.setInt(2, cid2);
    cs.setBigDecimal(3, subTotal);
    cs.setInt(4, tid);
    cs.execute();
   
    SQLWarning warning = cs.getWarnings(); //test to see there is a warning
    if (warning != null) {
      SQLHelper.printSQLWarning(warning);
    } 
  }
  
  @SuppressWarnings("unchecked")
  private boolean updatePortfolioOnSubtotal() {
    if (portfolioPartitionKeys ==null)
      portfolioPartitionKeys= (ArrayList<String>)SQLBB.getBB().getSharedMap().get("portfolioPartition");
    if (portfolioPartitionKeys.contains("subTotal")) return true;
    else return false;
  }
  
  protected void callProcedureUpdateSellordersSG(Connection dConn, Connection gConn) {
    List<SQLException> exList = new ArrayList<SQLException>();
    String derbyProc = DAPDDLStmt.UPDATEDERBYSELLORDERSSG;
    String gfxdProc = DAPDDLStmt.UPDATGFXDFSELLORDERSSG;
    
    if (dConn!=null) {
      if (concurrentDropDAPOp) {//only one thread is allowed to perform call procedure
        if (!doOp(gfxdProc)) {
          Log.getLogWriter().info("Other threads are performing op on the procedure " 
              + gfxdProc + ", abort this operation");
          return; //other threads performing operation on the procedure, abort      
        }
      }
      
      callProceduresSellordersSGUpdate(dConn, gConn, derbyProc, gfxdProc, exList);
     
      if (concurrentDropDAPOp){ 
        zeroCounter(gfxdProc);
      } //zero the counter after op is finished
    }    
    else {
      callProceduresSellordersSGUpdate(gConn, gfxdProc);
    } //no verification
  }
  
  
  //fire and forget
  protected void callProceduresSellordersSGUpdate(Connection dConn, Connection gConn, 
      String derbyProc, String gfxdProc, List<SQLException> exList) {
    if (updateSellordersOnOrderTime()) return; //do not call update if on partitioned key
    
    int tid = getMyTid();
    Timestamp orderTime = new Timestamp (System.currentTimeMillis());


    boolean success = callDerbyProceduresSellordersSGUpdate(dConn,derbyProc, 
       orderTime, tid, exList);
    if (!success) return;
    
    boolean[] successForHA = new boolean[1];
    callGfxdProceduresSellordersSGUpdate(gConn,gfxdProc, 
        orderTime, tid, exList, successForHA);
    while (!successForHA[0]) {
      Log.getLogWriter().info("failed to get result set from gfxd");
      callGfxdProceduresSellordersSGUpdate(gConn,gfxdProc, 
          orderTime, tid, exList, successForHA); //retry
    }
    SQLHelper.handleMissedSQLException(exList); 
    
  }
  
  protected static boolean callDerbyProceduresSellordersSGUpdate(Connection conn, 
      String proc, Timestamp orderTime, int tid, List<SQLException> exList) {
    String sql = "{call " + proc + "(?, ?)}";
    Log.getLogWriter().info("call Derby " + sql + ", myTid is " + tid);
    try {
      callProcedureSellordersSGUpdate(conn, sql, orderTime, tid);
    } catch (SQLException se) {
      if (!SQLHelper.checkDerbyException(conn, se))     return false;
      else    SQLHelper.handleDerbySQLException(se, exList);
    }   
    return true;
  }
  
  protected static void callGfxdProceduresSellordersSGUpdate(Connection conn, 
      String proc, Timestamp orderTime, int tid, List<SQLException> exList,
      boolean[] success) {
    String sellordersSG = SQLDAPTest.getServerGroups("sellorders");
    String sgOption;
    
    //sellordersSG is null only occurs when schema created on default servers
    //so data could be on any data nodes/servers, calling needs to reflect this
    //accordingly
    if (sellordersSG != null) {
      sgOption = SQLTest.random.nextInt(10) == 1 ?
        " ON ALL" :" ON SERVER GROUPS (" + sellordersSG + ")";
    } else {
      sgOption =  " ON ALL" ; 
      //on all server, with no on-clause, the procedure only invoked in one coordinate node
    }
    String sql = "{call " + proc + "(?, ?)" + sgOption + "}";
    Log.getLogWriter().info("call gfxd procedure " + sql + ", myTid is " + tid);
    success[0] = true;
    try {
      callProcedureSellordersSGUpdate(conn, sql, orderTime, tid);
    } catch (SQLException se) {
      if (se.getSQLState().equals("X0Z01") && isHATest) {
        //node went down during procedure call
        Log.getLogWriter().info("got expected exception X0Z01, continuing test");
        success[0] = false; //whether process the resultSet or not
      } else {
        SQLHelper.handleGFGFXDException(se, exList);
      }
    }  
  }
  
  protected static void callProcedureSellordersSGUpdate(Connection conn, String sql, 
     Timestamp orderTime, int tid) throws SQLException { 
    CallableStatement cs = null;
    cs = conn.prepareCall(sql);
    Log.getLogWriter().info(sql + " with order_time: " + orderTime + " and with tid: " + tid );
    cs.setTimestamp(1, orderTime);
    cs.setInt(2, tid);
    cs.execute();
   
    SQLWarning warning = cs.getWarnings(); //test to see there is a warning
    if (warning != null) {
      SQLHelper.printSQLWarning(warning);
    } 
  }
  
  @SuppressWarnings("unchecked")
  private boolean updateSellordersOnOrderTime() {
    if (sellordersPartitionKeys ==null)
      sellordersPartitionKeys= (ArrayList<String>)SQLBB.getBB().getSharedMap().get("sellordersPartition");
    if (sellordersPartitionKeys.contains("order_time")) return true;
    else return false;
  }
  
  protected void callCustomProcedure(Connection dConn, Connection gConn) {
    List<SQLException> exList = new ArrayList<SQLException>();
    //String derbyProc = DAPDDLStmt.CUSTOMDERBYPROC;
    String derbyProc = null;
    String gfxdProc = DAPDDLStmt.CUSTOMGFXDPROC;
    
    if (dConn!=null) {
      if (concurrentDropDAPOp) {//only one thread is allowed to perform call procedure
        if (!doOp(gfxdProc)) {
          Log.getLogWriter().info("Other threads are performing op on the procedure " 
              + gfxdProc + ", abort this operation");
          return; //other threads performing operation on the procedure, abort      
        }
      }
      

      callCustomProcedure(dConn, gConn, derbyProc, gfxdProc, exList);

      if (concurrentDropDAPOp){ 
        zeroCounter(gfxdProc);
      } //zero the counter after op is finished
    }    
    else {
      //TODO to implement non verification case
    } //no verification
  }
  
  protected void callCustomProcedure(Connection dConn, Connection gConn, 
      String derbyProc, String gfxdProc, List<SQLException> exList) {
    
    int tid = getMyTid();
    int[] rangeEnds = DAPHelper.getRangeEnds("portfolio", SQLTest.random.nextInt((int) 
        SQLBB.getBB().getSharedCounters().read(SQLBB.tradeCustomersPrimary)));
    int midPoint = (rangeEnds[0] + rangeEnds[1])/2;
    int cid1 = SQLTest.random.nextInt(midPoint-rangeEnds[0]) + rangeEnds[0];
    //if (cid1 == 0) cid1 = 1; //work around #42950
    int cid2 = SQLTest.random.nextInt(rangeEnds[1]-midPoint) + rangeEnds[1]; //for two buckets

    /*
    boolean success = callDerbyCustomProcedure(dConn,derbyProc, 
       cid1, cid2, tid, exList);
    if (!success) return;
    */
    
    boolean[] successForHA = new boolean[1];
    callGfxdCustomProcedure(gConn,gfxdProc, 
        cid1, cid2, tid, exList, successForHA);
    while (!successForHA[0]) {
      Log.getLogWriter().info("failed to get result set from gfxd");
      callGfxdCustomProcedure(gConn,gfxdProc, 
          cid1, cid2, tid, exList, successForHA); //retry
    }
    SQLHelper.handleMissedSQLException(exList); 
    
  }
  
  protected static void callGfxdCustomProcedure(Connection conn, 
      String proc, int cid1, int cid2, int tid, List<SQLException> exList,
      boolean[] success) {
    String sgOption =  " ON ALL" ; 
    //String processor = " With Result Processor 'trade.customProcessor' "; //reproduce #42995
    //String processor = " With Result Processor trade.customProcessor "; 
    String processor = " With Result Processor customProcessor";
    //String sql = "{call " + proc + "(?, ?, ?)" + processor + sgOption + "}";
    String sql = "{call " + proc + "(?, ?, ?)" + processor + " on table trade.customers where cid>=1" + "}";
    Log.getLogWriter().info("call gfxd procedure " + sql + ", myTid is " + tid);
    success[0] = true;
    try {
      ResultSet[] rs = callCustomProcedure(conn, sql, cid1, cid2, tid);
      List<Struct> list1 = ResultSetHelper.asList(rs[0], false);
      Log.getLogWriter().info("first custom result set is: \n" +ResultSetHelper.listToString(list1));
      List<Struct> list2 = ResultSetHelper.asList(rs[1], false);
      Log.getLogWriter().info("second custom result set is: \n" +ResultSetHelper.listToString(list2));
      List<Struct> list3 = ResultSetHelper.asList(rs[2], false);

      //added following for checking local with get convertible type query
      if (list3.size() > 1) {
        throw new TestException ("got duplicate resulst with <local> for get converitible type query " 
            + ResultSetHelper.listToString(list3));
      } else {
        Log.getLogWriter().info("second custom result set is: \n" +ResultSetHelper.listToString(list3));
      }
    } catch (SQLException se) {
      if (se.getSQLState().equals("X0Z01") && isHATest) {
        //node went down during procedure call
        Log.getLogWriter().info("got expected exception X0Z01, continuing test");
        success[0] = false; //whether process the resultSet or not
      } else {
        SQLHelper.handleGFGFXDException(se, exList);
      }
    }  
  }
  
  protected static ResultSet[] callCustomProcedure(Connection conn, String sql, 
      int cid1, int cid2, int tid) throws SQLException { 
     ResultSet[] rs = new ResultSet[3];
     CallableStatement cs = null;
     cs = conn.prepareCall(sql);
     Log.getLogWriter().info(sql + " with cid1: " + cid1 + " and with cid2: " + cid2
        + " and with tid: " + tid );
     cs.setInt(1, cid1);
     cs.setInt(2, cid2);
     cs.setInt(3, tid);
     cs.execute();
     
     rs[0] = cs.getResultSet();
     //printMetaData(rs[0]);
     //Log.getLogWriter().info("custom result set are: \n" +ResultSetHelper.asList(rs[0], false));
    

     if (cs.getMoreResults(Statement.KEEP_CURRENT_RESULT)) {
       rs[1] = cs.getResultSet();   
     }
     
     if (cs.getMoreResults(Statement.KEEP_CURRENT_RESULT)) {
       rs[2] = cs.getResultSet();   
     }
     
     SQLWarning warning = cs.getWarnings(); //test to see there is a warning
     if (warning != null) {
       SQLHelper.printSQLWarning(warning);
     } 
     
     return rs;
   }
  
  protected void callProceduresByTidList(Connection gConn, 
      String gfxdProc) {
    int oid = SQLTest.random.nextInt((int) SQLBB.getBB().getSharedCounters().read(SQLBB.tradeBuyOrdersPrimary));
    int tid = getMyTid();

    boolean[] successForHA = new boolean[1];
    ResultSet gfxdRS = callGfxdProceduresByTidList(gConn,gfxdProc, oid, tid, successForHA);
    while (!successForHA[0]) {
      Log.getLogWriter().info("failed to get result set from gfxd");
      gfxdRS = callGfxdProceduresByTidList(gConn,gfxdProc, oid, tid, successForHA); //retry
    }

    if (gfxdRS == null && concurrentDropDAPOp) return; 
    ResultSetHelper.asList(gfxdRS, false);
  }
  
  protected static ResultSet callGfxdProceduresByTidList(Connection conn, 
      String whichProc, int oid, int tid, boolean[] success) {
    ResultSet rs = null;
    //String sql = "{call " + whichProc + "(?, ?)" +
    //" ON Table trade.buyorders where oid < " + oid + " and tid= " + tid + "}"; // //comment out due to #42766
    String sql = "{call " + whichProc + "(?, ?)" +
      " ON Table trade.buyorders where oid < " + oid + " and tid= " + tid + "}";
    Log.getLogWriter().info("call gfxd procedure " + sql + ", myTid is " + tid);
    success[0] = true;
    try {
      rs = callProcedureByTidList(conn, sql, oid, tid);
    } catch (SQLException se) {
      if (se.getSQLState().equals("X0Z01") && isHATest) {
        //node went down during procedure call
        Log.getLogWriter().info("got expected exception, continuing test");
        success[0] = false; //whether process the resultSet or not
      } else {
        checkGFGFXDException(se);
      }
    }  
    return rs; 
  }
  
  @SuppressWarnings("unchecked")
  protected void callListStructProceduresByTidList(Connection gConn, 
      String gfxdProc) {
    int oid = SQLTest.random.nextInt((int) SQLBB.getBB().getSharedCounters().read(SQLBB.tradeBuyOrdersPrimary));
    int tid = getMyTid();
    
    boolean[] successForHA = new boolean[1];
    Object[] gfxdRS = callGfxdListStructProceduresByTidList(gConn,gfxdProc, oid, tid, successForHA);
    while (!successForHA[0]) {
      Log.getLogWriter().info("failed to get result set from gfxd");
      gfxdRS = callGfxdListStructProceduresByTidList(gConn,gfxdProc, oid, tid, successForHA); //retry
    }
    if (gfxdRS.length > 1 ) {
      StringBuilder aStr = new StringBuilder();
      for (int i=0; i<gfxdRS.length; i++) {
       aStr.append(" get the following results " + i + ": " + 
            ((List<Struct>)gfxdRS[i]).toString());
      }
      throw new TestException("should get only one Object[] but gets " + gfxdRS.length +
          aStr.toString());
    }
    if (gfxdRS == null && concurrentDropDAPOp) return;
  }
  
  protected static Object[] callGfxdListStructProceduresByTidList(Connection conn, 
      String whichProc, int oid, int tid, boolean[] success) {
    Object rs[] = null;
    String sql = "{call " + whichProc + "(?, ?)" +
        " ON Table trade.buyorders where oid < " + oid + " and tid= " + tid + "}";
    Log.getLogWriter().info("call gfxd " + sql + ", myTid is " + tid);
    success[0] = true;
    try {
      rs = callListStructProcedureByTidList(conn, sql, oid, tid);
    } catch (SQLException se) {
      if (se.getSQLState().equals("X0Z01") && isHATest) {
        //node went down during procedure call
        Log.getLogWriter().info("got expected exception X0Z01, continuing test");
        success[0] = false; //whether process the resultSet or not
      } else {
        checkGFGFXDException(se);
      }
    }  
    return rs; 
  }
  
  protected static void checkGFGFXDException(SQLException se) {
    if (se.getSQLState().equals("42Y55") || se.getSQLState().equals("42X94") 
        || se.getSQLState().equals("42Y03"))
      Log.getLogWriter().info("expected procedrue does not exist exception, continuing test");
    else if (se.getSQLState().equals("X0Y68"))
      Log.getLogWriter().info("expected procedrue already exist exception, continuing test");
    else {
      SQLHelper.handleSQLException(se);
    }
  }
  
  protected void callProceduresByCidRangePortfolio(Connection gConn, 
      String gfxdProc) {
    int[] rangeEnds = DAPHelper.getRangeEnds("portfolio", SQLTest.random.nextInt((int) 
        SQLBB.getBB().getSharedCounters().read(SQLBB.tradeCustomersPrimary)));
    int midPoint = (rangeEnds[0] + rangeEnds[1])/2;
    int cid1 = SQLTest.random.nextInt(midPoint-rangeEnds[0]) + rangeEnds[0];
    //if (cid1 == 0) cid1 = 1; //work around #42950
    int cid2 = SQLTest.random.nextInt(rangeEnds[1]-midPoint) + midPoint;
    int sid = SQLTest.random.nextInt((int) SQLBB.getBB().getSharedCounters().read(SQLBB.tradeSecuritiesPrimary));
    int tid = getMyTid();
    int[] gfxdData = new int[1];

    boolean[] successForHA = new boolean[1];
    ResultSet[] gfxdRS = callGfxdProceduresByCidRangePortfolio(gConn,gfxdProc, 
        cid1, cid2, sid, tid, gfxdData, successForHA);
    while (!successForHA[0]) {
      Log.getLogWriter().info("failed to get result set from gfxd");
      gfxdRS = callGfxdProceduresByCidRangePortfolio(gConn, gfxdProc, 
          cid1, cid2, sid, tid, gfxdData, successForHA); //retry
    }
    if (gfxdRS == null && concurrentDropDAPOp) return; //handle no procedure exist case
    
    List<Struct> gfxdList = null;
    for (int i =0; i<2; i++) {
      gfxdList = ResultSetHelper.asList(gfxdRS[i], false);
    }
    
    List<Struct> gfxdList2 = ResultSetHelper.asList(gfxdRS[2], false);
    if (gfxdList2 == null) {
      if (SQLTest.isHATest) {
        Log.getLogWriter().info("could not get results after a few retries");
        return;
      } else {
        throw new TestException("non HA test and gemfirexd query result is " + gfxdList);
      }
    } else if (SQLTest.testUniqueKeys){
      if (gfxdList != null) {
        ResultSetHelper.compareResultSets(gfxdList, gfxdList2);
        //only verify without concurrent dml to changed the result set.
      }
      
    }
    
    List<Struct> gfxdList3 = ResultSetHelper.asList(gfxdRS[3], false);
    //verify only one node is being invoked on the DAP
    if (gfxdList3.size() !=1 ) {
      throw new TestException("DAP on portfolio by cid range does not invoked on only one node" 
          + " got gfxd DAP added result set of " + gfxdList3.toString());
    } else Log.getLogWriter().info("gets the constructed result set");
    
  }
  
  protected static ResultSet[] callGfxdProceduresByCidRangePortfolio(Connection conn, 
      String proc, int cid1, int cid2, int sid, int tid, int[] data, 
      boolean[] success) {
    ResultSet[] rs = null;
    //String sql = "{call " + whichProc + "(?, ?)" +
    //" ON Table trade.buyorders where cid > " + cid1 + " and cid< " + cid2 + " and tid=" + tid + "}"; // //comment out due to #42766
    String sql = "{call " + proc + "(?, ?, ?, ?, ?)" +
      " ON Table trade.portfolio where cid > " + cid1 + " and cid< " + cid2 + " and tid=" + tid + "}";
    Log.getLogWriter().info("call gfxd procedure " + sql + ", myTid is " + tid);
    success[0] = true;
    try {
      rs = callProcedureByCidRangePortfolio(conn, sql, cid1, cid2, sid, tid, data);
    } catch (SQLException se) {
      if (se.getSQLState().equals("X0Z01")) {
        //node went down during procedure call
        Log.getLogWriter().info("got expected exception, continuing test");
        success[0] = false; //whether process the resultSet or not
      } else {
        checkGFGFXDException(se);
      }
    }  
    return rs; 
  }
  
  protected void callProceduresByCidRangePortfolioUpdate(Connection gConn, 
      String gfxdProc) {
    if (updatePortfolioOnSubtotal()) return; //do not call update if on partitioned key
    
    int[] rangeEnds = DAPHelper.getRangeEnds("portfolio", SQLTest.random.nextInt((int) 
        SQLBB.getBB().getSharedCounters().read(SQLBB.tradeCustomersPrimary)));
    int midPoint = (rangeEnds[0] + rangeEnds[1])/2;
    int cid1 = SQLTest.random.nextInt(midPoint-rangeEnds[0]) + rangeEnds[0];
    //if (cid1 == 0) cid1 = 1; //work around #42950
    int cid2 = SQLTest.random.nextInt(rangeEnds[1]-midPoint) + rangeEnds[1]; //for two buckets
    int tid = getMyTid();
    int qty = SQLTest.random.nextInt(2000);
    BigDecimal subTotal = new BigDecimal (Double.toString(( SQLTest.random.nextInt(10000)+1) 
        * .01)).multiply(new BigDecimal(String.valueOf(qty)));

    
    boolean[] successForHA = new boolean[1];
    callGfxdProceduresByCidRangePortfolioUpdate(gConn,gfxdProc, 
        cid1, cid2, subTotal, tid, successForHA);
    while (!successForHA[0]) {
      Log.getLogWriter().info("failed to get result set from gfxd");
      callGfxdProceduresByCidRangePortfolioUpdate(gConn,gfxdProc, 
          cid1, cid2, subTotal, tid, successForHA); //retry
    }
  }
  
  protected static void callGfxdProceduresByCidRangePortfolioUpdate(Connection conn, 
      String proc, int cid1, int cid2, BigDecimal subTotal, int tid, boolean[] success) {
    String sql = "{call " + proc + "(?, ?, ?, ?)" + 
      " ON Table trade.portfolio where cid > " + cid1 + " and cid< " + cid2 + " and tid=" + tid
      + "}";
    Log.getLogWriter().info("call gfxd procedure " + sql + ", myTid is " + tid);
    success[0] = true;
    try {
      callProcedureByCidRangePortfolioUpdate(conn, sql, cid1, cid2, subTotal, tid);
    } catch (SQLException se) {
      if (se.getSQLState().equals("X0Z01") && isHATest) {
        //node went down during procedure call
        Log.getLogWriter().info("got expected exception X0Z01, continuing test");
        success[0] = false; //whether process the resultSet or not
      } else {
        checkGFGFXDException(se);
      }
    }  
  }
  
  protected void callProceduresSellordersSGUpdate(Connection gConn, String gfxdProc) {
    if (updateSellordersOnOrderTime()) return; //do not call update if on partitioned key
    
    int tid = getMyTid();
    Timestamp orderTime = new Timestamp (System.currentTimeMillis());
    
    boolean[] successForHA = new boolean[1];
    callGfxdProceduresSellordersSGUpdate(gConn,gfxdProc, 
        orderTime, tid, successForHA);
    while (!successForHA[0]) {
      Log.getLogWriter().info("failed to get result set from gfxd");
      callGfxdProceduresSellordersSGUpdate(gConn,gfxdProc, 
          orderTime, tid, successForHA); //retry
    }    
  }
  
  protected static void callGfxdProceduresSellordersSGUpdate(Connection conn, 
      String proc, Timestamp orderTime, int tid, boolean[] success) {
    String sellordersSG = SQLDAPTest.getServerGroups("sellorders");
    String sgOption;
    
    //sellordersSG is null only occurs when schema created on default servers
    //so data could be on any data nodes/servers, calling needs to reflect this
    //accordingly
    if (sellordersSG != null) {
      sgOption = SQLTest.random.nextInt(10) == 1 ?
        " ON ALL" :" ON SERVER GROUPS (" + sellordersSG + ")";
    } else {
      sgOption =  " ON ALL" ; 
      //on all server, with no on-clause, the procedure only invoked in one coordinate node
    }
    String sql = "{call " + proc + "(?, ?)" + sgOption + "}";
    Log.getLogWriter().info("call gfxd procedure " + sql + ", myTid is " + tid);
    success[0] = true;
    try {
      callProcedureSellordersSGUpdate(conn, sql, orderTime, tid);
    } catch (SQLException se) {
      if (se.getSQLState().equals("X0Z01") && isHATest) {
        //node went down during procedure call
        Log.getLogWriter().info("got expected exception X0Z01, continuing test");
        success[0] = false; //whether process the resultSet or not
      } else {
        checkGFGFXDException(se);
      }
    }  
  }
  
  protected static void printMetaData(ResultSet rs) {
	int numOfColumns;
    ResultSetMetaData rsmd;
    try {
      rsmd = rs.getMetaData();
      numOfColumns = rsmd.getColumnCount();

    } catch (SQLException se) {
      throw new TestException ("could not get resultSet metaData" + TestHelper.getStackTrace(se));
    }

    ObjectType[] oTypes = new ObjectType[numOfColumns];
    String[] fieldNames = new String[numOfColumns];
    try {
      for (int i=0; i<numOfColumns; i++) {
        oTypes[i] = new ObjectTypeImpl (Class.forName(rsmd.getColumnClassName(i+1))); //resultSet column starts from 1
        fieldNames[i] = rsmd.getColumnName(i+1); //resultSet column starts from 1
        Log.getLogWriter().info("field name " + i + " is " + fieldNames[i] + " type is " + rsmd.getColumnClassName(i+1));
      }
    } catch (SQLException se) {
      throw new TestException ("could not getStruct from resultSet\n" + TestHelper.getStackTrace(se));
    } catch (ClassNotFoundException cnfe) {
      throw new TestException ("no class available for a column in result set\n" + TestHelper.getStackTrace(cnfe));
    }
  }
}
