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
  private static String selectBuyOrderByTidListSql = "select * " +
      "from trade.buyorders where oid <? and tid= ?";
  private static String selectPortfolioByCidRangeSql = "select * " +
  "from trade.portfolio where cid >? and cid <? and sid<? and tid= ?";
  private static String selectPortfolioByCidRangeSql2 = "select cid, sid, subTotal " +
  "from trade.portfolio where cid >? and cid <? and tid= ?";
  private static String updatePortfolioByCidRangeSql = "update trade.portfolio " +
  "set subTotal=? where cid> ? and cid < ? and tid=?";
  private static String updateSellordersSGSql = "update trade.sellorders set order_time=? " +
      "where tid = ?";
  private static String selectCustomerSql = "select cust_name from trade.customers " +
  "where cid>? and cid< ? and tid = ?";
  
  //static ProcedureExecutionContext context;
  
  /* result set only, will use the default procedureResultProcess */
  @SuppressWarnings("unchecked")
  public static void selectGfxdBuyordersByTidList(int oid, int tid, ResultSet[] rs,
      ProcedureExecutionContext context) throws SQLException {
  //public static void selectGfxdBuyordersByTidList(int oid, int tid, ResultSet[] rs,
  //    ProcedureExecutionContext c) throws SQLException {     
  //  context = c;
  // to reproduce 
    Connection conn = context.getConnection();
    
    if (context.getTableName() == null) {
      throw new TestException("ProcedureExecutionContext.getTableName() is : "
          + context.getTableName());
    }
    if (!selectBuyOrderByTidListSql.contains(context.getTableName())){
      throw new TestException("ProcedureExecutionContext.getTableName() is incorrect: "
          + context.getTableName());
    }  //#42836 is fixed
    if (!SQLTest.isHATest && context.isPossibleDuplicate()) {
      throw new TestException("possible duplicate is true and this is not a HA test.");
    }
    
    /*
    boolean isPortfolioPartitioned = ((ArrayList<String> )SQLBB.getBB().getSharedMap().
        get("buyordersPartition")).size() !=0;
    if (context.isPartitioned(context.getTableName()) != isPortfolioPartitioned) {
      throw new TestException("context reports that the table partition is " + 
          context.isPartitioned(context.getTableName()) + ", but it is " + isPortfolioPartitioned);
    }  //work around #43039select cid, sid, subTotal "
    */
    //test only with invocation with where clause so the <local> is optional here
    //String withLocal = (oid/2==0)? LOCAL : "" ;
    String withLocal = LOCAL;
    String sql = withLocal + selectBuyOrderByTidListSql;
    PreparedStatement ps1 = conn.prepareStatement(sql);
    Log.getLogWriter().info("executing the following query: " + sql);
    ps1.setInt(1, oid);
    ps1.setInt(2, tid);

    rs[0] = ps1.executeQuery();    
    
    //conn.close();
  }
  
  public static void getListOfGfxdBuyordersByTidList(int oid, int tid, List<Struct> rsList,
      ProcedureExecutionContext context) throws SQLException {
    Connection conn = context.getConnection();
    if (!selectBuyOrderByTidListSql.contains(context.getTableName())){
      throw new TestException("ProcedureExecutionContext.getTableName() is incorrect: "
          + context.getTableName());
    }
    if (!SQLTest.isHATest && context.isPossibleDuplicate()) {
      throw new TestException("possible duplicate is true and this is not a HA test.");
    }
    
    /* work around #43039
    if (!context.isPartitioned(context.getTableName())) {
      throw new TestException("context reports that the table is not partitioned.");
    }
    */ 
    
    //test only with invocation with where clause so the <local> is optional here
    //String withLocal = (oid/2==0)? LOCAL : "" ;   //work around #42839
    String withLocal = LOCAL;
    String sql = withLocal + selectBuyOrderByTidListSql;
    PreparedStatement ps1 = conn.prepareStatement(sql);
    Log.getLogWriter().info("executing the following query: " + sql);
    ps1.setInt(1, oid);
    ps1.setInt(2, tid);
  
    ResultSet rs = ps1.executeQuery();
    rsList = (List<Struct>) ResultSetHelper.asList(rs, false);
    
    rs.close(); //useCase2.
    
    ps1.close();
  
    //conn.close();
  }
  
  public static void selectGfxdPortfolioByCidRange(int cid1, int cid2, int sid, int tid, 
      int[] data, ResultSet[] rs, ResultSet[] rs2, ResultSet[] rs3, 
      ResultSet[] rs4, ProcedureExecutionContext context) 
      throws SQLException {
    Connection conn = context.getConnection();
    if (!SQLTest.isHATest && context.isPossibleDuplicate()) {
      throw new TestException("possible duplicate is true and this is not a HA test.");
    }
    String whereClause = context.getFilter();
    Log.getLogWriter().info("where clause is " + whereClause);
    if (!whereClause.contains(Integer.toString(cid1)) && !whereClause.contains(Integer.toString(cid2)))
      throw new TestException ("ProcedureExecutionContext.getFilter() does not have correct info: " 
          + context.getFilter());
    
    //test only with invocation with where clause so the <local> is optional here
    //String withLocal = (sid/2==0)? LOCAL : "" ;
    
    String withLocal = LOCAL;
    String sql = withLocal + selectPortfolioByCidRangeSql;
    ResultSet newrs = null;
    try {
      PreparedStatement ps1 = conn.prepareStatement(sql);
      Log.getLogWriter().info("executing the following query: " + sql);
      ps1.setInt(1, cid1);
      ps1.setInt(2, cid2);
      ps1.setInt(3, sid);
      ps1.setInt(4, tid);
  
      rs[0] = ps1.executeQuery();    
    } catch (SQLException se) {
      SQLHelper.printSQLException(se);
      Log.getLogWriter().warning(TestHelper.getStackTrace(se));
      throw se;
    }
    
    sql = withLocal + selectPortfolioByCidRangeSql2;
    try {
      PreparedStatement ps2 = conn.prepareStatement(sql);
      Log.getLogWriter().info("executing the following query: " + sql);
      ps2.setInt(1, cid1);
      ps2.setInt(2, cid2);
      ps2.setInt(3, tid);
  
      rs2[0] = ps2.executeQuery();    
    } catch (SQLException se) {
      SQLHelper.printSQLException(se);
      Log.getLogWriter().warning(TestHelper.getStackTrace(se));
      throw se;
    }
    
    PreparedStatement ps3 = conn.prepareStatement(sql);
    try {
     
      Log.getLogWriter().info("executing the following query: " + sql);
      ps3.setInt(1, cid1);
      ps3.setInt(2, cid2);
      ps3.setInt(3, tid);
  
      newrs = ps3.executeQuery();    
    } catch (SQLException se) {
      SQLHelper.printSQLException(se);
      Log.getLogWriter().warning(TestHelper.getStackTrace(se));
      throw se;
    }
    
    OutgoingResultSet ors = context.getOutgoingResultSet(3);
    int numOfColumns = newrs.getMetaData().getColumnCount();
    for (int i=1; i<=numOfColumns; i++) {
      ors.addColumn(newrs.getMetaData().getColumnName(i));
    }
    
    while (newrs.next()) {
      List<Object> row=new ArrayList<Object>(numOfColumns);
      for (int i=1; i<=numOfColumns; i++) {        
        row.add(newrs.getObject(i));
        Log.getLogWriter().info("add " + newrs.getObject(i) + " to column " + 
            newrs.getMetaData().getColumnName(i) +
        		" for Outgoing result set r3");
      }
      
      ors.addRow(row);
      Log.getLogWriter().info("add the row for Outgoing result set r3");
    }  
    ors.endResults();
       
    OutgoingResultSet ors2 = context.getOutgoingResultSet(4); //test only one node is invoked
    ors2.addColumn("f1");
    List<Object> row=new ArrayList<Object>();
    int i = SQLTest.random.nextInt();
    Log.getLogWriter().info("add " + i + " to f1 for Outgoing result set r4");
    row.add(i);
    ors2.addRow(row);
    ors2.endResults();   
    
    data[0] = 1;
    
    //newrs.close();
    //ps3.close();
    
    //conn.close();
  }
  
  @SuppressWarnings("unchecked")
  public static void updateGfxdPortfolioWrongSG(BigDecimal subTotal, int qty, 
      ProcedureExecutionContext context) throws SQLException {
    Connection conn = context.getConnection();
    if (!SQLTest.isHATest && context.isPossibleDuplicate()) {
      throw new TestException("possible duplicate is true and this is not a HA test.");
    }
    String whereClause = context.getFilter();
    Log.getLogWriter().info("where clause is " + whereClause);
    ArrayList<String> portfolioSGList = (ArrayList<String>)SQLBB.getBB().getSharedMap().
        get("portfolioSG" + SQLDAPTest.dapList);
    for (String sg: portfolioSGList) {
      if (whereClause.contains(sg))
        throw new TestException ("Wrong server groups are used to call this DAP. context filter is: " 
          + context.getFilter());   
    }
    
    PreparedStatement ps = null;
    //String withLocal = (qty/2==0)? LOCAL : "" ;
    String withLocal = LOCAL;
    String sql = withLocal + "update trade.portfolio set subTotal=? where qty > ?";
    try {
       ps = conn.prepareStatement(sql) ;
       Log.getLogWriter().info("executing the following update statement: " + sql);
    } catch (SQLException se) {
      if (se.getSQLState().equals("0A000")) {
        Log.getLogWriter().info("get expected non supported exception when updating " +
             "columns in portfolio table");
        return;
      }
    }
    
    ps.setBigDecimal(1, subTotal);
    ps.setInt(2, qty);
    int count = ps.executeUpdate();
    if (count !=0) throw new TestException("update porfolio in wrong server groups," +
        " and it is successful. The updated count is " + count);
    
    //conn.close();
  }
  
  public static void updateGfxdPortfolioByCidRange(int cid1, int cid2, BigDecimal subTotal, 
      int tid, ProcedureExecutionContext context) throws SQLException {
    Connection conn = context.getConnection();
    if (!SQLTest.isHATest && context.isPossibleDuplicate()) {
      throw new TestException("possible duplicate is true and this is not a HA test.");
    }
    String whereClause = context.getFilter();
    Log.getLogWriter().info("where clause is " + whereClause);
    
    PreparedStatement ps = null;
    //String withLocal = (qty/2==0)? LOCAL : "" ;
    String withLocal = LOCAL;
    String sql = withLocal + updatePortfolioByCidRangeSql;
    
    ps = conn.prepareStatement(sql) ;
    Log.getLogWriter().info("executing the following update statement: " + sql);
    ps.setBigDecimal(1, subTotal);
    ps.setInt(2, cid1);
    ps.setInt(3, cid2);
    ps.setInt(4, tid);
    int count = ps.executeUpdate();
    Log.getLogWriter().info("update gemfirexd portfolio, updated count is " + count);
    
    //conn.close();
  }
  
  @SuppressWarnings("unchecked")
  public static void updateGfxdSellordersSG(Timestamp orderTime, int tid, 
      ProcedureExecutionContext context) throws SQLException {
    Connection conn = context.getConnection();
    if (!SQLTest.isHATest && context.isPossibleDuplicate()) {
      throw new TestException("possible duplicate is true and this is not a HA test.");
    }
    int myVMId = RemoteTestModule.getMyVmid();
    String mySG = (String) SQLDAPBB.getBB().getSharedMap().get("dataStoreSG" + myVMId);
    ArrayList<String> sellordersSGList = (ArrayList<String>)SQLBB.getBB().getSharedMap().
        get("sellordersSG" + SQLDAPTest.dapList);
    for (String sg: sellordersSGList) {
      /* work around #42989
      if (!mySG.contains(sg)) {
        Log.getLogWriter().warning("Wrong server groups may be used to call this DAP " +
            "unless ON ALL is used here: " + " this node hosts " + mySG + 
            " but sellorders was created in " + sg);   
      }
      */
    }
    
    PreparedStatement ps = null;
    //String withLocal = (qty/2==0)? LOCAL : "" ;
    String withLocal = LOCAL;
    String sql = withLocal + updateSellordersSGSql;

    ps = conn.prepareStatement(sql) ;
    Log.getLogWriter().info("executing the following update statement: " + sql);

    ps.setTimestamp(1, orderTime);
    ps.setInt(2, tid);
    int count = ps.executeUpdate();
    Log.getLogWriter().info("gemfirexd updates " + count + " row(s).");
    
    //conn.close();
  }
  
  public static void customGfxdProc(int cid1, int cid2, int tid, ResultSet[] rs, 
      ResultSet[] rs2, ResultSet[] rs3, ProcedureExecutionContext context) throws SQLException {
    Connection conn = context.getConnection();
    String withLocal = LOCAL;
    String sql = withLocal + selectPortfolioByCidRangeSql2;
    
    try {
      Log.getLogWriter().info("executing the following query: " + sql + " with cid1: " + cid1
        + " with cid2: " + cid2 + " with tid: " + tid);
      PreparedStatement ps1 = conn.prepareStatement(sql);
      ps1.setInt(1, cid1);
      ps1.setInt(2, cid2);
      ps1.setInt(3, tid);
  
      rs[0] = ps1.executeQuery();    
    } catch (SQLException se) {
      SQLHelper.printSQLException(se);
      Log.getLogWriter().warning(TestHelper.getStackTrace(se));
      throw se;
    }
    

    sql = withLocal + selectCustomerSql;
    try {
      Log.getLogWriter().info("executing the following query: " + sql + " with cid1: " + cid1
          + " with cid2: " + cid2 + " with tid: " + tid);
      PreparedStatement ps2 = conn.prepareStatement(sql);      
      ps2.setInt(1, cid1);
      ps2.setInt(2, cid2);
      ps2.setInt(3, tid);
      rs2[0] = ps2.executeQuery();
    } catch (SQLException se) {
      SQLHelper.printSQLException(se);
      Log.getLogWriter().warning(TestHelper.getStackTrace(se));
      throw se;
    }

    sql = withLocal + "select cid from trade.customers where cid = ?";
    try {
      Log.getLogWriter().info("executing the following query: " + sql + " with cid1: " + cid1
         );
      PreparedStatement ps3 = conn.prepareStatement(sql);      
      ps3.setInt(1, cid1);
 
      rs3[0] = ps3.executeQuery();    
    } catch (SQLException se) {
      SQLHelper.printSQLException(se);
      Log.getLogWriter().warning(TestHelper.getStackTrace(se));
      throw se;
    }
  }

}
