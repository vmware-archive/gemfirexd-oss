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
package sql.generic.dmlstatements;

import hydra.Log;
import hydra.MasterController;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import sql.SQLHelper;
import sql.generic.SQLOldTest;
import sql.sqlutil.ResultSetHelper;
import util.TestException;
import util.TestHelper;


/**
 * GenericDMLExecutor
 * 
 * @author Namrata Thanvi
 */


public class GenericDMLExecutor implements DMLExecutor {

  private Connection dConn;

  private Connection gConn;

  private boolean verificationWithDerby;

  protected static int maxNumOfTries = 1;

  protected static int retrySleepMs = 100;
  
  boolean success , skipVerification;
  
  public GenericDMLExecutor(Connection dConn, Connection gConn) {
    this.dConn = dConn;
    this.gConn = gConn;
    if ( dConn == null )
      verificationWithDerby = false;
    else
      verificationWithDerby = true;
    
  }

  public GenericDMLExecutor(Connection gConn) {
    this.dConn = null;
    this.gConn = gConn;
    verificationWithDerby = false;
    
  }

  public void executeStatements(List<DMLOperation> operationsList)
      throws TestException {
    for (DMLOperation operation : operationsList) {
      if (operation.getExecutable())
      {
           operation.execute();
      }
    }
  }

  @Override
  public void query(DMLOperation operation)  {
    
    ResultSet discRS = null;
    ResultSet gfeRS = null;
    ArrayList<SQLException> exceptionList = new ArrayList<SQLException>();
    DMLOperationWrapper  wrapper = null;
    SQLException derbyException =null, gemxdException = null;
    if (verificationWithDerby) {
      try {
        
        wrapper=new DMLOperationWrapper(operation, dConn);
        discRS = executeQuery(wrapper);
        
        if (discRS == null) {
          Log
              .getLogWriter()
              .info(
                  "could not get the derby result set after retry, abort this query");
          Log
              .getLogWriter()
              .info(
                  "Could not finish the op in derby, will abort this operation in derby");
        }
      } catch (SQLException se) {
        derbyException=se;
        SQLHelper.handleDerbySQLException(se, exceptionList);
      }

      try {
        try{
        wrapper=new DMLOperationWrapper(operation, gConn);
        } catch (TestException te) {
          Log.getLogWriter().info("Received Test Exception :" + te.getMessage()  + te.getCause() );
          return;
        }
        gfeRS = executeQuery(wrapper);
        
        
        if (gfeRS == null) {
          if (SQLOldTest.isHATest) {
            Log.getLogWriter().info(
                "Testing HA and did not get GFXD result set");
            return;
          }
          else if (SQLOldTest.setCriticalHeap) {
            Log.getLogWriter().info("got XCL54 and does not get query result");
            return;
          }
          else
            throw new TestException(
                "Not able to get gfe result set after retry");
        }
      } catch (SQLException se) {
        gemxdException =se;
        SQLHelper.handleGFGFXDException(se, exceptionList);
      }
      
      if ( gemxdException == null &&   derbyException != null && derbyException.getSQLState().equals("22003")) {
        Log.getLogWriter().info("Received " + derbyException.getErrorCode() + derbyException.getMessage()  + " in Derby. Continuing test ");
        return;
      }
      
      if (derbyException !=null )  SQLHelper.handleDerbySQLException(derbyException, exceptionList);
      if (gemxdException != null ) SQLHelper.handleGFGFXDException(gemxdException, exceptionList);
         SQLHelper.handleMissedSQLException(exceptionList);
      

      if (discRS == null || gfeRS == null)
        return;

      boolean success = ResultSetHelper.compareResultSets(discRS, gfeRS);

      if (!success) {
        Log.getLogWriter().info(
            "Not able to compare results due to derby server error");
      }
    }
    else {
      try {
        try{
        wrapper=new DMLOperationWrapper(operation, gConn);
        } catch (TestException te) {
          Log.getLogWriter().info("Received Test Exception :" + te.getMessage()  + te.getCause() );
          return;
        }
        gfeRS = executeQuery(wrapper);
      } catch (SQLException se) {
        if (se.getSQLState().equals("42502") && SQLOldTest.testSecurity) {
          Log.getLogWriter().info(
              "Got expected no SELECT permission, continuing test");
          return;
        }
        else
          SQLHelper.handleSQLException(se);
      }

      if (gfeRS != null)
        ResultSetHelper.asList(gfeRS, false);
      else if (SQLOldTest.isHATest)
        Log.getLogWriter().info(
            "could not get gfxd query results after retry due to HA");
      else if (SQLOldTest.setCriticalHeap)
        Log.getLogWriter().info(
            "could not get gfxd query results after retry due to XCL54");
      else
        throw new TestException("gfxd query returns null and not a HA test");
    }
  }

  private ResultSet executeQuery(DMLOperationWrapper dmlOperationWrapper)
      throws SQLException {

    boolean[] success = new boolean[1];
    int count = 0;
    ResultSet rs = null;
    while (!success[0]) {
      if (count >= maxNumOfTries) {
        if (dmlOperationWrapper.isDerby())
          Log
              .getLogWriter()
              .info(
                  "Could not get the lock to finisht the op in derby, abort this operation");
        return null;
      }
      ;
      count++;
      MasterController.sleepForMs(GenericDML.rand.nextInt(retrySleepMs));
      rs = executeQuery(dmlOperationWrapper, success);
    }

    return rs;
  }

  private ResultSet executeQuery(DMLOperationWrapper dmlOperationWrapper,
      boolean[] success) throws SQLException {

    ResultSet rs = null;

    success[0] = true;

    try {      
      dmlOperationWrapper.executionMsg(DMLOperationWrapper.MSG_START);
      rs = dmlOperationWrapper.getPreparedStatement().executeQuery();
      dmlOperationWrapper.executionMsg(DMLOperationWrapper.MSG_END);
      return rs;
    } catch (SQLException se) {
      if (!SQLHelper.checkDerbyException(dmlOperationWrapper.getConnection(),
          se))
        success[0] = false;
      else if (!SQLHelper.checkGFXDException(dmlOperationWrapper
          .getConnection(), se))
        success[0] = false;
      else
        throw se;
    }
    return null;
  }

  @Override
  public void executeStatement(DMLOperation operation) {
    List<SQLException> exceptionList = new ArrayList<SQLException>();
    success = true;
    skipVerification = false;
    int derbyCount = 0;
    int gfeCount = 0;
    DMLOperationWrapper gfxdWrapper = null;
    DMLOperationWrapper derbyWrapper = null;
    // need to replace all the code below.

    if (verificationWithDerby) {
      derbyWrapper = new DMLOperationWrapper(operation, dConn);
      derbyCount = executeStatement(derbyWrapper, exceptionList);

      int count = 0;
      while (!success) {
        if (count >= maxNumOfTries) {
          Log.getLogWriter().info("Could not finish the delete op in derby, "
              + "will abort this operation in derby");
          return;
        }
        MasterController.sleepForMs(GenericDML.rand.nextInt(retrySleepMs));
        count++;
        exceptionList.clear();
        derbyCount = executeStatement(derbyWrapper, exceptionList);
        Log.getLogWriter().info(
            "after retrying operation in  derby statement size of exception list is"
                + exceptionList.size() + "success is " + success + "count "
                + derbyCount);
      }

      gfxdWrapper = new DMLOperationWrapper(operation, gConn);
      gfeCount = executeStatement(gfxdWrapper, exceptionList);

      if (derbyCount != gfeCount && !skipVerification) {
        Log.getLogWriter().info(
            "derby has different row count then gemfirexd" + "derby deleted "
                + derbyCount + " gemfirexd deleted " + gfeCount);
        throw new TestException(
            "gemfirexd deleted different row count than derby ");
      }

      if (!skipVerification) {
        SQLHelper.handleMissedSQLException(exceptionList);
      }
    } else {
      gfxdWrapper = new DMLOperationWrapper(operation, gConn);
      executeStatementWOVerification(gfxdWrapper, exceptionList);
    }
  }

  // add the exception to expceptionList to be compared with gfe
  protected int executeStatement(DMLOperationWrapper wrapper,
      List<SQLException> exList) {
    int count = 0;
    success = true;
    skipVerification = false;
    try {
      count = executeStatement(wrapper);
    } catch (SQLException se) {
      Log.getLogWriter().info(" got sql exception  " + se.getMessage());

      if (!wrapper.isDerby()) {
        // check if update on pK then skip it
        Log.getLogWriter().info("handling gfxd exception ");
        if (wrapper.getOperation().getOperation() == Operation.UPDATE
            && se.getSQLState().equals("0A000")
            && GenericDMLHelper.updateOnPk(wrapper.getOperation()
                .getPreparedStmtForLogging())) {
          skipVerification = true;
        } else{
          SQLHelper.handleGFGFXDException(se, exList);
        } 
        rollbackAll();
      } else if (!SQLHelper.checkDerbyException(wrapper.getConnection(), se)) {
        Log.getLogWriter().info("not a derby exception " + se.getMessage());
        success = false;
      } else {
        Log.getLogWriter().info("handling derby  " + se.getMessage());
        SQLHelper.handleDerbySQLException(se, exList); // handle the exception
      }
    }
    return count;
  }

  // compare whether the exceptions got are same as those from derby
  // no verification
  protected int executeStatementWOVerification(DMLOperationWrapper wrapper,
      List<SQLException> exList) {
    int rowCount = 0;
    try {
      rowCount = executeStatement(wrapper);      
    } catch (SQLException se) {
      
      if ((se.getSQLState().equals("42500") || se.getSQLState().equals("42502"))
          && SQLOldTest.testSecurity) {
        Log.getLogWriter().info(
            "Got the expected exception for authorization,"
                + " continuing tests");
      }
      else if ("23505".equals(se.getSQLState())) {
        Log.getLogWriter().info("Got the expected exception due to unique constraint check");
      }
      else if ("23503".equals(se.getSQLState()) )  {
          Log.getLogWriter().info("Got the expected exception due to unique constraint check");
        }
      else if (se.getSQLState().equals("42502") && SQLOldTest.testSecurity) {
        Log.getLogWriter().info("Got the expected exception for authorization," +
        " continuing tests");
      } else if (SQLOldTest.alterTableDropColumn && se.getSQLState().equals("42X14")) {
        Log.getLogWriter().info("Got expected column not found exception in update, continuing test");
      } else if ( se.getSQLState().equals("0A000")  && GenericDMLHelper.updateOnPk(wrapper.getOperation().getPreparedStmtForLogging()) ) {
        Log.getLogWriter().info("Got expected feature not supported exception in update, continuing test");
       } else if (se.getSQLState().equals("23513")){
         Log.getLogWriter().info("detected check constraint violation during update, continuing test");
       } else
        SQLHelper.handleSQLException(se); //handle the exception
      
      rollbackAll();
    }    
  
    return rowCount;
  }

  protected int executeStatement(DMLOperationWrapper wrapper)
      throws SQLException {

    success = true;
    int rowCount = 0;
    Log.getLogWriter().info("now stmt is " + wrapper.getOperation().getPreparedStmtForLogging());
    PreparedStatement stmt = wrapper.getPreparedStatement();
    
    wrapper.executionMsg(DMLOperationWrapper.MSG_START);
    rowCount = stmt.executeUpdate();
    
    Log.getLogWriter().info("after  stmt is " + wrapper.getOperation().getPreparedStmtForLogging());
    
    wrapper.executionMsg(DMLOperationWrapper.MSG_END);
    SQLWarning warning = stmt.getWarnings(); // test to see there is a
    // warning
    if (warning != null) {
      SQLHelper.printSQLWarning(warning);
    }

    return rowCount;
  }

  @Override
  public DBRow.Column getRandomColumnForDML(String query, Object value, int type)
       {
    
    try {
    PreparedStatement stmt = gConn.prepareStatement(query,
        ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    PrepareStatementSetter ps = new PrepareStatementSetter(stmt);
    ps.setValues(value, type);
    ResultSet rs = stmt.executeQuery();
    if (rs.next()) {
      rs.last();
      int randomRow = GenericDML.rand.nextInt(rs.getRow());
      rs.absolute(randomRow == 0 ? 1 : randomRow);
      return new DBRow.Column(rs.getMetaData().getColumnName(1), rs
          .getMetaData().getColumnType(1), rs.getObject(1));
    }
    else {
      return new DBRow.Column(null, 0, value);
    }
    
    } catch (  SQLException  se) {
      throw new TestException (" Error while retrieving a row from database " + se.getSQLState() + TestHelper.getStackTrace(se));
    }
  }

  @Override
  public DBRow getRandomRowForDml(String tableName)  {
  
    String query = "select * from  " + tableName + " where  tid = "
        + GenericDMLHelper.getMyTid();
    try {      
          ResultSet rs = gConn.prepareStatement(query, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY).executeQuery();           
      if (rs.next()){
      rs.last();
      int randomRow = GenericDML.rand.nextInt(rs.getRow());
      rs.absolute(randomRow == 0 ? 1 : randomRow);
      DBRow dbRow = new DBRow(tableName);
      for (int column = 1; column <= rs.getMetaData().getColumnCount(); column++) {
        dbRow.addColumn(rs.getMetaData().getColumnName(column).toLowerCase(),
            rs.getMetaData().getColumnType(column), rs.getObject(rs
                .getMetaData().getColumnName(column)));
      }      
      return dbRow;
      }
      else {       
          Log.getLogWriter().info(" Not a single row found in table :" + tableName + "with tid" +  GenericDMLHelper.getMyTid() + " for query " + query  + "resultset is " + rs); 
          return null;
      }
    } catch (SQLSyntaxErrorException se ) {
      Log.getLogWriter().info(" syntax issue : " + se.getMessage() + " Executed DML : " + query); 
      return null;
      
    }catch (SQLException se) {
      if(se instanceof SQLException && !SQLHelper.checkGFXDException(gConn, se)){
        SQLHelper.printSQLException(se);
        Log.getLogWriter().info("Got expected Exception, continue the test...");
        return null;
      }else{
        throw new TestException (" Error while retrieving a row from database " + se.getSQLState() + TestHelper.getStackTrace(se));  
      }
    } 
  }

  public List<DBRow> getRandomRowsForDml(String query, Object value, int type,
      int rows, String tableName)  {
    int currentRow = 0;
    List<DBRow> finalRows = new LinkedList<DBRow>();
    try {
      PreparedStatement stmt = gConn.prepareStatement(query);
      PrepareStatementSetter ps = new PrepareStatementSetter(stmt);
      ps.setValues(value, type);
      ResultSet rs = stmt.executeQuery();
      while (rs.next() && currentRow++ < rows) {
        DBRow dbRow = new DBRow(tableName);
        for (int column = 1; column <= rs.getMetaData().getColumnCount(); column++) {
          dbRow.addColumn(rs.getMetaData().getColumnName(column).toLowerCase(),
              rs.getMetaData().getColumnType(column), rs.getObject(rs
                  .getMetaData().getColumnName(column)));
        }
        finalRows.add(dbRow);
      }
      return finalRows;

    } catch (SQLException se) {
        throw new TestException (" Error while retrieving a row from database " + se.getSQLState() + TestHelper.getStackTrace(se));
    }
  }
  
  
  public void commitAll() {
    if (verificationWithDerby) {
      commitDerby();
    }
    commitGfxd();
  }
  
  public void rollbackAll() {
    if (verificationWithDerby) {
      rollbackDerby();
    }
    rollbackGfxd();
  }
  
  public void commitDerby (){
    try{
      Log.getLogWriter().info(" Derby - Commit  started " );
      dConn.commit();
      Log.getLogWriter().info(" Derby - Commit  Completed " );
    } catch (SQLException se) {
      throw new TestException(" Error while commiting the derby database " + " sqlState : " + se.getSQLState()  + " error message : " + se.getMessage()  + TestHelper.getStackTrace(se));
    }
  }
  
  public void commitGfxd (){
    try{
      Log.getLogWriter().info(" Gfxd - Commit  started " );
      gConn.commit();
      Log.getLogWriter().info(" Gfxd - Commit  Completed " );
    } catch (SQLException se) {
      throw new TestException(" Error while commiting the Gfxd database " + " sqlState : " + se.getSQLState()  + " error message : " + se.getMessage()  + TestHelper.getStackTrace(se));
    }
  }
  
  public void rollbackDerby(){
    try{
      Log.getLogWriter().info(" Derby - Rollback  started " );
      dConn.rollback();
      Log.getLogWriter().info(" Derby - Rollback  Completed " );
    } catch (SQLException se) {
      throw new TestException(" Error while doing rollback derby database " + " sqlState : " + se.getSQLState()  + " error message : " + se.getMessage()  + TestHelper.getStackTrace(se));
    }
  }
  
  
  public void rollbackGfxd(){
    try{
      Log.getLogWriter().info(" Gfxd - Rollback  started " );
      gConn.rollback();
      Log.getLogWriter().info(" Gfxd - Rollback  Completed " );
    } catch (SQLException se) {
      throw new TestException(" Error while doing rollback Gfxd database " + " sqlState : " + se.getSQLState()  + " error message : " + se.getMessage()  + TestHelper.getStackTrace(se));
    }
  }

}
