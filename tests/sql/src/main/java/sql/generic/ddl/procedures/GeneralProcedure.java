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
import hydra.MasterController;
import hydra.TestConfig;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import sql.SQLHelper;
import sql.SQLPrms;
import sql.SQLTest;
import sql.generic.SQLGenericBB;
import sql.generic.SQLGenericTest;
import sql.generic.SQLOldTest;
import sql.generic.SqlUtilityHelper;
import sql.generic.ddl.Executor;
import sql.generic.ddl.Executor.ExceptionAt;
import sql.generic.ddl.create.DDLStmtIF;
import sql.generic.ddl.procedures.ProcedureExceptionHandler.Action;
import sql.generic.dmlstatements.GenericDMLHelper;
import sql.sqlutil.ResultSetHelper;
import util.TestException;

public abstract  class GeneralProcedure implements DDLStmtIF{
  
  abstract protected String  getProcName();
  abstract protected String  getDerbyProcName();
 
  abstract protected String  getDdlStmt();
  abstract protected String  getDerbyDdlStmt();   
  abstract protected CallableStatement getCallableStatement(Connection conn) throws SQLException;
  abstract protected CallableStatement getDerbyCallableStatement(Connection conn) throws SQLException;
  abstract protected int getOutputResultSetCount();
  protected abstract ResultSet[]  getOutputValues(CallableStatement cs , Object[] inOut) throws SQLException;
  abstract protected void verifyInOutParameters( Object[] derby , Object[] gemxd);
  abstract protected void populateCalledFunctionInfoInBB();
  abstract public void callProcedure() ;
  
  Executor executor;
  Connection dConn, gConn;
  public static boolean testServerGroupsInheritence = TestConfig.tab().booleanAt(SQLPrms.testServerGroupsInheritence,false);
  ProcedureExceptionHandler exceptionHandler;
  protected int loc =0;
  protected int maxNumOfTries = 1;
  protected int resultSetCount;
  protected static boolean dropProc = SQLOldTest.dropProc;
  protected String ddlStmt = null, derbyStmt = null;
  protected String derbyProcName = null , procName =null;
  
  
  public void setExecutor(Executor executor , int loc){
    this.executor = executor;
    dConn = executor.getDConn();
    gConn = executor.getGConn();
    this.loc =loc;  
    ddlStmt= getDdlStmt();
    derbyStmt = getDerbyDdlStmt();
    derbyProcName = getDerbyProcName();
    procName=getProcName();
    populateCalledFunctionInfoInBB();
  }
  
  
  public void setExecutor(Executor executor ){
    this.executor = executor;
    dConn = executor.getDConn();
    gConn = executor.getGConn();
    ddlStmt=getDdlStmt();
    derbyStmt=getDerbyDdlStmt(); 
    derbyProcName = getDerbyProcName();
    procName=getProcName();
    populateCalledFunctionInfoInBB();
      Map<String, ArrayList<String>> procMap = (Map<String, ArrayList<String>>) SQLGenericBB.getBB().getSharedMap().get(SQLOldTest.FuncProcMap);
      if (procMap == null){
        SQLGenericBB.getBB().getSharedMap().put( SQLOldTest.FuncProcMap, new HashMap<String,ArrayList<String>>());
      }    
    }
  
  

  public void createDDL(){
    createProcedures();
  }
  
  public  void createProcedures() {
    SQLGenericBB.getBB().getSharedMap().put(procName.toUpperCase(), loc);    
    executor.executeOnBothDb(derbyStmt,ddlStmt, new ProcedureExceptionHandler(Action.CREATE , false));
  }
  
  
  //only one thread can do operation on the procedure, to see if any other thread is 
  //operating on the procedure
  protected boolean doOp() {   
    int doOp = getCounter();
    int count =0;
    while (doOp != 1) {
      if (count >maxNumOfTries) return false;  //try to operation, if not do not operate
      count ++;
      MasterController.sleepForMs(100 * SQLOldTest.random.nextInt(30)); //sleep from 0 - 2900 ms
      doOp = getCounter();
    }  
    return true;
  }  

  
 
  
//get counter to see if there is thread operation on the procedure
  //need to change generically
  protected  int getCounter() {
    //no concurrent dropping procedure, so allow concurrent procedure calls     
      int counter = (int)SQLGenericBB.getBB().getSharedCounters().incrementAndRead(SQLGenericBB.procedureCounter[loc]);      
      return counter;
}
  
  //zero counter after finished the operation
  //need to change generically
  protected void zeroCounter() {      
      SQLGenericBB.getBB().getSharedCounters().zero(SQLGenericBB.procedureCounter[loc]);
  }  
  
    
  private void execute(String derbyStmt , String gemxdStmt){
    if (dConn != null )
      if ( executor.executeOnBothDb(derbyStmt , gemxdStmt ,new ProcedureExceptionHandler(Action.CREATE , false)) == ExceptionAt.DERBY ) {
         zeroCounter();
      }
  
  else 
    executor.executeOnGfxdOnly( gemxdStmt, new ProcedureExceptionHandler(Action.CREATE , false));
  }
  
  protected void createDropProc(){
    //some operations will drop the procedure, some will create; some will do drop and create
    int choice = 3; //3 choices, 0 do drop only, 1 both drop and create, 2 create only
    int op = SQLOldTest.random.nextInt(choice);
    //int op = 1; //TODO, comment out this line, so it will be random.
      if ((op == 0 || op ==1) && dropProc) {        
        String dropStmt = "drop procedure " + getProcName();
        execute(dropStmt, dropStmt);        
    }

    if (op == 1 || op ==2) {
       execute(derbyStmt, ddlStmt );
    }
    
    if (dConn !=null){ 
       zeroCounter();
    } 
  }
  
  
  protected  boolean compareResultSets(ResultSet[] derbyRS, ResultSet[] gfeRS) {
    boolean success = true;
    if (derbyRS.length != gfeRS.length) {
      throw new TestException("got different number of resultSets: there are" +
          derbyRS.length + "result sets from derby and " + gfeRS.length + "result sets from gfe");
    } 
    for (int i=0; i<derbyRS.length; i++) {      
      success = compareResultSets(derbyRS[i], gfeRS[i]);
      if (!success) return success;
    }
    return success;
  }
  
  protected  boolean compareResultSets(ResultSet derbyRS, ResultSet gfeRS)  {
    try{
      if (derbyRS != null)
    Log.getLogWriter().info("taking metadata of derby " + derbyRS.getMetaData() );
      if (gfeRS!= null ) 
    Log.getLogWriter().info("taking metadata of gemxd  " + gfeRS.getMetaData() );
    }catch (SQLException se){
      Log.getLogWriter().info("error while reading metadata" );
    }
    return ResultSetHelper.compareResultSets(derbyRS, gfeRS);
  }
  
  protected  void processRS(ResultSet[] rs){
    for (int i=0; i<rs.length; i++) {
      List<com.gemstone.gemfire.cache.query.Struct> list = ResultSetHelper.asList(rs[i], false);
      if (list == null & SQLOldTest.isHATest) return; //may encounter node down in HA tests
    }
  }
  
//this returns result set
  protected  ResultSet[] callDerbyProcedure( Object[] inOut, boolean[] success) {    
    success[0] = true;
    try {
      return  callProcedure(dConn, inOut);
    } catch (SQLException se) {     
      if (! exceptionHandler.handleDerbyException(dConn, se) ) {      
        success[0] = false;
        return null;
      }
      }
    return null;
  }
  
  //returns result set
  protected ResultSet[] callGFEProcedure(Object[] inOut, boolean[] success) {
    success[0] = true;
    try {
      return callProcedure(gConn, inOut);
    } catch (SQLException se) {          
      success[0] =  exceptionHandler.handleGfxdExceptionOnly(se);
    }  
    return null; // this line won't be executed, either test failed with TestException or resultSet[] is returned
  }
  
  protected  ResultSet[] callProcedure(Connection conn , Object[] inOut) throws SQLException { 
    ResultSet[] rs = new ResultSet[getOutputResultSetCount()];
    CallableStatement cs = null;
    if (GenericDMLHelper.isDerby(conn)) {
       cs = getDerbyCallableStatement(conn);
    }
    else {
       cs = getCallableStatement(conn);
    }    
    cs.execute();     
    rs = getOutputValues(cs, inOut);   
    SQLWarning warning = cs.getWarnings(); //test to see there is a warning
    if (warning != null) {
      SQLHelper.printSQLWarning(warning);
    } 
    return rs;
  }
  
protected ResultSet[]  getOutputResultset(CallableStatement cs  , int resultSetCount) throws SQLException {
  ResultSet[] rs = new ResultSet[resultSetCount];
  if (resultSetCount ==0 ) return null; 
  
  if (resultSetCount>0) {
    rs[0] = cs.getResultSet();
    int i=1;
    while (cs.getMoreResults(Statement.KEEP_CURRENT_RESULT)) {
      Log.getLogWriter().info("has more results");
          rs[i] = cs.getResultSet();
          i++;
    }
    
  }    
    return rs;
  }
}
