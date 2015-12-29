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
package objects.query.largeobject;

import com.gemstone.gemfire.cache.Region;

import hydra.BasePrms;
import hydra.GsRandom;
import hydra.HydraConfigException;
import hydra.Log;
import hydra.TestConfig;

import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import objects.query.BaseSQLQueryFactory;
import objects.query.QueryObjectException;
import objects.query.QueryPrms;
import objects.query.SQLQueryFactory;
import objects.query.sector.SectorPrms;

import util.TestException;

public class SQLLargeObjectQueryFactory extends BaseSQLQueryFactory implements
    SQLQueryFactory {

  protected static final GsRandom rng = TestConfig.tab().getRandGen();

  protected static final String FIELD_SPEC = " stringField1 varchar(20), stringField2 varchar(20), stringField3 varchar(20), stringField4 varchar(20), stringField5 varchar(20), stringField6 varchar(20), stringField7 varchar(20), stringField8 varchar(20), stringField9 varchar(20), stringField10 varchar(20), stringField11 varchar(20), stringField12 varchar(20), stringField13 varchar(20), stringField14 varchar(20), stringField15 varchar(20), stringField16 varchar(20), stringField17 varchar(20), stringField18 varchar(20), stringField19 varchar(20), stringField20 varchar(20), intField1 int, intField2 int, intField3 int, intField4 int, intField5 int, intField6 int, intField7 int, intField8 int, intField9 int, intField10 int, intField11 int, intField12 int, intField13 int, intField14 int, intField15 int, intField16 int, intField17 int, intField18 int, intField19 int, intField20 int ";

  /*
   * for performance purposes, used to store total positions so 
   * that we don't need to recalculate for every query.
   */
  private int totalPositions;

  public SQLLargeObjectQueryFactory() {
  
  }

  //--------------------------------------------------------------------------
  // Constraints
  //--------------------------------------------------------------------------
  public List getConstraintStatements() {
    return new ArrayList();
  }

  //--------------------------------------------------------------------------
  // QueryFactory : Query Types
  //--------------------------------------------------------------------------
  public int getQueryType() {
    return LargeObjectPrms.getQueryType(QueryPrms.GFXD);
  }
  
  public int getUpdateQueryType() {
    return LargeObjectPrms.getUpdateQueryType(QueryPrms.GFXD);
  }
  
  public int getDeleteQueryType() {
    return LargeObjectPrms.getDeleteQueryType(QueryPrms.GFXD);
  }

  public String getQuery(int queryType, int i) {
    String query = "";
    int type = queryType;
    switch (type) {
      case LargeObjectPrms.RANDOM_EQUALITY_ON_LARGE_OBJECT_ID_QUERY:
        query = getRandomEqualityOnLargeObjectIdQuery();
        break;
      case LargeObjectPrms.PUT_NEW_LARGE_OBJECT_BY_LARGE_OBJECT_ID_QUERY:
        query = getPutNewLargeObjectByLargeObjectIdQuery(i);
        break;
      case LargeObjectPrms.GET_AND_PUT_LARGE_OBJECT_BY_LARGE_OBJECT_ID_QUERY:
        query = getGetAndPutLargeObjectByLargeObjectIdQuery(i);
        break;
      case LargeObjectPrms.DELETE_LARGE_OBJECT_BY_LARGE_OBJECT_ID_QUERY:
        query = getDeleteLargeObjectByLargeObjectIdQuery(i);
        break;
      default:
        throw new UnsupportedOperationException("Should not happen");
    }
    return query;
  }

  public String getPreparedQuery(int queryType) {
    String query = "";
    int type = queryType;
    switch (type) {
      case LargeObjectPrms.RANDOM_EQUALITY_ON_LARGE_OBJECT_ID_QUERY:
        query = getPreparedStatementForRandomEqualityOnLargeObjectIdQuery();
        break;
      case LargeObjectPrms.PUT_NEW_LARGE_OBJECT_BY_LARGE_OBJECT_ID_QUERY:
        query = getPreparedStatementForPutNewLargeObjectByLargeObjectIdQuery();
        break;
      case LargeObjectPrms.GET_AND_PUT_LARGE_OBJECT_BY_LARGE_OBJECT_ID_QUERY:
        query = getPreparedStatementForGetAndPutLargeObjectByLargeObjectIdQuery();
        break;
      case LargeObjectPrms.DELETE_LARGE_OBJECT_BY_LARGE_OBJECT_ID_QUERY:
        query = getPreparedStatementForDeleteLargeObjectByLargeObjectIdQuery();
        break;
      default:
        throw new UnsupportedOperationException("Should not happen");
    }
    return query;
  }

  //--------------------------------------------------------------------------
  // QueryFactory : primary keys
  //--------------------------------------------------------------------------
  public String getPrimaryKeyIndexOnLargeObjectId() {
    return "alter table " + LargeObject.getTableName() + " add primary key(id)";
  }

  //--------------------------------------------------------------------------
  // QueryFactory : indexes
  //--------------------------------------------------------------------------
  public List getIndexStatements() {
    List stmts = new ArrayList();
    Vector indexTypes = LargeObjectPrms.getIndexTypes();
    for (Iterator i = indexTypes.iterator(); i.hasNext();) {
      String indexTypeString = (String) i.next();
      int indexType = LargeObjectPrms.getIndexType(indexTypeString);
      String stmt = getIndexStatement(indexType);
      if (stmt != null) {
        stmts.add(stmt);
      }
    }
    return stmts;
  }

  public String getIndexStatement(int type) {
    String query;
    switch (type) {
      case LargeObjectPrms.PRIMARY_KEY_INDEX_ON_LARGE_OBJECT_ID_QUERY:
        query = getPrimaryKeyIndexOnLargeObjectId();
        break;
      case LargeObjectPrms.NO_QUERY:
        query = null;
        break;
      default:
        throw new UnsupportedOperationException("Should not happen");
    }
    return query;
  }
  
  //--------------------------------------------------------------------------
  // QueryFactory : Inserts
  //--------------------------------------------------------------------------

  public List getInsertStatements(int bid) {
    List stmts = new ArrayList();
    int numLargeObjects = LargeObjectPrms.getNumLargeObjects();
    if (bid >= numLargeObjects) {
      String s =
          "Attempt to get insert statement with bid=" + bid + " when "
              + BasePrms.nameForKey(LargeObjectPrms.numLargeObjects) + "=" + numLargeObjects;
      throw new QueryObjectException(s);
    }
  
    String stmt =
        "insert into " + LargeObject.getTableName()
            + " values (" + bid + ","
            + "'stringField1 " + bid + "'," + "'stringField2 " + bid + "',"
            + "'stringField3 " + bid + "'," + "'stringField4 " + bid + "',"
            + "'stringField5 " + bid + "'," + "'stringField6 " + bid + "',"
            + "'stringField7 " + bid + "'," + "'stringField8 " + bid + "',"
            + "'stringField9 " + bid + "'," + "'stringField10 " + bid + "',"
            + "'stringField11 " + bid + "'," + "'stringField12 " + bid + "',"
            + "'stringField13 " + bid + "'," + "'stringField14 " + bid + "',"
            + "'stringField15 " + bid + "'," + "'stringField16 " + bid + "',"
            + "'stringField17 " + bid + "'," + "'stringField18 " + bid + "',"
            + "'stringField19 " + bid + "'," + "'stringField20 " + bid + "',"
            
            + bid + "," +  bid + "," + bid + "," + bid + "," + bid + "," 
            + bid + "," +  bid + "," + bid + "," + bid + "," + bid + ","
            + bid + "," +  bid + "," + bid + "," + bid + "," + bid + "," 
            + bid + "," + bid + "," +  bid + "," + bid + "," + bid
            
            + ")";
    stmts.add(stmt);
    return stmts;
  }

  public List getPreparedInsertStatements() {
    List stmts = new ArrayList();
    String stmt = "insert into " + LargeObject.getTableName() + " values (?, " +
    		"?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?," +
    		"?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
    stmts.add(stmt);
    return stmts;
  }

  public int fillAndExecutePreparedInsertStatements(List pstmts, List stmts, int i)
      throws SQLException {
    int numLargeObjects = LargeObjectPrms.getNumLargeObjects();
    PreparedStatement pstmt = (PreparedStatement) pstmts.get(0);
    int results = 0;
    if (i >= numLargeObjects) {
      String s =
          "Attempt to get insert statement with bid=" + i + " when "
              + BasePrms.nameForKey(LargeObjectPrms.numLargeObjects) + "=" + numLargeObjects;
      throw new QueryObjectException(s);
    }
    if (logQueries) {
      Log.getLogWriter().info("Executing " + stmts.get(0) + " on " + i);
    }
    pstmt.setInt(1, i);
    pstmt.setString(2, "stringField1");
    pstmt.setString(3, "stringField2");
    pstmt.setString(4, "stringField3");
    pstmt.setString(5, "stringField4");
    pstmt.setString(6, "stringField5");
    pstmt.setString(7, "stringField6");
    pstmt.setString(8, "stringField7");
    pstmt.setString(9, "stringField8");
    pstmt.setString(10, "stringField9");
    pstmt.setString(11, "stringField10");
    pstmt.setString(12, "stringField11");
    pstmt.setString(13, "stringField12");
    pstmt.setString(14, "stringField13");
    pstmt.setString(15, "stringField14");
    pstmt.setString(16, "stringField15");
    pstmt.setString(17, "stringField16");
    pstmt.setString(18, "stringField17");
    pstmt.setString(19, "stringField18");
    pstmt.setString(20, "stringField19");
    pstmt.setString(21, "stringField20");
    
    pstmt.setInt(22, i);
    pstmt.setInt(23, i);
    pstmt.setInt(24, i);
    pstmt.setInt(25, i);
    pstmt.setInt(26, i);
    pstmt.setInt(27, i);
    pstmt.setInt(28, i);
    pstmt.setInt(29, i);
    pstmt.setInt(30, i);
    pstmt.setInt(31, i);
    pstmt.setInt(32, i);
    pstmt.setInt(33, i);
    pstmt.setInt(34, i);
    pstmt.setInt(35, i);
    pstmt.setInt(36, i);
    pstmt.setInt(37, i);
    pstmt.setInt(38, i);
    pstmt.setInt(39, i);
    pstmt.setInt(40, i);
    pstmt.setInt(41, i);
 
    return executeUpdatePreparedStatement(pstmt);
 
  }

  //--------------------------------------------------------------------------
  // QueryFactory : Schema statements
  //--------------------------------------------------------------------------
  public String getCreateSchemaStatement() {
    String stmt = "create schema perfTest";
    return stmt;
  }

  public String getDropSchemaStatement() {
    String stmt = "drop schema perfTest restrict";
    return stmt;
  }

  //--------------------------------------------------------------------------
  // QueryFactory : Table statements
  //--------------------------------------------------------------------------
  public List getTableStatements() {
    List stmts = new ArrayList();
    String stmt = "create table " + LargeObject.getTableName()
                + " (" + getIdCreateStatement() + "," + FIELD_SPEC + ")";
    stmts.add(stmt);
    return stmts;
  }

  public List getDropTableStatements() {
    List stmts = new ArrayList();
    stmts.add("drop table if exists " + LargeObject.getTableName());
    return stmts;
  }
  
  protected String getIdCreateStatement() {
    String stmt = "id int not null";
    List indexes = LargeObjectPrms.getLargeObjectCreateTableIndexes();
    Iterator iterator = indexes.iterator();
    
    while (iterator.hasNext()) {
      int indexType = LargeObjectPrms.getIndexType((String)iterator.next());
      if (indexType == LargeObjectPrms.PRIMARY_KEY_INDEX_ON_LARGE_OBJECT_ID_QUERY) {
        stmt += " primary key";
      }
      else if (indexType == LargeObjectPrms.UNIQUE_INDEX_ON_LARGE_OBJECT_ID_QUERY) {
        stmt += " unique";
      }
    }
  
    return stmt;
  }

  //--------------------------------------------------------------------------
  // QueryFactory : non prepared queries
  //--------------------------------------------------------------------------

  public String getRandomEqualityOnLargeObjectIdQuery() {
    String fields = LargeObjectPrms.getLargeObjectFields();

    int id = rng.nextInt(0, LargeObjectPrms.getNumLargeObjects() - 1);
    return "select " + fields + " from " + LargeObject.getTableAndShortName()
        + " where " + LargeObject.getTableShortName() + ".id=" + id;
  }

  /**
   * randomly puts a new row into a large object row.
   * there is a chance the row isn't truly updated due to the way this update statement is created
   * @param id
   * @return
   */
  public String getPutNewLargeObjectByLargeObjectIdQuery(int id) {
    StringBuffer sb =
        new StringBuffer("update " + LargeObject.getTableAndShortName()
            + " set "
            + LargeObject.getTableShortName() + ".stringField1" + "=" + "'stringField1 " + id
            + "', " 
            + LargeObject.getTableShortName() + ".stringField2" + "=" + "'stringField2 " + id
            + "', " 
            + LargeObject.getTableShortName() + ".stringField3" + "=" + "'stringField3 " + id
            + "', " 
            + LargeObject.getTableShortName() + ".stringField4" + "=" + "'stringField4 " + id
            + "', " 
            + LargeObject.getTableShortName() + ".stringField5" + "=" + "'stringField5 " + id
            + "', " 
            + LargeObject.getTableShortName() + ".stringField6" + "=" + "'stringField6 " + id
            + "', " 
            + LargeObject.getTableShortName() + ".stringField7" + "=" + "'stringField7 " + id
            + "', " 
            + LargeObject.getTableShortName() + ".stringField8" + "=" + "'stringField8 " + id
            + "', " 
            + LargeObject.getTableShortName() + ".stringField9" + "=" + "'stringField9 " + id
            + "', " 
            + LargeObject.getTableShortName() + ".stringField10" + "=" + "'stringField10 " + id
            + "', " 
            + LargeObject.getTableShortName() + ".stringField11" + "=" + "'stringField11 " + id
            + "', " 
            + LargeObject.getTableShortName() + ".stringField12" + "=" + "'stringField12 " + id
            + "', " 
            + LargeObject.getTableShortName() + ".stringField13" + "=" + "'stringField13 " + id
            + "', " 
            + LargeObject.getTableShortName() + ".stringField14" + "=" + "'stringField14 " + id
            + "', " 
            + LargeObject.getTableShortName() + ".stringField15" + "=" + "'stringField15 " + id
            + "', " 
            + LargeObject.getTableShortName() + ".stringField16" + "=" + "'stringField16 " + id
            + "', " 
            + LargeObject.getTableShortName() + ".stringField17" + "=" + "'stringField17 " + id
            + "', " 
            + LargeObject.getTableShortName() + ".stringField18" + "=" + "'stringField18 " + id
            + "', " 
            + LargeObject.getTableShortName() + ".stringField19" + "=" + "'stringField19 " + id
            + "', " 
            + LargeObject.getTableShortName() + ".stringField20" + "=" + "'stringField20 " + id
            + "', " 
            + LargeObject.getTableShortName() + ".intField1" + "=" + id
            + ", " 
            + LargeObject.getTableShortName() + ".intField2" + "=" + id
            + ", " 
            + LargeObject.getTableShortName() + ".intField3" + "=" + id
            + ", " 
            + LargeObject.getTableShortName() + ".intField4" + "=" + id
            + ", " 
            + LargeObject.getTableShortName() + ".intField5" + "=" + id
            + ", " 
            + LargeObject.getTableShortName() + ".intField6" + "=" + id
            + ", " 
            + LargeObject.getTableShortName() + ".intField7" + "=" + id
            + ", " 
            + LargeObject.getTableShortName() + ".intField8" + "=" + id
            + ", " 
            + LargeObject.getTableShortName() + ".intField9" + "=" + id
            + ", " 
            + LargeObject.getTableShortName() + ".intField10" + "=" + id
            + ", " 
            + LargeObject.getTableShortName() + ".intField11" + "=" + id
            + ", " 
            + LargeObject.getTableShortName() + ".intField12" + "=" + id
            + ", " 
            + LargeObject.getTableShortName() + ".intField13" + "=" + id
            + ", " 
            + LargeObject.getTableShortName() + ".intField14" + "=" + id
            + ", " 
            + LargeObject.getTableShortName() + ".intField15" + "=" + id
            + ", " 
            + LargeObject.getTableShortName() + ".intField16" + "=" + id
            + ", " 
            + LargeObject.getTableShortName() + ".intField17" + "=" + id
            + ", " 
            + LargeObject.getTableShortName() + ".intField18" + "=" + id
            + ", " 
            + LargeObject.getTableShortName() + ".intField19" + "=" + id
            + ", " 
            + LargeObject.getTableShortName() + ".intField20" + "=" + id
            + " where " + LargeObject.getTableShortName() + ".id=" + id);

    return sb.toString();
  }

  /**
   * randomly updates a large object row.
   * there is a chance the row isn't truly updated due to the way this update statement is created
   * @param id
   * @return
   */
  public String getGetAndPutLargeObjectByLargeObjectIdQuery(int id) {
    int n = rng.nextInt(1,20);
    StringBuffer sb =
        new StringBuffer("update " + LargeObject.getTableAndShortName()
            + " set "
            + LargeObject.getTableShortName() + ".stringField3" + "=" + "'stringField3 " + n
            + "', " 
            + LargeObject.getTableShortName() + ".intField12" + "=" + n
            + ", " 
            + LargeObject.getTableShortName() + ".intField18" + "=" + n
            + " where " + LargeObject.getTableShortName() + ".id=" + id);

    return sb.toString();
  }
  
  /**
   * @param id
   * @return
   */
  public String getDeleteLargeObjectByLargeObjectIdQuery(int id) {
    StringBuffer sb =
        new StringBuffer("delete from " + LargeObject.getTableAndShortName()
            + " where " + LargeObject.getTableShortName() + ".id=" + id);

    return sb.toString();
  }

  //--------------------------------------------------------------------------
  //  QueryFactory : Prepared Queries
  //--------------------------------------------------------------------------

  public ResultSet fillAndExecutePreparedQueryStatement(
      PreparedStatement pstmt, String stmt, int queryType, int i) throws SQLException {
    ResultSet rs = null;
    if (logQueries) {
      Log.getLogWriter().info("Executing Prepared Statement: ");
    }
    //do switch/if statement here
    switch (queryType) {
      case LargeObjectPrms.RANDOM_EQUALITY_ON_LARGE_OBJECT_ID_QUERY:
        rs =
            fillAndExecutePreparedStatementForRandomEqualityOnLargeObjectIdQuery(
                pstmt, stmt, i);
        break;
      default:
        throw new HydraConfigException("Unsupported, should not happen");

    }
    if (logQueries) {
      Log.getLogWriter().info("Executed Prepared Statement: ");
    }
    return rs;
  }

  public String getPreparedStatementForRandomEqualityOnLargeObjectIdQuery() {
    String fields = LargeObjectPrms.getLargeObjectFields();
    totalPositions = LargeObjectPrms.getNumLargeObjects();
    return "select " + fields + " from " + LargeObject.getTableAndShortName()
        + " where " + LargeObject.getTableShortName() + ".id=?";
  }

  //int tmpId = 10;
  public ResultSet fillAndExecutePreparedStatementForRandomEqualityOnLargeObjectIdQuery(
      PreparedStatement pstmt, String stmt, int bid) throws SQLException {
    int id = rng.nextInt(0, totalPositions - 1);
    pstmt.setInt(1, id);
    if (logQueries) {
      Log.getLogWriter().info("Executing " + stmt + " on " + id);
    }
    return executeQueryPreparedStatement(pstmt);
  }

  //--------------------------------------------------------------------------
  // QueryFactory : Prepared updates
  //--------------------------------------------------------------------------
  public int fillAndExecuteUpdatePreparedQueryStatement(
      PreparedStatement pstmt, String stmt, int queryType, int i) throws SQLException {
    int numUpdated = 0;
    if (logQueries) {
      Log.getLogWriter().info("Executing Prepared Statement: and i = "+i);
    }
    //do switch/if statement here
    switch (queryType) {
      case LargeObjectPrms.PUT_NEW_LARGE_OBJECT_BY_LARGE_OBJECT_ID_QUERY:
        numUpdated =
            fillAndExecutePreparedStatementForPutNewLargeObjectByLargeObjectIdQuery(
                pstmt, stmt, i);
        break;
      case LargeObjectPrms.GET_AND_PUT_LARGE_OBJECT_BY_LARGE_OBJECT_ID_QUERY:
        numUpdated =
            fillAndExecutePreparedStatementForGetAndPutLargeObjectByLargeObjectIdQuery(
                pstmt, stmt, i);
        break;
      case LargeObjectPrms.DELETE_LARGE_OBJECT_BY_LARGE_OBJECT_ID_QUERY:
        numUpdated =
            fillAndExecutePreparedStatementForDeleteLargeObjectByLargeObjectIdQuery(
                pstmt, stmt, i);
        break;
      default:
        throw new HydraConfigException(
            "Unsupported query type for updates, should not happen");

    }
    if (logQueries) {
      Log.getLogWriter().info("Executed Prepared Statement: ");
    }
    return numUpdated;
  }
  
  public String getPreparedStatementForPutNewLargeObjectByLargeObjectIdQuery() {
    StringBuffer sb =
        new StringBuffer("update " + LargeObject.getTableAndShortName() + " set "
            + LargeObject.getTableShortName() + ".stringField1=? ,"
            + LargeObject.getTableShortName() + ".stringField2=? ,"
            + LargeObject.getTableShortName() + ".stringField3=? ,"
            + LargeObject.getTableShortName() + ".stringField4=? ,"
            + LargeObject.getTableShortName() + ".stringField5=? ,"
            + LargeObject.getTableShortName() + ".stringField6=? ,"
            + LargeObject.getTableShortName() + ".stringField7=? ,"
            + LargeObject.getTableShortName() + ".stringField8=? ,"
            + LargeObject.getTableShortName() + ".stringField9=? ,"
            + LargeObject.getTableShortName() + ".stringField10=? ,"
            + LargeObject.getTableShortName() + ".stringField11=? ,"
            + LargeObject.getTableShortName() + ".stringField12=? ,"
            + LargeObject.getTableShortName() + ".stringField13=? ,"
            + LargeObject.getTableShortName() + ".stringField14=? ,"
            + LargeObject.getTableShortName() + ".stringField15=? ,"
            + LargeObject.getTableShortName() + ".stringField16=? ,"
            + LargeObject.getTableShortName() + ".stringField17=? ,"
            + LargeObject.getTableShortName() + ".stringField18=? ,"
            + LargeObject.getTableShortName() + ".stringField19=? ,"
            + LargeObject.getTableShortName() + ".stringField20=? ,"
            + LargeObject.getTableShortName() + ".intField1=? ,"
            + LargeObject.getTableShortName() + ".intField2=? ,"
            + LargeObject.getTableShortName() + ".intField3=? ,"
            + LargeObject.getTableShortName() + ".intField4=? ,"
            + LargeObject.getTableShortName() + ".intField5=? ,"
            + LargeObject.getTableShortName() + ".intField6=? ,"
            + LargeObject.getTableShortName() + ".intField7=? ,"
            + LargeObject.getTableShortName() + ".intField8=? ,"
            + LargeObject.getTableShortName() + ".intField9=? ,"
            + LargeObject.getTableShortName() + ".intField10=? ,"
            + LargeObject.getTableShortName() + ".intField11=? ,"
            + LargeObject.getTableShortName() + ".intField12=? ,"
            + LargeObject.getTableShortName() + ".intField13=? ,"
            + LargeObject.getTableShortName() + ".intField14=? ,"
            + LargeObject.getTableShortName() + ".intField15=? ,"
            + LargeObject.getTableShortName() + ".intField16=? ,"
            + LargeObject.getTableShortName() + ".intField17=? ,"
            + LargeObject.getTableShortName() + ".intField18=? ,"
            + LargeObject.getTableShortName() + ".intField19=? ,"
            + LargeObject.getTableShortName() + ".intField20=? "       
            + " where "+ LargeObject.getTableShortName() + ".id=?");
    return sb.toString();
  }
 
  public int fillAndExecutePreparedStatementForPutNewLargeObjectByLargeObjectIdQuery(
      PreparedStatement pstmt, String stmt, int id) throws SQLException {
    int tmpid = id%10000;
    pstmt.setString(1, "stringField1 " + tmpid);
    pstmt.setString(2, "stringField2 " + tmpid);
    pstmt.setString(3, "stringField3 " + tmpid);
    pstmt.setString(4, "stringField4 " + tmpid);
    pstmt.setString(5, "stringField5 " + tmpid);
    pstmt.setString(6, "stringField6 " + tmpid);
    pstmt.setString(7, "stringField7 " + tmpid);
    pstmt.setString(8, "stringField8 " + tmpid);
    pstmt.setString(9, "stringField9 " + tmpid);
    pstmt.setString(10, "stringField10 " + tmpid);
    pstmt.setString(11, "stringField11 " + tmpid);
    pstmt.setString(12, "stringField12 " + tmpid);
    pstmt.setString(13, "stringField13 " + tmpid);
    pstmt.setString(14, "stringField14 " + tmpid);
    pstmt.setString(15, "stringField15 " + tmpid);
    pstmt.setString(16, "stringField16 " + tmpid);
    pstmt.setString(17, "stringField17 " + tmpid);
    pstmt.setString(18, "stringField18 " + tmpid);
    pstmt.setString(19, "stringField19 " + tmpid);
    pstmt.setString(20, "stringField20 " + tmpid);
    pstmt.setInt(21, id);
    pstmt.setInt(22, id);
    pstmt.setInt(23, id);
    pstmt.setInt(24, id);
    pstmt.setInt(25, id);
    pstmt.setInt(26, id);
    pstmt.setInt(27, id);
    pstmt.setInt(28, id);
    pstmt.setInt(29, id);
    pstmt.setInt(30, id);
    pstmt.setInt(31, id);
    pstmt.setInt(32, id);
    pstmt.setInt(33, id);
    pstmt.setInt(34, id);
    pstmt.setInt(35, id);
    pstmt.setInt(36, id);
    pstmt.setInt(37, id);
    pstmt.setInt(38, id);
    pstmt.setInt(39, id);
    pstmt.setInt(40, id);
    pstmt.setInt(41, id);
    return executeUpdatePreparedStatement(pstmt);
  }
  
  public String getPreparedStatementForGetAndPutLargeObjectByLargeObjectIdQuery() {
    StringBuffer sb =
        new StringBuffer("update " + LargeObject.getTableAndShortName() + " set "
            + LargeObject.getTableShortName() + ".stringField3=? ,"
            + LargeObject.getTableShortName() + ".intField3=? ,"
            + LargeObject.getTableShortName() + ".intField12=? "       
            + " where "+ LargeObject.getTableShortName() + ".id=?");
    return sb.toString();
  }
 
  public int fillAndExecutePreparedStatementForGetAndPutLargeObjectByLargeObjectIdQuery(
      PreparedStatement pstmt, String stmt, int id) throws SQLException {
    int n = rng.nextInt(1,20);
    pstmt.setString(1, "stringField3 " + n);
    pstmt.setInt(2, n);
    pstmt.setInt(3, n);
    pstmt.setInt(4, id);
    return executeUpdatePreparedStatement(pstmt);
  }
  
  public String getPreparedStatementForDeleteLargeObjectByLargeObjectIdQuery() {
    StringBuffer sb =
        new StringBuffer("delete from " + LargeObject.getTableAndShortName()
            + " where "+ LargeObject.getTableShortName() + ".id=?");
    return sb.toString();
  }
 
  public int fillAndExecutePreparedStatementForDeleteLargeObjectByLargeObjectIdQuery(
      PreparedStatement pstmt, String stmt, int id) throws SQLException {
    pstmt.setInt(1,id);
    return executeUpdatePreparedStatement(pstmt);
  }

  //--------------------------------------------------------------------------
  // Read result sets
  //--------------------------------------------------------------------------
  public int readResultSet(int queryType, ResultSet rs) throws SQLException {
    switch (queryType) {
      case LargeObjectPrms.RANDOM_EQUALITY_ON_LARGE_OBJECT_ID_QUERY:
        return readResultSet(queryType, rs, LargeObject.getFields(LargeObjectPrms.getLargeObjectFieldsAsVector()));
      default:
        return readResultSet(queryType, rs, LargeObject.getFields(LargeObjectPrms.getLargeObjectFieldsAsVector()));
    }
  }

  public int readResultSet(int queryType, ResultSet rs, List fields) throws SQLException {
    int rsSize = 0;
    while (rs.next()) {
      readFields(rs, fields);
      rsSize++;
    }
    if (logQueryResultSize) {
      Log.getLogWriter().info("Returned " + rsSize + " rows");
    }
    if (validateResults) {
      int expectedSize = getResultSetSize(queryType);
      if (rsSize != expectedSize) {
        throw new HydraConfigException("ResultSetSize expected("
            + expectedSize + ") did not match actual("
            + rsSize + ")");
      }
    }
    return rsSize;
  }

  private void readFields(ResultSet rs, List fields) throws SQLException {
    //hard coded 41 since I know large object has 41 columns
    //we know that rs is 1 based, so we start at 1
    for (int i = 1; i < 42; i++) {
      if (i == 1) {
        rs.getInt(i);
      }
      else if (i > 1 && i < 22){
        rs.getString(i);
      }
      else if (i > 21 && i < 42){
        rs.getInt(i);
      }
    }
  }

  /**
   * Returns the expected size of the result set for the specified query type.
   */
  private int getResultSetSize(int queryType) {
    switch (queryType) {
      case LargeObjectPrms.RANDOM_EQUALITY_ON_LARGE_OBJECT_ID_QUERY:
        return 1;
      default:
        String s = "Query type not supported yet: " + queryType;
        throw new UnsupportedOperationException("");
    }
  }

  //----
  //For direct gets on a region in gfxd
  //----
  private Region getRegion(String regionName) {
    Region region;
    try {
      Class c = Class.forName("com.pivotal.gemfirexd.internal.engine.Misc");
      Method getRegionMethod =
          c.getMethod("getRegion", new Class[] { String.class });
      Object[] regionNameArray =
          { regionName };
      region = (Region) getRegionMethod.invoke(null, regionNameArray);
    } catch (ClassNotFoundException cnfe) {
      throw new TestException("Could not access Misc class");
    } catch (NoSuchMethodException nsme) {
      throw new TestException("Could not find Misc method");
    } catch (InvocationTargetException ite) {
      ite.printStackTrace();
      throw new TestException("Could not invoke Misc method");
    } catch (IllegalAccessException iae) {
      throw new TestException("Could not access Misc method");
    }
    return region;
  }
  
//direct get
  public Object directGet(Object key, Region region) {
    return directGetOnLargeObjectId(key, region);
  }

  public Object directGetOnLargeObjectId(Object key, Region region) {
    Object entry = region.get(key);
    return entry;
  }
  
  //direct put
  public void directPut(Object key, Region region, Object value) {
    directPutOnLargeObjectId(key, region, value);
  }

  public void directPutOnLargeObjectId(Object key, Region region, Object value) {
    Object entry = region.put(key, value);
  }
 
  
  //--------------------------------------------------------
  // Gets
  //--------------------------------------------------------
  //This method is actually a "hack" to get to a gfxd region for a specific query
  public Region getRegionForQuery(int queryType) {
    Region region;
    try {
      Class c = Class.forName("com.pivotal.gemfirexd.internal.engine.Misc");
      Method getRegionMethod =
          c.getMethod("getRegion", new Class[] { String.class });
      Object[] regionNameArray =
          { "APP." + LargeObject.getTableName().toUpperCase() };
      region = (Region) getRegionMethod.invoke(null, regionNameArray);
    } catch (ClassNotFoundException cnfe) {
      throw new TestException("Could not access Misc class");
    } catch (NoSuchMethodException nsme) {
      throw new TestException("Could not find Misc method");
    } catch (InvocationTargetException ite) {
      ite.printStackTrace();
      throw new TestException("Could not invoke Misc method");
    } catch (IllegalAccessException iae) {
      throw new TestException("Could not access Misc method");
    }
    return region;
  }
}
