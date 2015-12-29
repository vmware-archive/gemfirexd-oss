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
package objects.query.tinyobject;

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

public class SQLTinyObjectQueryFactory extends BaseSQLQueryFactory implements
    SQLQueryFactory {

  protected static final GsRandom rng = TestConfig.tab().getRandGen();

  protected static final String FIELD_SPEC = " intField int ";

  /*
   * for performance purposes, used to store total positions so 
   * that we don't need to recalculate for every query.
   */
  private int totalPositions;

  public SQLTinyObjectQueryFactory() {
  
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
    return TinyObjectPrms.getQueryType(QueryPrms.GFXD);
  }
  
  public int getUpdateQueryType() {
    return TinyObjectPrms.getUpdateQueryType(QueryPrms.GFXD);
  }
  
  public int getDeleteQueryType() {
    return TinyObjectPrms.getDeleteQueryType(QueryPrms.GFXD);
  }

  public String getQuery(int queryType, int i) {
    String query = "";
    int type = queryType;
    switch (type) {
      case TinyObjectPrms.RANDOM_EQUALITY_ON_TINY_OBJECT_ID_QUERY:
        query = getRandomEqualityOnTinyObjectIdQuery();
        break;
      case TinyObjectPrms.PUT_NEW_TINY_OBJECT_BY_TINY_OBJECT_ID_QUERY:
        query = getPutNewTinyObjectByTinyObjectIdQuery(i);
        break;
      case TinyObjectPrms.GET_AND_PUT_TINY_OBJECT_BY_TINY_OBJECT_ID_QUERY:
        query = getGetAndPutTinyObjectByTinyObjectIdQuery(i);
        break;
      case TinyObjectPrms.DELETE_TINY_OBJECT_BY_TINY_OBJECT_ID_QUERY:
        query = getDeleteTinyObjectByTinyObjectIdQuery(i);
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
      case TinyObjectPrms.RANDOM_EQUALITY_ON_TINY_OBJECT_ID_QUERY:
        query = getPreparedStatementForRandomEqualityOnTinyObjectIdQuery();
        break;
      case TinyObjectPrms.PUT_NEW_TINY_OBJECT_BY_TINY_OBJECT_ID_QUERY:
        query = getPreparedStatementForPutNewTinyObjectByTinyObjectIdQuery();
        break;
      case TinyObjectPrms.GET_AND_PUT_TINY_OBJECT_BY_TINY_OBJECT_ID_QUERY:
        query = getPreparedStatementForGetAndPutTinyObjectByTinyObjectIdQuery();
        break;
      case TinyObjectPrms.DELETE_TINY_OBJECT_BY_TINY_OBJECT_ID_QUERY:
        query = getPreparedStatementForDeleteTinyObjectByTinyObjectIdQuery();
        break;
      default:
        throw new UnsupportedOperationException("Should not happen");
    }
    return query;
  }

  //--------------------------------------------------------------------------
  // QueryFactory : primary keys
  //--------------------------------------------------------------------------
  public String getPrimaryKeyIndexOnTinyObjectId() {
    return "alter table " + TinyObject.getTableName() + " add primary key(id)";
  }

  //--------------------------------------------------------------------------
  // QueryFactory : indexes
  //--------------------------------------------------------------------------
  public List getIndexStatements() {
    List stmts = new ArrayList();
    Vector indexTypes = TinyObjectPrms.getIndexTypes();
    for (Iterator i = indexTypes.iterator(); i.hasNext();) {
      String indexTypeString = (String) i.next();
      int indexType = TinyObjectPrms.getIndexType(indexTypeString);
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
      case TinyObjectPrms.PRIMARY_KEY_INDEX_ON_TINY_OBJECT_ID_QUERY:
        query = getPrimaryKeyIndexOnTinyObjectId();
        break;
      case TinyObjectPrms.NO_QUERY:
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
    int numTinyObjects = TinyObjectPrms.getNumTinyObjects();
    if (bid >= numTinyObjects) {
      String s =
          "Attempt to get insert statement with bid=" + bid + " when "
              + BasePrms.nameForKey(TinyObjectPrms.numTinyObjects) + "=" + numTinyObjects;
      throw new QueryObjectException(s);
    }
  
    String stmt =
        "insert into " + TinyObject.getTableName()
            + " values (" + bid + "," + bid + ")";
    stmts.add(stmt);
    return stmts;
  }

  public List getPreparedInsertStatements() {
    List stmts = new ArrayList();
    String stmt = "insert into " + TinyObject.getTableName() + " values (?, " +
    		"?)";
    stmts.add(stmt);
    return stmts;
  }

  public int fillAndExecutePreparedInsertStatements(List pstmts, List stmts, int i)
      throws SQLException {
    int numTinyObjects = TinyObjectPrms.getNumTinyObjects();
    PreparedStatement pstmt = (PreparedStatement) pstmts.get(0);
    int results = 0;
    if (i >= numTinyObjects) {
      String s =
          "Attempt to get insert statement with bid=" + i + " when "
              + BasePrms.nameForKey(TinyObjectPrms.numTinyObjects) + "=" + numTinyObjects;
      throw new QueryObjectException(s);
    }
    pstmt.setInt(1, i);
    pstmt.setInt(2, i);
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
    String stmt = "create table " + TinyObject.getTableName()
                + " (" + getIdCreateStatement() + "," + FIELD_SPEC + ")";
    stmts.add(stmt);
    return stmts;
  }

  public List getDropTableStatements() {
    List stmts = new ArrayList();
    stmts.add("drop table if exists " + TinyObject.getTableName());
    return stmts;
  }
  
  protected String getIdCreateStatement() {
    String stmt = "id int not null";
    List indexes = TinyObjectPrms.getTinyObjectCreateTableIndexes();
    Iterator iterator = indexes.iterator();
    
    while (iterator.hasNext()) {
      int indexType = TinyObjectPrms.getIndexType((String)iterator.next());
      if (indexType == TinyObjectPrms.PRIMARY_KEY_INDEX_ON_TINY_OBJECT_ID_QUERY) {
        stmt += " primary key";
      }
      else if (indexType == TinyObjectPrms.UNIQUE_INDEX_ON_TINY_OBJECT_ID_QUERY) {
        stmt += " unique";
      }
    }
  
    return stmt;
  }

  //--------------------------------------------------------------------------
  // QueryFactory : non prepared queries
  //--------------------------------------------------------------------------

  public String getRandomEqualityOnTinyObjectIdQuery() {
    String fields = TinyObjectPrms.getTinyObjectFields();

    int id = rng.nextInt(0, TinyObjectPrms.getNumTinyObjects() - 1);
    return "select " + fields + " from " + TinyObject.getTableAndShortName()
        + " where " + TinyObject.getTableShortName() + ".id=" + id;
  }

  /**
   * randomly puts a new row into a tiny object row.
   * there is a chance the row isn't truly updated due to the way this update statement is created
   * @param id
   * @return
   */
  public String getPutNewTinyObjectByTinyObjectIdQuery(int id) {
    StringBuffer sb =
        new StringBuffer("update " + TinyObject.getTableAndShortName()
            + " set "
            + TinyObject.getTableShortName() + ".intField" + "=" + id
            + " where " + TinyObject.getTableShortName() + ".id=" + id);

    return sb.toString();
  }

  /**
   * randomly updates a tiny object row.
   * there is a chance the row isn't truly updated due to the way this update statement is created
   * @param id
   * @return
   */
  public String getGetAndPutTinyObjectByTinyObjectIdQuery(int id) {
    int n = rng.nextInt(1,20);
    StringBuffer sb =
        new StringBuffer("update " + TinyObject.getTableAndShortName()
            + " set "
            + TinyObject.getTableShortName() + ".intField" + "=" + n
            + " where " + TinyObject.getTableShortName() + ".id=" + id);

    return sb.toString();
  }
  
  /**
   * @param id
   * @return
   */
  public String getDeleteTinyObjectByTinyObjectIdQuery(int id) {
    StringBuffer sb =
        new StringBuffer("delete from " + TinyObject.getTableAndShortName()
            + " where " + TinyObject.getTableShortName() + ".id=" + id);

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
      case TinyObjectPrms.RANDOM_EQUALITY_ON_TINY_OBJECT_ID_QUERY:
        rs =
            fillAndExecutePreparedStatementForRandomEqualityOnTinyObjectIdQuery(
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

  public String getPreparedStatementForRandomEqualityOnTinyObjectIdQuery() {
    String fields = TinyObjectPrms.getTinyObjectFields();
    totalPositions = TinyObjectPrms.getNumTinyObjects();
    return "select " + fields + " from " + TinyObject.getTableAndShortName()
        + " where " + TinyObject.getTableShortName() + ".id=?";
  }

  //int tmpId = 10;
  public ResultSet fillAndExecutePreparedStatementForRandomEqualityOnTinyObjectIdQuery(
      PreparedStatement pstmt, String stmt, int bid) throws SQLException {
    int id = rng.nextInt(0, totalPositions - 1);
    pstmt.setInt(1, id);
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
      case TinyObjectPrms.PUT_NEW_TINY_OBJECT_BY_TINY_OBJECT_ID_QUERY:
        numUpdated =
            fillAndExecutePreparedStatementForPutNewTinyObjectByTinyObjectIdQuery(
                pstmt, stmt, i);
        break;
      case TinyObjectPrms.GET_AND_PUT_TINY_OBJECT_BY_TINY_OBJECT_ID_QUERY:
        numUpdated =
            fillAndExecutePreparedStatementForGetAndPutTinyObjectByTinyObjectIdQuery(
                pstmt, stmt, i);
        break;
      case TinyObjectPrms.DELETE_TINY_OBJECT_BY_TINY_OBJECT_ID_QUERY:
        numUpdated =
            fillAndExecutePreparedStatementForDeleteTinyObjectByTinyObjectIdQuery(
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
  
  public String getPreparedStatementForPutNewTinyObjectByTinyObjectIdQuery() {
    StringBuffer sb =
        new StringBuffer("update " + TinyObject.getTableAndShortName() + " set "
            + TinyObject.getTableShortName() + ".intField=? "
            + " where "+ TinyObject.getTableShortName() + ".id=?");
    return sb.toString();
  }
 
  public int fillAndExecutePreparedStatementForPutNewTinyObjectByTinyObjectIdQuery(
      PreparedStatement pstmt, String stmt, int id) throws SQLException {
    pstmt.setInt(1, id);
    return executeUpdatePreparedStatement(pstmt);
  }
  
  public String getPreparedStatementForGetAndPutTinyObjectByTinyObjectIdQuery() {
    StringBuffer sb =
        new StringBuffer("update " + TinyObject.getTableAndShortName() + " set "
            + TinyObject.getTableShortName() + ".intField=? "
            + " where "+ TinyObject.getTableShortName() + ".id=?");
    return sb.toString();
  }
 
  public int fillAndExecutePreparedStatementForGetAndPutTinyObjectByTinyObjectIdQuery(
      PreparedStatement pstmt, String stmt, int id) throws SQLException {
    int n = rng.nextInt(1,20);
    pstmt.setInt(1, n);
    pstmt.setInt(2, id);
    return executeUpdatePreparedStatement(pstmt);
  }
  
  public String getPreparedStatementForDeleteTinyObjectByTinyObjectIdQuery() {
    StringBuffer sb =
        new StringBuffer("delete from " + TinyObject.getTableAndShortName()
            + " where "+ TinyObject.getTableShortName() + ".id=?");
    return sb.toString();
  }
 
  public int fillAndExecutePreparedStatementForDeleteTinyObjectByTinyObjectIdQuery(
      PreparedStatement pstmt, String stmt, int id) throws SQLException {
    pstmt.setInt(1,id);
    return executeUpdatePreparedStatement(pstmt);
  }

  //--------------------------------------------------------------------------
  // Read result sets
  //--------------------------------------------------------------------------
  public int readResultSet(int queryType, ResultSet rs) throws SQLException {
    switch (queryType) {
      case TinyObjectPrms.RANDOM_EQUALITY_ON_TINY_OBJECT_ID_QUERY:
        return readResultSet(rs, TinyObject.getFields(TinyObjectPrms.getTinyObjectFieldsAsVector()));
      default:
        return readResultSet(rs, TinyObject.getFields(TinyObjectPrms.getTinyObjectFieldsAsVector()));
    }
  }

  public int readResultSet(ResultSet rs, List fields) throws SQLException {
    int rsSize = 0;
    while (rs.next()) {
      readFields(rs, fields);
      rsSize++;
    }
    if (logQueryResultSize) {
      Log.getLogWriter().info("Returned " + rsSize + " rows");
    }
    if (validateResults) {
      if (rsSize != TinyObjectPrms.getResultSetSize()) {
        throw new HydraConfigException("ResultSetSize expected("
            + TinyObjectPrms.getResultSetSize() + ") did not match actual("
            + rsSize + ")");
      }
    }
    return rsSize;
  }

  private void readFields(ResultSet rs, List fields) throws SQLException {
    //hard coded 41 since I know tiny object has 41 columns
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
    return directGetOnTinyObjectId(key, region);
  }

  public Object directGetOnTinyObjectId(Object key, Region region) {
    Object entry = region.get(key);
    return entry;
  }
  
  //direct put
  public void directPut(Object key, Region region, Object value) {
    directPutOnTinyObjectId(key, region, value);
  }

  public void directPutOnTinyObjectId(Object key, Region region, Object value) {
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
          { "APP." + TinyObject.getTableName().toUpperCase() };
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
