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
package objects.query.broker;

import cacheperf.CachePerfPrms;

import com.gemstone.gemfire.cache.Region;

import hydra.BasePrms;
import hydra.GsRandom;
import hydra.HydraConfigException;
import hydra.Log;
import hydra.TestConfig;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import objects.query.BaseSQLQueryFactory;
import objects.query.QueryObjectException;
import objects.query.QueryPrms;
import objects.query.SQLQueryFactory;

//------------------------------------------------------------------------------
// SQL Broker Query Factory
//------------------------------------------------------------------------------
public class SQLBrokerQueryFactory extends BaseSQLQueryFactory implements
    SQLQueryFactory {
  protected static final GsRandom rng = TestConfig.tab().getRandGen();

  protected SQLBrokerTicketQueryFactory brokerTicketQueryFactory;

  public SQLBrokerQueryFactory() {
    brokerTicketQueryFactory = new SQLBrokerTicketQueryFactory();
  }

  public void init() {
    super.init();
    brokerTicketQueryFactory.init();
  }

  public int getQueryType() {
    return BrokerPrms.getQueryType(QueryPrms.GFXD);
  }
  
  public int getUpdateQueryType() {
    return BrokerPrms.getUpdateQueryType(QueryPrms.GFXD);
  }
  
  public int getDeleteQueryType() {
    return BrokerPrms.getDeleteQueryType(QueryPrms.GFXD);
  }
  
  //------------------------------------------------------------------
  //Constraints
  //------------------------------------------------------------------
  public List getConstraintStatements() {
    return new ArrayList();
  }

  public String getQuery(int queryType, int i) {
    String query = "";
    int type = queryType;
    switch (type) {
      case BrokerPrms.RANDOM_EQUALITY_ON_BROKER_ID_QUERY:
        query = getRandomEqualityOnBrokerIdQuery();
        break;
      case BrokerPrms.EQUALITY_ON_BROKER_ID_QUERY:
        query = getEqualityOnBrokerIdQuery(i);
        break;
      case BrokerPrms.TICKETS_FROM_EQUALITY_ON_BROKER_ID_QUERY:
        query = getTicketsFromEqualityOnBrokerIdQuery(i);
        break;
      case BrokerPrms.TICKETS_FROM_RANDOM_RANGE_ON_TICKET_PRICE_QUERY:
        query = getTicketsFromRandomRangeOnTicketPriceQuery();
        break;
      case BrokerPrms.BROKERS_FROM_RANDOM_RANGE_ON_TICKET_PRICE_QUERY:
        query = getBrokersFromRandomRangeOnTicketPriceQuery();
        break;
      case BrokerPrms.BROKERS_FROM_RANDOM_SIZE_RANGE_ON_TICKET_PRICE_QUERY:
        query = getBrokersFromRandomSizeRangeOnTicketPriceQuery();
        break;
      case BrokerPrms.BROKERS_FROM_RANDOM_PERCENTAGE_RANGE_ON_TICKET_PRICE_QUERY:
        query = getBrokersFromRandomPercentageRangeOnTicketPriceQuery();
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
      case BrokerPrms.RANDOM_EQUALITY_ON_BROKER_ID_QUERY:
        query = getPreparedStatementForEqualityOnBrokerIdQuery();
        break;
      case BrokerPrms.EQUALITY_ON_BROKER_ID_QUERY:
        query = getPreparedStatementForEqualityOnBrokerIdQuery();
        break;
      case BrokerPrms.TICKETS_FROM_EQUALITY_ON_BROKER_ID_QUERY:
        query = getPreparedStatementForTicketsFromEqualityOnBrokerIdQuery();
        break;
      case BrokerPrms.TICKETS_FROM_RANDOM_RANGE_ON_TICKET_PRICE_QUERY:
        query =
            getPreparedStatementForTicketsFromRandomRangeOnTicketPriceQuery();
        break;
      case BrokerPrms.BROKERS_FROM_RANDOM_RANGE_ON_TICKET_PRICE_QUERY:
        query =
            getPreparedStatementForBrokersFromRandomRangeOnTicketPriceQuery();
        break;
      case BrokerPrms.BROKERS_FROM_RANDOM_SIZE_RANGE_ON_TICKET_PRICE_QUERY:
        query =
            getPreparedStatementForBrokersFromRandomRangeOnTicketPriceQuery();
        break;
      case BrokerPrms.BROKERS_FROM_RANDOM_PERCENTAGE_RANGE_ON_TICKET_PRICE_QUERY:
        query =
            getPreparedStatementForBrokersFromRandomRangeOnTicketPriceQuery();
        break;
      default:
        throw new UnsupportedOperationException("Should not happen");
    }
    return query;
  }
  
//--------------------------------------------------------------------------
  // QueryFactory : tables
  //--------------------------------------------------------------------------

  public String getCreateSchemaStatement() {
    return  "create schema perfTest";
  }

  public String getDropSchemaStatement() {
    return "drop schema perfTest";
  }


  //--------------------------------------------------------------------------
  // QueryFactory : tables
  //--------------------------------------------------------------------------

  public List getTableStatements() {
    List stmts = new ArrayList();
    stmts.add("create table " + Broker.getTableName()
        + " (id int not null, name varchar(100))");
    stmts.addAll(brokerTicketQueryFactory.getTableStatements());
    return stmts;
  }

  public List getDropTableStatements() {
    List stmts = new ArrayList();
    stmts.add("drop table if exists " + Broker.getTableName());
    stmts.addAll(brokerTicketQueryFactory.getDropTableStatements());
    return stmts;
  }

  //--------------------------------------------------------------------------
  // QueryFactory : primary keys
  //--------------------------------------------------------------------------
  public String getPrimaryKeyIndexOnBrokerId() {
    return "alter table " + Broker.getTableName() + " add primary key(id)";
  }

  //--------------------------------------------------------------------------
  // QueryFactory : indexes
  //--------------------------------------------------------------------------
  public List getIndexStatements() {
    List stmts = new ArrayList();
    Vector indexTypes = BrokerPrms.getIndexTypes();
    for (Iterator i = indexTypes.iterator(); i.hasNext();) {
      String indexTypeString = (String) i.next();
      int indexType = BrokerPrms.getIndexType(indexTypeString);
      String stmt = getIndexStatement(indexType);
      stmts.add(stmt);
    }
    return stmts;
  }

  public String getIndexStatement(int type) {
    String query;
    switch (type) {
      case BrokerPrms.PRIMARY_KEY_INDEX_ON_BROKER_ID_QUERY:
        query = getPrimaryKeyIndexOnBrokerId();
        break;
      case BrokerPrms.UNIQUE_INDEX_ON_BROKER_NAME_QUERY:
        query = getUniqueIndexOnBrokerName();
        break;
      case BrokerPrms.PRIMARY_KEY_INDEX_ON_BROKER_TICKET_ID_QUERY:
        query = getUniqueKeyIndexOnBrokerTicketId();
        break;
      case BrokerPrms.UNIQUE_INDEX_ON_BROKER_TICKET_NAME_QUERY:
        query = getUniqueKeyIndexOnBrokerTicketName();
        break;
      default:
        throw new UnsupportedOperationException("Should not happen");
    }
    return query;
  }

  public String getUniqueKeyIndexOnBrokerId() {
    return "create unique index id_uidx on " + Broker.getTableName() + " (id)";
  }

  /*
  public String getSQLGlobalIndexOnBrokerId() {
    return "create global index ... brokers ... id";
  }
  */
  public String getUniqueIndexOnBrokerName() {
    return "create unique index name_uidx on " + Broker.getTableName()
        + " (name)";
  }

  /*
  public String getSQLGlobalIndexOnBrokerName() {
    return "create global index ... brokers ... name";
  }
  */

  public String getUniqueKeyIndexOnBrokerTicketId() {
    return "create unique index id_uidx on " + Broker.getTableName() + " (id)";
  }

  public String getUniqueKeyIndexOnBrokerTicketName() {
    return "create unique index id_pkidx on " + Broker.getTableName() + " (id)";
  }

  //--------------------------------------------------------------------------
  // QueryFactory : inserts
  //--------------------------------------------------------------------------
  /**
   * Generate the list of insert statements required to create a broker with the
   * given broker id.
   *
   * @param bid the unique broker id
   * @throw QueryObjectException if bid exceeds {@link BrokerPrms#numBrokers}.
   */
  public List getInsertStatements(int bid) {
    int numBrokers = BrokerPrms.getNumBrokers();
    if (bid >= numBrokers) {
      String s =
          "Attempt to get insert statement with bid=" + bid + " when "
              + BasePrms.nameForKey(BrokerPrms.numBrokers) + "=" + numBrokers;
      throw new QueryObjectException(s);
    }
    List stmts = new ArrayList();
    String bname = Broker.getName(bid);
    String stmt =
        "insert into " + Broker.getTableName() + " (id, name) values (" + bid
            + ",'" + bname + "')";
    stmts.add(stmt);
    stmts.addAll(brokerTicketQueryFactory.getInsertStatements(bid));
    return stmts;
  }

  public List getPreparedInsertStatements() {
    List stmts = new ArrayList();
    String stmt = "insert into " + Broker.getTableName() + " values (?,?)";
    stmts.add(stmt);
    stmts.add(brokerTicketQueryFactory.getPreparedInsertStatements());
    return stmts;
  }

  // @todo Logging (either move it or implement it here)
  /**
   * @throw SQLException if bid has already been inserted.
   */
  public int fillAndExecutePreparedInsertStatements(List pstmts, List stmts, int i)
      throws SQLException {
    PreparedStatement stmt = (PreparedStatement) pstmts.get(0);
    int bid = i;
    int results = 0;
    String bname = Broker.getName(i);

    stmt.setInt(1, bid); // id
    stmt.setString(2, bname); // name
    if (logUpdates) {
      Log.getLogWriter().info("Executing update: " + stmt);
    }
    results += executeUpdatePreparedStatement(stmt);
    if (logUpdates) {
      Log.getLogWriter().info(
          "Executed update: " + stmt + " with result = " + results);
    }
    results += brokerTicketQueryFactory.fillAndExecutePreparedInsertStatements(
        (List) pstmts.get(1), (List)stmts.get(1), bid);
    //  ... = new BrokerTicket(i*numBrokers+this.id, this.id, double price, int quantity, String tickerPrefix, int tickerNumber)
    //BrokerCompany.fillAndExecutePreparedInsertStatements(pstmts.get(2), stmts,get(2), i, bid);
    return results;
  }

  //--------------------------------------------------------------------------
  // QueryFactory: queries
  //--------------------------------------------------------------------------

  public String getTicketsFromEqualityOnBrokerIdQuery(int bid) {
    if (CachePerfPrms.getMaxKeys() == BrokerPrms.getNumBrokers()) {
      //then bid can be CachePerfClient loop index, using keyAllocationScheme
      String fields = BrokerPrms.getBrokerFields();
      return "select " + fields + " from " + BrokerTicket.getTableName()
          + " where brokerId=" + bid;
    }
    else {
      String s =
          BasePrms.nameForKey(BrokerPrms.numBrokers)
              + " must be smaller then : "
              + BasePrms.nameForKey(CachePerfPrms.maxKeys);
      throw new HydraConfigException(s);
    }
  }

  public String getPreparedStatementForTicketsFromEqualityOnBrokerIdQuery() {
    if (CachePerfPrms.getMaxKeys() == BrokerPrms.getNumBrokers()) {
      //then bid can be CachePerfClient loop index, using keyAllocationScheme
      String fields = BrokerPrms.getBrokerTicketFields();
      return "select " + fields + " from " + BrokerTicket.getTableName()
          + " where brokerId=?";
    }
    else {
      String s =
          BasePrms.nameForKey(BrokerPrms.numBrokers)
              + " must be smaller then : "
              + BasePrms.nameForKey(CachePerfPrms.maxKeys);
      throw new HydraConfigException(s);
    }
  }

  public ResultSet fillAndExecutePreparedStatementForTicketsFromEqualityOnBrokerIdQuery(
      PreparedStatement pstmt, String stmt, int bid) throws SQLException {
    pstmt.setInt(1, bid); // id
    return executeQueryPreparedStatement(pstmt);
  }

  //Tmp Comment  select all tickets where X is in a range and hydra clients don't overlap

  public String getEqualityOnBrokerIdQuery(int bid) {
    if (CachePerfPrms.getMaxKeys() == BrokerPrms.getNumBrokers()) {
      //then bid can be CachePerfClient loop index, using keyAllocationScheme
      String fields = BrokerPrms.getBrokerFields();
      return "select " + fields + " from " + Broker.getTableAndShortName()
          + " where " + Broker.getTableShortName() + ".id=" + bid;
    }
    else {
      String s =
          BasePrms.nameForKey(BrokerPrms.numBrokers)
              + " must be smaller then : "
              + BasePrms.nameForKey(CachePerfPrms.maxKeys);
      throw new HydraConfigException(s);
    }
  }

  public String getPreparedStatementForEqualityOnBrokerIdQuery() {
    String fields = BrokerPrms.getBrokerFields();
    return "select " + fields + " from " + Broker.getTableAndShortName()
        + " where " + Broker.getTableShortName() + ".id=?";
  }

  public ResultSet fillAndExecutePreparedStatementForEqualityOnBrokerIdQuery(
      PreparedStatement pstmt, String stmt, int bid) throws SQLException {
    pstmt.setInt(1, bid); // id
    return executeQueryPreparedStatement(pstmt);
  }

  public String getRandomEqualityOnBrokerIdQuery() {
    String fields = BrokerPrms.getBrokerFields();
    //pick random bid based on BrokerPrms.getNumBrokers()
    int bid = rng.nextInt(0, BrokerPrms.getNumBrokers() - 1);
    return "select " + fields + " from " + Broker.getTableAndShortName()
        + " where " + Broker.getTableShortName() + ".id=" + bid;
  }

  public ResultSet fillAndExecutePreparedStatementForRandomEqualityOnBrokerIdQuery(
      PreparedStatement pstmt, String stmt, int bid) throws SQLException {
    int randomBid = rng.nextInt(0, BrokerPrms.getNumBrokers() - 1);
    pstmt.setInt(1, randomBid); // id
    return executeQueryPreparedStatement(pstmt);
  }

  public String getTicketsFromRandomRangeOnTicketPriceQuery() {
    int range = BrokerPrms.getResultSetSize();
    return getTicketsFromRandomRangeOnTicketPriceQuery(range);
  }

  public String getTicketsFromRandomRangeOnTicketPriceQuery(int range) {
    String fields = BrokerPrms.getBrokerTicketFields();
    int min = rng.nextInt(0, BrokerPrms.getNumTicketPrices() - range);
    int max = min + range;
    return "select " + fields + " from " + BrokerTicket.getTableAndShortName()
        + " where " + BrokerTicket.getTableShortName() + ".price >= " + min
        + " and " + BrokerTicket.getTableShortName() + ".price < " + max;
    //if this guy is to compute a percentage or size for result set, it needs to know the value of distinct and how data set was created and still might not be able to fulfill the request
  }

  public String getPreparedStatementForTicketsFromRandomRangeOnTicketPriceQuery() {
    String fields = BrokerPrms.getBrokerTicketFields();
    return "select " + fields + " from " + BrokerTicket.getTableAndShortName()
        + " where " + BrokerTicket.getTableShortName() + ".price >= ?"
        + " and " + BrokerTicket.getTableShortName() + ".price < ?";
    //if this guy is to compute a percentage or size for result set, it needs to know the value of distinct and how data set was created and still might not be able to fulfill the request
  }

  public ResultSet fillAndExecutePreparedStatementForTicketsFromRandomRangeOnTicketPriceQuery(
      PreparedStatement pstmt, String stmt, int bid) throws SQLException {
    int range = BrokerPrms.getResultSetSize();
    return fillAndExecutePreparedStatementForTicketsFromRandomRangeOnTicketPriceQuery(
        pstmt, stmt, bid, range);
  }

  public ResultSet fillAndExecutePreparedStatementForTicketsFromRandomRangeOnTicketPriceQuery(
      PreparedStatement pstmt, String stmt, int bid, int range) throws SQLException {
    int min = rng.nextInt(0, BrokerPrms.getNumTicketPrices() - range);
    int max = min + range;
    pstmt.setInt(1, min);
    pstmt.setInt(2, max);
    return executeQueryPreparedStatement(pstmt);
  }

  public String getBrokersFromRandomRangeOnTicketPriceQuery() {
    int range = BrokerPrms.getResultSetSize();
    return getBrokersFromRandomRangeOnTicketPriceQuery(range);
  }

  private String getBrokersFromRandomRangeOnTicketPriceQuery(int range) {
    /*
     * String fields =
        BrokerPrms.getBrokerFields() + "," + BrokerPrms.getBrokerTicketFields();
    //handle case where one is *
    if (fields.indexOf("*") != -1) {
      fields = "*";
    }
    */
    String fields = BrokerPrms.getFields();
    int min = rng.nextInt(0, BrokerPrms.getNumTicketPrices() - range);
    int max = min + range;
    return "select " + fields + " from " + Broker.getTableAndShortName() + ","
        + BrokerTicket.getTableAndShortName() + " where "
        + BrokerTicket.getTableShortName() + ".brokerId = "
        + Broker.getTableShortName() + ".id and "
        + BrokerTicket.getTableShortName() + ".price >= " + min + " and "
        + BrokerTicket.getTableShortName() + ".price < " + max;
  }

  public String getPreparedStatementForBrokersFromRandomRangeOnTicketPriceQuery() {
    /*
     * String fields =
        BrokerPrms.getBrokerFields() + "," + BrokerPrms.getBrokerTicketFields();
    //handle case where one is *
    if (fields.indexOf("*") != -1) {
      fields = "*";
    }
    */
    String fields = BrokerPrms.getFields();
    return "select " + fields + " from " + Broker.getTableAndShortName() + ","
        + BrokerTicket.getTableAndShortName() + " where "
        + BrokerTicket.getTableShortName() + ".brokerId = "
        + Broker.getTableShortName() + ".id and "
        + BrokerTicket.getTableShortName() + ".price >= ?" + " and "
        + BrokerTicket.getTableShortName() + ".price < ?";
  }

  public ResultSet fillAndExecutePreparedStatementForBrokersFromRandomRangeOnTicketPriceQuery(
      PreparedStatement pstmt, String stmt, int bid) throws SQLException {
    int range = BrokerPrms.getResultSetSize();
    return fillAndExecutePreparedStatementForBrokersFromRandomRangeOnTicketPriceQuery(
        pstmt, stmt, bid, range);
  }

  public ResultSet fillAndExecutePreparedStatementForBrokersFromRandomRangeOnTicketPriceQuery(
      PreparedStatement pstmt, String stmt, int bid, int range) throws SQLException {
    int min = rng.nextInt(0, BrokerPrms.getNumTicketPrices() - range);
    int max = min + range;
    pstmt.setInt(1, min);
    pstmt.setInt(2, max);
    return executeQueryPreparedStatement(pstmt);
  }

  public String getBrokersFromRandomSizeRangeOnTicketPriceQuery() {
    int size = BrokerPrms.getResultSetSize();
    return getBrokersFromRandomSizeRangeOnTicketPriceQuery(size);
  }

  public String getBrokersFromRandomSizeRangeOnTicketPriceQuery(int size) {
    int ticketsPerPrice =
        BrokerPrms.getNumBrokers() * BrokerPrms.getNumTicketsPerBroker()
            / BrokerPrms.getNumTicketPrices();
    if (!BrokerPrms.useBestFit() && size % ticketsPerPrice != 0) {
      String s =
          "Attempted result set size (" + size
              + ") does not divide evenly by ticketsPerPrice("
              + ticketsPerPrice + ")";
      throw new HydraConfigException(s);
    }
    int range = (int) Math.ceil(size / ticketsPerPrice);
    return getBrokersFromRandomRangeOnTicketPriceQuery(range);
  }

  public ResultSet fillAndExecutePreparedStatementForBrokersFromRandomSizeRangeOnTicketPriceQuery(
      PreparedStatement pstmt, String stmt, int bid) throws SQLException {
    int size = BrokerPrms.getResultSetSize();
    return fillAndExecutePreparedStatementForBrokersFromRandomSizeRangeOnTicketPriceQuery(
        pstmt, stmt, bid, size);
  }

  public ResultSet fillAndExecutePreparedStatementForBrokersFromRandomSizeRangeOnTicketPriceQuery(
      PreparedStatement pstmt, String stmt, int bid, int size) throws SQLException {
    int ticketsPerPrice =
        BrokerPrms.getNumBrokers() * BrokerPrms.getNumTicketsPerBroker()
            / BrokerPrms.getNumTicketPrices();
    if (!BrokerPrms.useBestFit() && size % ticketsPerPrice != 0) {
      String s =
          "Attempted result set size (" + size
              + ") does not divide evenly by ticketsPerPrice("
              + ticketsPerPrice + ")";
      throw new HydraConfigException(s);
    }
    int range = (int) Math.ceil(size / ticketsPerPrice);
    return fillAndExecutePreparedStatementForBrokersFromRandomRangeOnTicketPriceQuery(
        pstmt, stmt, bid, range);
  }

  public String getBrokersFromRandomPercentageRangeOnTicketPriceQuery() {
    String fields = BrokerPrms.getBrokerFields();
    int percent = BrokerPrms.getResultSetPercentage();
    int tickets =
        BrokerPrms.getNumBrokers() * BrokerPrms.getNumTicketsPerBroker();
    //use best fit?
    int size = (int) Math.ceil(tickets * percent / 100.0);
    return getBrokersFromRandomSizeRangeOnTicketPriceQuery(size);
  }

  public ResultSet fillAndExecutePreparedStatementForBrokersFromRandomPercentageRangeOnTicketPriceQuery(
      PreparedStatement pstmt, String stmt, int bid) throws SQLException {
    String fields = BrokerPrms.getBrokerFields();
    int percent = BrokerPrms.getResultSetPercentage();
    int tickets =
        BrokerPrms.getNumBrokers() * BrokerPrms.getNumTicketsPerBroker();
    //use best fit?
    int size = (int) Math.ceil(tickets * percent / 100.0);
    return fillAndExecutePreparedStatementForBrokersFromRandomSizeRangeOnTicketPriceQuery(
        pstmt, stmt, bid, size);
  }

  //----------------------------------------------------------------------------
  // execution
  //----------------------------------------------------------------------------

  public ResultSet fillAndExecutePreparedQueryStatement(
      PreparedStatement pstmt, String stmt, int queryType, int i) throws SQLException {
    ResultSet rs = null;
    if (logQueries) {
      Log.getLogWriter().info("Executing Prepared Statement: ");
    }
    //do switch/if statement here
    if (queryType == BrokerPrms.EQUALITY_ON_BROKER_ID_QUERY) {
      rs = fillAndExecutePreparedStatementForEqualityOnBrokerIdQuery(pstmt, stmt, i);
    }
    else if (queryType == BrokerPrms.TICKETS_FROM_RANDOM_RANGE_ON_TICKET_PRICE_QUERY) {
      rs =
          fillAndExecutePreparedStatementForTicketsFromRandomRangeOnTicketPriceQuery(
              pstmt, stmt, i);
    }
    else if (queryType == BrokerPrms.BROKERS_FROM_RANDOM_PERCENTAGE_RANGE_ON_TICKET_PRICE_QUERY) {
      rs =
          fillAndExecutePreparedStatementForBrokersFromRandomRangeOnTicketPriceQuery(
              pstmt, stmt, i);
    }
    else if (queryType == BrokerPrms.BROKERS_FROM_RANDOM_RANGE_ON_TICKET_PRICE_QUERY) {
      rs =
          fillAndExecutePreparedStatementForBrokersFromRandomRangeOnTicketPriceQuery(
              pstmt, stmt, i);
    }
    else if (queryType == BrokerPrms.BROKERS_FROM_RANDOM_SIZE_RANGE_ON_TICKET_PRICE_QUERY) {
      rs =
          fillAndExecutePreparedStatementForBrokersFromRandomSizeRangeOnTicketPriceQuery(
              pstmt, stmt, i);
    }
    else if (queryType == BrokerPrms.RANDOM_EQUALITY_ON_BROKER_ID_QUERY) {
      rs =
          fillAndExecutePreparedStatementForRandomEqualityOnBrokerIdQuery(
              pstmt, stmt, i);
    }
    else if (queryType == BrokerPrms.TICKETS_FROM_EQUALITY_ON_BROKER_ID_QUERY) {
      rs =
          fillAndExecutePreparedStatementForTicketsFromEqualityOnBrokerIdQuery(
              pstmt, stmt, i);
    }
    if (logQueries) {
      Log.getLogWriter().info("Executed Prepared Statement: ");
    }
    return rs;
  }
  
  public int fillAndExecuteUpdatePreparedQueryStatement(
      PreparedStatement pstmt, String stmt, int queryType, int i) throws SQLException {
    return 0;
  }

  public String resultSetToString(ResultSet resultSet) throws SQLException {
    ResultSetMetaData metaData = resultSet.getMetaData();
    int numColumns = metaData.getColumnCount();
    StringBuffer sb = new StringBuffer("");
    int rowNum = 0;
    while (resultSet.next() == true) {
      sb.append("Row " + rowNum++ + " : ");
      for (int i = 0; i < numColumns; i++) {
        String columnName = metaData.getColumnName(i);
        int type = metaData.getColumnType(i);
        sb.append(columnName);
        sb.append("=");
        sb.append(resultSet.getObject(i).toString());
        if (i < numColumns - 1) {
          sb.append(",");
        }
      }
    }
    return sb.toString();
  }

  public int readResultSet(int queryType, ResultSet rs) throws SQLException {
    //todo implement a read method for each query supported.
    return 0;
  }
  
  public Region getRegionForQuery(int queryType) {
    return null;
  }
  
  public Object directGet(Object key, Region region) {
    return null;
  }
}
