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
package objects.query.sector;

import com.gemstone.gemfire.cache.Region;

import hydra.BasePrms;
import hydra.GsRandom;
import hydra.HydraConfigException;
import hydra.Log;
import hydra.RemoteTestModule;
import hydra.TestConfig;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import objects.query.BaseSQLQueryFactory;
import objects.query.QueryFactoryException;
import objects.query.QueryObjectException;
import objects.query.QueryPrms;
import objects.query.SQLQueryFactory;

import util.TestException;

public class SQLSectorQueryFactory extends BaseSQLQueryFactory implements SQLQueryFactory {

  protected final GsRandom rng = new GsRandom(RemoteTestModule.getCurrentThread().getThreadId());

  protected SQLInstrumentQueryFactory instrumentQueryFactory;

  protected static final int numSectors = SectorPrms.getNumSectors();
  protected static final int numInstrumentsPerSector = SectorPrms.getNumInstrumentsPerSector();
  protected static final int numPositionsPerInstrument = SectorPrms.getNumPositionsPerInstrument();
  protected static final int numInstruments = numSectors * numInstrumentsPerSector;
  protected static final int numPositions = numInstruments * numPositionsPerInstrument;

  protected static final int numBookValues = SectorPrms.getNumBookValues();
  protected static final int numSymbolValues = SectorPrms.getNumSymbolValues();
  protected static final int numMarketCapValues = SectorPrms.getNumMarketCapValues();

  protected static final int numAmountsPerRangeClause = SectorPrms.getNumAmountsPerRangeClause();
  protected static final int numInstrumentsPerInClause = SectorPrms.getNumInstrumentsPerInClause();
  protected static final int numMarketCapValuesPerOrClause = SectorPrms.getNumMarketCapValuesPerOrClause();
  protected static final int numOwnersPerOrClause = SectorPrms.getNumOwnersPerOrClause();
  protected static final int numPositionsPerInClause = SectorPrms.getNumPositionsPerInClause();
  protected static final int numSectorsPerInClause = SectorPrms.getNumSectorsPerInClause();
  protected static final int numSymbolsPerInClause = SectorPrms.getNumSymbolsPerInClause();

  protected int[] sectors = new int[numSectors];
  protected int[] instruments = new int[numInstrumentsPerSector];
  protected int[] positions = new int[numPositionsPerInstrument];
  protected int[] symbolValues = new int[numSymbolValues];
  protected int[] ownerValues = new int[numPositionsPerInstrument];

  protected int[] random; // random number buffer

  //Position Query Factory only used for testing creates on the position table.
  //This is only done through the preparedQueryQueryTask.
  //The preparedCreateDataTask will be using the instrumentQueryFactory that uses its own
  //positionQueryFactory
  protected SQLPositionQueryFactory positionQueryFactory;

  public SQLSectorQueryFactory() {
    instrumentQueryFactory = new SQLInstrumentQueryFactory();
    positionQueryFactory = new SQLPositionQueryFactory();
  }

  public void init() {
    super.init();
    instrumentQueryFactory.init();
    positionQueryFactory.init();
    // initialize the sets of unique ids used for choosing random values
    for (int i = 0; i < numSectors; i++) {
      sectors[i] = i;
    }
    for (int i = 0; i < numInstrumentsPerSector; i++) {
      instruments[i] = i;
    }
    for (int i = 0; i < numPositionsPerInstrument; i++) {
      positions[i] = i;
    }
    for (int i = 0; i < numSymbolValues; i++) {
      symbolValues[i] = i;
    }
    for (int i = 0; i < numPositionsPerInstrument; i++) {
      ownerValues[i] = i;
    }
    // create a buffer big enough to hold the random values for any clause
    int max = Math.max(0, numAmountsPerRangeClause);
    max = Math.max(max, numInstrumentsPerInClause);
    max = Math.max(max, numMarketCapValuesPerOrClause);
    max = Math.max(max, numOwnersPerOrClause);
    max = Math.max(max, numPositionsPerInClause);
    max = Math.max(max, numSectorsPerInClause);
    max = Math.max(max, numSymbolsPerInClause);
    random = new int[max];
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
    return SectorPrms.getQueryType(QueryPrms.GFXD);
  }
  
  public int getUpdateQueryType() {
    return SectorPrms.getUpdateQueryType(QueryPrms.GFXD);
  }
  
  public int getDeleteQueryType() {
    return SectorPrms.getDeleteQueryType(QueryPrms.GFXD);
  }

  public String getQuery(int queryType, int i) {
    String query = "";
    int type = queryType;
    switch (type) {
      case SectorPrms.RANDOM_POSITION_PK_QUERY:
        query = getRandomPositionPKQuery();
        break;
      case SectorPrms.SECTOR_FILTER_IN_QUERY:
        query = getSectorFilterInQuery(i);
        break;
      case SectorPrms.SECTOR_FILTER_IN_AND_MARKET_CAP_QUERY:
        query = getSectorFilterInAndMarketCapQuery(i);
        break;
      case SectorPrms.POSITION_PRUNE_BY_INSTRUMENT_QUERY:
        query = getPositionPruneByInstrumentQuery(i);
        break;
      case SectorPrms.POSITION_AMOUNT_RANGE_AND_QUERY:
        query = getPositionAmountRangeAndQuery(i);
        break;
      case SectorPrms.POSITION_AMOUNT_RANGE_OR_QUERY:
        query = getPositionAmountRangeOrQuery(i);
        break;
      case SectorPrms.POSITION_PRUNE_BY_INSTRUMENT_AND_POSITION_AMOUNT_QUERY:
        query = getPositionPruneByInstrumentAndPositionAmountQuery(i);
        break;
      case SectorPrms.POSITION_PRUNE_BY_INSTRUMENT_AND_FILTER_SYMBOL_AND_OWNER_QUERY:
        query = getPositionPruneByInstrumentAndFilterSymbolAndOwnerQuery(i);
        break;
      case SectorPrms.JOIN_PRUNE_BY_POSITION_AMOUNT_AND_INSTRUMENT_NAME_QUERY:
        query = getJoinPruneByPositionAmountAndInstrumentNameQuery(i);
        break;
      case SectorPrms.JOIN_PRUNE_BY_POSITION_AMOUNT_AND_INSTRUMENT_NAME_AND_SECTOR_NAME_QUERY:
        query =
            getJoinPruneByPositionAmountAndInstrumentNameAndSectorNameQuery(i);
        break;
      case SectorPrms.INSTRUMENT_IN_QUERY:
        query = getInstrumentInQuery(i);
        break;
      case SectorPrms.JOIN_INSTRUMENT_AND_POSITION_IN_QUERY:
        query = getJoinInstrumentAndPositionInQuery(i);
        break;
      case SectorPrms.POSITION_FILTER_IN_QUERY:
        query = getPositionFilterInQuery(i);
        break;
      case SectorPrms.UPDATE_POSITION_BY_POSITION_ID_QUERY:
        query = getUpdatePositionByPositionIdQuery(i);
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
      case SectorPrms.RANDOM_POSITION_PK_QUERY:
        query = getPreparedStatementForRandomPositionPKQuery();
        break;
      case SectorPrms.SECTOR_FILTER_IN_QUERY:
        query = getPreparedStatementForSectorFilterInQuery();
        break;
      case SectorPrms.SECTOR_FILTER_IN_AND_MARKET_CAP_QUERY:
        query = getPreparedStatementForSectorFilterInAndMarketCapQuery();
        break;
      case SectorPrms.POSITION_PRUNE_BY_INSTRUMENT_QUERY:
        query = getPreparedStatementForPositionPruneByInstrumentQuery();
        break;
      case SectorPrms.POSITION_AMOUNT_RANGE_AND_QUERY:
        query =
            getPreparedStatementForPositionAmountRangeAndQuery();
        break;
      case SectorPrms.POSITION_AMOUNT_RANGE_OR_QUERY:
        query =
            getPreparedStatementForPositionAmountRangeOrQuery();
        break;
      case SectorPrms.POSITION_PRUNE_BY_INSTRUMENT_AND_POSITION_AMOUNT_QUERY:
        query =
            getPreparedStatementForPositionPruneByInstrumentAndPositionAmountQuery();
        break;
      case SectorPrms.POSITION_PRUNE_BY_INSTRUMENT_AND_FILTER_SYMBOL_AND_OWNER_QUERY:
        query =
            getPreparedStatementForPositionPruneByInstrumentAndFilterSymbolAndOwnerQuery();
        break;
      case SectorPrms.JOIN_PRUNE_BY_POSITION_AMOUNT_AND_INSTRUMENT_NAME_QUERY:
        query =
            getPreparedStatementForJoinPruneByPositionAmountAndInstrumentNameQuery();
        break;
      case SectorPrms.JOIN_PRUNE_BY_POSITION_AMOUNT_AND_INSTRUMENT_NAME_AND_SECTOR_NAME_QUERY:
        query =
            getPreparedStatementForJoinPruneByPositionAmountAndInstrumentNameAndSectorNameQuery();
        break;
      case SectorPrms.INSTRUMENT_IN_QUERY:
        query = getPreparedStatementForInstrumentInQuery();
        break;
      case SectorPrms.JOIN_INSTRUMENT_AND_POSITION_IN_QUERY:
        query = getPreparedStatementForJoinInstrumentAndPositionInQuery();
        break;
      case SectorPrms.JOIN_INSTRUMENT_AND_POSITION_OR_QUERY:
        query = getPreparedStatementForJoinInstrumentAndPositionOrQuery();
        break;
      case SectorPrms.POSITION_FILTER_IN_QUERY:
        query = getPreparedStatementForPositionFilterInQuery();
        break;
      case SectorPrms.UPDATE_POSITION_BY_POSITION_ID_QUERY:
        query = getPreparedStatementForUpdatePositionByPositionIdQuery();
        break;
      case SectorPrms.INSERT_INTO_POSITION_QUERY:
        query = getPreparedInsertStatementForPosition();
        break;
      case SectorPrms.JOIN_TO_SINGLE_NODE_QUERY:
        query = getPreparedStatementForJoinToSingleNodeQuery();
        break;
      default:
        throw new UnsupportedOperationException("Should not happen");
    }
    return query;
  }

  //--------------------------------------------------------------------------
  // QueryFactory : primary keys
  //--------------------------------------------------------------------------
  public String getPrimaryKeyIndexOnSectorId() {
    return "alter table " + Sector.getTableName() + " add primary key(id)";
  }

  public String getPrimaryKeyIndexOnInstrumentId() {
    return "alter table " + Instrument.getTableName() + " add primary key(id)";
  }

  public String getPrimaryKeyIndexOnPositionId() {
    return "alter table " + Position.getTableName() + " add primary key(id)";
  }

  //--------------------------------------------------------------------------
  // QueryFactory : indexes
  //--------------------------------------------------------------------------
  public List getIndexStatements() {
    List stmts = new ArrayList();
    Vector indexTypes = SectorPrms.getIndexTypes();
    for (Iterator i = indexTypes.iterator(); i.hasNext();) {
      String indexTypeString = (String) i.next();
      int indexType = SectorPrms.getIndexType(indexTypeString);
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
      case SectorPrms.PRIMARY_KEY_INDEX_ON_SECTOR_ID_QUERY:
        query = getPrimaryKeyIndexOnSectorId();
        break;
      case SectorPrms.UNIQUE_INDEX_ON_SECTOR_NAME_QUERY:
        query = getUniqueIndexOnSectorName();
        break;
      case SectorPrms.PRIMARY_KEY_INDEX_ON_INSTRUMENT_ID_QUERY:
        query = getPrimaryKeyIndexOnInstrumentId();
        break;
      case SectorPrms.PRIMARY_KEY_INDEX_ON_POSITION_ID_QUERY:
        query = getPrimaryKeyIndexOnPositionId();
        break;
      case SectorPrms.INDEX_ON_SECTOR_ID_QUERY:
        query = getIndexOnSectorId();
        break;
      case SectorPrms.INDEX_ON_INSTRUMENT_ID_QUERY:
        query = getIndexOnInstrumentId();
        break;
      case SectorPrms.INDEX_ON_POSITION_ID_QUERY:
        query = getIndexOnPositionId();
        break;
      case SectorPrms.INDEX_ON_POSITION_SYNTHETIC_QUERY:
        query = getIndexOnPositionSynthetic();
        break;
      case SectorPrms.INDEX_ON_POSITION_INSTRUMENT_QUERY:
        query = getIndexOnPositionInstrument();
        break;
      case SectorPrms.INDEX_ON_POSITION_SYMBOL_QUERY:
        query = getIndexOnPositionSymbol();
        break;
      case SectorPrms.INDEX_ON_POSITION_OWNER_QUERY:
        query = getIndexOnPositionOwner();
        break;
      case SectorPrms.INDEX_ON_POSITION_AMOUNT_QUERY:
        query = getIndexOnPositionAmount();
        break;
      case SectorPrms.INDEX_ON_SECTOR_NAME_QUERY:
        query = getIndexOnSectorName();
        break;
      case SectorPrms.INDEX_ON_SECTOR_MARKET_CAP_QUERY:
        query = getIndexOnSectorMarketCap();
        break;
      case SectorPrms.NO_QUERY:
        query = null;
        break;
      default:
        throw new UnsupportedOperationException("Should not happen");
    }
    return query;
  }

  public String getUniqueIndexOnSectorName() {
    return "create unique index name_uidx on " + Sector.getTableName()
        + " (name)";
  }

  public String getIndexOnSectorId() {
    return "create index sector_id_idx on " + Sector.getTableName() + " (id)";
  }

  public String getIndexOnInstrumentId() {
    return "create index instrument_id_idx on " + Instrument.getTableName()
        + " (id)";
  }

  public String getIndexOnPositionId() {
    return "create index position_id_idx on " + Position.getTableName()
        + " (id)";
  }

  public String getIndexOnPositionSynthetic() {
    return "create index position_synthetic_idx on " + Position.getTableName()
        + " (synthetic)";
  }

  public String getIndexOnPositionInstrument() {
    return "create index position_instrument_idx on " + Position.getTableName()
        + " (instrument)";
  }

  public String getIndexOnPositionSymbol() {
    return "create index position_symbol_idx on " + Position.getTableName()
        + " (symbol)";
  }

  public String getIndexOnPositionOwner() {
    return "create index position_owner_idx on " + Position.getTableName()
        + " (owner)";
  }

  public String getIndexOnPositionAmount() {
    return "create index position_amount_idx on " + Position.getTableName()
        + " (amount)";
  }

  public String getIndexOnSectorName() {
    return "create index sector_name_idx on " + Sector.getTableName()
        + " (name)";
  }

  public String getIndexOnSectorMarketCap() {
    return "create index sector_market_cap_idx on " + Sector.getTableName()
        + " (market_cap)";
  }

  //--------------------------------------------------------------------------
  // QueryFactory : Inserts
  //--------------------------------------------------------------------------

  public List getInsertStatements(int bid) {
    List stmts = new ArrayList();
    double marketCap = bid % numMarketCapValues;
    if (bid >= numSectors) {
      String s =
          "Attempt to get insert statement with sid=" + bid + " when "
              + BasePrms.nameForKey(SectorPrms.numSectors) + "=" + numSectors;
      throw new QueryObjectException(s);
    }
   
    String sname = Sector.getSectorName(bid);
    String stmt =
        "insert into " + Sector.getTableName()
            + " (id, name, market_cap) values (" + bid + ",'" + sname + "',"
            + marketCap + ")";
    stmts.add(stmt);
    //do book table here before instrument table
    stmts.addAll(instrumentQueryFactory.getInsertStatements(bid));
    
    return stmts;
  }

  public List getPreparedInsertStatements() {
    List stmts = new ArrayList();
    //do book table here before instrument table
    String stmt = "insert into " + Sector.getTableName() + " values (?,?,?)";
    stmts.add(stmt);
    stmts.add(instrumentQueryFactory.getPreparedInsertStatements());
    return stmts;
  }

  public int fillAndExecutePreparedInsertStatements(List pstmts, List stmts, int i)
      throws SQLException {
    double marketCap = i % numMarketCapValues;
    PreparedStatement pstmt = (PreparedStatement) pstmts.get(0);
    String stmt = (String)stmts.get(0);
    int results = 0;
    if (i >= numSectors) {
      String s =
          "Attempt to get insert statement with sid=" + i + " when "
              + BasePrms.nameForKey(SectorPrms.numSectors) + "=" + numSectors;
      throw new QueryObjectException(s);
    }
    String sectorName = Sector.getSectorName(i);
    pstmt.setInt(1, i);
    pstmt.setString(2, sectorName);
    pstmt.setDouble(3, marketCap);
    if (logQueries) {
      Log.getLogWriter().info("EXECUTING: " + stmt + " with id=" + i
         + " name=" + sectorName + " market_cap=" + marketCap);
    }
    results += pstmt.executeUpdate();
    results += instrumentQueryFactory.fillAndExecutePreparedInsertStatements((List)pstmts.get(1), (List)stmts.get(1), i);
    return results;
  }

  private String getPreparedInsertStatementForPosition() {
    return positionQueryFactory.getPreparedInsertStatement();
  }
  
  private int fillAndExecutePreparedInsertStatementForPosition(PreparedStatement pstmt, String stmt, int i)
      throws SQLException {
    double marketCap = i % numMarketCapValues;
    positionQueryFactory.fillPreparedInsertStatement(pstmt, stmt, i);
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
    String stmt = "create table " + Sector.getTableName()
      + " (" + getIdCreateStatement() + ", " + getNameCreateStatement() +", " + getMarketCapCreateStatement() + ")";
    stmts.add(stmt);
    //do book table here before instrument table
    stmts.addAll(instrumentQueryFactory.getTableStatements());
    return stmts;
  }

  public List getDropTableStatements() {
    List stmts = new ArrayList();
    //do book table here before instrument table
    stmts.addAll(instrumentQueryFactory.getDropTableStatements());
    stmts.add("drop table if exists " + Sector.getTableName());
    return stmts;
  }
  
  protected String getIdCreateStatement() {
    String stmt = "id int not null";
    List indexes = SectorPrms.getSectorCreateTableIndexes();
    Iterator iterator = indexes.iterator();
    
    while (iterator.hasNext()) {
      int indexType = SectorPrms.getIndexType((String)iterator.next());
      if (indexType == SectorPrms.PRIMARY_KEY_INDEX_ON_SECTOR_ID_QUERY) {
        stmt += " primary key";
      }
      else if (indexType == SectorPrms.UNIQUE_INDEX_ON_SECTOR_ID_QUERY) {
        stmt += " unique";
      }
    }
  
    return stmt;
  }
  
  protected String getNameCreateStatement() {
    String stmt = "name varchar(20)";
    List indexes = SectorPrms.getSectorCreateTableIndexes();
    Iterator iterator = indexes.iterator();
    
    while (iterator.hasNext()) {
      int indexType = SectorPrms.getIndexType((String)iterator.next());
      if (indexType == SectorPrms.PRIMARY_KEY_INDEX_ON_SECTOR_NAME_QUERY) {
        stmt += " primary key";
      }
      else if (indexType == SectorPrms.UNIQUE_INDEX_ON_SECTOR_NAME_QUERY) {
        stmt += " unique";
      }
    }
    return stmt;
  }
  
  protected String getMarketCapCreateStatement() {
    String stmt = "market_cap double";
    List indexes = SectorPrms.getSectorCreateTableIndexes();
    Iterator iterator = indexes.iterator();
    
    while (iterator.hasNext()) {
      int indexType = SectorPrms.getIndexType((String)iterator.next());
      if (indexType == SectorPrms.PRIMARY_KEY_INDEX_ON_MARKET_CAP_QUERY) {
        stmt += " primary key";
      }
      else if (indexType == SectorPrms.UNIQUE_INDEX_ON_MARKET_CAP_QUERY) {
        stmt += " unique";
      }
    }
    return stmt;
  }

  //--------------------------------------------------------------------------
  // QueryFactory : non prepared queries
  //--------------------------------------------------------------------------

  public String getRandomPositionPKQuery() {
    String fields = SectorPrms.getPositionFields();
    int id = rng.nextInt(0, numPositions - 1);
    return "select " + fields + " from " + Position.getTableAndShortName()
        + " where " + Position.getTableShortName() + ".id=" + id;
  }

  /**
   * Method will return a query that filters with an IN clause.
   * The IN parameters are generated sequentially.
   * 
   * @return a query that filters with an IN clause
   */
  public String getSectorFilterInQuery(int sid) {
    String fields = SectorPrms.getSectorFields();
    StringBuffer sb =
        new StringBuffer("select " + fields + " from "
            + Sector.getTableAndShortName() + " where "
            + Sector.getTableShortName() + ".name in ('");
    pickRandomSectors(numSectorsPerInClause);
    for (int i = 0; i < numSectorsPerInClause; i++) {
      if (i != 0) sb.append("','");
      sb.append(Sector.getSectorName(random[i]));
    }
    sb.append("')");
    return sb.toString();
  }

  public String getPositionFilterInQuery(int sid) {
    String fields = SectorPrms.getPositionFields();
    StringBuffer sb =
        new StringBuffer("select " + fields + " from "
            + Position.getTableAndShortName() + " where "
            + Position.getTableShortName() + ".id in ('");
    if (SectorPrms.getPositionPartitionType() == QueryPrms.PARTITION_BY_RANGE &&
        SectorPrms.getPositionPartitionColumn().equalsIgnoreCase("id")) {
      // pick positions on the fewest nodes possible, starting with a random node
      int numNodes = SectorPrms.getNumServers();
      int node = rng.nextInt(0, numNodes - 1);
      int range = numPositions / numNodes;
      int remainder = numPositions - numNodes * range;
      int lower = SQLPositionQueryFactory.getLowerIdForNode(node, range, remainder);
      // pick positions in order, wrapping around if needed
      // @todo start at a random point on the node (as long as it fits on the node)
      for (int i = 1; i <= numPositionsPerInClause; i++) {
        if (i != 1) sb.append("','");
        int val = (lower + i - 1) % numPositions;
        sb.append(val);
      }
    } else {
      pickRandomPositions(numPositionsPerInClause);
      for (int i = 0; i < numPositionsPerInClause; i++) {
        if (i != 0) sb.append("','");
        sb.append(random[i]);
      }
    }
    sb.append("')");
    return sb.toString();
  }

  public String getSectorFilterInAndMarketCapQuery(int sid) {
    String fields = SectorPrms.getSectorFields();
    StringBuffer sb =
        new StringBuffer("select " + fields + " from "
            + Sector.getTableAndShortName() + " where "
            + Sector.getTableShortName() + ".name in ('");
    int secid = rng.nextInt(0, numSectors - numSectorsPerInClause - 1);
    int mcv = secid % numMarketCapValues;
    for (int i = 0; i < numSectorsPerInClause; i++) {
      if (i != 0) sb.append("','");
      sb.append(Sector.getSectorName(secid % numSectors));
      ++secid;
    }
    sb.append("') and (");
    for (int i = numSectorsPerInClause; i < numSectorsPerInClause + numMarketCapValuesPerOrClause; i++) {
      if (i != numSectorsPerInClause) sb.append(" or ");
      int mval = mcv % numMarketCapValues;
      sb.append(Sector.getTableShortName() + ".market_cap=" + mval);
      ++mcv;
    }
    sb.append(")");
    return sb.toString();
  }

  /**
   * number of rows returned will be numPositionsPerInstrument / 2
   */
  public String getPositionPruneByInstrumentQuery(int sid) {
    String fields = SectorPrms.getPositionFields();
    int iid = rng.nextInt(0, numInstruments - 1);
    String s =
        "select " + fields + " from " + Position.getTableAndShortName()
            + " where " + Position.getTableShortName() + ".synthetic=1"
            + " and " + Position.getTableShortName() + ".instrument='"
            + Instrument.getInstrument(iid) + "'";
    return s;
  }

  public String getPositionAmountRangeAndQuery(int sid) {
    String fields = SectorPrms.getPositionFields();
    int iid = rng.nextInt(0, numInstruments - 1);
    int min = iid * numPositionsPerInstrument + rng.nextInt(0, numPositionsPerInstrument - numAmountsPerRangeClause);
    int max = min + numAmountsPerRangeClause;
    String s =
        "select " + fields + " from " + Position.getTableAndShortName()
            + " where "
            + Position.getTableShortName() + ".amount >= " + min + " and "
            + Position.getTableShortName() + ".amount < " + max;
    return s;
  }

  public String getPositionAmountRangeOrQuery(int sid) {
    String fields = SectorPrms.getPositionFields();
    int iid1 = rng.nextInt(0, numInstruments - 1);
    int min1 = iid1 * numPositionsPerInstrument + rng.nextInt(0, numPositionsPerInstrument - numAmountsPerRangeClause);
    int max1 = min1 + numAmountsPerRangeClause;
    int iid2 = rng.nextInt(0, numInstruments - 1);
    int min2 = iid2 * numPositionsPerInstrument + rng.nextInt(0, numPositionsPerInstrument - numAmountsPerRangeClause);
    int max2 = min2 + numAmountsPerRangeClause;
    String s =
        "select " + fields + " from " + Position.getTableAndShortName()
            + " where ("
            + Position.getTableShortName() + ".amount >= " + min1 + " and "
            + Position.getTableShortName() + ".amount < " + max1 + ") or ("
            + Position.getTableShortName() + ".amount >= " + min2 + " and "
            + Position.getTableShortName() + ".amount < " + max2 + ")";
    return s;
  }

  public String getPositionPruneByInstrumentAndPositionAmountQuery(int sid) {
    String fields = SectorPrms.getPositionFields();
    int iid = rng.nextInt(0, numInstruments - 1);
    int min = iid * numPositionsPerInstrument + rng.nextInt(0, numPositionsPerInstrument - numAmountsPerRangeClause);
    int max = min + numAmountsPerRangeClause;
    String s =
        "select " + fields + " from " + Position.getTableAndShortName()
            + " where " + Position.getTableShortName() + ".instrument='"
            + Instrument.getInstrument(iid) + "'" + " and "
            + Position.getTableShortName() + ".amount >= " + min + " and "
            + Position.getTableShortName() + ".amount < " + max;
    return s;
  }

  public String getPositionPruneByInstrumentAndFilterSymbolAndOwnerQuery(int sid) {
    String fields = SectorPrms.getPositionFields();
    int instrumentId = rng.nextInt(0, numInstruments - 1);
    StringBuffer sb =
        new StringBuffer("select " + fields + " from "
            + Position.getTableAndShortName() + " where "
            + Position.getTableShortName() + ".instrument='"
            + Instrument.getInstrument(instrumentId) + "'" + " and "
            + Position.getTableShortName() + ".symbol in ('");
    int iid = rng.nextInt(0, numInstruments - 1);
    String instrument = Instrument.getInstrument(iid);
    String symbols = logQueries ? "" : null;
    pickRandomSymbolValues(numSymbolsPerInClause);
    for (int i = 2; i < 2 + numSymbolsPerInClause; i++) {
      if (i != 2) sb.append("','");
      int secid = random[i-2];
      sb.append(Position.getSymbol(secid));
    }
    sb.append("')");
    sb.append(" and (");
    String owners = logQueries ? "" : null;
    pickRandomOwnerValues(numOwnersPerOrClause);
    for (int i = 2 + numSymbolsPerInClause; i < 2 + numSymbolsPerInClause + numOwnersPerOrClause; i++) {
      if (i != 2 + numSymbolsPerInClause) sb.append(" or ");
      int oid = random[i - (2 + numSymbolsPerInClause)];
      sb.append(Position.getTableShortName() + ".owner='"
          + Position.getOwner(oid) + "'");
    }
    sb.append(")");
    return sb.toString();
  }

  public String getJoinPruneByPositionAmountAndInstrumentNameQuery(int sid) {
    String fields = getPositionAndInstrumentFields();
    int iid = rng.nextInt(0, numInstruments - 1);
    int min = iid * numPositionsPerInstrument + rng.nextInt(0, numPositionsPerInstrument - numAmountsPerRangeClause);
    int max = min + numAmountsPerRangeClause;
    StringBuffer sb =
        new StringBuffer("select " + fields + " from "
            + Position.getTableAndShortName() + ","
            + Instrument.getTableAndShortName() + " where "
            + Instrument.getTableShortName() + ".id = "
            + Position.getTableShortName() + ".instrument" + " and "
            + Position.getTableShortName() + ".amount >= " + min + " and "
            + Position.getTableShortName() + ".amount < " + max + " and "
            + Instrument.getTableShortName() + ".id='"
            + Instrument.getInstrument(iid) + "'");
    return sb.toString();
  }

  public String getJoinPruneByPositionAmountAndInstrumentNameAndSectorNameQuery(
      int sid) {
    int iid = sid * numInstrumentsPerSector + rng.nextInt(0, numInstrumentsPerSector - 1);
    int min = iid * numPositionsPerInstrument + rng.nextInt(0, numPositionsPerInstrument - numAmountsPerRangeClause);
    int max = min + numAmountsPerRangeClause;

    String fields = SectorPrms.getPositionFields();
    StringBuffer sb =
        new StringBuffer("select " + fields + " from "
            + Position.getTableAndShortName() + ","
            + Instrument.getTableAndShortName() + ","
            + Sector.getTableAndShortName() + " where "
            + Instrument.getTableShortName() + ".id = "
            + Position.getTableShortName() + ".instrument" + " and "
            + Instrument.getTableShortName() + ".sector_id = "
            + Sector.getTableShortName() + ".id" + " and "
            + Position.getTableShortName() + ".amount >= " + min + " and "
            + Position.getTableShortName() + ".amount < " + max + " and "
            + Instrument.getTableShortName() + ".id='"
            + Instrument.getInstrument(iid) + "'" + " and "
            + Sector.getTableShortName() + ".name='" + Sector.getSectorName(sid)
            + "'");
    return sb.toString();
  }

  public String getInstrumentInQuery(int sid) {
    String fields = SectorPrms.getInstrumentFields();
    StringBuffer sb =
        new StringBuffer("select " + fields + " from "
            + Instrument.getTableAndShortName() + " where id in (");
    pickRandomInstruments(numInstrumentsPerInClause);
    for (int i = 0; i < numInstrumentsPerInClause; i++) {
      if (i != 0) sb.append(",");
      sb.append("'" + Instrument.getInstrument(random[i]) + "'");
    }
    sb.append(")");
    return sb.toString();
  }

  public String getJoinInstrumentAndPositionInQuery(int sid) {
    String fields = getPositionAndInstrumentFields();
    StringBuffer sb =
        new StringBuffer("select " + fields + " from "
            + Instrument.getTableAndShortName() + ","
            + Position.getTableAndShortName() + " where "
            + Instrument.getTableShortName() + ".id = "
            + Position.getTableShortName() + ".instrument" + " and "
            + Instrument.getTableShortName() + ".id in (");
    pickRandomInstruments(numInstrumentsPerInClause);
    for (int i = 0; i < numInstrumentsPerInClause; i++) {
      if (i != 0) sb.append(",");
      sb.append("'" + Instrument.getInstrument(random[i]) + "'");
    }
    sb.append(")");
    return sb.toString();
  }

  /**
   * randomly updates a position row.
   * there is a chance the row isn't truly updated due to the way this update statement is created
   */
  public String getUpdatePositionByPositionIdQuery(int sid) {
    int id = rng.nextInt(0, numPositions - 1);
    StringBuffer sb =
        new StringBuffer("update " + Position.getTableAndShortName()
            + " set "
            + Position.getTableShortName()
            + ".symbol="
            //randomly generate a symbol using 0-numSymbolValues rather than generating the symbol from the id
            + Position.getSymbol(rng.nextInt(numSymbolValues))
            + ", " + Position.getTableShortName() + ".book_id="
            + rng.nextInt(numBookValues) + " where "
            + Position.getTableShortName() + ".id=" + id);

    return sb.toString();
  }

  //--------------------------------------------------------------------------
  // QueryFactory : Prepared updates
  //--------------------------------------------------------------------------
  public int fillAndExecuteUpdatePreparedQueryStatement(
      PreparedStatement pstmt, String stmt, int queryType, int i) throws SQLException {
    int numUpdated = 0;
    //do switch/if statement here
    switch (queryType) {
      case SectorPrms.UPDATE_POSITION_BY_POSITION_ID_QUERY:
        numUpdated =
            fillAndExecutePreparedStatementForUpdatePositionByPositionIdQuery(
                pstmt, stmt, i);
        break;
      case SectorPrms.INSERT_INTO_POSITION_QUERY:
        numUpdated =
            fillAndExecutePreparedInsertStatementForPosition(pstmt, stmt, i);
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

  //--------------------------------------------------------------------------
  //  QueryFactory : Prepared Queries
  //--------------------------------------------------------------------------

  public ResultSet fillAndExecutePreparedQueryStatement(
      PreparedStatement pstmt, String stmt, int queryType, int i) throws SQLException {
    ResultSet rs = null;
    //do switch/if statement here
    switch (queryType) {
      case SectorPrms.RANDOM_POSITION_PK_QUERY:
        rs =
            fillAndExecutePreparedStatementForRandomPositionPKQuery(
                pstmt, stmt, i);
        break;
      case SectorPrms.SECTOR_FILTER_IN_QUERY:
        rs =
            fillAndExecutePreparedStatementForSectorFilterInQuery(pstmt,
                stmt, i);
        break;
      case SectorPrms.SECTOR_FILTER_IN_AND_MARKET_CAP_QUERY:
        rs =
            fillAndExecutePreparedStatementForSectorFilterInAndMarketCapQuery(
                pstmt, stmt, i);
        break;
      case SectorPrms.POSITION_PRUNE_BY_INSTRUMENT_QUERY:
        rs =
            fillAndExecutePreparedStatementForPositionPruneByInstrumentQuery(
                pstmt, stmt, i);
        break;
      case SectorPrms.POSITION_AMOUNT_RANGE_AND_QUERY:
        rs =
            fillAndExecutePreparedStatementForPositionAmountRangeAndQuery(
                pstmt, stmt, i);
        break;
      case SectorPrms.POSITION_AMOUNT_RANGE_OR_QUERY:
        rs =
            fillAndExecutePreparedStatementForPositionAmountRangeOrQuery(
                pstmt, stmt, i);
        break;
      case SectorPrms.POSITION_PRUNE_BY_INSTRUMENT_AND_POSITION_AMOUNT_QUERY:
        rs =
            fillAndExecutePreparedStatementForPositionPruneByInstrumentAndPositionAmountQuery(
                pstmt, stmt, i);
        break;
      case SectorPrms.POSITION_PRUNE_BY_INSTRUMENT_AND_FILTER_SYMBOL_AND_OWNER_QUERY:
        rs =
            fillAndExecutePreparedStatementForPositionPruneByInstrumentAndFilterSymbolAndOwnerQuery(
                pstmt, stmt, i);
        break;
      case SectorPrms.JOIN_PRUNE_BY_POSITION_AMOUNT_AND_INSTRUMENT_NAME_QUERY:
        rs =
            fillAndExecutePreparedStatementForJoinPruneByPositionAmountAndInstrumentNameQuery(
                pstmt, stmt, i);
        break;
      case SectorPrms.JOIN_PRUNE_BY_POSITION_AMOUNT_AND_INSTRUMENT_NAME_AND_SECTOR_NAME_QUERY:
        rs =
            fillAndExecutePreparedStatementForJoinPruneByPositionAmountAndInstrumentNameAndSectorNameQuery(
                pstmt, stmt, i);
        break;
      case SectorPrms.INSTRUMENT_IN_QUERY:
        rs = fillAndExecutePreparedStatementForInstrumentInQuery(pstmt, stmt, i);
        break;
      case SectorPrms.JOIN_INSTRUMENT_AND_POSITION_IN_QUERY:
        rs =
            fillAndExecutePreparedStatementForJoinInstrumentAndPositionInQuery(
                pstmt, stmt, i);
        break;
      case SectorPrms.JOIN_INSTRUMENT_AND_POSITION_OR_QUERY:
        rs =
            fillAndExecutePreparedStatementForJoinInstrumentAndPositionOrQuery(
                pstmt, stmt, i);
        break;
      case SectorPrms.POSITION_FILTER_IN_QUERY:
        rs =
            fillAndExecutePreparedStatementForPositionFilterInQuery(
                pstmt, stmt, i);
        break;
      case SectorPrms.JOIN_TO_SINGLE_NODE_QUERY:
        rs =
            fillAndExecutePreparedStatementForJoinToSingleNodeQuery(
                pstmt, stmt, i);
        break;
      default:
        throw new HydraConfigException("Unsupported, should not happen");

    }
    return rs;
  }

  public String getPreparedStatementForRandomPositionPKQuery() {
    String fields = SectorPrms.getPositionFields();
    return "select " + fields + " from " + Position.getTableAndShortName()
        + " where " + Position.getTableShortName() + ".id=?";
  }

  public ResultSet fillAndExecutePreparedStatementForRandomPositionPKQuery(
      PreparedStatement pstmt, String stmt, int bid) throws SQLException {
    int id = rng.nextInt(0, numPositions - 1);
    pstmt.setInt(1, id);
    if (logQueries) {
      Log.getLogWriter().info("EXECUTING: " + stmt + " with p.id=" + id);
    }
    return executeQueryPreparedStatement(pstmt);
  }

  public String getPreparedStatementForPositionFilterInQuery() {
    String fields = SectorPrms.getPositionFields();
    StringBuffer sb =
        new StringBuffer("select " + fields + " from "
            + Position.getTableAndShortName() + " where "
            + Position.getTableShortName() + ".id in (");
    for (int i = 0; i < numPositionsPerInClause; i++) {
      if (i != 0) sb.append(",");
      sb.append("?");
    }
    sb.append(")");
    return sb.toString();
  }

  public ResultSet fillAndExecutePreparedStatementForPositionFilterInQuery (
      PreparedStatement pstmt, String stmt, int bid) throws SQLException {
    String vals = logQueries ? "" : null;
    if (numPositions < numPositionsPerInClause) {
      String s = "numPositionsPerInClause=" + numPositionsPerInClause
               + " must be <= numPositions=" + numPositions;
      throw new HydraConfigException(s);
    }

    if (SectorPrms.getPositionPartitionType() == QueryPrms.PARTITION_BY_RANGE &&
        SectorPrms.getPositionPartitionColumn().equalsIgnoreCase("id")) {
      // pick positions on the fewest nodes possible, starting with a random node
      int numNodes = SectorPrms.getNumServers();
      int node = rng.nextInt(0, numNodes - 1);
      int range = numPositions / numNodes;
      int remainder = numPositions - numNodes * range;
      int lower = SQLPositionQueryFactory.getLowerIdForNode(node, range, remainder);
      // pick positions in order, wrapping around if needed
      // @todo start at a random point on the node (as long as it fits on the node)
      for (int i = 1; i <= numPositionsPerInClause; i++) {
        int val = (lower + i - 1) % numPositions;
        pstmt.setInt(i, val);
        if (logQueries) {
          vals += " " + val;
        }
      }
    } else {
      pickRandomPositions(numPositionsPerInClause);
      for (int i = 1; i <= numPositionsPerInClause; i++) {
        int val = random[i-1];
        pstmt.setInt(i, val);
        if (logQueries) {
          vals += " " + val;
        }
      }
    }
    if (logQueries) {
      Log.getLogWriter().info("EXECUTING: " + stmt + " with p.id in" + vals);
    }
    return executeQueryPreparedStatement(pstmt);
  }

  public String getPreparedStatementForSectorFilterInQuery() {
    String fields = SectorPrms.getSectorFields();
    StringBuffer sb =
        new StringBuffer("select " + fields + " from "
            + Sector.getTableAndShortName() + " where "
            + Sector.getTableShortName() + ".name in (");
    for (int i = 0; i < numSectorsPerInClause; i++) {
      if (i != 0) sb.append(",");
      sb.append("?");
    }
    sb.append(")");
    return sb.toString();
  }

  public ResultSet fillAndExecutePreparedStatementForSectorFilterInQuery(
      PreparedStatement pstmt, String stmt, int bid) throws SQLException {
    String vals = logQueries ? "" : null;
    String val = null;
    pickRandomSectors(numSectorsPerInClause);
    for (int i = 1; i <= numSectorsPerInClause; i++) {
      val = Sector.getSectorName(random[i-1]);
      pstmt.setString(i, val);
      if (logQueries) {
        vals += " " + val;
      }
    }
    if (logQueries) {
      Log.getLogWriter().info("EXECUTING: " + stmt + " with s.id in" + vals);
    }
    return executeQueryPreparedStatement(pstmt);
  }

  public String getPreparedStatementForSectorFilterInAndMarketCapQuery() {
    String fields = SectorPrms.getSectorFields();
    StringBuffer sb =
        new StringBuffer("select " + fields + " from "
            + Sector.getTableAndShortName() + " where "
            + Sector.getTableShortName() + ".name in (");
    for (int i = 0; i < numSectorsPerInClause; i++) {
      if (i != 0) sb.append(",");
      sb.append("?");
    }
    sb.append(") and (");
    for (int i = 0; i < numMarketCapValuesPerOrClause; i++) {
      if (i != 0) sb.append(" or ");
      sb.append(Sector.getTableShortName() + ".market_cap=?");
    }
    sb.append(")");
    return sb.toString();
  }

  public ResultSet fillAndExecutePreparedStatementForSectorFilterInAndMarketCapQuery(
      PreparedStatement pstmt, String stmt, int bid) throws SQLException {
    if (numSectorsPerInClause > numSectors) {
      String s = "numSectorsPerInClause=" + numSectorsPerInClause
               + " must be <= numSectors=" + numSectors;
      throw new HydraConfigException(s);
    }
    if (numMarketCapValuesPerOrClause > numMarketCapValues) {
      String s = "numMarketCapValuesPerOrClause=" + numMarketCapValuesPerOrClause
               + " must be <= numMarketCapValues=" + numMarketCapValues;
      throw new HydraConfigException(s);
    }
    String nvals = logQueries ? "" : null;
    String mvals = logQueries ? "" : null;
    int sid = rng.nextInt(0, numSectors - numSectorsPerInClause - 1);
    int mcv = sid % numMarketCapValues;
    for (int i = 1; i <= numSectorsPerInClause; i++) {
      String nval = Sector.getSectorName(sid % numSectors);
      pstmt.setString(i, nval);
      if (logQueries) {
        nvals += nval + " ";
      }
      ++sid;
    }
    for (int i = numSectorsPerInClause + 1; i <= numSectorsPerInClause + numMarketCapValuesPerOrClause; i++) {
      int mval = mcv % numMarketCapValues;
      pstmt.setInt(i, mval);
      if (logQueries) {
        mvals += mval + " or ";
      }
      ++mcv;
    }
    if (logQueries) {
      Log.getLogWriter().info("EXECUTING: " + stmt + " with s.name in " + nvals + " and s.market_cap = one of (or) " + mvals);
    }
    return executeQueryPreparedStatement(pstmt);
  }

  public String getPreparedStatementForPositionPruneByInstrumentQuery() {
    String fields = SectorPrms.getPositionFields();
    String s =
        "select " + fields + " from " + Position.getTableAndShortName()
            + " where " + Position.getTableShortName() + ".synthetic=1"
            + " and " + Position.getTableShortName() + ".instrument=?";

    return s;
  }

  public ResultSet fillAndExecutePreparedStatementForPositionPruneByInstrumentQuery(
      PreparedStatement pstmt, String stmt, int sid) throws SQLException {
    if (validateResults && numPositionsPerInstrument % 2 != 0) {
      String s = "numPositionsPerInstrument needs to be an even number to validate results";
      throw new HydraConfigException(s);
    }
    int iid = rng.nextInt(0, numInstruments - 1);
    String instrument = Instrument.getInstrument(iid);
    pstmt.setString(1, instrument);
    if (logQueries) {
      Log.getLogWriter().info("EXECUTING: " + stmt + " with p.instrument=" + instrument);
    }
    return executeQueryPreparedStatement(pstmt);
  }

  public String getPreparedStatementForPositionAmountRangeAndQuery() {
    String fields = SectorPrms.getPositionFields();
    String s =
        "select " + fields + " from " + Position.getTableAndShortName()
            + " where "
            + Position.getTableShortName() + ".amount >= ?" + " and "
            + Position.getTableShortName() + ".amount < ?";
    return s;
  }

  public ResultSet fillAndExecutePreparedStatementForPositionAmountRangeAndQuery(
      PreparedStatement pstmt, String stmt, int sid) throws SQLException {
    if (numPositionsPerInstrument < numAmountsPerRangeClause) {
      String s = "numPositionsPerInstrument " + numPositionsPerInstrument
               + " must be >= numAmountsPerRangeClause=" + numAmountsPerRangeClause;
      throw new HydraConfigException(s);
    }
    int iid = rng.nextInt(0, numInstruments - 1);
    int min = iid * numPositionsPerInstrument + rng.nextInt(0, numPositionsPerInstrument - numAmountsPerRangeClause);
    int max = min + numAmountsPerRangeClause;
    pstmt.setInt(1, min);
    pstmt.setInt(2, max);
    if (logQueries) {
      Log.getLogWriter().info("EXECUTING: " + stmt + " with p.amount >= " + min + " p.amount < " + max);
    }
    return executeQueryPreparedStatement(pstmt);
  }

  public String getPreparedStatementForPositionAmountRangeOrQuery() {
    String fields = SectorPrms.getPositionFields();
    String s =
        "select " + fields + " from " + Position.getTableAndShortName()
            + " where ("
            + Position.getTableShortName() + ".amount >= ? and "
            + Position.getTableShortName() + ".amount < ?) or ("
            + Position.getTableShortName() + ".amount >= ? and "
            + Position.getTableShortName() + ".amount < ?)";
    return s;
  }

  public ResultSet fillAndExecutePreparedStatementForPositionAmountRangeOrQuery(
      PreparedStatement pstmt, String stmt, int sid) throws SQLException {
    if (numInstruments < 2) {
      String s = "numInstruments " + numInstruments + " must be >= 2";
      throw new HydraConfigException(s);
    }
    if (numPositionsPerInstrument < numAmountsPerRangeClause) {
      String s = "numPositionsPerInstrument " + numPositionsPerInstrument
               + " must be >= numAmountsPerRangeClause=" + numAmountsPerRangeClause;
      throw new HydraConfigException(s);
    }
    int iid1 = rng.nextInt(0, numInstruments/2 - 1);
    int min1 = iid1 * numPositionsPerInstrument + rng.nextInt(0, numPositionsPerInstrument - numAmountsPerRangeClause);
    int max1 = min1 + numAmountsPerRangeClause;
    int iid2 = rng.nextInt(numInstruments/2, numInstruments - 1);
    int min2 = iid2 * numPositionsPerInstrument + rng.nextInt(0, numPositionsPerInstrument - numAmountsPerRangeClause);
    int max2 = min2 + numAmountsPerRangeClause;
    pstmt.setInt(1, min1);
    pstmt.setInt(2, max1);
    pstmt.setInt(3, min2);
    pstmt.setInt(4, max2);
    if (logQueries) {
      Log.getLogWriter().info("EXECUTING: " + stmt + " with (p.amount >= " + min1 + " p.amount < " + max1
      + ") or (p.amount >= " + min2 + " p.amount < " + max2 + ")");
    }
    return executeQueryPreparedStatement(pstmt);
  }

  public String getPreparedStatementForPositionPruneByInstrumentAndPositionAmountQuery() {
    String fields = SectorPrms.getPositionFields();
    String s =
        "select " + fields + " from " + Position.getTableAndShortName()
            + " where " + Position.getTableShortName() + ".instrument=?"
            + " and " + Position.getTableShortName() + ".amount >= ?" + " and "
            + Position.getTableShortName() + ".amount < ?";
    return s;
  }

  public ResultSet fillAndExecutePreparedStatementForPositionPruneByInstrumentAndPositionAmountQuery(
      PreparedStatement pstmt, String stmt, int sid) throws SQLException {
    if (numPositionsPerInstrument < numAmountsPerRangeClause) {
      String s = "numPositionsPerInstrument " + numPositionsPerInstrument
               + " must be >= numAmountsPerRangeClause=" + numAmountsPerRangeClause;
      throw new HydraConfigException(s);
    }
    int iid = rng.nextInt(0, numInstruments - 1);
    String instrument = Instrument.getInstrument(iid);
    int min = iid * numPositionsPerInstrument + rng.nextInt(0, numPositionsPerInstrument - numAmountsPerRangeClause);
    int max = min + numAmountsPerRangeClause;
    pstmt.setString(1, instrument);
    pstmt.setInt(2, min);
    pstmt.setInt(3, max);
    if (logQueries) {
      Log.getLogWriter().info("EXECUTING: " + stmt + " with p.instrument=" + instrument + " p.amount >= " + min + " p.amount < " + max);
    }
    return executeQueryPreparedStatement(pstmt);
  }

  public String getPreparedStatementForPositionPruneByInstrumentAndFilterSymbolAndOwnerQuery() {
    String fields = SectorPrms.getPositionFields();
    StringBuffer sb =
        new StringBuffer("select " + fields + " from "
            + Position.getTableAndShortName() + " where "
            + Position.getTableShortName() + ".instrument=?" + " and "
            + Position.getTableShortName() + ".symbol in (");
    for (int i = 0; i < numSymbolsPerInClause; i++) {
      if (i != 0) sb.append(",");
      sb.append("?");
    }
    sb.append(") and (");
    for (int i = 0; i < numOwnersPerOrClause; i++) {
      if (i != 0) sb.append(" or ");
      sb.append(Position.getTableShortName() + ".owner=?");
    }
    sb.append(")");
    return sb.toString();
  }

  public ResultSet fillAndExecutePreparedStatementForPositionPruneByInstrumentAndFilterSymbolAndOwnerQuery(
      PreparedStatement pstmt, String stmt, int sid) throws SQLException {
    int iid = rng.nextInt(0, numInstruments - 1);
    String instrument = Instrument.getInstrument(iid);
    pstmt.setString(1, instrument);
    String symbols = logQueries ? "" : null;
    pickRandomSymbolValues(numSymbolsPerInClause);
    for (int i = 2; i < 2 + numSymbolsPerInClause; i++) {
      int secid = random[i-2];
      String symbol = Position.getSymbol(secid);
      pstmt.setString(i, symbol);
      if (logQueries) {
        symbols += symbol + " ";
      }
    }
    String owners = logQueries ? "" : null;
    pickRandomOwnerValues(numOwnersPerOrClause);
    for (int i = 2 + numSymbolsPerInClause; i < 2 + numSymbolsPerInClause + numOwnersPerOrClause; i++) {
      int oid = random[i - (2 + numSymbolsPerInClause)];
      String owner = Position.getOwner(oid);
      pstmt.setString(i, owner);
      if (logQueries) {
        owners += owner + " ";
      }
    }
    if (logQueries) {
      Log.getLogWriter().info("EXECUTING: " + stmt + " with p.instrument=" + instrument + " p.symbol in " + symbols + " p.owner = one of (or) " + owners);
    }
    return executeQueryPreparedStatement(pstmt);
  }

  public String getPreparedStatementForJoinPruneByPositionAmountAndInstrumentNameQuery() {
    String fields = getPositionAndInstrumentFields();
    StringBuffer sb =
        new StringBuffer("select " + fields + " from "
            + Position.getTableAndShortName() + ","
            + Instrument.getTableAndShortName() + " where "
            + Instrument.getTableShortName() + ".id = "
            + Position.getTableShortName() + ".instrument" + " and "
            + Position.getTableShortName() + ".amount >= ?" + " and "
            + Position.getTableShortName() + ".amount < ?" + " and "
            + Instrument.getTableShortName() + ".id=?");
    return sb.toString();
  }

  public ResultSet fillAndExecutePreparedStatementForJoinPruneByPositionAmountAndInstrumentNameQuery(
      PreparedStatement pstmt, String stmt, int sid) throws SQLException {
    if (numPositionsPerInstrument < numAmountsPerRangeClause) {
      String s = "numPositionsPerInstrument " + numPositionsPerInstrument
               + " must be >= numAmountsPerRangeClause=" + numAmountsPerRangeClause;
      throw new HydraConfigException(s);
    }
    int iid = rng.nextInt(0, numInstruments - 1);
    String instrument = Instrument.getInstrument(iid);
    int min = iid * numPositionsPerInstrument + rng.nextInt(0, numPositionsPerInstrument - numAmountsPerRangeClause);
    int max = min + numAmountsPerRangeClause;
    pstmt.setInt(1, min);
    pstmt.setInt(2, max);
    pstmt.setString(3, instrument);
    if (logQueries) {
      Log.getLogWriter().info("EXECUTING: " + stmt + " with p.amount >= " + min + " p.amount < " + max + " i.id=" + instrument);
    }
    return executeQueryPreparedStatement(pstmt);
  }

  public String getPreparedStatementForJoinPruneByPositionAmountAndInstrumentNameAndSectorNameQuery() {
    String fields = getPositionAndInstrumentFields();
    StringBuffer sb =
        new StringBuffer("select " + fields + " from "
            + Position.getTableAndShortName() + ","
            + Instrument.getTableAndShortName() + ","
            + Sector.getTableAndShortName() + " where "
            + Instrument.getTableShortName() + ".id = "
            + Position.getTableShortName() + ".instrument" + " and "
            + Instrument.getTableShortName() + ".sector_id = "
            + Sector.getTableShortName() + ".id" + " and "
            + Position.getTableShortName() + ".amount >= ?" + " and "
            + Position.getTableShortName() + ".amount < ?" + " and "
            + Instrument.getTableShortName() + ".id=?" + " and "
            + Sector.getTableShortName() + ".name=?");
    return sb.toString();
  }

  public ResultSet fillAndExecutePreparedStatementForJoinPruneByPositionAmountAndInstrumentNameAndSectorNameQuery(
      PreparedStatement pstmt, String stmt, int sid) throws SQLException {
    if (numPositionsPerInstrument < numAmountsPerRangeClause) {
      String s = "numPositionsPerInstrument " + numPositionsPerInstrument
               + " must be >= numAmountsPerRangeClause=" + numAmountsPerRangeClause;
      throw new HydraConfigException(s);
    }
    String sector = Sector.getSectorName(sid);
    int iid = sid * numInstrumentsPerSector + rng.nextInt(0, numInstrumentsPerSector - 1);
    String instrument = Instrument.getInstrument(iid);
    int min = iid * numPositionsPerInstrument + rng.nextInt(0, numPositionsPerInstrument - numAmountsPerRangeClause);
    int max = min + numAmountsPerRangeClause;

    pstmt.setInt(1, min);
    pstmt.setInt(2, max);
    pstmt.setString(3, instrument);
    pstmt.setString(4, sector);
    if (logQueries) {
      Log.getLogWriter().info("EXECUTING: " + stmt + " with p.amount >= " + min + " p.amount < " + max + " i.id=" + instrument + " s.name=" + sector);
    }
    return executeQueryPreparedStatement(pstmt);
  }

  public String getPreparedStatementForInstrumentInQuery() {
    String fields = SectorPrms.getInstrumentFields();
    StringBuffer sb = new StringBuffer("select " + fields + " from "
            + Instrument.getTableAndShortName() + " where "
            + Instrument.getTableShortName() + ".id in (");
    for (int i = 0; i < numInstrumentsPerInClause; i++) {
      if (i != 0) sb.append(",");
      sb.append("?");
    }
    sb.append(")");
    return sb.toString();
  }

  public ResultSet fillAndExecutePreparedStatementForInstrumentInQuery(
      PreparedStatement pstmt, String stmt, int sid) throws SQLException {
    String instruments = logQueries ? "" : null;
    pickRandomInstruments(numInstrumentsPerInClause);
    for (int i = 1; i <= numInstrumentsPerInClause; i++) {
      String instrument = Instrument.getInstrument(random[i-1]);
      pstmt.setString(i, instrument);
      if (logQueries) {
        instruments += " " + instrument;
      }
    }
    if (logQueries) {
      Log.getLogWriter().info("EXECUTING: " + stmt + " with i.id in" + instruments);
    }
    return executeQueryPreparedStatement(pstmt);
  }

  public String getPreparedStatementForJoinInstrumentAndPositionInQuery() {
    String fields = getPositionAndInstrumentFields();
    StringBuffer sb =
        new StringBuffer("select " + fields + " from "
            + Instrument.getTableAndShortName() + ","
            + Position.getTableAndShortName() + " where "
            + Instrument.getTableShortName() + ".id = "
            + Position.getTableShortName() + ".instrument" + " and "
            + Instrument.getTableShortName() + ".id in (");
    for (int i = 0; i < numInstrumentsPerInClause; i++) {
      if (i != 0) sb.append(",");
      sb.append("?");
    }
    sb.append(")");
    return sb.toString();
  }

  public ResultSet fillAndExecutePreparedStatementForJoinInstrumentAndPositionInQuery(
      PreparedStatement pstmt, String stmt, int sid) throws SQLException {
    String instruments = logQueries ? "" : null;
    pickRandomInstruments(numInstrumentsPerInClause);
    for (int i = 1; i <= numInstrumentsPerInClause; i++) {
      String instrument = Instrument.getInstrument(random[i-1]);
      pstmt.setString(i, instrument);
      if (logQueries) {
        instruments += " " + instrument;
      }
    }
    if (logQueries) {
      Log.getLogWriter().info("EXECUTING: " + stmt + " with i.id in" + instruments);
    }
    return executeQueryPreparedStatement(pstmt);
  }
  
  public String getPreparedStatementForJoinInstrumentAndPositionOrQuery() {
    throw new UnsupportedOperationException("TBD");
  /*
    if (resultSetSize < numPositionsPerInstrument) {
      String s =
          "NumPositionsPerInstrument too large for resultSetSize of " + resultSetSize
              + "  Will need NumPositionsPerInstrument of " + resultSetSize;
      throw new HydraConfigException(s);
    }
    int range = resultSetSize / numPositionsPerInstrument;
    String fields = getPositionAndInstrumentFields();
    StringBuffer sb =
        new StringBuffer("select " + fields + " from "
            + Instrument.getTableAndShortName() + ","
            + Position.getTableAndShortName() + " where "
            + Instrument.getTableShortName() + ".id = "
            + Position.getTableShortName() + ".instrument" + " and (");
    for (int i = 0; i < range; i++) {
      sb.append(Instrument.getTableShortName() + ".id = ");
      sb.append("?");
      if (range - i != 1) {
        sb.append(" or ");
      }
    }
    sb.append(")");
    return sb.toString();
  */
  }

  public ResultSet fillAndExecutePreparedStatementForJoinInstrumentAndPositionOrQuery(
      PreparedStatement pstmt, String stmt, int sid) throws SQLException {
    throw new UnsupportedOperationException("TBD");
    /*
    int range = resultSetSize / numPositionsPerInstrument;
    String instruments = logQueries ? "" : null;
    for (int i = 1; i <= range; i++) {
      int iid = rng.nextInt(numInstruments/ range * (i - 1), numInstruments/ range * i - 1 );
      String instrument = Instrument.getInstrument(iid);
      pstmt.setString(i, instrument);
      if (logQueries) {
        instruments += instrument + " ";
      }
    }
    if (logQueries) {
      Log.getLogWriter().info("EXECUTING: " + stmt + " with i.id in " + instruments);
    }
    return executeQueryPreparedStatement(pstmt);
    */
  }

  public String getPreparedStatementForUpdatePositionByPositionIdQuery() {
    int id = rng.nextInt(0, numPositions - 1);
    StringBuffer sb =
        new StringBuffer("update " + Position.getTableAndShortName() + " set "
            + Position.getTableShortName() + ".symbol=?"
            //randomly generate a symbol using 0-numSymbolValues rather than generating the symbol from the id
            + ", " + Position.getTableShortName() + ".book_id=?" + " where "
            + Position.getTableShortName() + ".id=?");
    return sb.toString();
  }

  public int fillAndExecutePreparedStatementForUpdatePositionByPositionIdQuery(
      PreparedStatement pstmt, String stmt, int sid) throws SQLException {
    int id = rng.nextInt(0, numPositions - 1);
    String symbol = Position.getSymbol(rng.nextInt(numSymbolValues));
    int bookId = rng.nextInt(numBookValues);
    pstmt.setString(1, symbol);
    pstmt.setInt(2, bookId);
    pstmt.setInt(3,id);
    if (logQueries) {
      Log.getLogWriter().info("EXECUTING: " + stmt + " with p.symbol=" + symbol
         + " p.book_id=" + bookId + " p.id=" + id);
    }
    return executeUpdatePreparedStatement(pstmt);
  }

  public String getPreparedStatementForJoinToSingleNodeQuery() {
    throw new UnsupportedOperationException("TBD");
  /*
    String fields = SectorPrms.getPositionFields();
    if (resultSetSize < 1 || resultSetSize % numPositionsPerInstrument != 0) {
      String s =
          "NumPositionsPerInstrument too large for resultSetSize of " + resultSetSize
              + "  Will need NumPositionsPerInstrument of " + resultSetSize;
      throw new HydraConfigException(s);
    }
    
    StringBuffer sb = new StringBuffer( "select " + fields + " from " + Position.getTableAndShortName() + " , " + Instrument.getTableAndShortName()
        + " where " + Position.getTableShortName() + ".instrument = " + Instrument.getTableShortName() + ".id" 
        + " and " + Instrument.getTableAndShortName() + ".id in (");
        
    for (int i = 0; i < resultSetSize; i++) {
      sb.append("?");
      if (resultSetSize - i != 1) {
        sb.append(",");
      }
    }
    sb.append(")");
    return sb.toString();
  */
  }

  public ResultSet fillAndExecutePreparedStatementForJoinToSingleNodeQuery(
      PreparedStatement pstmt, String stmt, int bid) throws SQLException {
    throw new UnsupportedOperationException("TBD");
    /*
    int iid = rng.nextInt(0, numInstruments - resultSetSize);
    String instruments = logQueries ? "" : null;
    for (int i = 1; i <= resultSetSize; i++) {
      String instrument = Instrument.getInstrument(iid++);
      pstmt.setString(i, instrument);
      if (logQueries) {
        instruments += instrument + " ";
      }
    }
    if (logQueries) {
      Log.getLogWriter().info("EXECUTING: " + stmt + " with i.id in " + instruments);
    }
    return executeQueryPreparedStatement(pstmt);
    */
  }
  
  //--------------------------------------------------------------------------
  //  QueryFactory : Helper methods for field concatenation
  //--------------------------------------------------------------------------
  private String getPositionAndInstrumentFields() {
    String fields = "";
    if (SectorPrms.getPositionFields().indexOf("NONE") != -1) {
      if (SectorPrms.getInstrumentFields().indexOf("NONE") != -1) {
        //both can't be none
        String s =
            BasePrms.nameForKey(SectorPrms.instrumentFields) + " and "
                + BasePrms.nameForKey(SectorPrms.positionFields)
                + " cannot both be NONE";
        throw new HydraConfigException(s);
      }
      else {
        fields = SectorPrms.getInstrumentFields();
      }
    }
    else if (SectorPrms.getInstrumentFields().indexOf("NONE") != -1) {
      fields = SectorPrms.getPositionFields();
    }
    else {
      fields =
          SectorPrms.getInstrumentFields() + ","
              + SectorPrms.getPositionFields();
    }
    if (fields.indexOf("*") != -1) {
      fields = "*";
    }
    return fields;
  }

  private String getSectorAndPositionFields() {
    String fields = "";
    if (SectorPrms.getSectorFields().indexOf("NONE") != -1) {
      if (SectorPrms.getPositionFields().indexOf("NONE") != -1) {
        //both can't be none
        String s =
            BasePrms.nameForKey(SectorPrms.positionFields) + " and "
                + BasePrms.nameForKey(SectorPrms.sectorFields)
                + " cannot both be NONE";
        throw new HydraConfigException(s);
      }
      else {
        fields = SectorPrms.getPositionFields();
      }
    }
    else if (SectorPrms.getPositionFields().indexOf("NONE") != -1) {
      fields = SectorPrms.getSectorFields();
    }
    else {
      fields =
          SectorPrms.getPositionFields() + "," + SectorPrms.getSectorFields();
    }
    if (fields.indexOf("*") != -1) {
      fields = "*";
    }
    return fields;
  }

  private String getSectorAndInstrumentFields() {
    String fields = "";
    if (SectorPrms.getSectorFields().indexOf("NONE") != -1) {
      if (SectorPrms.getInstrumentFields().indexOf("NONE") != -1) {
        //both can't be none
        String s =
            BasePrms.nameForKey(SectorPrms.instrumentFields) + " and "
                + BasePrms.nameForKey(SectorPrms.sectorFields)
                + " cannot both be NONE";
        throw new HydraConfigException(s);
      }
      else {
        fields = SectorPrms.getInstrumentFields();
      }
    }
    else if (SectorPrms.getInstrumentFields().indexOf("NONE") != -1) {
      fields = SectorPrms.getSectorFields();
    }
    else {
      fields =
          SectorPrms.getInstrumentFields() + "," + SectorPrms.getSectorFields();
    }
    if (fields.indexOf("*") != -1) {
      fields = "*";
    }
    return fields;
  }

  private String getPositionAndInstrumentAndSectorFields() {
    String fields = "";
    if (SectorPrms.getPositionFields().indexOf("NONE") != -1) {
      if (SectorPrms.getInstrumentFields().indexOf("NONE") != -1) {
        if (SectorPrms.getSectorFields().indexOf("NONE") != -1) {
          //all three can't be none
          //both can't be none
          String s =
              BasePrms.nameForKey(SectorPrms.instrumentFields) + " and "
                  + BasePrms.nameForKey(SectorPrms.positionFields)
                  + " cannot both be NONE";
          throw new HydraConfigException(s);
        }
        //all but sector are none
        fields = SectorPrms.getSectorFields();
      }
      else {
        if (SectorPrms.getSectorFields().indexOf("NONE") != -1) {
          //all but instruments are none
          fields = SectorPrms.getInstrumentFields();
        }
        else {
          //only positions is none
          fields = getSectorAndInstrumentFields();
        }
      }
    }
    else if (SectorPrms.getInstrumentFields().indexOf("NONE") != -1) {
      //this means that instruments are none but positions have values
      if (SectorPrms.getSectorFields().indexOf("NONE") != -1) {
        //we know that positions fields the only non none fields
        fields = SectorPrms.getPositionFields();
      }
      else {
        fields = getSectorAndPositionFields();
      }
    }
    else {
      //we know that positions AND instruments have values
      if (SectorPrms.getSectorFields().indexOf("NONE") != -1) {
        fields = getPositionAndInstrumentFields();
      }
      else {
        fields =
            SectorPrms.getInstrumentFields() + ","
                + SectorPrms.getPositionFields() + ","
                + SectorPrms.getSectorFields();
      }
    }
    if (fields.indexOf("*") != -1) {
      fields = "*";
    }
    return fields;
  }

  //Helper methods for getting fields as list

  private List getPositionAndInstrumentFieldsAsList() {
    List fields = null;
    if (SectorPrms.getPositionFields().indexOf("NONE") != -1) {
      if (SectorPrms.getInstrumentFields().indexOf("NONE") != -1) {
        //both can't be none
        String s =
            BasePrms.nameForKey(SectorPrms.instrumentFields) + " and "
                + BasePrms.nameForKey(SectorPrms.positionFields)
                + " cannot both be NONE";
        throw new HydraConfigException(s);
      }
      else {
        fields = Instrument.getFields(SectorPrms.getInstrumentFieldsAsVector());
      }
    }
    else if (SectorPrms.getInstrumentFields().indexOf("NONE") != -1) {
      fields = Position.getFields(SectorPrms.getPositionFieldsAsVector());
    }
    else {
      fields = Position.getFields(SectorPrms.getPositionFieldsAsVector());
      fields.addAll(Instrument.getFields(SectorPrms.getInstrumentFieldsAsVector()));
    }
    if (fields.indexOf("*") != -1) {
      fields = Position.getFields(SectorPrms.getPositionFieldsAsVector());
      fields.addAll(Instrument.getFields(SectorPrms.getInstrumentFieldsAsVector()));
    }
    return fields;
  }

  private List getSectorAndPositionFieldsAsList() {
    List fields = null;
    if (SectorPrms.getSectorFields().indexOf("NONE") != -1) {
      if (SectorPrms.getPositionFields().indexOf("NONE") != -1) {
        //both can't be none
        String s =
            BasePrms.nameForKey(SectorPrms.positionFields) + " and "
                + BasePrms.nameForKey(SectorPrms.sectorFields)
                + " cannot both be NONE";
        throw new HydraConfigException(s);
      }
      else {
        fields = Position.getFields(SectorPrms.getPositionFieldsAsVector());
        ;
      }
    }
    else if (SectorPrms.getPositionFields().indexOf("NONE") != -1) {
      fields = Sector.getFields(SectorPrms.getSectorFieldsAsVector());
    }
    else {
      fields = Position.getFields(SectorPrms.getPositionFieldsAsVector());
      fields.addAll(Sector.getFields(SectorPrms.getSectorFieldsAsVector()));
    }
    if (fields.indexOf("*") != -1) {
      fields = Position.getFields(SectorPrms.getPositionFieldsAsVector());
      fields.addAll(Sector.getFields(SectorPrms.getSectorFieldsAsVector()));
    }
    return fields;
  }

  private List getSectorAndInstrumentFieldsAsList() {
    List fields = null;
    if (SectorPrms.getSectorFields().indexOf("NONE") != -1) {
      if (SectorPrms.getInstrumentFields().indexOf("NONE") != -1) {
        //both can't be none
        String s =
            BasePrms.nameForKey(SectorPrms.instrumentFields) + " and "
                + BasePrms.nameForKey(SectorPrms.sectorFields)
                + " cannot both be NONE";
        throw new HydraConfigException(s);
      }
      else {
        fields = Instrument.getFields(SectorPrms.getInstrumentFieldsAsVector());
      }
    }
    else if (SectorPrms.getInstrumentFields().indexOf("NONE") != -1) {
      fields = Sector.getFields(SectorPrms.getSectorFieldsAsVector());
    }
    else {
      fields = Sector.getFields(SectorPrms.getSectorFieldsAsVector());
      fields.addAll(Instrument.getFields(SectorPrms.getInstrumentFieldsAsVector()));
    }
    if (fields.indexOf("*") != -1) {
      fields = Sector.getFields(SectorPrms.getSectorFieldsAsVector());
      fields.addAll(Instrument.getFields(SectorPrms.getInstrumentFieldsAsVector()));
    }
    return fields;
  }

  private List getPositionAndInstrumentAndSectorFieldsAsList() {
    List fields = null;
    if (SectorPrms.getPositionFields().indexOf("NONE") != -1) {
      if (SectorPrms.getInstrumentFields().indexOf("NONE") != -1) {
        if (SectorPrms.getSectorFields().indexOf("NONE") != -1) {
          //all three can't be none
          //both can't be none
          String s =
              BasePrms.nameForKey(SectorPrms.instrumentFields) + " and "
                  + BasePrms.nameForKey(SectorPrms.positionFields)
                  + " cannot both be NONE";
          throw new HydraConfigException(s);
        }
        //all but sector are none
        fields = Sector.getFields(SectorPrms.getSectorFieldsAsVector());
      }
      else {
        if (SectorPrms.getSectorFields().indexOf("NONE") != -1) {
          //all but instruments are none
          fields = Instrument.getFields(SectorPrms.getInstrumentFieldsAsVector());
        }
        else {
          //only positions is none
          fields = Instrument.getFields(SectorPrms.getInstrumentFieldsAsVector());
          fields.addAll(Sector.getFields(SectorPrms.getSectorFieldsAsVector()));
        }
      }
    }
    else if (SectorPrms.getInstrumentFields().indexOf("NONE") != -1) {
      //this means that instruments are none but positions have values
      if (SectorPrms.getSectorFields().indexOf("NONE") != -1) {
        //we know that positions fields the only non none fields
        fields = Position.getFields(SectorPrms.getPositionFieldsAsVector());
      }
      else {
        fields = getSectorAndPositionFieldsAsList();
      }
    }
    else {
      //we know that positions AND instruments have values
      if (SectorPrms.getSectorFields().indexOf("NONE") != -1) {
        fields = getPositionAndInstrumentFieldsAsList();
      }
      else {
        fields = Position.getFields(SectorPrms.getPositionFieldsAsVector());
        fields.addAll(Sector.getFields(SectorPrms.getSectorFieldsAsVector()));
        fields.addAll(Instrument.getFields(SectorPrms.getInstrumentFieldsAsVector()));
      }
    }
    if (fields.indexOf("*") != -1) {
      fields = Position.getFields(SectorPrms.getPositionFieldsAsVector());
      fields.addAll(Sector.getFields(SectorPrms.getSectorFieldsAsVector()));
      fields.addAll(Instrument.getFields(SectorPrms.getInstrumentFieldsAsVector()));
    }
    return fields;
  }

  //--------------------------------------------------------------------------
  // Random number support
  //--------------------------------------------------------------------------

  /**
   * Sets first n elements of "random" array to unique random sector ids.
   */
  protected void pickRandomSectors(int n) {
    if (n > sectors.length) {
      String s = "Unable to choose " + n + " unique random sectors from "
               + sectors.length + " total sectors";
      throw new QueryFactoryException(s);
    }
    int last = sectors.length;
    for (int i = 0; i < n; i++) {
      int j = rng.nextInt(0, last-1);
      random[i] = sectors[j];
      sectors[j] = sectors[last-1];
      sectors[last-1] = random[i];
      last--;
    }
  }

  /**
   * Sets first n elements of "random" array to unique random instrument ids.
   */
  protected void pickRandomInstruments(int n) {
    if (n > instruments.length) {
      String s = "Unable to choose " + n + " unique random instruments from "
               + instruments.length + " instruments per sector";
      throw new QueryFactoryException(s);
      // @todo instead use a different algorithm (go around again but remove duplicates)
    }
    int last = instruments.length;
    int secid;
    for (int i = 0; i < n; i++) {
      secid = rng.nextInt(0, numSectors-1);
      int j = rng.nextInt(0, last-1);
      random[i] = secid * numInstrumentsPerSector
                + instruments[j];
      instruments[j] = instruments[last-1];
      instruments[last-1] = random[i];
      last--;
    }
  }

  /**
   * Sets first n elements of "random" array to unique random position ids.
   */
  protected void pickRandomPositions(int n) {
    if (n > positions.length) {
      String s = "Unable to choose " + n + " unique random positions from "
               + positions.length + " positions per instrument";
      throw new QueryFactoryException(s);
      // @todo instead use a different algorithm (go around again but remove duplicates)
    }
    int last = positions.length;
    int secid, iid, tmp, j;
    for (int i = 0; i < n; i++) {
      secid = rng.nextInt(0, numSectors-1);
      iid = rng.nextInt(0, numInstrumentsPerSector-1);
      j = rng.nextInt(0, last-1);
      tmp = positions[j];
      positions[j] = positions[last-1];
      positions[last-1] = tmp;
      random[i] = (secid * numInstrumentsPerSector + iid)
                         * numPositionsPerInstrument + tmp;
      last--;
    }
  }

  /**
   * Sets first n elements of "random" array to unique random owner values.
   */
  protected void pickRandomOwnerValues(int n) {
    if (n > ownerValues.length) {
      String s = "Unable to choose " + n + " unique random ownerValues from "
               + ownerValues.length + " total ownerValues";
      throw new QueryFactoryException(s);
    }
    int last = ownerValues.length;
    for (int i = 0; i < n; i++) {
      int j = rng.nextInt(0, last-1);
      random[i] = ownerValues[j];
      ownerValues[j] = ownerValues[last-1];
      ownerValues[last-1] = random[i];
      last--;
    }
  }

  /**
   * Sets first n elements of "random" array to unique random symbol values.
   */
  protected void pickRandomSymbolValues(int n) {
    if (n > symbolValues.length) {
      String s = "Unable to choose " + n + " unique random symbolValues from "
               + symbolValues.length + " total symbolValues";
      throw new QueryFactoryException(s);
    }
    int last = symbolValues.length;
    for (int i = 0; i < n; i++) {
      int j = rng.nextInt(0, last-1);
      random[i] = symbolValues[j];
      symbolValues[j] = symbolValues[last-1];
      symbolValues[last-1] = random[i];
      last--;
    }
  }

  //--------------------------------------------------------------------------
  // Read result sets
  //--------------------------------------------------------------------------
  public int readResultSet(int queryType, ResultSet rs) throws SQLException {
    switch (queryType) {
      case SectorPrms.RANDOM_POSITION_PK_QUERY:
        return readResultSet(queryType, rs, Position.getFields(SectorPrms.getPositionFieldsAsVector()));
      case SectorPrms.SECTOR_FILTER_IN_QUERY:
        return readResultSet(queryType, rs, Sector.getFields(SectorPrms.getSectorFieldsAsVector()));
      case SectorPrms.SECTOR_FILTER_IN_AND_MARKET_CAP_QUERY:
        return readResultSet(queryType, rs, Sector.getFields(SectorPrms.getSectorFieldsAsVector()));
      case SectorPrms.POSITION_PRUNE_BY_INSTRUMENT_QUERY:
        return readResultSet(queryType, rs, Position.getFields(SectorPrms.getPositionFieldsAsVector()));
      case SectorPrms.POSITION_PRUNE_BY_INSTRUMENT_AND_FILTER_SYMBOL_AND_OWNER_QUERY:
        return readResultSet(queryType, rs, Position.getFields(SectorPrms.getPositionFieldsAsVector()));
      case SectorPrms.POSITION_AMOUNT_RANGE_AND_QUERY:
        return readResultSet(queryType, rs, Position.getFields(SectorPrms.getPositionFieldsAsVector()));
      case SectorPrms.POSITION_AMOUNT_RANGE_OR_QUERY:
        return readResultSet(queryType, rs, Position.getFields(SectorPrms.getPositionFieldsAsVector()));
      case SectorPrms.POSITION_PRUNE_BY_INSTRUMENT_AND_POSITION_AMOUNT_QUERY:
        return readResultSet(queryType, rs, Position.getFields(SectorPrms.getPositionFieldsAsVector()));
      case SectorPrms.INSTRUMENT_IN_QUERY:
        return readResultSet(queryType, rs, Instrument.getFields(SectorPrms.getInstrumentFieldsAsVector()));
      case SectorPrms.JOIN_INSTRUMENT_AND_POSITION_IN_QUERY:
        return readResultSet(queryType, rs, getPositionAndInstrumentFieldsAsList());
      case SectorPrms.JOIN_PRUNE_BY_POSITION_AMOUNT_AND_INSTRUMENT_NAME_QUERY:
        return readResultSet(queryType, rs, getPositionAndInstrumentFieldsAsList());
      case SectorPrms.JOIN_PRUNE_BY_POSITION_AMOUNT_AND_INSTRUMENT_NAME_AND_SECTOR_NAME_QUERY:
        return readResultSet(queryType, rs, getPositionAndInstrumentAndSectorFieldsAsList());
      case SectorPrms.POSITION_FILTER_IN_QUERY:
        return readResultSet(queryType, rs, Position.getFields(SectorPrms.getPositionFieldsAsVector()));
      default:
        return readResultSet(queryType, rs, Position.getFields(SectorPrms.getPositionFieldsAsVector()));
    }
  }

  private int readResultSet(int queryType, ResultSet rs, List fields) throws SQLException {
    int rows = 0;
    while (rs.next()) {
      readFields(rs, fields);
      rows++;
    }
    if (logQueryResultSize) {
      Log.getLogWriter().info("Returned " + rows + " rows");
    }
    if (validateResults) {
      int rss = getResultSetSize(queryType); // returns 0 to disable validation
      if (rss != 0 && rows != rss) {
        throw new HydraConfigException("Result set size expected("
            + rss + ") did not match actual(" + rows + ")");
      }
    }
    return rows;
  }

  /**
   * Returns the expected size of the result set for the specified query type.
   */
  private int getResultSetSize(int queryType) {
    switch (queryType) {
      case SectorPrms.INSTRUMENT_IN_QUERY:
        return numInstrumentsPerInClause;
      case SectorPrms.JOIN_INSTRUMENT_AND_POSITION_IN_QUERY:
        return numInstrumentsPerInClause * numPositionsPerInstrument;
      case SectorPrms.JOIN_PRUNE_BY_POSITION_AMOUNT_AND_INSTRUMENT_NAME_QUERY:
        return numAmountsPerRangeClause;
      case SectorPrms.JOIN_PRUNE_BY_POSITION_AMOUNT_AND_INSTRUMENT_NAME_AND_SECTOR_NAME_QUERY:
        return numAmountsPerRangeClause;
      case SectorPrms.POSITION_FILTER_IN_QUERY:
        return numPositionsPerInClause;
      case SectorPrms.POSITION_PRUNE_BY_INSTRUMENT_QUERY:
        return numPositionsPerInstrument / 2;
      case SectorPrms.POSITION_PRUNE_BY_INSTRUMENT_AND_FILTER_SYMBOL_AND_OWNER_QUERY:
        return 0; // no check
      case SectorPrms.POSITION_AMOUNT_RANGE_AND_QUERY:
        return numAmountsPerRangeClause;
      case SectorPrms.POSITION_AMOUNT_RANGE_OR_QUERY:
        return 2 * numAmountsPerRangeClause;
      case SectorPrms.POSITION_PRUNE_BY_INSTRUMENT_AND_POSITION_AMOUNT_QUERY:
        return numAmountsPerRangeClause;
      case SectorPrms.RANDOM_POSITION_PK_QUERY:
        return 1;
      case SectorPrms.SECTOR_FILTER_IN_QUERY:
        return numSectorsPerInClause;
      case SectorPrms.SECTOR_FILTER_IN_AND_MARKET_CAP_QUERY:
        if (numMarketCapValues >= numSectorsPerInClause) {
          return Math.min(numSectorsPerInClause, numMarketCapValuesPerOrClause);
        } else { // know numMarketCapValuesPerOrClause <= numMarketCapValues < numSectorsPerInClause
          int sectorsPerMarketCapValue = numSectors / numMarketCapValues;
          int n = sectorsPerMarketCapValue * numMarketCapValuesPerOrClause;
          int r = numSectorsPerInClause - numMarketCapValues * (numSectorsPerInClause / numMarketCapValues);
          return n + r;
        }
      default:
        String s = "Query type not supported yet: " + queryType;
        throw new UnsupportedOperationException("");
    }
  }

  private void readFields(ResultSet rs, List fields) throws SQLException {
    Iterator iterator = fields.iterator();
    while (iterator.hasNext()) {
      rs.getObject((String) iterator.next());
    }
  }

  //direct get
  public Object directGet(Object key, Region region) {
    return directGetOnPositionId(key, region);
  }

  public Object directGetOnPositionId(Object key, Region region) {
    Object entry = region.get(key);
    return entry;
  }
  
  //direct put
  public void directPut(Object key, Region region, Object value) {
    directPutOnPositionId(key, region, value);
  }

  public void directPutOnPositionId(Object key, Region region, Object value) {
    Object entry = region.put(key, value);
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
          { "APP." + Position.getTableName().toUpperCase() };
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
