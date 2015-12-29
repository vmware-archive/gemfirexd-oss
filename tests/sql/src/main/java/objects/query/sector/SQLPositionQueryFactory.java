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

import hydra.Log;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import objects.query.BaseQueryFactory;

public class SQLPositionQueryFactory extends BaseQueryFactory {

  protected SQLRiskQueryFactory riskQueryFactory;

  public SQLPositionQueryFactory() {
    riskQueryFactory = new SQLRiskQueryFactory();
  }

  public void init() {
    super.init();
    riskQueryFactory.init();
  }

  //--------------------------------------------------------------------------
  // QueryFactory : Insert statements
  //--------------------------------------------------------------------------
  //bid represents the Instrument id
  public List getInsertStatements(int bid) {
    String instrument = Instrument.getInstrument(bid);
    int numPositionsPerInstrument = SectorPrms.getNumPositionsPerInstrument();
    int numBookValues = SectorPrms.getNumBookValues();
    int synthetic = 0;
    List stmts = new ArrayList();
    for (int i = 0; i < numPositionsPerInstrument; i++) {
      int id = bid * numPositionsPerInstrument + i; // unique
      int amount = id;
      int bookId = id % numBookValues;
      String owner = Position.getOwner(id);
      synthetic = id % 2;
      String symbol = Position.getSymbol(id);
      String stmt =
          "insert into " + Position.getTableName()
              + " (id, book_id, instrument, amount, "
              + "synthetic, owner, symbol) values (" + id + "," + bookId + ",'"
              + instrument + "'," + amount + "," + synthetic + ",'" + owner
              + "','" + symbol + "')";
      stmts.add(stmt);
      stmts.addAll(riskQueryFactory.getInsertStatements(id));
    }
    return stmts;
  }

  public List getPreparedInsertStatements() {
    List stmts = new ArrayList();
    stmts.add(getPreparedInsertStatement());
    stmts.add(riskQueryFactory.getPreparedInsertStatements());
    return stmts;
  }
  
  public String getPreparedInsertStatement() {
    String stmt =
        "insert into " + Position.getTableName() + " values (?,?,?,?,?,?,?)";
    return stmt;
  }

  public int fillAndExecutePreparedInsertStatements(List pstmts, List stmts, int iid)
      throws SQLException {

    String instrument = Instrument.getInstrument(iid);
    int numPositionsPerInstrument = SectorPrms.getNumPositionsPerInstrument();
    int numBookValues = SectorPrms.getNumBookValues();
    PreparedStatement pstmt = (PreparedStatement) pstmts.get(0);
    String stmt = (String)stmts.get(0);
    int synthetic = 0;
    int results = 0;
    for (int i = 0; i < numPositionsPerInstrument; i++) {
      int id = iid * numPositionsPerInstrument + i; // unique
      int amount = id;
      int bookId = id % numBookValues;
      String owner = Position.getOwner(id);
      synthetic = 1 - synthetic;
      String symbol = Position.getSymbol(id);
      pstmt.setInt(1, id);
      pstmt.setInt(2, bookId);
      pstmt.setString(3, instrument);
      pstmt.setInt(4, amount);
      pstmt.setInt(5, synthetic);
      pstmt.setString(6, owner);
      pstmt.setString(7, symbol);
      if (logQueries) {
        Log.getLogWriter().info("EXECUTING: " + stmt + " with id=" + id
           + " book_id=" + bookId + " instrument=" + instrument
           + " amount=" + amount + " synthetic=" + synthetic
           + " owner=" + owner + " symbol=" + symbol);
      }
      results += riskQueryFactory.fillAndExecutePreparedInsertStatements((List) pstmts
          .get(1), (List)stmts.get(1), id);
      pstmt.addBatch();
      results++;
    }
    pstmt.executeBatch();
    pstmt.clearBatch();
    List rpstmts = (List)pstmts.get(1);
    PreparedStatement rpstmt = (PreparedStatement)rpstmts.get(rpstmts.size() - 1);
    rpstmt.executeBatch();
    rpstmt.clearBatch();
    Connection c = pstmt.getConnection();
    if (c.getTransactionIsolation() != Connection.TRANSACTION_NONE
        && SectorPrms.commitBatches()) {
      c.commit();
    }
    return results;
  }

  public void fillPreparedInsertStatement(PreparedStatement pstmt, String stmt, int iid)
      throws SQLException {
    String instrument = Instrument.getInstrument(iid);
    int numPositionsPerInstrument = SectorPrms.getNumPositionsPerInstrument();
    int numBookValues = SectorPrms.getNumBookValues();
    int synthetic = 0;
    
    //for (int i = 0; i < numPositionsPerInstrument; i++) {
      int id = iid; // unique
      int amount = id;
      int bookId = id % numBookValues;
      String owner = Position.getOwner(id);
      synthetic = 1 - synthetic;
      String symbol = Position.getSymbol(id);
      pstmt.setInt(1, id);
      pstmt.setInt(2, bookId);
      pstmt.setString(3, instrument);
      pstmt.setInt(4, amount);
      pstmt.setInt(5, synthetic);
      pstmt.setString(6, owner);
      pstmt.setString(7, symbol);
    //}
      
  }

  //--------------------------------------------------------------------------
  // QueryFactory : Table statements
  //--------------------------------------------------------------------------
  public List getTableStatements() {
    List stmts = new ArrayList();
    String stmt =
      "create table " + Position.getTableName()
          + " (" + getIdCreateStatement() + ", " + getBookIdCreateStatement()+ ", " + getInstrumentIdCreateStatement() + ", "
          + getAmountCreateStatement() + ", " + getSyntheticCreateStatement() + ", " + getOwnerCreateStatement() + ", "
          + getSymbolCreateStatement() + ")";
    stmts.add(stmt);
    stmts.addAll(riskQueryFactory.getTableStatements());
    return stmts;
  }
  
  protected String getRangeValuesStmt(String columnName) {
    String stmt = "";
    if (columnName.equalsIgnoreCase("id")) {
      int numNodes = SectorPrms.getNumServers();
      int numPositions = SectorPrms.getNumPositions();
      int range = numPositions / numNodes;
      int remainder = numPositions - numNodes * range;
      int lower = 0;
      int upper;
      for (int i = 0; i < numNodes; i++) {
        if (i != 0) stmt += ",";
        upper = lower + range;
        if (i < remainder) ++upper;
        stmt += " VALUES BETWEEN " + lower + " AND " + upper;
        lower = upper;
      }
    } else {
      String s = "Partition by range on column " + columnName;
      throw new UnsupportedOperationException(s);
    }
    return stmt;
  }

  protected static int getLowerIdForNode(int node, int range, int remainder) {
    int lower = 0;
    int upper = 0;
    for (int i = 0; i < node; i++) {
      upper = lower + range;
      if (i < remainder) ++upper;
      lower = upper;
    }
    return lower;
  }

  protected String getIdCreateStatement() {
    String stmt = "id int not null";
    List indexes = SectorPrms.getPositionCreateTableIndexes();
    Iterator iterator = indexes.iterator();
    
    while (iterator.hasNext()) {
      int indexType = SectorPrms.getIndexType((String)iterator.next());
      if (indexType == SectorPrms.PRIMARY_KEY_INDEX_ON_POSITION_ID_QUERY) {
        stmt += " primary key";
      }
      else if (indexType == SectorPrms.UNIQUE_INDEX_ON_POSITION_ID_QUERY) {
        stmt += " unique";
      }
    }
    return stmt;
  }
  
  protected String getBookIdCreateStatement() {
    String stmt = "book_id int";
    List indexes = SectorPrms.getPositionCreateTableIndexes();
    Iterator iterator = indexes.iterator();
    
    while (iterator.hasNext()) {
      int indexType = SectorPrms.getIndexType((String)iterator.next());
      if (indexType == SectorPrms.PRIMARY_KEY_INDEX_ON_BOOK_ID_QUERY) {
        stmt += " primary key";
      }
      else if (indexType == SectorPrms.UNIQUE_INDEX_ON_BOOK_ID_QUERY) {
        stmt += " unique";
      }
    }
    return stmt;
  }
  
  protected String getInstrumentIdCreateStatement() {
    String stmt = "instrument varchar(20) not null";
    List indexes = SectorPrms.getPositionCreateTableIndexes();
    Iterator iterator = indexes.iterator();
    
    while (iterator.hasNext()) {
      int indexType = SectorPrms.getIndexType((String)iterator.next());
      if (indexType == SectorPrms.PRIMARY_KEY_INDEX_ON_INSTRUMENT_ID_QUERY) {
        stmt += " primary key";
      }
      else if (indexType == SectorPrms.UNIQUE_INDEX_ON_INSTRUMENT_ID_QUERY) {
        stmt += " unique";
      }
    }
    return stmt;
  }
  
  protected String getAmountCreateStatement() {
    String stmt = "amount int";
    List indexes = SectorPrms.getPositionCreateTableIndexes();
    Iterator iterator = indexes.iterator();
    
    while (iterator.hasNext()) {
      int indexType = SectorPrms.getIndexType((String)iterator.next());
      if (indexType == SectorPrms.PRIMARY_KEY_INDEX_ON_AMOUNT_QUERY) {
        stmt += " primary key";
      }
      else if (indexType == SectorPrms.UNIQUE_INDEX_ON_AMOUNT_QUERY) {
        stmt += " unique";
      }
    }
    return stmt;
  }
  
  protected String getSyntheticCreateStatement() {
    String stmt = "synthetic int";
    List indexes = SectorPrms.getPositionCreateTableIndexes();
    Iterator iterator = indexes.iterator();
    
    while (iterator.hasNext()) {
      int indexType = SectorPrms.getIndexType((String)iterator.next());
      if (indexType == SectorPrms.PRIMARY_KEY_INDEX_ON_SYNTHETIC_QUERY) {
        stmt += " primary key";
      }
      else if (indexType == SectorPrms.UNIQUE_INDEX_ON_SYNTHETIC_QUERY) {
        stmt += " unique";
      }
    }
    return stmt;
  }
  
  protected String getOwnerCreateStatement() {
    String stmt = "owner varchar(20)";
    List indexes = SectorPrms.getPositionCreateTableIndexes();
    Iterator iterator = indexes.iterator();
    
    while (iterator.hasNext()) {
      int indexType = SectorPrms.getIndexType((String)iterator.next());
      if (indexType == SectorPrms.PRIMARY_KEY_INDEX_ON_OWNER_QUERY) {
        stmt += " primary key";
      }
      else if (indexType == SectorPrms.UNIQUE_INDEX_ON_OWNER_QUERY) {
        stmt += " unique";
      }
    }
    return stmt;
  }
  
  protected String getSymbolCreateStatement() {
    String stmt = " symbol varchar(5)";
    List indexes = SectorPrms.getPositionCreateTableIndexes();
    Iterator iterator = indexes.iterator();
    
    while (iterator.hasNext()) {
      int indexType = SectorPrms.getIndexType((String)iterator.next());
      if (indexType == SectorPrms.PRIMARY_KEY_INDEX_ON_SYMBOL_QUERY) {
        stmt += " primary key";
      }
      else if (indexType == SectorPrms.UNIQUE_INDEX_ON_SYMBOL_QUERY) {
        stmt += " unique";
      }
    }
    return stmt;
  }

  public List getDropTableStatements() {
    List stmts = new ArrayList();
    stmts.addAll(riskQueryFactory.getDropTableStatements());
    stmts.add("drop table if exists " + Position.getTableName());
    return stmts;
  }
}
