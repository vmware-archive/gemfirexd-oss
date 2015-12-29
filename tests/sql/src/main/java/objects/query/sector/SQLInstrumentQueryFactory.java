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

public class SQLInstrumentQueryFactory extends BaseQueryFactory {

  protected SQLPositionQueryFactory positionQueryFactory;

  public SQLInstrumentQueryFactory() {
    positionQueryFactory = new SQLPositionQueryFactory();
  }

  public void init() {
    super.init();
    positionQueryFactory.init();
  }

  //--------------------------------------------------------------------------
  // QueryFactory : Insert statements
  //--------------------------------------------------------------------------
  public List getInsertStatements(int bid) {
    int numInstrumentsPerSector = SectorPrms.getNumInstrumentsPerSector();
    List stmts = new ArrayList();
    for (int i = 0; i < numInstrumentsPerSector; i++) {
      int id = bid * numInstrumentsPerSector + i; // unique
      String typeName = Instrument.getInstrument(id);
      String stmt =
          "insert into " + Instrument.getTableName() + " (id, sector_id)"
              + " values ('" + typeName + "'," + bid + ")";
      stmts.add(stmt);
      stmts.addAll(positionQueryFactory.getInsertStatements(id));
    }
    return stmts;
  }

  public List getPreparedInsertStatements() {
    List stmts = new ArrayList();
    String stmt = "insert into " + Instrument.getTableName() + " values(?,?)";
    stmts.add(stmt);
    stmts.add(positionQueryFactory.getPreparedInsertStatements());
    return stmts;
  }

  public int fillAndExecutePreparedInsertStatements(List pstmts, List stmts, int sid)
      throws SQLException {
    int numInstrumentsPerSector = SectorPrms.getNumInstrumentsPerSector();
    PreparedStatement pstmt = (PreparedStatement) pstmts.get(0);
    String stmt = (String)stmts.get(0);
    int results = 0;
    for (int i = 0; i < numInstrumentsPerSector; i++) {
      int id = sid * numInstrumentsPerSector + i; // unique
      String typeName = Instrument.getInstrument(id);
      pstmt.setString(1, typeName);
      pstmt.setInt(2, sid);
      //pstmt.setString(3, typeName);
      if (logQueries) {
        Log.getLogWriter().info("EXECUTING: " + stmt + " with id=" + typeName
           + " sector_id=" + sid);
      }
      results += positionQueryFactory.fillAndExecutePreparedInsertStatements(
          (List)pstmts.get(1), (List)stmts.get(1), id);
      pstmt.addBatch();
      results++;
    }
    pstmt.executeBatch();
    pstmt.clearBatch();
    Connection c = pstmt.getConnection();
    if (c.getTransactionIsolation() != Connection.TRANSACTION_NONE
        && SectorPrms.commitBatches()) {
      c.commit();
    }
    return results;
  }

  //--------------------------------------------------------------------------
  // QueryFactory : Table statements
  //--------------------------------------------------------------------------
  public List getTableStatements() {
    List stmts = new ArrayList();
    String stmt = "create table " + Instrument.getTableName()
    + " (" + getIdCreateStatement() + ", " + getSectorIdCreateStatement() + ")"; 
    stmts.add(stmt);
    stmts.addAll(positionQueryFactory.getTableStatements());
    return stmts;
  }
  

  protected String getIdCreateStatement() {
    String stmt = "id varchar(20) not null";
    
    List indexes = SectorPrms.getInstrumentCreateTableIndexes();
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
  
  protected String getSectorIdCreateStatement() {
    String stmt = "sector_id int";
    
    List indexes = SectorPrms.getInstrumentCreateTableIndexes();
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
  
  public List getDropTableStatements() {
    List stmts = new ArrayList();
    stmts.addAll(positionQueryFactory.getDropTableStatements());
    stmts.add("drop table if exists " + Instrument.getTableName());
    return stmts;
  }
}
