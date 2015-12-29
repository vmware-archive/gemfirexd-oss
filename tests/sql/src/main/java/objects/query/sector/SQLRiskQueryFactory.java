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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import objects.query.BaseQueryFactory;

public class SQLRiskQueryFactory extends BaseQueryFactory {

  //--------------------------------------------------------------------------
  // QueryFactory : Insert statements
  //--------------------------------------------------------------------------
  public List getInsertStatements(int bid) {
    int positionId = bid;
    int numRiskValues = SectorPrms.getNumRiskValues();
    int riskValue = bid % numRiskValues;
    List stmts = new ArrayList();
    String stmt =
      "insert into " + Risk.getTableName() + " (id, position_id, risk_value)"
          + " values (" + positionId + "," + positionId + "," + riskValue
          + ")";
    stmts.add(stmt);
    return stmts;
  }

  public List getPreparedInsertStatements() {
    List stmts = new ArrayList();
    String stmt = "insert into " + Risk.getTableName() + " values (?,?,?)";
    stmts.add(stmt);
    return stmts;
  }

  public int fillAndExecutePreparedInsertStatements(List pstmts, List stmts, int posId)
      throws SQLException {
    int positionId = posId;
    int numRiskValues = SectorPrms.getNumRiskValues();
    int riskValue = posId % numRiskValues;
    PreparedStatement pstmt = (PreparedStatement) pstmts.get(pstmts.size() - 1);
    String stmt = (String)stmts.get(stmts.size() - 1);
    pstmt.setInt(1, positionId);
    pstmt.setInt(2, positionId);
    pstmt.setInt(3, riskValue);
    pstmt.addBatch();
    if (logQueries) {
      Log.getLogWriter().info("EXECUTING: " + stmt + " with id=" + positionId
         + " position_id=" + positionId + " risk_value=" + riskValue);
    }
    return 1;
  }

  //--------------------------------------------------------------------------
  // QueryFactory : Table statements
  //--------------------------------------------------------------------------
  public List getTableStatements() {
    List stmts = new ArrayList();
    String stmt = "create table " + Risk.getTableName()
      + " (" + getIdCreateStatement() + ", " + getPositionIdCreateStatement() + ", " + getRiskValueCreateStatement() + ") ";
    stmts.add(stmt);
    return stmts;
  }
  

  protected String getIdCreateStatement() {
    String stmt = "id int not null";
    List indexes = SectorPrms.getRiskCreateTableIndexes();
    Iterator iterator = indexes.iterator();
    
    while (iterator.hasNext()) {
      int indexType = SectorPrms.getIndexType((String)iterator.next());
      if (indexType == SectorPrms.PRIMARY_KEY_INDEX_ON_RISK_ID_QUERY) {
        stmt += " primary key";
      }
      else if (indexType == SectorPrms.UNIQUE_INDEX_ON_RISK_ID_QUERY) {
        stmt += " unique";
      }
    }
    return stmt;
  }
  
  protected String getPositionIdCreateStatement() {
    String stmt = "position_id int not null";
    List indexes = SectorPrms.getRiskCreateTableIndexes();
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
  
  
  protected String getRiskValueCreateStatement() {
    String stmt = "risk_value int";
    List indexes = SectorPrms.getRiskCreateTableIndexes();
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

  
  public List getDropTableStatements() {
    List stmts = new ArrayList();
    stmts.add("drop table if exists " + Risk.getTableName());
    //stmts.addAll(brokerTicketQueryFactory.getTableStatements());
    return stmts;
  }
}
