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
package com.pivotal.gemfirexd.internal.engine.procedure.cohort;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.cache.ColocationHelper;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.procedure.coordinate.ProxyResultDescription;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.jdbc.ConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.services.context.ContextManager;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultDescription;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ResultSetFactory;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnection;
import com.pivotal.gemfirexd.internal.impl.jdbc.Util;
import com.pivotal.gemfirexd.internal.impl.sql.execute.BaseActivation;
import com.pivotal.gemfirexd.procedure.OutgoingResultSet;
import com.pivotal.gemfirexd.procedure.ProcedureExecutionContext;

public final class ProcedureExecutionContextImpl implements ProcedureExecutionContext {
  private final BaseActivation activation;
  private final ProcedureSender procedureSender;
  private final ResultSet[][] dynamicResultSets;
  private final String tableName;
  private final String procedureName;  
  private final ResultSetFactory rsFactory;  
  private final ConnectionContext cc;

  private Region region;
  private final boolean isPossDup;
  private final String whereClause;
  private ArrayList<Connection> nestedConnsList;
  EmbedConnection parentConn;
  private int batchSize;

  public ProcedureExecutionContextImpl(Activation activation,    
                                       ProcedureSender sender,
                                       String whereClause,
                                       boolean isPossibleDup,
                                       ResultSet[][]  dynamicResultSets,                                    
                                       String tableName,
                                       String procedureName) {
    this.activation=(BaseActivation)activation;
    this.dynamicResultSets=dynamicResultSets;
    this.tableName=tableName;
    this.procedureName=procedureName;
    this.whereClause = whereClause;
    this.isPossDup = isPossibleDup;
    this.rsFactory=this.activation.getExecutionFactory().getResultSetFactory();    
    this.procedureSender=sender;
    if (this.procedureSender != null) {
      this.parentConn = this.procedureSender.setProcedureExecutionContext(this);
    }
    LanguageConnectionContext lcc=activation.getLanguageConnectionContext();
    ContextManager cm=lcc.getContextManager();
    this.cc=(ConnectionContext)cm.getContext(ConnectionContext.CONTEXT_ID);
  }

  public String[] getColocatedTableNames() {
    if (this.tableName != null) {
      this.region = Misc.getRegionForTable(this.tableName, false);
    }
    if (this.region != null
        && this.region.getAttributes().getDataPolicy().withPartitioning()) {
      Map<String, PartitionedRegion> map = ColocationHelper
          .getAllColocationRegions((PartitionedRegion)region);
      if (map != null) {
        String[] ret = new String[map.size()];
        Iterator<String> itr = map.keySet().iterator();
        int i = -1;
        while (itr.hasNext()) {
          String fpath = itr.next();
          String[] parts = fpath.split("/");
          assert parts.length == 2;
          ret[i++] = parts[0] + "." + parts[1];
        }
        if (ret.length > 0) {
          return ret;
        }
      }
    }
    return null;
  }

  public Connection getConnection() {
    Connection conn = null;
    try {
      if (this.parentConn != null) {
        conn = this.parentConn.getLocalDriver().getNewNestedConnection(
            this.parentConn);
      }
      else if (this.cc != null) {
        conn = this.cc.getNestedConnection(true);
      }
      if (conn != null) {
        if (this.nestedConnsList == null) {
          this.nestedConnsList = new ArrayList<Connection>();
        }
        this.nestedConnsList.add(conn);
      }
    } catch (SQLException e) {
      Misc.getCacheLogWriter().warning("could not get nested connection", e);
    }
    return conn;
  }

  public ArrayList<Connection> getNestedConnectionList() {
    return this.nestedConnsList;  
  }
  
  public String getFilter() {
    return this.whereClause;
  }

  public OutgoingResultSet getOutgoingResultSet(int rsNumber) {
    int number = this.dynamicResultSets != null ? this.dynamicResultSets.length : 0;
    if (number == 0) {
      return null;
    }
    if(rsNumber<1 || rsNumber>number) {
        throw new AssertionError("The result set number is not between 1 and "+number);
    }
    
    int resultSetNumber=rsNumber-1;
    ResultSet oldResultSet=this.dynamicResultSets[resultSetNumber][0];
    if(oldResultSet!=null) {
       throw new AssertionError(" The "+resultSetNumber+" result set has been set!");
    }
    ResultDescription rd=new ProxyResultDescription(false);
    this.activation.switchResultDescription(rd);
    OutgoingResultSet resultSet=this.rsFactory.getOutgoingResultSet(this.activation,resultSetNumber,rd);
    //Generate EmbedResultSet
    ResultSet embedResultSet;
    try {
      embedResultSet = this.cc
          .getResultSet((com.pivotal.gemfirexd.internal.iapi.sql.ResultSet)resultSet);
    } catch (SQLException e) {
      throw new AssertionError("Cannot allocate a outgoing result set!");
    }
    
    this.dynamicResultSets[resultSetNumber][0]=embedResultSet;   
    if(this.procedureSender!=null) {
       this.procedureSender.addOutgoingResultSet(resultSet);
    }    
    return resultSet;
  }

  public String getProcedureName() {
    return this.procedureName;
  }

  public String getTableName() {
    return this.tableName;
  }

  public boolean isPartitioned(String tableName) {
    if (tableName == null || tableName.isEmpty()) {
      throw new IllegalArgumentException(
          "ProcedureExecutionContextImpl::isPartitioned tableName passed is either null or empty");
    }
    Region reg = Misc.getRegionForTableByPath(tableName, true);
    if (reg == null) {
      throw new IllegalArgumentException(
          "ProcedureExecutionContextImpl::isPartitioned no region found corresponding to the tableName: "
              + tableName);
    }
    return reg.getAttributes().getDataPolicy().withPartitioning();
  }

  public boolean isPossibleDuplicate() {
    return this.isPossDup;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void checkQueryCancelled() throws SQLException {
    if (this.activation != null) {
      try {
        this.activation.checkCancellationFlag();
      } catch (StandardException se) {
        throw Util.generateCsSQLException(se);
      }
    }
  }
}
