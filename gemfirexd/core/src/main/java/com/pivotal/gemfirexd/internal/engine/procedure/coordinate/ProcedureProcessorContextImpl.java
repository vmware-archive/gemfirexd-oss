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
package com.pivotal.gemfirexd.internal.engine.procedure.coordinate;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.cache.ColocationHelper;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.iapi.jdbc.ConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.services.context.ContextManager;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultSet;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.procedure.IncomingResultSet;
import com.pivotal.gemfirexd.procedure.ProcedureProcessorContext;

public class ProcedureProcessorContextImpl implements ProcedureProcessorContext {

  private final ProcedureResultCollector prc;

  private final ConnectionContext cc;

  private final LanguageConnectionContext lcc;

  private final Region region;

  private final String whereClause;
  
  private final String tableName;
  
  private final DistributedProcedureCallNode dpnode;

  public ProcedureProcessorContextImpl(LanguageConnectionContext lcc,
      ProcedureResultCollector prc, Region r, String whereClause,
      DistributedProcedureCallNode dnode, String tableName) {
    this.lcc = lcc;
    ContextManager cm = lcc.getContextManager();
    this.cc = (ConnectionContext)cm.getContext(ConnectionContext.CONTEXT_ID);
    this.prc = prc;
    this.region = r;
    this.whereClause = whereClause;
    this.dpnode = dnode;
    this.tableName = tableName;
  }

  public IncomingResultSet[] getIncomingOutParameters() {
    return this.prc.getOutParameters();
  }

  public IncomingResultSet[] getIncomingResultSets(int resultSetNumber) {
    return this.prc.getIncomingResultSets(resultSetNumber);
  }

  public String[] getColocatedTableNames() {
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
    if (this.cc != null) {
      try {
        return this.cc.getNestedConnection(true);
      } catch (SQLException e) {
        Misc.getCacheLogWriter().warning("could not get nested connection", e);
      }
    }
    return null;
  }

  public String getFilter() {
    return this.whereClause;
  }

  public String getProcedureName() {
    return this.dpnode.getMethodName();
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

  /**
   * This method creates the jdbc result set from the execution result set.
   * 
   * @param rs
   * @return
   * @throws SQLException
   */
  java.sql.ResultSet getResultSet(ResultSet rs) throws SQLException {
    return this.cc.getResultSet(rs);
  }

  public int getCurrentResultSetNumber() {
    return 0;
  }

  public boolean isPossibleDuplicate() {
    return this.prc.getIfReExecute();
  }

}
