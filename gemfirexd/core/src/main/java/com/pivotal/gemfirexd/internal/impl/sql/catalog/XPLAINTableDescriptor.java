/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.catalog.XPLAINTableDescriptor

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

/*
 * Changes for GemFireXD distributed data platform (some marked by "GemStone changes")
 *
 * Portions Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
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

package com.pivotal.gemfirexd.internal.impl.sql.catalog;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.SystemColumn;
import com.pivotal.gemfirexd.internal.iapi.util.IdUtil;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnection;

public abstract class XPLAINTableDescriptor {
  /** static registrations */
  protected static final List<XPLAINTableDescriptor> statDescriptors = new ArrayList<XPLAINTableDescriptor>(
      7);

  static {
    statDescriptors.add(new XPLAINStatementDescriptor());
    statDescriptors.add(new XPLAINResultSetDescriptor());
  }

  public static Iterator<XPLAINTableDescriptor> getRegisteredDescriptors() {
    return statDescriptors.iterator();
  }

  public static final void registerStatements(
      EmbedConnection conn) throws StandardException {
    final LanguageConnectionContext lcc = conn.getLanguageConnection();
    if (lcc.explainStatementsExists()) {
      return;
    }

    for (Iterator<XPLAINTableDescriptor> i = XPLAINTableDescriptor
        .getRegisteredDescriptors(); i.hasNext();) {

      XPLAINTableDescriptor t = i.next();
      String ins = t.getTableInsert();
      try {
        conn.prepareStatement(ins).close();
      } catch (SQLException e) {
        String trace = e.toString();
        if (GemFireXDUtils.TraceQuery | GemFireXDUtils.TraceNCJ) {
          trace = SanityManager.getStackTrace(e);
        }
        SanityManager.DEBUG_PRINT("warning:"
            + GfxdConstants.TRACE_PLAN_GENERATION,
            "StatementPlanCollector::doXPLAIN: got exception while "
                + "enabling plan collection: " + trace);
        throw StandardException
            .newException(e.getSQLState(), e, e.getMessage());
      }
      lcc.setExplainStatement(t.getCatalogName(), ins);
    }
  }

  public abstract String getCatalogName();

  protected abstract SystemColumn[] buildColumnList();

  // GemStone changes BEGIN
  protected abstract void addConstraints(
      StringBuilder sb);

  // GemStone changes END

  private String tableInsertStmt;

  public String[] getTableDDL(
      String schemaName) {
    String escapedSchema = IdUtil.normalToDelimited(schemaName);
    String escapedTableName = IdUtil.normalToDelimited(getCatalogName());
    SystemColumn[] cols = buildColumnList();
    StringBuilder buf = new StringBuilder();
    StringBuilder idx = new StringBuilder();
    StringBuilder insBuf = new StringBuilder();
    StringBuilder valsBuf = new StringBuilder();
    for (int c = 0; c < cols.length; c++) {
      if (c == 0) {
        buf.append("(");
        insBuf.append("(");
        valsBuf.append("(");
      }
      else {
        buf.append(",");
        insBuf.append(",");
        valsBuf.append(",");
      }
      buf.append(cols[c].getName());
      insBuf.append(cols[c].getName());
      valsBuf.append("?");
      buf.append(" ");
      buf.append(cols[c].getType().getCatalogType().getSQLstring());
    }
    // GemStone changes BEGIN
    addConstraints(buf);
    // GemStone changes END
    buf.append(")");
    // GemStone changes BEGIN
    buf.append(" LOCAL");
    // GemStone changes END
    insBuf.append(")");
    valsBuf.append(")");
    String query = "create table " + escapedSchema + "." + escapedTableName
        + buf.toString();

    createIndex(idx,schemaName);

    tableInsertStmt = "insert into " + escapedSchema + "." + escapedTableName
        + insBuf.toString() + " values " + valsBuf.toString();

    return new String[] { query, idx.toString() };
  }

  protected abstract void createIndex(StringBuilder idx, String schemaName);

  public String getTableInsert() {
    return tableInsertStmt;
  }
}
