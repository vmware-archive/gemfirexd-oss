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
package com.pivotal.vfabric.booksdb.storedproc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.gemstone.gemfire.cache.CacheFactory;
import com.pivotal.gemfirexd.procedure.OutgoingResultSet;
import com.pivotal.gemfirexd.procedure.ProcedureExecutionContext;

public class ListBooksStoreProc {
  public static void execute(String idToSelect, ResultSet[] outResultSet,
      ProcedureExecutionContext context) {
    try {
      Connection cconnection = context.getConnection();
      PreparedStatement stmt = cconnection
          .prepareStatement("SELECT ID FROM APP.BOOKS where ID=? ");
      stmt.setString(1, idToSelect);
      // execute prepared statement
      ResultSet rs = stmt.executeQuery();

      // create programatically empty resultSet
      OutgoingResultSet ors = context.getOutgoingResultSet(1);

      // add a column definition to outgoing resultset
      ors.addColumn("ID");

      while (rs.next()) {
        List<Object> row = new ArrayList<Object>();
        String id = rs.getString(1);
        row.add(id);
        ors.addRow(row);
      }

      // add end of result marker
      ors.endResults();

    } catch (SQLException sqle) {
      sqle.printStackTrace();
      throw new RuntimeException(sqle);
    }
  }
}
