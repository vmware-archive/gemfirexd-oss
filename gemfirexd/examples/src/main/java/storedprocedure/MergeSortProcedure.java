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
package examples.storedprocedure;

import com.pivotal.gemfirexd.procedure.*;
import java.sql.*;

public class MergeSortProcedure {

  public static void mergeSort(ResultSet[] outResults,
      ProcedureExecutionContext context) throws SQLException {

    Connection cxn = context.getConnection();
    String tableName = context.getTableName();
    if (tableName == null || tableName.length() == 0) {
      // some default table in case no ON TABLE was used in execution
      tableName = "EMP.PARTITIONTESTTABLE";
    }
    // some fixed column being used for ordering here; instead the code can
    // find out the schema of the table and use some column
    String queryString = "<local> SELECT * FROM " + tableName
        + " ORDER BY SECONDID";
    Statement stmt = cxn.createStatement();
    ResultSet rs = stmt.executeQuery(queryString);
    outResults[0] = rs;
    // Do not close the connection since this would also
    // close the result set.
  }
}
