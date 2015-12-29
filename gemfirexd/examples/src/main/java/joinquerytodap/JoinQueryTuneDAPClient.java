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
package examples.joinquerytodap;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class JoinQueryTuneDAPClient {

  public static void main(String[] args) {
    long startTime = System.currentTimeMillis();
    
    try {
      Connection cxn = DriverManager.getConnection("jdbc:gemfirexd://pnq-rdiyewar:1528/");
      System.out.println("Connected to : jdbc:gemfirexd://pnq-rdiyewar:1528/");
            
      CallableStatement callableStmt = cxn.prepareCall("{CALL QUERY_TUNE_DAP() WITH RESULT PROCESSOR examples.joinquerytodap.JoinQueryTuneProcessor ON ALL}");
      callableStmt.setQueryTimeout(10);
      callableStmt.execute();
      System.out.println("Query execution time = " + (System.currentTimeMillis() - startTime) + " ms");
      
      ResultSet thisResultSet;
      int rowCount = 0;
      thisResultSet = callableStmt.getResultSet();
      ResultSetMetaData meta = thisResultSet.getMetaData();
      int colCnt = meta.getColumnCount();
      for (int i = 1; i < colCnt + 1; i++) {
        System.out.print(meta.getColumnLabel(i));
        if (i != colCnt) {
          System.out.print(',');
        }
      }
      System.out.println();
      
      while (thisResultSet.next()) {
        for (int i = 1; i < colCnt + 1; i++) {
          Object o = thisResultSet.getObject(i);
          assert o != null;
        }
        rowCount++;
      }
      System.out.println("ResultSet ends, row count = " + rowCount);

      System.out.println("Total time = " + (System.currentTimeMillis() - startTime) + " ms");
    } catch (SQLException e) {
      System.out.println("Got Exception, execution time = " + (System.currentTimeMillis() - startTime) + " ms");
      e.printStackTrace();
    }
  }
}
