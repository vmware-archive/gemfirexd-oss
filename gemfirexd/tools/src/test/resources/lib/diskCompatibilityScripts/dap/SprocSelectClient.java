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
package examples.dap;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class SprocSelectClient {

  public static void main(String[] args) {
    try {
      Connection cxn = DriverManager.getConnection("jdbc:gemfirexd://10.112.204.75:1527/");
      
      long startTime = System.currentTimeMillis();      
      CallableStatement callableStmt = cxn.prepareCall("{CALL SPROC_SELECT_DAP('a')}");
      callableStmt.execute();
      System.out.println("rdrd Query execution time = " + (System.currentTimeMillis() - startTime) + " ms");
      
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}
