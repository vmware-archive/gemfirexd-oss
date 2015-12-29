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
package sql.dmlStatements;

import java.sql.*;
import hydra.*;
import sql.SQLHelper;

public class TradeCustomersInsertStmt extends TradeCustomersDMLStmt {
  // remove null cid part  
  protected int insertToTable(PreparedStatement stmt, int cid, String cust_name, Date since, String addr, int tid) throws SQLException {   
    String database = SQLHelper.isDerbyConn(stmt.getConnection())?"Derby - " :"gemfirexd - "  ;
    Log.getLogWriter().info(database + "inserting into trade.customers CID:" + cid + ":CUSTNAME:" + cust_name +
           ",SINCE:" + since + ",ADDR:" + addr + ",TID:" + tid);        
      stmt.setInt(1, cid);
      stmt.setString(2, cust_name);
      stmt.setDate(3, since);
      stmt.setString(4, addr);       
      stmt.setInt(5, tid);      
      int rowCount = stmt.executeUpdate();
      SQLWarning warning = stmt.getWarnings(); //test to see there is a warning
      if (warning != null) {
        SQLHelper.printSQLWarning(warning);
      } 
      return rowCount;
    }  
}
