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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

import com.pivotal.gemfirexd.procedure.ProcedureExecutionContext;

public class SprocSelectProcedure {   
  
  public static void execute(String str,
      ProcedureExecutionContext context) {
    Logger logger = Logger.getLogger("com.pivotal.gemfirexd");    
    logger.info("SprocProcedure::rdrdexecute called");
    //HashSet<SprocProcedure.Key> cachedRows = new HashSet<SprocProcedure.Key>();
    ResultSet rs = null;    
    
    try {
      Connection conn = context.getConnection();
      String lsql = " select * "
          + "from CONTEXT_HISTORY ";
      
      logger.info("SprocProcedure::execute  processing query started");
      Statement st = conn.createStatement();
      //PreparedStatement st = conn.prepareStatement(lsql);
      rs = st.executeQuery(lsql);
      //st.setString(1, str);
      //st.executeUpdate();
      while(rs.next()){
        try {
          Thread.sleep(1000);
        } catch(InterruptedException e){
          Thread.currentThread().interrupt();
        }
      }
      
      logger.info("SprocProcedure::execute  executed insert query" );
    } catch (SQLException sqle) {
      sqle.printStackTrace();
      throw new RuntimeException(sqle);
    }
    finally {
      if (rs != null) {
        try {
          rs.close();
        }
        catch (SQLException e) {
          e.printStackTrace();
        }
      }
    }
  }
}
