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
package com.pivotal.gemfirexd.internal.engine.distributed;

import java.sql.ResultSet;
import java.util.Arrays;

import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverAdapter;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.QueryInfo;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.types.SQLInteger;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedPreparedStatement;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedStatement;

/**
 * Tests the distribued update functionality using 
 * PreparedStatement. 
 * It will also test other Update functionalities like constraint 
 * checks etc
 * @author Asif
 */
@SuppressWarnings("serial")
public class UpdatePreparedStatementDUnit extends DistributedSQLTestBase {
  protected static volatile boolean isQueryExecutedOnNode = false;

  public UpdatePreparedStatementDUnit(String name) {
    super(name);
  }
  
  
  /**
   *Tests whether fields set as null work correctly in update
   * 
   */
  public void testNullUpdate()
      throws Exception {
    // Create a table from client using partition by column 
    // Start one client a three servers
    startVMs(1, 3);

    clientSQLExecute(
        1,
        "create table TESTTABLE (ID int not null, "
            + " DESCRIPTION decimal(5,2) , ADDRESS varchar(1024) , primary key (ID))"
            + " PARTITION BY RANGE ( ID )"
            + " ( VALUES BETWEEN 1 and 4, VALUES BETWEEN  4 and 8 )" +getOverflowSuffix());

    // Insert values 1 to 9
    for (int i = 0; i <= 8; ++i) {
      clientSQLExecute(1, "insert into TESTTABLE values (" + (i + 1)
          + ","+  (i + 1) + ", 'J 604')");
    }
    Object[][] queries = new Object[][] {
      //update 1
        {  "Update TESTTABLE SET DESCRIPTION = ?  where ID  > ?  ",
           //Conditions
           new Object[][] {
             { new Integer(NodesPruningHelper.byrange),new SQLInteger(6),Boolean.FALSE,null,null }            
           },
           //Input parameters
           new Object[]{null, new Integer(6)},
           //Num rows updated
           new Integer(3),
           // sqltypes
           new int[] {java.sql.Types.DECIMAL, java.sql.Types.INTEGER}
        }  
      
    };
      TestUtil.setupConnection();
      try {
        for( int i=0; i < queries.length; ++i) {
           final QueryInfo[] uArr = new QueryInfo[1];
           final Activation[] actArr = new Activation[1];
           NodesPruningHelper.setupObserverOnClient(uArr,actArr);
           
           String queryString = (String)queries[i][0];           
           TestUtil.setupConnection();
           EmbedPreparedStatement ps = (EmbedPreparedStatement)TestUtil.jdbcConn.prepareStatement(queryString);    

           Object[] params = (Object[])queries[i][2];
           int[] types = (int[])queries[i][4];
           String log = "\nexecuting Query " + (i + 1) + " : \"" + ps.getSQLText()
               + "\"" + " with args: " + Arrays.toString(params);
           getLogWriter().info(log);
           //Set params
           for(int j = 0 ; j <params.length;++j ) {
             if(params[j] !=null) {
               ps.setObject(j+1, params[j]);
             }
             else {
               ps.setNull(j+1, types[j]);
             }
           }
           int numUpdates  = ps.executeUpdate();
           NodesPruningHelper.validateNodePruningForQuery(queryString, uArr[0], (Object[][])queries[i][1],this,actArr[0]);
           assertEquals(numUpdates, ((Integer) queries[i][3]).intValue());
           getLogWriter().info("Query " + (i+1) + " : succeeded");    
         }     
    }
    finally {
      GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter());
      clientSQLExecute(1, "Drop table TESTTABLE ");
    }

  } 
  
  /**
   *Tests whether fields set as null work correctly in update
   * 
   */
  public void testPrimaryKeyUpdateNotSupported_Bug39913()
      throws Exception {
    // Create a table from client using partition by column
    // Start one client a three servers
    startVMs(1, 3);

    clientSQLExecute(1, "create table TESTTABLE (ID int not null, "
        + " DESCRIPTION varchar(1024) , ADDRESS varchar(1024), "
        + "primary key (ID))"+getOverflowSuffix());

    try { 
    // Insert values 1 to 9
    for (int i = 0; i <= 8; ++i) {
      clientSQLExecute(1, "insert into TESTTABLE values (" + (i + 1)
          + ",'"+  (i + 1) + "', 'J 604')");
    }
    String query =   "Update TESTTABLE SET ID = 10  where DESCRIPTION =  '9' and ID = 9  " ;
      TestUtil.setupConnection();
      EmbedStatement stmnt = (EmbedStatement)TestUtil.jdbcConn
          .createStatement();
         try {
           int numUpdates  = stmnt.executeUpdate(query);
            fail("Update of primary key column should not be allowed");
           assertEquals(numUpdates, 1);
            ResultSet rs = stmnt.executeQuery("Select * from TESTTABLE where ID = 9");
           assertFalse(rs.next());
        }catch(Exception se) {
           String message = se.getMessage();
           if(message.indexOf("Update of column which is primary key or is part of the primary key, not supported") == -1 && message.indexOf("Update of partitioning column not supported") == -1) {
              fail("Unexpected type of StandardException."+se.toString());
           }else {
                //OK
           }
            
        }
   
    }
    finally {
      GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter());
      clientSQLExecute(1, "Drop table TESTTABLE ");
    }

  }
  
  public String getOverflowSuffix() {
    return  " ";
  }

}
