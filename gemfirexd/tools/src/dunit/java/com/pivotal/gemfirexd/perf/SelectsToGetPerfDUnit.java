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
package com.pivotal.gemfirexd.perf;

import java.util.Random;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;

/**
 * Verify Get queries & check for relative performance 
 *
 * @author Soubhik Chakraborty
 */
@SuppressWarnings("serial")
public class SelectsToGetPerfDUnit extends DistributedSQLTestBase {

  public SelectsToGetPerfDUnit(String name) {
    super(name);
  }
  
  public void testDummy(){
    
  }
  
  public void PERF_testPrimayKeyGets() throws Exception {
    startVMs(1, 2);

    // Use this VM as the client
//    Properties info = new Properties();
//    info.setProperty("host-data", "false");
//    info.setProperty("gemfire.mcast-port", String.valueOf(mcastport));
//    info.setProperty("gemfire.log-level", "warning");
//    TestUtil.loadDriver();
//    TestUtil.setupConnection(info);
    
    int totalRows = 20;
    this.serverVMs.get(0).invoke(SelectsToGetPerfDUnit.class,
        "prepareTableWithOneKey", new Object[] { new Integer(totalRows) });
    this.serverVMs.get(1).invoke(SelectsToGetPerfDUnit.class,
        "prepareTableWithTwoKey", new Object[] { new Integer(totalRows) });

    Random rand = new Random(System.currentTimeMillis());
    
    Object queries[][][] = { 
        {  { TestUtil.jdbcConn.prepareStatement("select * from Account where id = ?") }
          ,{ new Integer(Types.INTEGER) }
        },
        { { TestUtil.jdbcConn.prepareStatement("select * from finOrder where id = ? and account = ?") } 
          ,{ new Integer(Types.INTEGER) } //test auto sqlType promotion by derby
          ,{ new Integer(Types.VARCHAR), new Integer(1), "Account "}
        },
        { { TestUtil.jdbcConn.prepareStatement("select * from finOrder where account = ? and id = ?") } 
          ,{ new Integer(Types.VARCHAR), new Integer(1), "Account "}
          ,{ new Integer(Types.INTEGER) } //test auto sqlType promotion by derby
        }
      };
      
      int qps = 0, totqry = queries.length;
      long startQ = System.currentTimeMillis(), stopQ = System.currentTimeMillis();
      long samplingTimes = 100;
      
      for ( int iterations = 1; iterations <= 5; iterations++) {
        
        startQ = System.currentTimeMillis();
        for ( int j = 1; j <= samplingTimes; j++) {
              int pkey = rand.nextInt(totalRows) + 1;
              ResultSet rs = null;
              
              int qryRow = rand.nextInt( totqry );
              PreparedStatement ps = (PreparedStatement) queries[qryRow][0][0];
              
              for( int prm_1 = 1; prm_1 < queries[qryRow].length; prm_1++) {
                int paramType = ((Integer)queries[qryRow][prm_1][0]).intValue(); 
                switch( paramType ) {
                 case Types.VARCHAR:
                   if( ((Integer)queries[qryRow][prm_1][1]).intValue() == 1 ) { //if additional string is a prefix 
                     ps.setObject(prm_1, String.valueOf( queries[qryRow][prm_1][2] )+String.valueOf(pkey), Types.VARCHAR );
                   }
                   break;
                 default:
                   ps.setObject(prm_1, new Integer(pkey), ((Integer)queries[qryRow][prm_1][0]).intValue() ); // 1 based parameter
                   break;
                }
              }
              
              rs = ps.executeQuery();
              assertTrue( rs != null );
              
              rs.next();
              
              assertEquals( rs.getObject("id").toString(), String.valueOf(pkey) );
              assertTrue( rs.next() == false);
              
              rs.close();
          } //end of sampling..
          stopQ = System.currentTimeMillis();
           
          if(iterations > 4 && ((stopQ-startQ)/1000L) > 0) {
             qps = (int) (samplingTimes/ ((stopQ-startQ)/1000L));
             if(qps < 5000)
               getGlobalLogger().warn(" [Get query/sec dropped to ] " + String.valueOf(qps));
          }
           
    } //end of iterations.

  }

  public static void prepareTableWithOneKey(int rows) throws SQLException {
    
    Statement s = TestUtil.jdbcConn.createStatement();
    getGlobalLogger().info(" creating tables ");
    
    s.execute("create table Account (" +
               " id varchar(10) primary key, name varchar(100), type int )");
    TestUtil.jdbcConn.commit();

    getGlobalLogger().info(" populating values ");
    PreparedStatement ps = TestUtil.jdbcConn.prepareStatement("insert into Account values(?,?,?)"); 
    while( rows > 0) {
      ps.setString(1, String.valueOf(rows));
      ps.setString(2, "Dummy Account " + String.valueOf(rows));
      ps.setInt(3, rows % 2);
      ps.executeUpdate();
      rows--;
    }
    
  }

  public static void prepareTableWithTwoKey(int rows) throws SQLException {
    Statement s = TestUtil.jdbcConn.createStatement();
    
    s.execute("create table finOrder (" +
                " id bigint, name varchar(100), type int, account varchar(100), " +
                " constraint order_pk primary key(account, id ) )" );
    
    TestUtil.jdbcConn.commit();
    getGlobalLogger().info( " populating values ");
    PreparedStatement ps = TestUtil.jdbcConn.prepareStatement("insert into finOrder values(?,?,?,?)"); 
    while( rows > 0) {
      ps.setLong(1, rows);
      ps.setString(2, "Dummy Order " + String.valueOf(rows));
      ps.setInt(3, rows % 4);
      ps.setString(4, "Account " + String.valueOf(rows));
      ps.executeUpdate();
      rows--;
    }
    
  }

  
}
