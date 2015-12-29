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
/*
 * Utils.java
 *
 * Created on March 8, 2005, 4:16 PM
 */

package jta;

import java.sql.*;
import javax.naming.Context;
import javax.naming.NamingException;
import javax.sql.DataSource;

import hydra.*;

/** Utility class for sql access in jta tests.
 *  Derived from com/gemstone/gemfire/internal/jta/CacheUtils.java
 */
public class JTAUtil {

  public static void createTable(String tableName) throws NamingException, SQLException {
    Context ctx = CacheHelper.getCache().getJNDIContext();
    DataSource ds = (DataSource) ctx.lookup("java:/SimpleDataSource");
    
    String sql = "create table " + tableName + " (id integer NOT NULL, name varchar(50), CONSTRAINT the_key PRIMARY KEY(id))";
    Log.getLogWriter().info("createTable: " + sql);
    Connection conn = ds.getConnection();
    Statement sm = conn.createStatement();
    sm.execute(sql);
    sm.close();
    sm = conn.createStatement();
    for (int i = 1; i <= 100; i++) {
      sql = "insert into " + tableName + " values (" + i + ",'name" + i + "')";
      sm.addBatch(sql);
      Log.getLogWriter().info("createTable: " + sql);
    }
    sm.executeBatch();
    conn.close();
  }
  
  public static void listTableData(String tableName) throws NamingException, SQLException {
    Context ctx = CacheHelper.getCache().getJNDIContext();
    DataSource ds = (DataSource) ctx.lookup("java:/SimpleDataSource");
    
    String sql = "select * from " + tableName;
    Log.getLogWriter().info("listTableData: " + sql);
    
    Connection conn = ds.getConnection();
    Statement sm = conn.createStatement();
    ResultSet rs = sm.executeQuery(sql);
    while (rs.next()) {
      Log.getLogWriter().info("id " + rs.getString(1) + " name " + rs.getString(2));
    }
    rs.close();
    conn.close();
  }
  
  public static void destroyTable(String tableName) throws NamingException, SQLException {
    Context ctx = CacheHelper.getCache().getJNDIContext();
    DataSource ds = (DataSource) ctx.lookup("java:/SimpleDataSource");
    Connection conn = ds.getConnection();
    String sql = "drop table " + tableName;
    Log.getLogWriter().info("destroyTable: " + sql);
    Statement sm = conn.createStatement();
    sm.execute(sql);
    conn.close();
  }
}

