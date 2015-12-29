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
package com.pivotal.gemfirexd.functions;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class TestFunctions {

  private static String nestedProtocol = "jdbc:default:connection";

  public static int times(int value) {
    return value * 2;
  }

  public static ResultSet subset(String tableName, int low, int high)
      throws SQLException {
    Connection conn = DriverManager.getConnection(nestedProtocol);
    String query = "SELECT ID, SECURITY_ID FROM " + tableName + " where id>"
        + low + " and id<" + high;
    Statement statement = conn.createStatement();
    ResultSet rs = statement.executeQuery(query);
    return rs;
  }

  public static int timesWithLog(int value) throws SQLException {
    System.out.println("XXXthe times is called!");
    log();
    return value * 2;
  }

  public static ResultSet subsetWithLog(String tableName, int low, int high)
      throws SQLException {
    log();
    Connection conn = DriverManager.getConnection(nestedProtocol);
    String query = "SELECT ID, SECURITY_ID FROM " + tableName + " where id>"
        + low + " and id<" + high;
    Statement statement = conn.createStatement();
    ResultSet rs = statement.executeQuery(query);
    return rs;

  }

  public static void log() throws SQLException {
    Connection conn = DriverManager.getConnection(nestedProtocol);
    Statement s = conn.createStatement();
    long id = Thread.currentThread().getId();
    s.execute("INSERT INTO APP.LOG values(" + id + ")");
    conn.close();

  }

  public static String substring(String str, int startIndex, int endIndex) {
    return str.substring(startIndex, endIndex);
  }
}
