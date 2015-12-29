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
package cacheperf.comparisons.gemfirexd.useCase1.datagen;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

public class GenerateMapper {

  protected String url = "jdbc:gemfirexd://localhost:1530/";

  protected String driver = "com.pivotal.gemfirexd.jdbc.ClientDriver";

  int[] rowCount = new int[] {100000};
  
  enum RelationalOperator {
    Equal,
    GreaterThan,
    LessThan;
  };

  public GenerateMapper(String[] args) {
    if (args.length == 0) {
      System.out.println("Usage <script> <tablenames> "
          + "[<host:port> <rowcount> <column mapping file>]");
      System.exit(1);
    }
    try {
      if (args.length > 1) {
        url = "jdbc:gemfirexd://" + args[1];
        System.out.println("url=" + url);
      }
      final Connection conn = getConnection();
      
      final DatabaseMetaData meta = conn.getMetaData();
      final String[] types = { "TABLE" };
      
      String schemaName = "SEC_OWNER";
      //select tablename from sys.systables where tableschemaname like 'SEC_OWNER' and partitionattrs is not null
      String tableQuery = args[0];
      
      ResultSet rs;
      if (tableQuery == null) {
        rs = meta.getTables(null, schemaName, tableQuery, types);
      }
      else {
        rs = conn.createStatement().executeQuery(
            tableQuery);
      }
      while (rs.next()) {
        long startTime = System.currentTimeMillis();
        String tableName = rs.getString("TABLE_NAME");
        int existingRowCount = -1;
        ResultSet tableHaveRows = conn.createStatement().executeQuery(
            "select count(1) from " + schemaName + "." + tableName
                + " having count(1) > 0");
        if (tableHaveRows.next()) {
          if (tableQuery == null) {
            continue;
          }
          else {
            existingRowCount = tableHaveRows.getInt(1);
          }
        }
        
        ResultSet keyList = meta.getPrimaryKeys(null, schemaName, tableName);
        HashSet<String> importedKeys = null;
        HashSet<String> primaryKey = null;
        while (keyList.next()) {
          if (primaryKey == null) {
            primaryKey = new HashSet<String>();
          }
          primaryKey.add(keyList.getString("COLUMN_NAME"));
        }

        ResultSet importList = meta.getImportedKeys(null, schemaName, tableName);
        while(importList.next()) {
          if (importedKeys == null) {
            importedKeys = new HashSet<String>();
          }
          importedKeys.add(importList.getString("PKCOLUMN_NAME") + " => " + importList.getString("FKTABLE_NAME") + "." + importList.getString("FKCOLUMN_NAME"));
        }

//        System.out.println("Processing Table: " + schemaName + "." + tableName
//            + " with primary key: " + primaryKey + (importedKeys != null && importedKeys.size() > 0 ? " with foreign key mapping: " + importedKeys : "") + "  " + existingRowCount );
//        FileWriter fstream = new FileWriter(tableName + ".dat");
//        BufferedWriter out = new BufferedWriter(fstream);
        ResultSet rsColumns = meta.getColumns(null, schemaName, tableName, null);
        ArrayList<Object[]> columnsMeta = new ArrayList<Object[]>();
        while (rsColumns.next()) {
          Object[] columnMeta = new Object[4];
          columnMeta[0] = rsColumns.getString("COLUMN_NAME");
          columnMeta[1] = rsColumns.getInt("DATA_TYPE");
          columnMeta[2] = rsColumns.getInt("COLUMN_SIZE");
          columnMeta[3] = rsColumns.getInt("DECIMAL_DIGITS");
          columnsMeta.add(columnMeta);
        }
        
        if (existingRowCount > 0) {
          String fullTableName = schemaName + "." + tableName;
          System.out.println("Printing column mapping for out of " + existingRowCount + " sample rows");
          Map<String, ArrayList<String>> valueMapping = new LinkedHashMap<String, ArrayList<String>>();
          ResultSet sampleData = conn.createStatement().executeQuery("select * from " +  fullTableName);
          while(sampleData.next()) {
            for(Object[] columnMeta : columnsMeta) {
              String colName = (String)columnMeta[0];
              String qColName = fullTableName + '.' + colName;
              ArrayList<String> valuelist = valueMapping.get(qColName);
              if (valuelist == null) {
                valuelist = new ArrayList<String>();
              }
              valuelist.add(sampleData.getString(colName));
              valueMapping.put(qColName, valuelist);
            }
          } // end of sample data traversal.
          
          for (Map.Entry<String, ArrayList<String>> entry : valueMapping.entrySet()) {
             System.out.println(entry.getKey() + " = " + entry.getValue());
          }
        }
      }

      final char sep = getFieldSeparator();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(2);
    }
  }

  protected char getFieldSeparator() {
    return ',';
  }

  protected String getFormatterTimestamp() {
    return "yyyy-MM-dd HH:mm:ss";
  }

  protected String getFormatterDate() {
    return "yyyy-MM-dd";
  }

  protected String getBaseTime() {
    return "2012-01-01 01:01:00";
  }

  static final char[] chooseChars =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();

  static final char[] chooseBytes =
      "0123456789abcdef".toCharArray();

  protected String generateString(int len) {
    // first randomly choose the size of string to be generated
    if (len > 32000) {
      len = 32000;
    }
    len = (int)(Math.random() * 9 * len / 10) + 1 + (len / 10);
    final char[] chars = new char[len];
    for (int i = 0; i < len; i++) {
      chars[i] = chooseChars[(int)(Math.random() * chooseChars.length)];
    }
    return new String(chars);
  }

  protected String generateBytes(int len) {
    // first randomly choose the size of string to be generated
    if (len > 32000) {
      len = 32000;
    }
    len = (int)(Math.random() * 9 * len / 10) + 1 + (len / 10);
    len <<= 1;
    final char[] chars = new char[len];
    for (int i = 0; i < len; i++) {
      chars[i] = chooseBytes[(int)(Math.random() * chooseBytes.length)];
    }
    return new String(chars);
  }

  protected String generateDate(Date baseDate) {

    final SimpleDateFormat formatterTimestamp = new SimpleDateFormat(
        getFormatterTimestamp());
    try {
      baseDate = formatterTimestamp.parse(getBaseTime());
    } catch (ParseException e) {
      e.printStackTrace();
      System.exit(2);
    }

    double addOn = Math.random() * 86400000 * 100;

    long newTime = baseDate.getTime() + (long)addOn;
    Date newDate = new Date(newTime);

    return formatterTimestamp.format(newDate);
  }

  protected String generateTimestamp(Date baseDate) {

    final SimpleDateFormat formatterTimestamp = new SimpleDateFormat(
        getFormatterTimestamp());
    try {
      baseDate = formatterTimestamp.parse(getBaseTime());
    } catch (ParseException e) {
      e.printStackTrace();
      System.exit(2);
    }

    double addOn = Math.random() * 86400000 * 100;

    long newTime = baseDate.getTime() + (long)addOn;
    Date newDate = new Date(newTime);

    return formatterTimestamp.format(newDate);
  }

  static final char[] chooseInts = new char[] { '0', '1', '2', '3', '4', '5',
      '6', '7', '8', '9' };

  protected String generateNumeric(int len, int prec) {
    int major = len - prec;
    // first randomly choose the size of string to be generated
    if (major > 1) {
      if (major > 15) {
        major = 15;
      }
      major = (int)(Math.random() * major) + 1;
    }
    if (prec > 1) {
      prec = (int)(Math.random() * prec) + 1;
    }
    len = major + prec;
    char[] chars = new char[len + 1];
    int i;
    for (i = 0; i < major; i++) {
      chars[i] = chooseInts[(int)(Math.random() * chooseInts.length)];
    }
    if (i < len) {
      chars[i] = '.';
      while (++i <= len) {
        chars[i] = chooseInts[(int)(Math.random() * chooseInts.length)];
      }
    }
    return new String(chars, 0, i);
  }

  public Connection getConnection() {
    Connection con = null;
    try {
      Class.forName(driver);
    } catch (java.lang.ClassNotFoundException e) {
      System.err.print("ClassNotFoundException: ");
      System.err.println(e.getMessage());
      System.exit(3);
    }

    try {
      java.util.Properties p = new java.util.Properties();
      p.setProperty("user", "locatoradmin");
      p.setProperty("password", "locatorpassword");
      con = DriverManager.getConnection(url, p);
    } catch (SQLException ex) {
      System.err.println("SQLException: " + ex.getMessage());
      System.exit(3);
    }

    return con;
  }

  public static void main(String[] args) {
    new GenerateMapper(args);
  }
}

