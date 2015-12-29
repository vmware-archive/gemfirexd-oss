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
package cacheperf.comparisons.gemfirexd.poc;

import hydra.Log;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;

public class UseCase3Client extends POCClient {

  SimpleDateFormat formatterTimestamp = new SimpleDateFormat(
      "yyyyMMddHH:mm:ss");

  SimpleDateFormat formatterDate = new SimpleDateFormat("yyyyMMdd");

  public static void generateDataTask() throws IOException, SQLException {
    UseCase3Client client = new UseCase3Client();
    client.initialize();
    client.generateData(UseCase3Prms.getTableName(), UseCase3Prms.getRowCount(), client.connection);
  }

  private void generateData(String tableName, int rowCount, Connection conn)
  throws IOException, SQLException {
      ResultSet rsColumns = null;
      String schemaName = null;
      int schemaIndex;
      if ((schemaIndex = tableName.indexOf('.')) >= 0) {
        schemaName = tableName.substring(0, schemaIndex);
        tableName = tableName.substring(schemaIndex + 1);
      }

      DatabaseMetaData meta = conn.getMetaData();
      String[] types = { "TABLE" };
      ResultSet rs = meta.getTables(null, schemaName, tableName, types);
      while (rs.next()) {
        long startTime = System.currentTimeMillis();
        if (!tableName.equals(rs.getString("TABLE_NAME"))) {
          throw new RuntimeException("unexpected table name "
              + rs.getString("TABLE_NAME") + ", expected: " + tableName);
        }
        ResultSet keyList = meta.getPrimaryKeys(null, schemaName, tableName);
        HashSet<String> primaryKey = null;
        while (keyList.next()) {
          if (primaryKey == null) {
            primaryKey = new HashSet<String>();
          }
          primaryKey.add(keyList.getString("COLUMN_NAME"));
        }

        System.out.println("Processing Table:" + tableName
            + " with primary key: " + primaryKey);
        FileWriter fstream = new FileWriter(tableName + ".dat");
        BufferedWriter out = new BufferedWriter(fstream);
        rsColumns = meta.getColumns(null, schemaName, tableName, null);
        ArrayList<Object[]> columnsMeta = new ArrayList<Object[]>();
        while (rsColumns.next()) {
          Object[] columnMeta = new Object[4];
          columnMeta[0] = rsColumns.getString("COLUMN_NAME");
          columnMeta[1] = rsColumns.getInt("DATA_TYPE");
          columnMeta[2] = rsColumns.getInt("COLUMN_SIZE");
          columnMeta[3] = rsColumns.getInt("DECIMAL_DIGITS");
          columnsMeta.add(columnMeta);
        }

        for (int i = 0; i < rowCount; ++i) {
          if (i%100000 == 0) {
            Log.getLogWriter().info("Current row count: " + i);
          }
          StringBuilder sb = new StringBuilder();
          int j = 0;
          for (Object[] columnMeta : columnsMeta) {
            String colName = (String)columnMeta[0];
            int type = (Integer)columnMeta[1];
            String value = null;
            switch (type) {
              case java.sql.Types.CHAR:
              case java.sql.Types.VARCHAR: {
                int cLen = (Integer)columnMeta[2];
                value = generateString(cLen);
                break;
              }
              case java.sql.Types.BIGINT:
              case java.sql.Types.INTEGER:
              case java.sql.Types.SMALLINT: {
                if (primaryKey == null || !primaryKey.contains(colName)) {
                  value = String.valueOf((int)(Math.random() * rowCount));
                }
                else {
                  value = String.valueOf(i + 1);
                }
                break;
              }
              case java.sql.Types.DOUBLE: {
                value = String.valueOf(Math.random() * rowCount);
                break;
              }
              case java.sql.Types.FLOAT: {
                value = Float.toString((float)(Math.random() * rowCount));
                break;
              }
              case java.sql.Types.DATE:
                value = generateDate(null);
                break;
              case java.sql.Types.TIMESTAMP:
                value = generateTimestamp(null);
                break;
              case java.sql.Types.NUMERIC: {
                int cLen = (Integer)columnMeta[2];
                int prec = (Integer)columnMeta[3];
                if (primaryKey == null || !primaryKey.contains(colName)) {
                  value = generateNumeric(cLen, prec);
                }
                else {
                  value = String.valueOf(i + 1);
                }
                break;
              }
            }
            sb.append(value).append(",");
            ++j;
          }
          sb.setCharAt(sb.length() - 1, '\n');
          out.write(sb.toString());
        }
        out.close();
        System.out.println("    generation time elapsed: "
            + (System.currentTimeMillis() - startTime));
      }
  }

  static final char[] chooseChars =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();

  protected String generateString(int len) {
    final char[] chars = new char[len];
    for (int i = 0; i < len; i++) {
      chars[i] = chooseChars[(int)(Math.random() * chooseChars.length)];
    }
    return new String(chars);
  }

  protected String generateDate(Date baseDate) {

    try {
      baseDate = formatterTimestamp.parse("2012010101:01:00");
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

    try {
      baseDate = formatterTimestamp.parse("2012010101:01:00");
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
    char[] chars = new char[len + 1];
    int i;
    for (i = 0; i < major; i++) {
      chars[i] = chooseInts[(int)(Math.random() * chooseInts.length)];
    }
    if (i < len) {
      chars[i] = '.';
      while (++i < len) {
        chars[i] = chooseInts[(int)(Math.random() * chooseInts.length)];
      }
    }
    return new String(chars, 0, i);
  }

  public static void importDataTask() throws SQLException {
    UseCase3Client client = new UseCase3Client();
    client.initialize();
    if (client.ttgid == 0) {
      client.importData(UseCase3Prms.getTableName(), UseCase3Prms.getImportThreads(), client.connection);
    }
  }

  private void importData(String tableName, int importThreads, Connection conn)
  throws SQLException {
    String call = "call syscs_util.import_table_ex('APP', '" + tableName + "', './" + tableName + ".dat', ',', NULL, NULL, 0, 0, " + importThreads + ", 0, 'cacheperf.comparisons.gemfirexd.poc.ImportOra', NULL)";
    Log.getLogWriter().info("Importing data for table " + tableName + " using " + call);
    CallableStatement cs = conn.prepareCall(call);
    cs.execute();
    cs.close();
    Log.getLogWriter().info("Imported data for table " + tableName);
  }
}
