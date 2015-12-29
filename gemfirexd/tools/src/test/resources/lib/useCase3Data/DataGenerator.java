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
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
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

public class DataGenerator {

  protected String url = "jdbc:gemfirexd://localhost:1530/";

  protected String driver = "com.pivotal.gemfirexd.jdbc.ClientDriver";

  int rowCount = 100000;

  public DataGenerator(String[] args) {
    if (args.length == 0) {
      System.out.println("Usage <script> <tablenames> "
          + "[<host:port> <rowcount> <column mapping file>]");
      System.exit(1);
    }
    try {
      if (args.length > 1) {
        url = "jdbc:gemfirexd://" + args[1];
      }
      HashMap<String, Object> columnNameMapping = null;
      HashSet<String> columnsMapped = null;
      HashMap<String, ArrayList<String>> columnValueMapping = null;
      if (args.length > 3) {
        // setup the column mapping
        columnNameMapping = new HashMap<String, Object>();
        columnsMapped = new HashSet<String>();
        columnValueMapping = new HashMap<String, ArrayList<String>>();
        FileReader freader = new FileReader(args[3]);
        BufferedReader in = new BufferedReader(freader);
        String line;
        while ((line = in.readLine()) != null) {
          line = line.trim();
          if (line.length() > 0) {
            String[] mapping = line.split("=");
            String col = mapping[0].trim();
            String mappedColumnS = mapping[1].trim();
            final Object mappedColumn;
            if (mappedColumnS.indexOf(',') >= 0) {
              mappedColumn = mappedColumnS.split(",");
              System.out.println("Registering value map for column " + col
                  + ": " + Arrays.toString((String[])mappedColumn));
            }
            else {
              mappedColumn = mappedColumnS;
              System.out.println("Registering mapping for column " + col
                  + " to " + mappedColumnS);
            }
            if (columnNameMapping.put(col, mappedColumn) != null) {
              System.err.println("Multiple mappings for column " + mapping[0]);
              System.exit(2);
            }
            if (mappedColumn == mappedColumnS) {
              columnsMapped.add(mappedColumnS);
            }
          }
        }
        in.close();
      }
      final Connection conn = getConnection();
      final DatabaseMetaData meta = conn.getMetaData();
      final String[] types = { "TABLE" };
      final ArrayList<String> prevTables = new ArrayList<String>();
      for (String fullTableName : args[0].split(",")) {
        ResultSet rsColumns = null;
        fullTableName = fullTableName.trim();
        String schemaName = null;
        String tableName = fullTableName;
        int schemaIndex;
        if ((schemaIndex = fullTableName.indexOf('.')) >= 0) {
          schemaName = fullTableName.substring(0, schemaIndex);
          tableName = fullTableName.substring(schemaIndex + 1);
        }

        if (args.length > 2) {
          rowCount = Integer.parseInt(args[2]);
        }

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

          final char sep = getFieldSeparator();
          for (int i = 0; i < rowCount; ++i) {
            StringBuilder sb = new StringBuilder();
            int j = 0;
            for (Object[] columnMeta : columnsMeta) {
              String colName = (String)columnMeta[0];
              String qColName = fullTableName + '.' + colName;
              String value = null;
              // check for any column mapping
              if (columnNameMapping != null) {
                Object mappedColumn;
                if ((mappedColumn = columnNameMapping.get(qColName)) != null) {
                  if (mappedColumn.getClass() == String.class) {
                    value = columnValueMapping.get(mappedColumn).get(i);
                    if (i == 0) {
                      System.out.println("Mapping column " + qColName
                          + " to " + mappedColumn);
                    }
                  }
                  else {
                    String[] mappedValues = (String[])mappedColumn;
                    value = mappedValues[i % mappedValues.length];
                    if (i == 0) {
                      System.out.println("Mapping column " + qColName
                          + " to values: " + Arrays.toString(mappedValues));
                    }
                  }
                }
              }
              if (value == null) {
                int type = (Integer)columnMeta[1];
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
                  default:
                    throw new RuntimeException("cannot handle column of type " + type);
                }
              }
              // if this is a mapped column then store its values
              if (columnsMapped != null && columnsMapped.contains(qColName)) {
                final ArrayList<String> columnValues;
                if (i == 0) {
                  columnValues = new ArrayList<String>();
                  System.out.println("Registering values for mapped column "
                      + qColName);
                  columnValueMapping.put(qColName, columnValues);
                }
                else {
                  columnValues = columnValueMapping.get(qColName);
                }
                columnValues.add(value);
              }

              sb.append(value).append(sep);
              ++j;
            }
            sb.setCharAt(sb.length() - 1, '\n');
            out.write(sb.toString());
          }
          out.close();
          System.out.println("    generation time elapsed: "
              + (System.currentTimeMillis() - startTime));
        }
        prevTables.add(fullTableName);
      }
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

  protected String generateString(int len) {
    // first randomly choose the size of string to be generated
    len = (int)(Math.random() * len) + 1;
    final char[] chars = new char[len];
    for (int i = 0; i < len; i++) {
      chars[i] = chooseChars[(int)(Math.random() * chooseChars.length)];
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
      con = DriverManager.getConnection(url);
    } catch (SQLException ex) {
      System.err.println("SQLException: " + ex.getMessage());
      System.exit(3);
    }

    return con;
  }

  public static void main(String[] args) {
    new DataGenerator(args);
  }
}
