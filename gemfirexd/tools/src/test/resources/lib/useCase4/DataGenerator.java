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
import java.util.Random;

public class DataGenerator {

  protected String url = "jdbc:gemfirexd://localhost:1530/";

  protected String driver = "com.pivotal.gemfirexd.jdbc.ClientDriver";

  int[] rowCount = new int[] {100000};
  
  enum RelationalOperator {
    Equal,
    GreaterThan,
    LessThan;
  };

  static class ColumnRange {
    final Object start;
    final Object increment;
    final int minNumber;
    final int maxNumber;
    final String foreachColumn;
    final boolean isDecimal;
    String[] valueList;
    boolean hasDependentForeach;
    Object currentValue;
    int currentIndex;
    final String foreignKeyColumn;
    int fkValueListIndex = 0;
    int currentLimit;
    final Random rand = new Random();

    private ColumnRange(Object start, Object increment, int minNumber,
        int maxNumber, String foreachColumn, boolean isDecimal,
        String[] valueList) {
      this.start = start;
      this.increment = increment;
      this.minNumber = minNumber;
      this.maxNumber = maxNumber;
      this.foreachColumn = foreachColumn;
      this.isDecimal = isDecimal;
      this.valueList = valueList;
      this.foreignKeyColumn = null;
      this.currentIndex = -1;
      this.currentLimit = -1;
    }
    
    private ColumnRange(Object start, Object increment, int minNumber,
        int maxNumber, String foreignKeyColumn) {
      this.start = start;
      this.increment = increment;
      this.minNumber = minNumber;
      this.maxNumber = maxNumber;
      this.foreachColumn = null;
      this.isDecimal = false;
      // will be later populated, once FK column is done.
      this.valueList = null;
      this.foreignKeyColumn = foreignKeyColumn;
      this.currentIndex = -1;
      this.currentLimit = -1;
    }
    

    public String getNext(int row, HashMap<String, Object> columnNameMapping, 
        HashMap<String, ArrayList<String>> columnValueMapping) {
      
      if (this.foreignKeyColumn != null && this.valueList == null) {
        this.valueList = columnValueMapping.get(this.foreignKeyColumn).toArray(
            new String[0]);
        System.out.println("Initialized foreignKeyColumn " + this.foreignKeyColumn + " with " + (this.valueList != null ? this.valueList.length : 0) + " values");
      }
      
      if (!this.hasDependentForeach) {
        ColumnRange range = this;
        // increment the parents iteratively, if any, on rollover
        while (range.getNext()) {
          if (range.foreachColumn != null) {
            range = (ColumnRange)columnNameMapping.get(range.foreachColumn);
          }
          else {
            break;
          }
        }
      }
      else {
        // parent will always be incremented by the child rollover
        if (this.currentValue == null) {
          getNext();
        }
      }
      return this.currentValue.toString();
    }
    
    private boolean getNext() {
      boolean lastReached = false;
      if (this.valueList != null && this.minNumber == -1 && this.maxNumber == -1) {
          this.currentIndex++;
          this.currentValue = this.valueList[this.currentIndex];
          if (this.currentIndex >= (this.valueList.length - 1)) {
            lastReached = true;
            this.currentIndex = 0;
          }
      }
      else {
        this.currentIndex++;
        if (this.currentValue == null || (this.currentLimit > 0 &&
              this.currentIndex > this.currentLimit)) {
          if (this.minNumber == this.maxNumber) {
            this.currentLimit = this.minNumber;
          }
          else {
            this.currentLimit = rand.nextInt(
                this.maxNumber - this.minNumber) + this.minNumber;
          }
          this.currentValue = this.start;
          this.currentIndex = 0;
          if(foreignKeyColumn != null) {
            this.fkValueListIndex++;
            if (fkValueListIndex >= this.valueList.length) {
              this.fkValueListIndex = 0;
            }
            this.currentValue = this.valueList[fkValueListIndex];
          }
          lastReached = true;
        }
        else {
          if(this.foreignKeyColumn != null && this.valueList != null) {
            currentValue = this.valueList[this.fkValueListIndex];
          }
          else {
            if (this.isDecimal) {
              currentValue = ((BigDecimal)currentValue).add((BigDecimal)increment);
            }
            else {
              currentValue = (Integer)currentValue + (Integer)increment;
            }
          }
        }
      }
      return lastReached;
    }

    public String toString() {
      StringBuilder sb = new StringBuilder();
      if (this.valueList != null) {
        sb.append("value list: ").append(Arrays.toString(this.valueList));
      }
      else {
        sb.append("start=").append(start).append(";increment=").append(increment);
        if (this.minNumber != -1 || this.maxNumber != -1) {
          sb.append(";number=").append(this.minNumber);
          if (this.minNumber != this.maxNumber) {
            sb.append('-').append(this.maxNumber);
          }
        }
        if (this.foreachColumn != null) {
          sb.append(";foreach=").append(this.foreachColumn);
        }
      }
      sb.append(";hasDependentForeach=").append(this.hasDependentForeach);
      return sb.toString();
    }

    static Object parse(String col, String line, Connection conn) throws SQLException {
      if (line.indexOf(',') > 0 || line.indexOf('[') > 0) {
        if (line.startsWith("::") && !line.startsWith("::valuelist")) {
          System.out.println("Registering range for column " + col
              + ": " + line);
          String[] elems = line.substring(2).split(",");
          String start = null;
          String increment = null;
          int minNumber = -1;
          int maxNumber = -1;
          String foreachColumn = null;
          String number;
          for (String elem : elems) {
            elem = elem.trim();
            int spaceIndex = elem.indexOf(' ');
            String name = elem.substring(0, spaceIndex).trim();
            if ("start".equalsIgnoreCase(name)) {
              start = elem.substring(spaceIndex + 1).trim();
            }
            else if ("increment".equalsIgnoreCase(name)) {
              increment = elem.substring(spaceIndex + 1).trim();
            }
            else if ("number".equalsIgnoreCase(name)) {
              number = elem.substring(spaceIndex + 1);
              int rangeIndex = number.indexOf('-');
              if (rangeIndex > 0) {
                minNumber = Integer.parseInt(number.substring(0, rangeIndex)
                    .trim());
                maxNumber = Integer.parseInt(number.substring(rangeIndex + 1)
                    .trim());
              }
              else {
                minNumber = maxNumber = Integer.parseInt(number.trim());
              }
            }
            else if ("foreach".equalsIgnoreCase(name)) {
              foreachColumn = elem.substring(spaceIndex + 1).trim();
            }
            else {
              throw new IllegalArgumentException("unknown token '" + name + "'");
            }
          }
          if (start == null || increment == null) {
            throw new IllegalArgumentException("either start or increment not specified in range");
          }
          if (start.indexOf('.') >= 0 || increment.indexOf('.') >= 0) {
            return new ColumnRange(new BigDecimal(start), new BigDecimal(increment),
                minNumber, maxNumber, foreachColumn, true, null);
          }
          else {
            return new ColumnRange(Integer.parseInt(start), Integer.parseInt(increment),
                minNumber, maxNumber, foreachColumn, false, null);
          }
        }
        else {
          
          if(!line.startsWith("::valuelist")) {
            System.out.println("Registering value map for column " + col
                + ": " + line);
            return new ColumnRange(null, null, -1, -1, null, false,
                line.split(","));
          }

          String[] elems = line.substring(2).split(",");
          if(elems.length > 1) {
            
            int minNumber = -1;
            int maxNumber = -1;
            String foreignKeyColumn = null;
            String number;
            for (String elem : elems) {
              elem = elem.trim();
              int spaceIndex = elem.indexOf(' ');
              String name = elem.substring(0, spaceIndex).trim();
              if ("valuelist".equalsIgnoreCase(name)) {
               foreignKeyColumn = elem.substring(spaceIndex + 1).trim();
              }
              else if ("number".equalsIgnoreCase(name)) {
                number = elem.substring(spaceIndex + 1);
                int rangeIndex = number.indexOf('-');
                if (rangeIndex > 0) {
                  minNumber = Integer.parseInt(number.substring(0, rangeIndex)
                      .trim());
                  maxNumber = Integer.parseInt(number.substring(rangeIndex + 1)
                      .trim());
                }
                else {
                  minNumber = maxNumber = Integer.parseInt(number.trim());
                }
              }
            }
            
            if( foreignKeyColumn == null || minNumber == -1 || maxNumber == -1) {
              throw new IllegalArgumentException("either foreignKey or cardinality (number) not mentioned.");
            }
            
            System.out.println("Registering foreign key valuelist for column " + col 
                + " foreignKeyColumn=" + foreignKeyColumn
                + ": " + line);
            return new ColumnRange(null, null, minNumber, maxNumber, foreignKeyColumn);
          }
          else {
            String valueListQuery = line.substring(line.indexOf('[')+1, line.indexOf(']'));
            ResultSet r = conn.createStatement().executeQuery(valueListQuery);
            ArrayList<String> values = new ArrayList<String>();
            while(r.next()) {
              values.add(r.getString(1));
            }
            
            System.out.println("Registering dynamic value map for column " + col
                + ": " + line + " with " + values.size() + " elements ");
            return new ColumnRange(null, null, -1, -1, null, false,
                values.toArray(new String[0]));
          }
        }
      }
      else {
        return null;
      }
    }
  }


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
      final Connection conn = getConnection();
      HashMap<String, Object> columnNameMapping = null;
      HashSet<String> columnsMapped = null;
      HashMap<String, RelationalOperator> colMapRelation = null;
      HashMap<String, ArrayList<String>> columnValueMapping = null;
      if (args.length > 3) {
        // setup the column mapping
        columnNameMapping = new HashMap<String, Object>();
        columnsMapped = new HashSet<String>();
        colMapRelation = new HashMap<String, RelationalOperator>();
        columnValueMapping = new HashMap<String, ArrayList<String>>();
        FileReader freader = new FileReader(args[3]);
        BufferedReader in = new BufferedReader(freader);
        String line;
        while ((line = in.readLine()) != null) {
          if(line.startsWith("#")) {
            continue;
          }
          RelationalOperator op = RelationalOperator.Equal;
          line = line.trim();
          if (line.length() > 0) {
            String[] mapping = line.split("=");
            if(mapping.length == 1) {
              mapping = line.split("<");
              assert mapping.length != 0;
              if(mapping.length > 1) {
                op = RelationalOperator.LessThan;
              }
            }
            String col = mapping[0].trim();
            String mappedColumnS = mapping[1].trim();
            Object mappedColumn;
            if ((mappedColumn = ColumnRange.parse(col, mappedColumnS, conn)) == null) {
              mappedColumn = mappedColumnS;
              System.out
                  .println("Registering mapping for column " + col + " to "
                      + mappedColumnS + " with " + op.name() + " relation");
            }
            else {
              String parentCol = ((ColumnRange)mappedColumn).foreachColumn;
              if (parentCol != null) {
                Object parentColRange = columnNameMapping.get(parentCol);
                if (parentColRange == null) {
                  parentColRange = new ColumnRange(null, null,
                        -1, -1, null, false, null);
                  ((ColumnRange)parentColRange).hasDependentForeach = true;
                  columnNameMapping.put(parentCol, parentColRange);
                }
                else if (parentColRange instanceof ColumnRange) {
                  ((ColumnRange)parentColRange).hasDependentForeach = true;
                }
                else {
                  throw new IllegalArgumentException("no value specifier for parent '"
                      + parentCol + "' found for child '" + col + "'");
                }
              }
            }
            if (columnNameMapping.put(col, mappedColumn) != null) {
              System.err.println("Multiple mappings for column " + col);
              System.exit(2);
            }
            
            if (mappedColumn == mappedColumnS) {
              columnsMapped.add(mappedColumnS);
              colMapRelation.put(mappedColumnS, op);
            }
          }
        }
        in.close();
      }
      final DatabaseMetaData meta = conn.getMetaData();
      final String[] types = { "TABLE" };
      final ArrayList<String> prevTables = new ArrayList<String>();
      int tableIndex = -1;
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
          if(args[2].indexOf(',') > 0) {
            String[] multipleRowCount = args[2].split(",");
            rowCount = new int[multipleRowCount.length];
            for(int i = 0; i < multipleRowCount.length; i++) {
              rowCount[i] = Integer.parseInt(multipleRowCount[i]);
            }
            tableIndex++;
          }
          else {
            rowCount[0] = Integer.parseInt(args[2]);
          }
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
              + " with primary key: " + primaryKey + " for " + rowCount[tableIndex] + " rows");
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
          String prevVal = null;
          for (int i = 0; i < rowCount[tableIndex]; ++i) {
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
                  Class<?> colClass = mappedColumn.getClass();
                  if (colClass == String.class) {
                    ArrayList<String> columnValues = columnValueMapping.get(mappedColumn);
                    value = columnValues.get(i % columnValues.size());
                    if (i == 0) {
                      System.out.println("Mapping column " + qColName
                          + " to " + mappedColumn);
                    }
                  }
                  else if (colClass == String[].class) {
                    String[] mappedValues = (String[])mappedColumn;
                    value = mappedValues[i % mappedValues.length];
                    if (i == 0) {
                      System.out.println("Mapping column " + qColName
                          + " to values: " + Arrays.toString(mappedValues));
                    }
                  }
                  else {
                    value = ((ColumnRange)mappedColumn).getNext(i,
                        columnNameMapping, columnValueMapping);
                    if (i == 0) {
                      System.out.println("Mapping column " + qColName
                          + " to: " + mappedColumn);
                    }
                  }
                }
              }
              if (value == null) {
                int type = (Integer)columnMeta[1];
                switch (type) {
                  case java.sql.Types.CHAR:
                  case java.sql.Types.VARCHAR:
                  case java.sql.Types.CLOB: {
                    int cLen = (Integer)columnMeta[2];
                    value = generateString(cLen);
                    break;
                  }
                  case java.sql.Types.BIGINT:
                  case java.sql.Types.INTEGER:
                  case java.sql.Types.SMALLINT: {
                    if (primaryKey == null || !primaryKey.contains(colName)) {
                      value = String.valueOf((int)(Math.random() * rowCount[tableIndex]));
                    }
                    else {
                      value = String.valueOf(i + 1);
                    }
                    break;
                  }
                  case java.sql.Types.DOUBLE: {
                    value = String.valueOf(Math.random() * rowCount[tableIndex]);
                    break;
                  }
                  case java.sql.Types.FLOAT: {
                    value = Float.toString((float)(Math.random() * rowCount[tableIndex]));
                    break;
                  }
                  case java.sql.Types.DATE:
                    value = generateDate(null);
                    break;
                  case java.sql.Types.TIMESTAMP:
                    value = generateTimestamp(null);
                    break;
                  case java.sql.Types.DECIMAL:
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
                  case java.sql.Types.BLOB: {
                    int cLen = (Integer)columnMeta[2];
                    value = generateBytes(cLen);
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
                
                String storeValue = value;
                
                // adjust the value before storing
                RelationalOperator op = colMapRelation != null ? colMapRelation.get(qColName) : null;
                if (op != null && op != RelationalOperator.Equal ) {
                  if (op == RelationalOperator.LessThan) {
                    if (i == 0) {
                      System.out.println("Reducing value for mapped column "
                          + qColName);
                    }
                    int type = (Integer)columnMeta[1];
                    switch (type) {
                      case java.sql.Types.BIGINT:
                      case java.sql.Types.INTEGER:
                      case java.sql.Types.SMALLINT: {
                        long lv = Long.parseLong(value);
                        lv -= (lv *0.05); // reduce by 20%
                        storeValue = Long.toString(lv);
                        break;
                      }
                      case java.sql.Types.DOUBLE: {
                        double dv = Double.parseDouble(value);
                        dv -= (dv * 0.05); // reduce by 20%
                        storeValue = Double.toString(dv);
                        break;
                      }
                      case java.sql.Types.FLOAT: {
                        float fv = Float.parseFloat(value);
                        fv -= (fv * 0.05); // reduce by 20%
                        storeValue = Float.toString(fv);
                        break;
                      }
                      case java.sql.Types.DECIMAL:
                      case java.sql.Types.NUMERIC: {
                        BigDecimal bv = new BigDecimal(value.toCharArray());
                        bv = bv.movePointLeft(1);
                        storeValue = bv.toPlainString();
                        break;
                      }
                      default:
                        throw new RuntimeException("cannot handle column of type " + type);
                    }
                    
                  }
                }

                columnValues.add(storeValue);
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
