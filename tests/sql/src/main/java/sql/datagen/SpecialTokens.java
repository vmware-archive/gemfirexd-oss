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
package sql.datagen;

import hydra.Log;
import hydra.MethExecutor;
import hydra.MethExecutorResult;

import java.io.BufferedReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import com.gemstone.gemfire.LogWriter;

/**
 * 
 * @author Rahul Diyewar
 */

public enum SpecialTokens {
  foreignkey {
    // APP.CHART.PRTN_ID = ::foreignkey APP.HISTORY.PRTN_ID repeat 5
    // APP.CHART.PRTN_ID = ::foreignkey APP.HISTORY.PRTN_ID repeat 5-10
    @Override
    public MappedColumnInfo parseToken(String col, String line, Connection conn)
        throws Exception {
      int minNumber = -1;
      int maxNumber = -1;
      String foreignKeyColumn = null;
      String number;
      String[] elems = line.split(" ");
      if (elems.length != 4) {
        throw new IllegalArgumentException(
            "incorrect defination for '::foreignkey' token for " + col
                + ", value=" + line);
      }

      foreignKeyColumn = elems[1].toUpperCase().trim();
      number = elems[3];
      int rangeIndex = number.indexOf('-');
      if (rangeIndex > 0) {
        minNumber = Integer.parseInt(number.substring(0, rangeIndex).trim());
        maxNumber = Integer.parseInt(number.substring(rangeIndex + 1).trim());
      } else {
        minNumber = maxNumber = Integer.parseInt(number.trim());
      }

      if (foreignKeyColumn == null || minNumber == -1 || maxNumber == -1) {
        throw new IllegalArgumentException(
            "either foreignKey or cardinality (number) not mentioned for "
                + col);
      }

      FKMappedColumn m = new FKMappedColumn(col);
      m.setFKRelationship(foreignKeyColumn, minNumber, maxNumber);

      log.info("Registering foreign key maping for column " + col + ": " + m);
      return m;
    }

  },
  valuelist {
    public MappedColumnInfo parseToken(String col, String line, Connection conn)
        throws Exception {
      if (line.indexOf("bashexecute") > 0) {
        // String valueListCmd = "bash -c \"" +
        // line.substring(line.indexOf('[') + 1, line.indexOf(']')) + "\"";
        String valueListCmd = line.substring(line.indexOf('[') + 1,
            line.indexOf(']'));
        ArrayList<String> cmd = new ArrayList<String>(4);
        cmd.add("bash");
        cmd.add("--norc");
        cmd.add("-c");
        // cmd.add("echo {A..Z}{A..Z},|sed 's/ //g'");
        cmd.add(valueListCmd);
        log.info("executing:" + cmd);
        ProcessBuilder procBuilder = new ProcessBuilder(cmd);

        Process pr = procBuilder.start();
        BufferedReader buf = new BufferedReader(new java.io.InputStreamReader(
            pr.getInputStream()));

        ArrayList<String> outputLines = new ArrayList<String>();
        String outline = null; // buf.readLine();
        while ((outline = buf.readLine()) != null) {
          outputLines.add(outline);
        }

        buf = new BufferedReader(new java.io.InputStreamReader(
            pr.getErrorStream()));
        while ((outline = buf.readLine()) != null) {
          log.error(outline);
        }

        pr.waitFor();

        String[] values = outputLines.get(0).split(",");

        if (outputLines.size() > 1) {
          log.error("More than one line output is getting IGNORED for now ....");
        }

        boolean randomize = false;
        if (line.endsWith("randomize")) {
          randomize = true;
        }

        // + Arrays.toString(valueStr));
        MappedColumnInfo m = new ValueListMappedColumn(col, values, randomize);

        log.info("Registering values for column " + col + ": " + m);
        return m;
      } else if (line.indexOf("javaexecute") > 0) {
        // ::valuelist [javaexecute Classname.methodName()] randomize

        String valueListStr = line.substring(line.indexOf('[') + 1,
            line.indexOf(']'));
        String[] elems = valueListStr.split(" ");
        if (elems.length != 2) {
          throw new IllegalArgumentException("incorrect defination for " + col
              + " value=" + line);
        }

        String e = elems[1].trim();
        String className = e.substring(0, e.lastIndexOf("."));
        String methodName = e.substring(e.lastIndexOf(".") + 1, e.indexOf("("));
        MethExecutorResult result = MethExecutor.executeInstance(className,
            methodName);
        if (result.exceptionOccurred()) {
          throw new Exception("occured invoking " + className + "."
              + methodName, result.getException());
        }
        List vList = (List) result.getResult();
        Object[] values = vList.toArray(new Object[0]);

        boolean randomize = false;
        if (line.endsWith("randomize")) {
          randomize = true;
        }

        MappedColumnInfo m = new ValueListMappedColumn(col, values, randomize);
        log.info("Registering values for column " + col + ": " + m);
        return m;

      } else if (line.indexOf("{") > 0) {
        // ::valuelist {val1, val2, val3} randomize
        String valueList = line.substring(line.indexOf('{') + 1,
            line.indexOf('}'));
        String[] values = valueList.split(",");
        for (int i = 0; i < values.length; i++) {
          values[i] = values[i].trim();
        }

        boolean randomize = false;
        if (line.endsWith("randomize")) {
          randomize = true;
        }

        MappedColumnInfo m = new ValueListMappedColumn(col, values, randomize);
        log.info("Registering values for column " + col + ": " + m);
        return m;

      } else {
        // ::valuelist [select col1 from table1] randomize
        String valueListQuery = line.substring(line.indexOf('[') + 1,
            line.indexOf(']'));
        ResultSet r = conn.createStatement().executeQuery(valueListQuery);
        ArrayList<String> values = new ArrayList<String>();
        while (r.next()) {
          values.add(r.getString(1));
        }

        boolean randomize = false;
        if (line.endsWith("randomize")) {
          randomize = true;
        }

        MappedColumnInfo m = new ValueListMappedColumn(col,
            values.toArray(new String[0]), randomize);
        log.info("Registering values for column " + col + ": " + m);
        return m;
      }
    }
  },
  random {
    @Override
    public MappedColumnInfo parseToken(String col, String line, Connection conn) {
      int beginIndex = line.indexOf("[");
      if (beginIndex == -1) {
        throw new IllegalArgumentException("Incomplete randome specifier: "
            + line);
      }
      int endIndex = line.indexOf("]");
      if (endIndex == -1) {
        throw new IllegalArgumentException("Incomplete randome specifier: "
            + line);
      }

      String valueSpec = line.substring(beginIndex + 1, endIndex);

      if (valueSpec.indexOf("-") <= 0) {

        // only fixed length is mentioned.
        MappedColumnInfo m = new MappedColumnInfo(col);
        m.setRange(Integer.parseInt(valueSpec), -1, -1);
        log.info("Registering random values for column " + col + ": " + m);
        return m;
      } else {
        String[] valueLengths = valueSpec.split("-");
        int begin = Integer.parseInt(valueLengths[0]), end = Integer
            .parseInt(valueLengths[1]);
        // a range but fixed length.
        MappedColumnInfo m = new MappedColumnInfo(col);
        m.setRange(begin, end, -1);
        log.info("Registering random values for column " + col + ": " + m);
        return m;
      }
    }
  },
  format {
    @Override
    public MappedColumnInfo parseToken(String col, String line, Connection conn)
        throws Exception {
      int beginLength = -1, endLength = -1;
      int prec = -1;
      final String fSpec = line.substring(name().length()).trim();
      int dot = fSpec.indexOf(".");
      if (dot == -1) {
        // ::format 9999
        beginLength = fSpec.length();
      } else {
        // ::format 9999.99
        beginLength = fSpec.length();
        prec = fSpec.length() - dot - 1;
      }

      MappedColumnInfo m = new MappedColumnInfo(col);
      m.setRange(beginLength, endLength, prec);
      log.info("Registering fixed format values for column " + col + ": " + m);
      return m;
    }
  },
  UUID {
    @Override
    public MappedColumnInfo parseToken(String col, String line, Connection conn) {

      MappedColumnInfo m = new FixedTokenMappedColumn(col, this);
      log.info("Registering UUID value for column " + col + ": " + m);
      return m;
    }

    @Override
    public String getNext() {
      return java.util.UUID.randomUUID().toString();
    }
  },
  LMTS {
    @Override
    public MappedColumnInfo parseToken(String col, String line, Connection conn) {
      log.info("Registering LMTS value for column " + col + ": " + line);
      return new MappedColumnInfo(col);
    }
  },
  LCTS {
    @Override
    public MappedColumnInfo parseToken(String col, String line, Connection conn) {
      log.info("Registering LCTS value for column " + col + ": " + line);
      return new MappedColumnInfo(col);
    }
  },
  UDT {
    // ::UDT <udt-classname>.<methodname>()
    String classname = null;
    String methodname = null;

    @Override
    public MappedColumnInfo parseToken(String col, String line, Connection conn) {
      String[] elem = line.split(" ");
      for (String e : elem) {
        e = e.trim();
        if (!e.equalsIgnoreCase("UDT") && !e.equalsIgnoreCase("")) {
          this.classname = e.substring(0, e.lastIndexOf("."));
          this.methodname = e.substring(e.lastIndexOf(".") + 1, e.indexOf("("));
        }
      }
      MappedColumnInfo m = new FixedTokenMappedColumn(col, this);
      log.info("Registering UDIT value for column " + col + ": " + m);
      return m;
    }

    @Override
    public String getNext() {
      MethExecutorResult result = MethExecutor.executeInstance(classname,
          methodname);
      if (result.getStackTrace() != null)
        throw new RuntimeException(result.toString());
      return result.getResult().toString();
    }

    @Override
    public String toString() {
      return "UDT " + classname + "." + methodname + "()";
    }
  },
  nullvalue {
    @Override
    public boolean lineStartsWith(String line) {
      return line.startsWith(Mapper.nullToken);
    }

    @Override
    public MappedColumnInfo parseToken(String col, String line, Connection conn)
        throws Exception {
      log.info("Registering NULL value for column " + col + ": " + line);
      return new FixedTokenMappedColumn(col, this);
    }

    public String getNext() {
      return Mapper.nullToken;
    }
  },

  skipvalue {
    @Override
    public boolean lineStartsWith(String line) {
      return line.startsWith(Mapper.skipToken);
    }

    @Override
    public MappedColumnInfo parseToken(String col, String line, Connection conn)
        throws Exception {
      System.out.println("NOT Registering, rather skipping value for column "
          + col + ": " + line);
      return new FixedTokenMappedColumn(col, this);
    }

    public String getNext() {
      return Mapper.skipToken;
    }
  };

  public boolean lineStartsWith(String line) {
    return line.startsWith(name());
  }

  public String getNext() {
    return null;
  }

  public abstract MappedColumnInfo parseToken(String col, String line,
      Connection conn) throws Exception;

  LogWriter log = Log.getLogWriter();
}
