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
package sql.mbeans;

import hydra.Log;
import hydra.RemoteTestModule;
import hydra.blackboard.SharedLock;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.ObjectName;

import sql.SQLBB;
import sql.SQLHelper;
import sql.SQLTest;
import sql.mbeans.MBeanTest.OutputType;
import util.TestException;

import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.management.internal.MBeanJMXAdapter;
import com.pivotal.gemfirexd.internal.engine.store.GemFireStore;

public class MBeanHelper extends SQLTest {

  public Number runQueryAndPrintValue(String sql) {

    Log.getLogWriter().info("Trying to Execute Query : " + sql);
    Connection conn = null;
    try {
      conn = getGFEConnection();
      Statement ps = conn.createStatement();
      ResultSet rs = ps.executeQuery(sql);

      while (rs.next()) {
        for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
          Log.getLogWriter().info(
              "Param " + rs.getMetaData().getColumnName(i) + " : "
                  + rs.getObject(i));
        }
      }

    } catch (SQLException e) {
      Log.getLogWriter().error(e);
    } finally {
      if (conn != null) {
        closeGFEConnection(conn);
      }
    }

    return 0;
  }

  public Object runQueryAndGetValue(String sql, OutputType outputType) {

    Log.getLogWriter().info(
        "Trying to Execute Query : " + sql + ", Expecting output to be in "
            + outputType);
    Connection conn = null;
    try {
      conn = getGFEConnection();
      Statement ps = conn.createStatement();
      ResultSet rs = ps.executeQuery(sql);
      if (rs.next()) {
        if (outputType.isLong()) {
          return rs.getLong(1);
        } else if (outputType.isInt()) {
          return rs.getInt(1);
        } else if (outputType.isFloat()) {
          return rs.getFloat(1);
        } else if (outputType.isString()) {
          return rs.getString(1);
        } else {
          Object value = rs.getObject(1);
          Log.getLogWriter().info(
              rs.getMetaData().getColumnName(1) + " type is "
                  + value.getClass() + ", value : " + value);
        }
      }
    } catch (SQLException e) {
      Log.getLogWriter().error("Error while executing query : " + sql, e);
    } finally {
      if (conn != null) {
        closeGFEConnection(conn);
      }
    }

    return 0;
  }

  public static void matchValues(AttributeList list, Map<String, Object> expectedMap) {
    for (Entry<String, Object> entry : expectedMap.entrySet()) {
      boolean found = false;
      for (int i = 0; i < list.size(); i++) {
        Attribute attr = (Attribute) list.get(i);
        if (attr.getName().equals(entry.getKey())) {
          found = true;
          Log.getLogWriter().info(
              entry.getKey() + "--> JMX : " + attr.getValue() + " BB : "
                  + entry.getValue());
          if (!attr.getValue().equals(entry.getValue())) {
            printAndSaveError("Attribute", null, attr, entry.getValue());
          }
        }
      }
      if (!found)
        throw new TestException("Error attribute " + entry.getKey()
            + " not found");
    }
  }

  public static void ensureNonZero(AttributeList list, String[] expectedAttrs) {
    for (String entry : expectedAttrs) {
      boolean found = false;
      for (int i = 0; i < list.size(); i++) {
        Attribute attr = (Attribute) list.get(i);
        if (attr.getName().equals(entry)) {
          found = true;
          Log.getLogWriter().info(entry + "--> JMX : " + attr.getValue());
          Object value = attr.getValue();
          boolean isZero = true;
          if (value == null)
            isZero = true;
          else if (value instanceof Number) {
            if (value instanceof Integer) {
              Integer intVal = (Integer) value;
              if (intVal.intValue() == 0)
                isZero = true;
              else
                isZero = false;
            }
            if (value instanceof Long) {
              Long intVal = (Long) value;
              if (intVal.longValue() == 0l)
                isZero = true;
              else
                isZero = false;
            } else if (value instanceof Float) {
              Float falotVal = (Float) value;
              if (falotVal.floatValue() == 0.0f)
                isZero = true;
              else
                isZero = false;
            } else if (value instanceof Double) {
              Double doubleVal = (Double) value;
              if (doubleVal.doubleValue() == 0.0d)
                isZero = true;
              else
                isZero = false;
            }
            if (isZero) {
              saveError("Attribute " + attr.getName()
                  + " is expected non-Zero " + " actual value "
                  + attr.getValue() + " class " + attr.getValue().getClass());
            }
          } else {
            saveError("Attribute " + attr.getName() + " is expected number "
                + " actual value " + attr.getValue() + " class "
                + attr.getValue().getClass());
          }
        }
      }
      if (!found)
        throw new TestException("Error attribute " + entry + " not found");
    }
  }

  public static void printAndSaveError(String prefix, String tableName,
      Attribute attribute, Object expected) {
    saveError(prefix + " " + attribute.getName() + " did not match "
        + (tableName != null ? (" for " + tableName) : "")
        + ", Expected Value :  " + expected + " and Actual Value  : "
        + attribute.getValue());
  }

  public static void printActualAndExpectedForAttribute(String messageSuffix,
      Attribute attribute, Number expected) {
    Log.getLogWriter().info(
        messageSuffix + " " + attribute.getName() + ", " + "Expected Value :  "
            + expected + " and Actual Value  : " + attribute.getValue());
  }

  public static void saveError(String error) {
    String clientName = RemoteTestModule.getMyClientName();
    int pid = RemoteTestModule.MyPid;
    String threadName = Thread.currentThread().getName();

    StringBuilder newError = new StringBuilder("from client");
    newError.append(" ").append(clientName).append(" PID=").append(pid)
        .append(" on thread ").append(threadName).append(" : ").append(error);
    Log.getLogWriter().error("saveError - Start newError=" + newError);
    SharedLock lock = SQLBB.getBB().getSharedLock();
    try {
      lock.lock();
      ArrayList<String> errorList = (ArrayList<String>) SQLBB.getBB().getSharedMap().get("DATA_ERROR");
      if (errorList == null) {
        errorList = new ArrayList<String>();
      }
      // Build the line number trace route for the error message
      // StringBuilder execLineNbrs = new StringBuilder();
      // StackTraceElement[] stackTrace =
      // Thread.currentThread().getStackTrace();
      // //for (int i = 3;i < stackTrace.length;i++) {
      // for (int i = stackTrace.length - 1;i >= 0;i--) {
      // // execLineNbrs.append(stackTrace[i].getLineNumber());
      // }
      // // newError.append(execLineNbrs.toString());
      newError.append("\n");
      errorList.add(newError.toString());
      SQLBB.getBB().getSharedMap().put("DATA_ERROR", errorList);
    } finally {
      lock.unlock();
    }

  }

  public void compareValues(AttributeList list,
      Map<String, Number> expectedMap, boolean considerExpectedCanBeGreater) {
    for (Entry<String, Number> entry : expectedMap.entrySet()) {
      boolean found = false;
      for (int i = 0; i < list.size(); i++) {
        Attribute attr = (Attribute) list.get(i);
        if (attr.getName().equals(entry.getKey())) {
          found = true;
          Log.getLogWriter().info(
              entry.getKey() + "--> JMX : " + attr.getValue() + " BB : "
                  + entry.getValue());
          if (considerExpectedCanBeGreater ? (((Number) attr.getValue())
              .longValue() > entry.getValue().longValue()) : (!attr.getValue()
              .toString().equals(entry.getValue().toString()))) {
            printAndSaveError("Attribute", null, attr, entry.getValue());
          }
        }
      }
      if (!found)
        throw new TestException("Error attribute " + entry.getKey()
            + " not found");
    }
  }

  public Object getTotalRegions() {
    Connection conn = getGFEConnection();
    // printOpenConnection("getTotalRegions");
    ResultSet rs = null;
    int max = 0;
    try {
      rs = conn.createStatement().executeQuery("select count(TABLENAME) from sys.SYSTABLES where TABLETYPE='T'");
      while (rs.next()) {
        max = rs.getInt(1);
      }
      rs.close();
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
      throw new TestException("Failed", se);
    }
    return max;
  }

  public Object getTotalTxCommits() {
    SharedLock lock = SQLBB.getBB().getSharedLock();
    int count = 0;
    try {
      lock.lock();
      for (Object e : SQLBB.getBB().getSharedMap().getMap().entrySet()) {
        @SuppressWarnings("rawtypes")
        Map.Entry entry = (Map.Entry) e;
        String key = (String) entry.getKey();
        if (key.startsWith("COMMITS_")) {
          Integer cInt = (Integer) SQLBB.getBB().getSharedMap().get(key);
          if (cInt != null)
            count += cInt.intValue();
        }
      }
      return count;
    } finally {
      lock.unlock();
    }
  }

  public static String quoteIfNeeded(String stringToCheck) {
    return (stringToCheck != null && (stringToCheck.indexOf('"') != -1)) ? ObjectName.quote(stringToCheck) : stringToCheck;
  }

  public static String makeCompliantName(String value) {
    return MBeanJMXAdapter.makeCompliantName(value);
  }

  public Object getTotalTxRollbacks() {
    SharedLock lock = SQLBB.getBB().getSharedLock();
    int count = 0;
    try {
      lock.lock();
      for (Object e : SQLBB.getBB().getSharedMap().getMap().entrySet()) {
        @SuppressWarnings("rawtypes")
        Map.Entry entry = (Map.Entry) e;
        String key = (String) entry.getKey();
        if (key.startsWith("ROLLBACKS_")) {
          Integer cInt = (Integer) SQLBB.getBB().getSharedMap().get(key);
          if (cInt != null)
            count += cInt.intValue();
        }
      }
      return count;
    } finally {
      lock.unlock();
    }
  }

  public static boolean isLocator() {
    return GemFireStore.getBootedInstance().getMyVMKind().isLocator();
  }
  
  public static void printConnectionStats(Statistics statistics) {
    StatisticDescriptor[] descrs = statistics.getType().getStatistics();
    for(StatisticDescriptor descr : descrs){
      Log.getLogWriter().info(descr.getName() + " = "+statistics.get(descr));
    }
  }
}
