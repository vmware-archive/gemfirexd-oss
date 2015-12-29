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
package gfxdperf.tpch;

import gfxdperf.PerfTestException;
import hydra.Log;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * Encapsulates a TPC-H query.
 */
public abstract class Query {

  protected boolean logDML = false;
  protected boolean logDMLResults = false;
  protected PreparedStatement preparedQueryStmt = null;
  protected PreparedStatement preparedQueryPlanStmt = null;
  protected Connection connection = null;
  protected Random rng = null;
  protected String queryName = null;
  protected String query = null;
  
  // Parameter values to be randomized and specified by the TPC-H specification
  protected static final String[] REGION = {"AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"};
  protected static final String[] SEGMENT = {"AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD"};
  protected static final String[] TYPE = {"TIN", "NICKEL", "BRASS", "STEEL", "COPPER"};
  protected static final String[] NATION = {"ALGERIA", "ARGENTINA", "BRAZIL", "CANADA", "EGYPT", "ETHIOPIA", "FRANCE", 
                                            "GERMANY", "INDIA", "INDONESIA", "IRAN", "IRAQ", "JAPAN", "JORDAN", "KENYA", 
                                            "MOROCCO", "MOZAMBIQUE", "PERU", "CHINA", "ROMANIA", "SAUDI ARABIA", "VIETNAM", 
                                            "RUSSIA", "UNITED KINGDOM", "UNITED STATES"};
  protected static final String[] SYLLABLE1 = {"STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"};
  protected static final String[] SYLLABLE2 = {"ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED"};
  protected static final String[] SYLLABLE3 = {"TIN", "NICKEL", "BRASS", "STEEL", "COPPER"};

  protected static Map<String, String> nationToRegionMap;

  // Region column is a char(25), it must be padded out to that length for Oracle to properly set the
  // parameter on the prepared statement. GFXD can handle it either way.
  protected static final int REGION_COL_LENGTH = 25;
  protected static final String REGION_FORMAT_STR = "%-" + REGION_COL_LENGTH + "s";

  // Segment column is a char(10), it must be padded out to that length for Oracle to properly set the
  // parameter on the prepared statement. GFXD can handle it either way.
  protected static final int SEGMENT_COL_LENGTH = 10;
  protected static final String SEGMENT_FORMAT_STR = "%-" + SEGMENT_COL_LENGTH + "s";
  
  static {
    nationToRegionMap = new HashMap<String, String>();
    nationToRegionMap.put("ALGERIA", "AFRICA");
    nationToRegionMap.put("ARGENTINA", "AMERICA");
    nationToRegionMap.put("BRAZIL", "AMERICA");
    nationToRegionMap.put("CANADA", "AMERICA");
    nationToRegionMap.put("EGYPT", "MIDDLE EAST");
    nationToRegionMap.put("ETHIOPIA", "AFRICA");
    nationToRegionMap.put("FRANCE", "EUROPE");
    nationToRegionMap.put("GERMANY", "EUROPE");
    nationToRegionMap.put("INDIA", "ASIA");
    nationToRegionMap.put("INDONESIA", "ASIA");
    nationToRegionMap.put("IRAN", "MIDDLE EAST");
    nationToRegionMap.put("IRAQ", "MIDDLE EAST");
    nationToRegionMap.put("JAPAN", "ASIA");
    nationToRegionMap.put("JORDAN", "MIDDLE EAST");
    nationToRegionMap.put("KENYA", "AFRICA");
    nationToRegionMap.put("MOROCCO", "AFRICA");
    nationToRegionMap.put("MOZAMBIQUE", "AFRICA");
    nationToRegionMap.put("PERU", "AMERICA");
    nationToRegionMap.put("CHINA", "ASIA");
    nationToRegionMap.put("ROMANIA", "EUROPE");
    nationToRegionMap.put("SAUDI ARABIA", "MIDDLE EAST");
    nationToRegionMap.put("VIETNAM", "ASIA");
    nationToRegionMap.put("RUSSIA", "EUROPE");
    nationToRegionMap.put("UNITED KINGDOM", "EUROPE");
    nationToRegionMap.put("UNITED STATES", "AMERICA");
  }

  public Query() {
    this.logDML = TPCHPrms.logDML();
    this.logDMLResults = TPCHPrms.logDMLResults();
  }
  
  //====================================================================================
  // Abstract method declarations
  
  /**
   * Validates the query with specific parameters for which TPC-H provides an
   * expected answer. This can only be used with a scaling factor of 1G.
   * 
   * @return A String describing the query failure.
   * @throws SQLException Thrown if an exception occurs during validation.
   */
  public abstract String validateQuery() throws SQLException;

  /**
   * Executes the query with certain parameter(s) randomized according to the
   * TPC-H specification. If successful, returns the number of rows found.
   */
  public abstract int executeQuery() throws SQLException;
  
  /**
   * Executes the query to obtain a query plan, with certain parameter(s) randomized according to the
   * TPC-H specification. 
   */
  public abstract void executeQueryPlan() throws SQLException;

  /** Create and return an instance of ResultRow specific to this TPC-H query
   */
  public abstract ResultRow createResultRow();
  
  //====================================================================================
  // Method implementations
  
  /** Validate the results of this TPC-H query with the given result set
   * 
   * @param rs The result set obtained from executing the query.
   * @return A String describing the query failure.
   * @throws SQLException Thrown if an exception occurs during validation.
   */
  protected String validateQuery(ResultSet rs) throws SQLException {
    return validateQuery(rs, false, true);
  }
  
  /** Validate the results of this TPC-H query with the given result set
   * 
   * @param rs The result set obtained from executing the query.
   * @param limitRowCount If true, then only consider the first N rows of rs, where N
   *                      is the number of rows in the answer file for the query. If false
   *                      do not limit the rows. Some queries (#2, #3, #10, #18 and #21)
   *                      limit the query results to the first N rows. The TPC-H spec allows
   *                      for the implementation of TPC-H to use a loop to control the number
   *                      of rows returned.
   * @param checkResultOrder If true, then check that the query results are ordered like
   *                         in the answer file, otherwise treat the results as an unordered set.
   * @return A String describing the query failure.
   * @throws SQLException Thrown if an exception occurs during validation.
   *                         
   */
  protected String validateQuery(ResultSet rs, boolean limitRowCount, boolean checkResultOrder) throws SQLException {
    // get the expected results from the answer file
    String answerFile = hydra.EnvHelper.expandPath("$JTESTS/gfxdperf/tpch/answers/" + queryName.toLowerCase() + ".out");
    List<String> answerLines = new ArrayList<String>();
    try {
      Log.getLogWriter().info("Reading query answers from " + answerFile);
      answerLines = hydra.FileUtil.getTextAsList(answerFile);
      answerLines.remove(0); // toss the header line
    } catch (FileNotFoundException e) {
      String s = "Unable to find answer file: " + answerFile;
      throw new PerfTestException(s);
    } catch (IOException e) {
      String s = "Unable to read answer file: " + answerFile;
      throw new PerfTestException(s);
    }
    int numExpectedRows = answerLines.size();
    StringBuilder sb = new StringBuilder();
    sb.append("Answer lines\n");
    for (String line: answerLines) {
      sb.append("Answer line:" + line + "\n");
    }
    Log.getLogWriter().info(sb.toString());

    // get the query results
    List<String> qResults = new ArrayList<String>();
    int rowCount = 0;
    while (rs.next()) {
      rowCount++;
      ResultRow result = createResultRow();
      result.init(rs);
      String answerLineFormat = result.toAnswerLineFormat();
      qResults.add(answerLineFormat);
      if (logDMLResults) {
        Log.getLogWriter().info("Result row " + rowCount + ": " + result + "\n" + "   Answer line format:" + answerLineFormat);
      }
      if (limitRowCount && (rowCount == numExpectedRows)) {
        if (rs.next()) { // see if there is another row; if so log that we stopped iterating rows early
          Log.getLogWriter().info("Stopped reading rows at row count " + rowCount);
        }
        break;
      }
    }
    rs.close();
    rs = null;

    // check the number of results against expected
    StringBuilder errStr = new StringBuilder();
    Log.getLogWriter().info("Expected number of rows is " + answerLines.size() + ", number of rows in result set is " + qResults.size());
    if (qResults.size() != answerLines.size()) {
      String s = "For " + queryName + " expected the query result size to be " + answerLines.size() + " but it is " + qResults.size() + "\n";
      errStr.append(s);
    }

    // check for missing or extra results
    Set<String> extraResults = new HashSet();
    extraResults.addAll(qResults);
    extraResults.removeAll(answerLines);
    Set<String> missingResults = new HashSet();
    missingResults.addAll(answerLines);
    missingResults.removeAll(qResults);
    if (extraResults.size() != 0) {
      sb = new StringBuilder();
      for (String result: extraResults) {
        sb.append("Extra:" + result + "\n");
      }
      errStr.append("For " + queryName + " found " + extraResults.size() + " extra rows in query result:\n" + sb + "\n");
    }
    if (missingResults.size() != 0) {
      sb = new StringBuilder();
      for (String result: missingResults) {
        sb.append("Missing:" + result + "\n");
      }
      errStr.append("For " + queryName + " found " + missingResults.size() + " missing rows in query result:\n" + sb + "\n");
    }
    
    // check the order of the results
    if (checkResultOrder) {
      Log.getLogWriter().info("Checking order of rows");
      for (int i = 0; i < Math.min(answerLines.size(), qResults.size()); i++) {
        String answerLineRow = answerLines.get(i);
        String resultRow = qResults.get(i);
        if (!resultRow.equals(answerLineRow)) {
          int rowNumber = i+1;
          errStr.append("Rows are not in the expected order starting at row " + rowNumber + ", " +
                 " answerLine " + rowNumber + ": " + answerLineRow + ", " +
                 " resultRow " + rowNumber + ": " + resultRow + "\n");
          break;
        }
      }
    }

    if (errStr.length() > 0) {
      Log.getLogWriter().info(errStr.toString());
      String additionalLogStr = getKnownBugNumber(qResults, answerLines, missingResults, extraResults);
      return additionalLogStr + "Validation for " + queryName + " failed, see log for details\n";
    } else {
      Log.getLogWriter().info("Query " + queryName + " was successfully validated");
      return "";
    }
  }

  /** For queries that return known bugs, this method can be overridden in subclasses to return
   *  aString that cites a particular bug number. 
   * @param qResults The results of the query executed.
   * @param answerLines The expected results of the query executed.
   * @param missingResults The rows that are missing from qResults.
   * @param extraResults The rows that are extra in qResults.
   * @return A String citing any known bug number.
   */
  protected String getKnownBugNumber(List<String> qResults,
      List<String> answerLines, Set<String> missingResults,
      Set<String> extraResults) {
    return "";
  }

  /** Read the given result set and return the number of rows. Throws an exception
   *  if the result set is empty.
   * 
   * @param rs The results of query execution.
   * @return The number of rows in the ResultSet.
   * @throws SQLException Thrown if an exception is encountered while reading the rows.
   */
  protected int readResultSet(ResultSet rs) throws SQLException {
    int resultSize = 0;
    ResultRow result = createResultRow();
    while (rs.next()) {
      result.init(rs);
      if (logDMLResults) {
        Log.getLogWriter().info("Result:" + result);
      }
      ++resultSize;
    }
    if (!TPCHPrms.allowEmptyResultSet() && resultSize == 0) {
      String s = "Unexpected empty result set for " + queryName;
      throw new PerfTestException(s);
    }
    rs.close();
    rs = null;
    if (logDML) {
      Log.getLogWriter().info("Completed execution of " + queryName + ", result set size is " + resultSize);
    }
    return resultSize;
  }
  
  /** Log the query plan in the given ResultSet.
   * @param rs The ResultSet containing the results of a query plan
   * @throws SQLException
   */
  protected void logQueryPlan(ResultSet rs) throws SQLException {
    StringBuilder sb = new StringBuilder();
    while (rs.next()) {
      Clob clob = rs.getClob(1);
      sb.append(clob.getSubString(1, (int) clob.length()) + "\n");
    }
    Log.getLogWriter().info("Query plan for " + queryName + ":\n" + sb.toString());
    rs.close();
  }

}
