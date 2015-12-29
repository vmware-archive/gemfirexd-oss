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
package com.pivotal.gemfirexd.dataawareprocedure.listAgg;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.logging.Logger;

import com.pivotal.gemfirexd.procedure.OutgoingResultSet;
import com.pivotal.gemfirexd.procedure.ProcedureExecutionContext;

public class ListAggProcedure {

  static Logger logger = Logger.getLogger("com.pivotal.gemfirexd");

  final static String BAD_COL_ERROR =
    "Unexpected Groupby or ListAgg column definition";
  final static String NO_DELIMITER =
    "Invalid delimiter specified for Groupby or ListAgg column names";
  public static final String DUPLICATE_LISTAGG =
    "ListAgg column cannot also be a GroupBy column or duplicated in list";

  public static String version = ".95";

  /**
   * This method is the entry point for the stored procedure. Through the
   * parameters passed near emulation of the Oracle ListAgg capabilities can be
   * provided.
   * 
   * ASSUMPTIONS: 1.) All parameters present 2.) No aggregation functions
   * 
   * 
   * TEST: 1.) Bad parameters 2.) Use of ORDER BY in the query rather than GROUP
   * BY. Is it possible to simulate aggregation by dropping the other records.
   * 
   * @param groupBy
   * @param listAggCols
   * @param tableName
   * @param whereClause
   * @param whereParams
   * @param delimiter
   * @param dynamicRS
   * @param c
   * @throws SQLException
   */
  public static void ListAgg(String groupBy, String listAggCols,
      String tableName, String whereClause, String whereParams,
      String delimiter, ResultSet[] dynamicRS, ProcedureExecutionContext c)
      throws SQLException {

    System.out.println("LISTAGG: Invoking ListAgg version:" + version);
    if (delimiter != null && delimiter.length() > 0) {

      Connection cxn = c.getConnection();
      ArrayList<ColumnDef> cols = processInputParameters(listAggCols, groupBy,
          delimiter);

      if (whereClause != null) {
        whereClause = whereClause.trim();
      }
      if (whereParams != null) {
        whereParams = whereParams.trim();
      }

      String queryString = buildQueryString(cols, tableName, whereClause);

      Statement stmt;
      ResultSet rs;
      // set parameters in prepared statement if required, else execute
      // as unprepared statement to avoid thrashing the prepared statement cache
      boolean hasWhereParams = whereParams != null && whereParams.length() > 0;
      if (hasWhereParams || (whereClause == null || whereClause.length() == 0)) {
        PreparedStatement pstmt = cxn.prepareStatement(queryString);
        if (hasWhereParams) {
          // set the parameters
          StringTokenizer tokenizer = new StringTokenizer(whereParams, ",");
          int paramIndex = 1;
          while (tokenizer.hasMoreTokens()) {
            String param = tokenizer.nextToken();
            // setString works for all types other than BINARY/BLOB/UDT
            pstmt.setString(paramIndex++, param);
          }
        }
        rs = pstmt.executeQuery();
        stmt = pstmt;
      }
      else {
        stmt = cxn.createStatement();
        //System.out.println("LISTAGG: Executing: " + queryString);
        rs = stmt.executeQuery(queryString);
      }

      OutgoingResultSet outRS = createOutgoingResultSet(c, cols);
      // first write the meta row
      @SuppressWarnings({ "unchecked", "rawtypes" })
      final ArrayList<Object> colsRow = (ArrayList)cols;
      outRS.addRow(colsRow);
      processListAggInResultSet(cols, rs, outRS);

      rs.close();
      stmt.close();
    }
    else {
      throw new SQLException(NO_DELIMITER);
    }
  }

  /**
   * Builds a query for use in a prepared/unprepared statement
   * 
   * @param groupBy
   *          - Fields to order by
   * @param listAggCols
   *          - Fields to be aggregated
   * @param tableName
   *          - Table to query...note: multiple tables not supported
   * @param whereClause
   *          - where clause for query
   * 
   * @return the select SQL query
   */
  public static String buildQueryString(ArrayList<ColumnDef> cdList,
      String tableName, String whereClause) {
    boolean firstRow = true;
    StringBuilder queryString = new StringBuilder();

    queryString.append("<local> SELECT ");
    for (ColumnDef cd : cdList) {
      if (!firstRow) {
        queryString.append(',');
      }
      else {
        firstRow = false;
      }
      queryString.append(cd.columnName);
    }

    queryString.append(" FROM ").append(tableName);
    if (whereClause != null && whereClause.length() > 0) {
      queryString.append(" WHERE ").append(whereClause);
    }
    queryString.append(" ORDER BY ");

    for (ColumnDef cd : cdList) {
      if (cd.getOrderBy(queryString)) {
        queryString.append(',');
      }
    }
    assert queryString.charAt(queryString.length() - 1) == ',';
    queryString.setLength(queryString.length() - 1);

    //System.out.println("LISTAGG: " + queryString.toString());
    return queryString.toString();
  }

  /**
   * Loop through the result set inspecting each of the specified cols
   * definitions. For group by columns, just take the first value and place it
   * in the outgoing row. For the each ListAgg(col) found, create a temporary
   * stringBuilder object to hold the delimited string in progress. Once the
   * group def changes, then the method writes it to the outgoing row.
   * 
   * Assumptions: 1.) ResultSet is valid 2.) Okay to take the first value found
   * for a group by row 3.) Outgoing result set if valid 4.) No other
   * aggregation functions in column list
   * 
   * @param cols
   * @param rs
   * @param outRS
   */
  public static void processListAggInResultSet(ArrayList<ColumnDef> cols,
      ResultSet rs, OutgoingResultSet outRS) {

    final int numCols = cols.size();
    int numGroupByCols = 0;
    for (ColumnDef col : cols) {
      if (col.isGroupByCol()) {
        numGroupByCols++;
        continue;
      }
      break;
    }

    try {

      final Object[] groupingKey = new Object[numGroupByCols];
      ArrayList<Object> listAggValues = new ArrayList<Object>(numCols);

GETNEXT:
      while (rs.next()) {
        // compare previous grouping with current
        if (listAggValues.size() > 0) {
          int i;
          for (i = 0; i < numGroupByCols; i++) {
            final Object v1 = rs.getObject(i + 1);
            final Object v2 = listAggValues.get(i);
            groupingKey[i] = v1;
            if ((v1 == null && v2 != null) || !v1.equals(v2)) {
              // new group, flush old one
              outRS.addRow(listAggValues);
              listAggValues = new ArrayList<Object>(numCols);
              int j;
              for (j = 0; j <= i; j++) {
                listAggValues.add(groupingKey[j]);
              }
              // for aggregated columns add as a list of values
              for (j++; j <= numCols; j++) {
                ArrayList<Object> agg = new ArrayList<Object>();
                agg.add(rs.getObject(j));
                listAggValues.add(agg);
              }
              continue GETNEXT;
            }
          }
          // at this point the new value has to be aggregated with previous
          for (; i < numCols; i++) {
            // for merging just add to the list of aggregated values
            @SuppressWarnings("unchecked")
            ArrayList<Object> agg = (ArrayList<Object>)listAggValues.get(i);
            agg.add(rs.getObject(i + 1));
          }
        }
        else {
          int i;
          for (i = 1; i <= numGroupByCols; i++) {
            listAggValues.add(rs.getObject(i));
          }
          // for aggregated columns add as a list of values
          for (; i <= numCols; i++) {
            ArrayList<Object> agg = new ArrayList<Object>();
            agg.add(rs.getObject(i));
            listAggValues.add(agg);
          }
        }
      }
      // System.out.println("LISTAGG: Write last row: " + i);
      // displayResultRow(listAggValues);
      if (listAggValues.size() > 0) {
        outRS.addRow(listAggValues);
      }
      outRS.endResults();
    } catch (SQLException e) {
      // catch and what to do about them
      System.err.println("LISTAGG: Gemfirexd exception:" + e.getMessage());
      e.printStackTrace();
    }
  }

  static void displayResultRow(ArrayList<Object> col,
      ArrayList<ColumnDef> colsMeta) {

    System.out.println("LISTAGG: ------------------------------------");
    System.out.println("LISTAGG: Result Row");
    System.out.println("LISTAGG: Group By Columns:");

    int i = 0;
    for (Object o : col) {
      ColumnDef meta = colsMeta.get(i);
      if (meta.aggType == ColumnDef.GROUPBY) {
        System.out.print("Group By:\t");
      }
      else if (meta.aggType == ColumnDef.LISTAGG) {
        System.out.print("List Agg:\t");
      }
      else if (meta.aggType == ColumnDef.GROUPBY_AND_LISTAGG) {
        System.out.print("Group By and List Agg:\t");
      }
      System.out.println("LISTAGG: ----->" + o.toString());
      i++;
    }
    System.out.println("LISTAGG: ------------------------------------");
  }

  /**
   * Create the OutgoingResultSet object and then setup the output columns based
   * on the select field list passed to the procedure.
   * 
   * @param cols
   * @return
   */
  public static OutgoingResultSet createOutgoingResultSet(
      ProcedureExecutionContext c, ArrayList<ColumnDef> cols) {
    OutgoingResultSet outRS = c.getOutgoingResultSet(1);
    outRS.setBatchSize(5000);

    for (ColumnDef col : cols) {
      outRS.addColumn(col.columnName);
    }
    return outRS;
  }

  /**
   * Creates a ColumnDef Arraylist of columns to process. Some basic rules: -
   * NULL indicates an error during processing - If either parameters are null,
   * then return null. - Any non-ListAgg() column must be present in the groupBy
   * string. Otherwise there is an error.
   * 
   * Error indicated by 0 size return set. CAUTION: A bad field delimiter will
   * not be detected here.
   * 
   * @param outCols
   * @param groupBy
   * @return
   * @throws SQLException
   */
  public static ArrayList<ColumnDef> processInputParameters(String listAgg,
      String groupBy, String delimiter) throws SQLException {

    ArrayList<ColumnDef> columnList = new ArrayList<ColumnDef>();
    if (listAgg != null && groupBy != null && listAgg.length() > 0
        && groupBy.length() > 0) {

      StringTokenizer tokenizer = new StringTokenizer(groupBy, ",");
      int columnNumber = 1;
      while (tokenizer.hasMoreTokens()) {
        columnList.add(new ColumnDef(tokenizer.nextToken().trim(),
            columnNumber++));
      }
      tokenizer = new StringTokenizer(listAgg, ",");
      boolean hasListAgg = false;
      while (tokenizer.hasMoreTokens()) {
        ColumnDef cd = new ColumnDef(tokenizer.nextToken().trim(),
            columnNumber++, delimiter);
        int index = columnList.indexOf(cd);
        if (index != -1) {
          cd = columnList.get(index);
          cd.delimiter = delimiter;
          cd.setListAggCol();
        }
        else {
          columnList.add(cd);
        }
        hasListAgg = true;
      }
      if (!hasListAgg) {
        columnList.clear();
      }
      /*
      System.out.println("---------------\nColumnDefinitions");
      for (ColumnDef cv : columnList) {
        System.out.println(cv.toString());
      }
      System.out.println("--------------");
      */
    }
    else {
      throw new SQLException(BAD_COL_ERROR);
    }

    return columnList;
  }
}
