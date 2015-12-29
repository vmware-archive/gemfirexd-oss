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
package sql.poc.useCase2.oldListAgg;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.pivotal.gemfirexd.procedure.OutgoingResultSet;
import com.pivotal.gemfirexd.procedure.ProcedureExecutionContext;

public class ListAggProcedure {
	//static ProcedureExecutionContext context;
	final static Logger logger = Logger.getLogger("com.pivotal.gemfirexd");
	final static String BAD_COL_ERROR = "Unexpected Groupby or ListAgg column definition";
	final static String NO_DELIMITER = "No delimiter present";
	public static final String DUPLICATE_LISTAGG = "ListAgg column cannot also be a GroupBy column or duplicated in list";
	public static String version=".92x4";

	static {
	  logger.info("LISTAGG: Loading ListAgg version: " + version);
	}

	/**
	 * This method is the entry point for the stored procedure. Through the
	 * parameters passed near emulation of the Oracle ListAgg capabilities can
	 * be provided.
	 * 
	 * ASSUMPTIONS: 1.) All parameters present 2.) No aggregation functions
	 * 
	 * TODO: Allow for the passing of variables for more efficient queries.
	 * 
	 * 
	 * TEST: 1.) Bad parameters 2.) Use of ORDER BY in the query rather than
	 * GROUP BY. Is it possible to simulate aggregation by dropping the other
	 * records.
	 * 
	 * @param outCols
	 * @param tableName
	 * @param groupBy
	 * @param whereClause
	 * @param outResults
	 * @param context
	 * @throws SQLException
	 */
	public static void ListAgg(String groupBy,
			String listAggCols, String tableName, String whereClause,
			String delimiter, ResultSet[] outResults,
			ProcedureExecutionContext c) throws SQLException {

		//System.out.println("LISTAGG: Invoking ListAgg version:"+version);
		if (delimiter != null && delimiter.length() == 1) {
			
			Connection cxn = c.getConnection();
			ArrayList<ColumnDef> cols = processInputParameters(listAggCols,
					groupBy, delimiter);
			

			String queryString = buildQueryString(cols, tableName, whereClause);

			Statement stmt = cxn.createStatement();
			ResultSet rs = stmt.executeQuery(queryString);

			OutgoingResultSet outRS = createOutgoingResultSet(c, cols);

			if (outRS != null) {
				outRS = processListAggInResultSet(cols, rs, outRS);
			} else {
				throw new SQLException("Unable to create outgoing result set");
			}

			rs.close();
			stmt.close();
		} else {
			throw new SQLException(NO_DELIMITER);
		}
	}

	/**
	 * Builds a query for use in a prepared statement
	 * 
	 * @param groupBy
	 *            - Fields to order by
	 * @param listAggCols
	 *            - Fields to be aggregated
	 * @param tableName
	 *            - Table to query...note: multiple tables not supported
	 * @param whereClause
	 *            - where clause for query
	 * @return
	 */
	public static String buildQueryString(ArrayList<ColumnDef> cdList,
			String tableName, String whereClause) {
		StringBuilder result = new StringBuilder();
		boolean firstRow = true;
		boolean orderByFirst = true;
		StringBuilder selectCols = new StringBuilder();
		StringBuilder orderByClause = new StringBuilder();

		for (ColumnDef cd : cdList) {
			if (!firstRow) {
				selectCols.append(",").append(cd.columnName);
			} else {
				firstRow = false;
				selectCols.append(cd.columnName);
			}

			if (!orderByFirst) {
				orderByClause.append(",").append(cd.getOrderBy());
			} else {
				orderByFirst = false;
				orderByClause.append(cd.getOrderBy());
			}

		}

		String queryString = "<local> SELECT " + selectCols + " FROM "
				+ tableName;
		if (whereClause != null && whereClause.trim().length() > 0) {
			queryString += " WHERE " + whereClause;
		}
		queryString += " ORDER BY " + orderByClause;
		if (logger.isLoggable(Level.FINE)) {
		  logger.fine("LISTAGG: " + queryString);
		}
		return queryString;
	}

	/**
	 * Loop through the result set inspecting each of the specified cols
	 * definitions. For group by columns, just take the first value and place it
	 * in the outgoing row. For the each ListAgg(col) found, create a temporary
	 * stringBuilder object to hold the delimited string in progress. Once the
	 * group def changes, then the method writes it to the outgoing row.
	 * 
	 * Assumptions: 1.) ResultSet is valid 2.) Okay to take the first value
	 * found for a group by row 3.) Outgoing result set if valid 4.) No other
	 * aggregation functions in column list
	 * 
	 * @param cols
	 * @param rs
	 * @param outRS
	 * @return
	 */
	public static OutgoingResultSet processListAggInResultSet(
			ArrayList<ColumnDef> cols, ResultSet rs, OutgoingResultSet outRS) {

		try {

			String groupKey = null;
			String newKey = null;

			ArrayList<Object> listAggValues = ColumnValues
					.listAggValuesBuilderFactory(cols);

			int i = 0;
			while (rs.next()) {
				newKey = makeKey(rs, cols).trim();
				if (!newKey.equals(groupKey)) {
					if (groupKey != null) {
						//System.out.println("LISTAGG: Before addRow");
						outRS.addRow(listAggValues);

						listAggValues = ColumnValues
								.listAggValuesBuilderFactory(cols);

					}
					groupKey = newKey;
				}
				++i;
				processColumns(rs, listAggValues);
			}
			//System.out.println("LISTAGG: Before addRow");
			outRS.addRow(listAggValues);
			outRS.endResults();
		} catch (SQLException e) {
		  // catch and what to do about them
		  StringWriter sw = new StringWriter();
		  PrintWriter pw = new PrintWriter(sw);
		  e.printStackTrace(pw);
		  logger.severe("LISTAGG: GemFireXD exception: "
		      + e.getMessage() + ": " + sw.toString());
		}

		return outRS;
	}

	/**
	 * 
	 * @param cols
	 * @param rs
	 * @param tempValue
	 * @param listAggValues
	 * @throws SQLException
	 */
	public static void processColumns(ResultSet rs,
			ArrayList<Object> listAggValues) throws SQLException {

		for (Object o : listAggValues) {
			ColumnValues cv = (ColumnValues) o;
			if ((cv.aggType == ColumnDef.LISTAGG)
					|| (cv.aggType == ColumnDef.GROUPBY && cv.getValues()
							.size() == 0)) {
				cv.appendValue(rs.getObject(cv.colNumber));
			}
		}
	}

	private static void displayResultRow(ArrayList<Object> col) {

		System.out.println("LISTAGG: ------------------------------------");
		System.out.println("LISTAGG: Result Row");
		System.out.println("LISTAGG: Group By Columns:");

		for (Object o : col) {
			ColumnValues cv = (ColumnValues) o;
			if (cv.aggType == ColumnDef.GROUPBY) {
				System.out.print("Group By:\t");
			} else if (cv.aggType == ColumnDef.LISTAGG) {
				System.out.print("List Agg:\t");
			}
			System.out.println("LISTAGG: ----->" + cv.toString());
		}
		System.out.println("LISTAGG: ------------------------------------");
	}

	public static String makeKey(ResultSet rs, ArrayList<ColumnDef> cols) {
		StringBuilder newKey = new StringBuilder();
		try {
			for (ColumnDef col : cols) {
				if (!col.isListAggCol()) {
					newKey.append(rs.getString(col.getColumnName()));
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return newKey.toString();
	}

	/**
	 * Create the OutgoingResultSet object and then setup the output columns
	 * based on the select field list passed to the procedure.
	 * 
	 * TODO: Determine how to specify the column type
	 * 
	 * @param cols
	 * @return
	 */
	public static OutgoingResultSet createOutgoingResultSet(ProcedureExecutionContext c,
			ArrayList<ColumnDef> cols) {
		OutgoingResultSet outRS = c.getOutgoingResultSet(1);

		for (ColumnDef col : cols) {
			outRS.addColumn(col.columnName);
		}
		return outRS;
	}

	/**
	 * Creates a ColumnDef Arraylist of columns to process. Some basic rules: -
	 * NULL indicates an error during processing - If either parameters are
	 * null, then return null. - Any non-ListAgg() column must be present in the
	 * groupBy string. Otherwise there is an error.
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

			String temp[] = groupBy.split(",");
			if (temp.length > 0) {
				for (int i = 0; i < temp.length; ++i) {
					ColumnDef cd = new ColumnDef(temp[i], i + 1);
					columnList.add(cd);
				}
				int groupByCount = temp.length;

				temp = listAgg.split(",");
				if (temp.length > 0) {
					for (int i = 0; i < temp.length; ++i) {
						ColumnDef cd = new ColumnDef(temp[i], groupByCount + i
								+ 1, delimiter);
						if (columnList.contains(cd)) {
							int index = columnList.indexOf(cd);
							cd = columnList.get(index);
							cd.delimiter=delimiter;
							cd.aggType=ColumnDef.LISTAGG;
							columnList.set(index, cd);
						} else {
							cd.colNumber = columnList.size()+1;
							columnList.add(cd);
						}
					}
				} else {
					columnList.clear();
				}
			}
/*			System.out.println("---------------\nColumnDefinitions");
			for (ColumnDef cv: columnList ) {
				System.out.println(cv.toString());
			}
			System.out.println("--------------");*/

		} else {
			throw new SQLException(BAD_COL_ERROR);
		}

		return columnList;
	}
}
