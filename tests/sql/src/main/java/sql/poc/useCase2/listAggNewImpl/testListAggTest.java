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
package sql.poc.useCase2.listAggNewImpl;
import junit.framework.TestCase;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;


public class testListAggTest extends TestCase {

	static Connection cxn = null;
	static Statement stmt = null;
	static CallableStatement cs = null;
	static ResultSetMetaData resultMetaData = null;
	@SuppressWarnings("rawtypes")
	static ArrayList<ArrayList> rowData = null;
	static String resultErr = null;
	static int rowCount = 0;
	static int colCount = 0;

	public static void setupBeforeClass() throws Exception {
		System.out.println("Starting the test");
		cxn = DriverManager.getConnection("jdbc:gemfirexd:");
		stmt = cxn.createStatement();
		stmt.execute("DROP PROCEDURE ListAgg");
		stmt.execute("DROP ALIAS  ListAggProcessor");
		stmt.execute("DROP TABLE XML_DOC_1");
		stmt.execute("DROP TABLE NoRows");

		stmt.execute("CREATE PROCEDURE ListAgg(IN groupBy VARCHAR(256), "
				+ "IN ListAggCols VARCHAR(256), IN tableName VARCHAR(128), "
				+ "IN whereClause VARCHAR(256), IN delimiter VARCHAR(10)) "
				+ "LANGUAGE JAVA PARAMETER STYLE JAVA READS SQL DATA "
				+ "DYNAMIC RESULT SETS 1 EXTERNAL NAME 'ListAggProcedure.ListAgg';");
		String aliasString = "CREATE ALIAS ListAggProcessor FOR '"
				+ LISTAGGPROCESSOR.class.getName() + "'";
		stmt.execute(aliasString);

		String tableDDL = "create table XML_DOC_1 (ID int NOT NULL,"
				+ " SECONDID DECIMAL not null, THIRDID varchar(10) not null) PARTITION BY COLUMN (ID)";

		stmt.execute(tableDDL);
		stmt.execute("INSERT INTO XML_DOC_1  VALUES (2, 1, '3'); ");
		stmt.execute("INSERT INTO XML_DOC_1  VALUES (3, 3, '3'); ");
		stmt.execute("INSERT INTO XML_DOC_1  VALUES (4, 4, '3'); ");
		stmt.execute("INSERT INTO XML_DOC_1  VALUES (5, 5, '3'); ");
		stmt.execute("INSERT INTO XML_DOC_1  VALUES (2, 1, '9'); ");
		stmt.execute("INSERT INTO XML_DOC_1  VALUES (2, 3, '4'); ");
		stmt.execute("INSERT INTO XML_DOC_1  VALUES (3, 3, '4'); ");
		stmt.execute("select count(*) from XML_DOC_1 ");
		tableDDL = "create table NoRows (ID int NOT NULL,"
				+ " SECONDID int not null, THIRDID varchar(10) not null) PARTITION BY COLUMN (ID)";
		stmt.execute(tableDDL);
		String queryString = "{CALL APP.ListAgg(?,?,?,?,?) WITH RESULT PROCESSOR ListAggProcessor }";
		System.out.println("before procedure call.");
		cs = cxn.prepareCall(queryString);
		System.out.println("After procedure call.");
	}

	public void setUp() throws Exception {

	}

	public static void tearDownAfterClass() throws Exception {

		stmt.close();
		cxn.close();

	}

	public void tearDown() throws Exception {
	}

	@SuppressWarnings("rawtypes")
	public void collectResults(CallableStatement cs) {
		try {
			ResultSet thisResultSet = cs.getResultSet();

			resultMetaData = thisResultSet.getMetaData();
			rowData = new ArrayList<ArrayList>();
			colCount = resultMetaData.getColumnCount();

			rowCount = 0;
			assertTrue(thisResultSet != null);
			while (thisResultSet.next()) {
				ArrayList<Object> colData = new ArrayList<Object>(colCount);
				for (int i = 1; i < colCount + 1; i++) {
					Object o = thisResultSet.getObject(i);
					System.out.print(o.toString());
					System.out.print("\t");

					colData.add(o);
				}
				System.out.println("");
				rowData.add(colData);
				++rowCount;
			}
			System.out.println("End");
			displayResults();
		} catch (SQLException e) {
			String err = e.getMessage();
			e.printStackTrace();
			fail("Unexpected exception:" + err);
		
		}

	}

	private void displayResults() {
		System.out.println("-------------------");
		System.out.println("Results:");
		for (ArrayList<Object> row : rowData) {
			for (Object col : row) {
				System.out.print(col.toString());
				System.out.print("\t");
			}
			System.out.println("");

		}
		System.out.println("-------------------");

	}


	public void testTwoGroupBy() {

		try {
			String result = setParameters("ID,SECONDID", "THIRDID",
					"XML_DOC_1", "", ",");
			assertTrue(result.equals("OK"));
			long startTime = new Date().getTime();
			cs.execute();
			long endTime = new Date().getTime();
			long elapsed = endTime - startTime;
			assertTrue("Elapsed time:" + elapsed, elapsed < 500);
			System.out.println("Two columns results:");
			checkResultsMeta(5, 3);
			String colValue = getCellValue(0, 2);
			assertTrue("Value=" + colValue, colValue.equals("3,9"));
			colValue = getCellValue(1, 2);
			assertTrue("Value=" + colValue, colValue.equals("4"));

		} catch (SQLException e) {
			String err = e.getMessage();
			fail("Unexpected exception:" + err);
		}
	}

	@SuppressWarnings("unchecked")
	private String getCellValue(int row, int col) {
		ArrayList<Object> checkRow1 = rowData.get(row);
		return checkRow1.get(col).toString();
		//String colValue = cv.groupValues(",");
		//return colValue;
	}

	private void checkResultsMeta(int row, int col) {
		collectResults(cs);
		assertTrue("Row count =" + rowCount, rowCount == row);
		assertTrue("Col Count = " + colCount, colCount == col);
	}

	public void testOneGroupBy() {

		try {
			String result = setParameters("ID", "THIRDID", "XML_DOC_1", "", ",");
			assertTrue(result.equals("OK"));

			long startTime = new Date().getTime();
			cs.execute();
			long endTime = new Date().getTime();
			//assertTrue(elapsed < 50);
			System.out.println("One columns results:");
			checkResultsMeta(4, 2);
			String colValue = getCellValue(0, 1);

			// 2 - 3,9,4
			// 3 - 3,4
			// 4 - 3
			// 5 - 3
			assertTrue(colValue, colValue.equals("3,4,9"));
			colValue = getCellValue(3, 1);

			assertTrue(colValue, colValue.equals("3"));

		} catch (SQLException e) {
			String err = e.getMessage();
			fail("Unexpected exception:" + err);
		}
	}

	private String setParameters(String groupBy, String listAgg, String table,
			String whereClause, String delim) {
		String result = "OK";
		try {
			cs.setString(1, groupBy);
			cs.setString(2, listAgg);
			cs.setString(3, table);
			cs.setString(4, whereClause);
			cs.setString(5, delim);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			result = e.getMessage();
			e.printStackTrace();
		}
		return result;

	}

	public void testIdenticalGroupByAndListAggCols1() {

		try {
			System.out
					.println("-----\testIdenticalGroupByAndListAggCols1\n------");
			String result = setParameters("ID,THIRDID", "THIRDID", "XML_DOC_1",
					"", ",");
			assertTrue(result.equals("OK"));
			long startTime = new Date().getTime();
			cs.execute();
			//displayResults();
			long endTime = new Date().getTime();
			//assertTrue(elapsed < 50);
			System.out.println("One columns results:");
			checkResultsMeta(4, 2);
			String colValue = getCellValue(0, 1);

			assertTrue(colValue, colValue.equals("3,4,9"));
			colValue = getCellValue(3, 1);

			assertTrue(colValue, colValue.equals("3"));

		} catch (SQLException e) {
			String err = e.getMessage();
			fail("Unexpected exception: "+err);
			assertTrue(err, err.equals(ListAggProcedure.DUPLICATE_LISTAGG));

			e.printStackTrace();
		}
	}

	public void testIdenticalGroupByAndListAggCols2() {
		System.out.println("-----\testIdenticalGroupByAndListAggCols2\n------");
		try {
			String result = setParameters("ID", "ID", "XML_DOC_1", "", ",");
			assertTrue(result.equals("OK"));

			cs.execute();
			displayResults();
			

		} catch (SQLException e) {
			String err = e.getMessage();
			fail("Unexpected err:"+err);
			assertTrue(err, err.equals(ListAggProcedure.DUPLICATE_LISTAGG));

			e.printStackTrace();
		}
	}


	public void testNoRowsFound() {
		System.out.println("-----\ntestNoRowsFound\n------");
		try {
			String result = setParameters("ID", "THIRDID", "NoRows", "", ",");
			assertTrue(result.equals("OK"));
			long startTime = new Date().getTime();
			cs.execute();
			long endTime = new Date().getTime();
			//assertTrue(elapsed < 50);
			//checkResultsMeta(0, 2);
			collectResults(cs);
			displayResults();
		} catch (SQLException e) {
			String err = e.getMessage();
			fail("Unexpected exception:" + err);
		}
	}


	public void testDuplicateValueRemoval() {
		try {
			System.out.println("-----\ntestDuplicateValueRemoval\n------");
			String result = setParameters("ID", "SECONDID", "XML_DOC_1", "",
					",");
			assertTrue(result.equals("OK"));

			long startTime = new Date().getTime();
			cs.execute();
			long endTime = new Date().getTime();
			long elapsed = endTime - startTime;
			assertTrue(elapsed < 50);

			checkResultsMeta(4, 2);
			String colValue = (String) getCellValue(0, 1);

			// 2 - 1,1,3
			// 3 - 3,3
			// 4 - 4
			// 5 - 5
			assertTrue("Duplicate value:" + colValue, colValue.equals("1,3"));

		} catch (SQLException e) {
			String err = e.getMessage();
			fail("Unexpected exception:" + err);

		}
	}


	public void testWhereClause() {
		try {
			System.out.println("testWhereClause");
			String result = setParameters("ID", "THIRDID", "XML_DOC_1",
					"SECONDID=1", ",");
			assertTrue(result.equals("OK"));

			long startTime = new Date().getTime();
			cs.execute();
			long endTime = new Date().getTime();
			long elapsed = endTime - startTime;
			//assertTrue(elapsed < 500);
			System.out.println("One columns results:");
			checkResultsMeta(1, 2);

			String colValue = (String) getCellValue(0, 1);
			assertTrue("TestWhere Caluse:" + colValue, colValue.equals("3,9"));

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	public void testNullGroupBYClause() {
		try {
			System.out.println("testNullGroupBYClause");
			String result = setParameters(null, "SECONDID", "XML_DOC_1", "",
					",");
			assertTrue(result.equals("OK"));

			cs.execute();
			fail("Was expecting an exception");

		} catch (Exception e) {
			String err = e.getMessage();
			assertTrue(err, err.equals(ListAggProcedure.BAD_COL_ERROR));
			e.printStackTrace();
		}
	}


	public void testNullListAggClause() {
		try {
			String result = setParameters("ID", null, "XML_DOC_1", "", ",");
			assertTrue(result.equals("OK"));

			cs.execute();
			fail("Expected an exception");
		} catch (Exception e) {
			String err = e.getMessage();
			assertTrue(err, err.equals(ListAggProcedure.BAD_COL_ERROR));

		}
	}


	public void testNullWhereClause() {
		try {
			String result = setParameters("ID", "SECONDID", "XML_DOC_1", null,
					",");
			assertTrue(result.equals("OK"));
			long startTime = new Date().getTime();
			cs.execute();
			long endTime = new Date().getTime();
			long elapsed = endTime - startTime;
			assertTrue(elapsed < 50);
			checkResultsMeta(4, 2);

		} catch (Exception e) {
			String err = e.getMessage();
			fail("Unexpected exception:" + err);
		}
	}


	public void testPoorFormedWhereClause() {
		try {
			String result = setParameters("ID", "SECONDID", "XML_DOC_1",
					"THIRDID=", ",");
			assertTrue(result.equals("OK"));

			cs.execute();
			fail("Expected exception");

		} catch (Exception e) {
			String err = e.getMessage();
			assertTrue(err, err.startsWith("Syntax error:"));

		}
	}


	public void testBlankDelimiter() {
		try {
			String result = setParameters("ID", "SECONDID", "XML_DOC_1", "", "");
			assertTrue(result.equals("OK"));

			cs.execute();
			fail("Unexpected success");

		} catch (Exception e) {
			String err = e.getMessage();
			assertTrue(err, err.equals(ListAggProcedure.BAD_COL_ERROR));

		}
	}
	


	public void testSortMixColumns() {
		assertTrue("NI", false);
	}

	public void testSortStringASCColumns() {
		assertTrue("NI", false);
	}
	

	public void testSortStringDESCColumns() {
		assertTrue("NI", false);
	}


	public void testSortDecimalASCColumns() {
		try {
			String result = setParameters("SECONDID ASC", "THIRDID",
					"XML_DOC_1", "", ",");
			assertTrue(result.equals("OK"));
			long startTime = new Date().getTime();
			cs.execute();
			long endTime = new Date().getTime();
			long elapsed = endTime - startTime;
			assertTrue("Elapsed time:" + elapsed, elapsed < 500);
			System.out.println("Two columns results:");
			checkResultsMeta(4, 2);
			String colValue = (String) getCellValue(3, 0);
			assertTrue("Value=" + colValue, colValue.equals("5"));
			colValue = (String) getCellValue(3, 1);
			assertTrue("Value=" + colValue, colValue.equals("3"));

			colValue = (String) getCellValue(2, 0);
			assertTrue("Value=" + colValue, colValue.equals("4"));
			colValue = (String) getCellValue(2, 1);
			assertTrue("Value=" + colValue, colValue.equals("3"));

			colValue = (String) getCellValue(1, 0);
			assertTrue("Value=" + colValue, colValue.equals("3"));
			colValue = (String) getCellValue(1, 1);
			assertTrue("Value=" + colValue, colValue.equals("3,4"));
			
			colValue = (String) getCellValue(0, 0);
			assertTrue("Value=" + colValue, colValue.equals("1"));
			colValue = (String) getCellValue(0, 1);
			assertTrue("Value=" + colValue, colValue.equals("3,9"));
			
			

		} catch (SQLException e) {
			String err = e.getMessage();
			fail("Unexpected exception:" + err);
		}

	}
	



	public void testSortDecimalDESCColumns() {
		try {
			String result = setParameters("SECONDID DESC", "THIRDID",
					"XML_DOC_1", "", ",");
			assertTrue(result.equals("OK"));
			long startTime = new Date().getTime();
			cs.execute();
			long endTime = new Date().getTime();
			long elapsed = endTime - startTime;
			assertTrue("Elapsed time:" + elapsed, elapsed < 500);
			System.out.println("Two columns results:");
			checkResultsMeta(4, 2);
			String colValue = (String) getCellValue(0, 0);
			assertTrue("Value=" + colValue, colValue.equals("5"));
			colValue = (String) getCellValue(0, 1);
			assertTrue("Value=" + colValue, colValue.equals("3"));

			colValue = (String) getCellValue(1, 0);
			assertTrue("Value=" + colValue, colValue.equals("4"));
			colValue = (String) getCellValue(1, 1);
			assertTrue("Value=" + colValue, colValue.equals("3"));

			colValue = (String) getCellValue(2, 0);
			assertTrue("Value=" + colValue, colValue.equals("3"));
			colValue = (String) getCellValue(2, 1);
			assertTrue("Value=" + colValue, colValue.equals("3,4"));
			
			colValue = (String) getCellValue(3, 0);
			assertTrue("Value=" + colValue, colValue.equals("1"));
			colValue = (String) getCellValue(3, 1);
			assertTrue("Value=" + colValue, colValue.equals("3,9"));
			
			

		} catch (SQLException e) {
			String err = e.getMessage();
			fail("Unexpected exception:" + err);
		}
	}
	


}
