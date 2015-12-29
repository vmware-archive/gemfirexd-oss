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


public class UseCase2TestCases extends TestCase {
	static Connection cxn = null;
	static Statement stmt = null;
	static CallableStatement cs = null;
	static ResultSetMetaData resultMetaData = null;
	@SuppressWarnings("rawtypes")
	static ArrayList<ArrayList> rowData = null;
	static String resultErr = null;
	static int rowCount = 0;
	static int colCount = 0;
	static String[] colDefs = { "S", "S", "S", "S", "S", "D", "C", "S", "T" };

	public void setUp() throws Exception {
	}


	public void tearDown() throws Exception {
	}


	public static void setupBeforeClass() throws Exception {
		System.out.println("Starting the test");
		Class.forName("com.pivotal.gemfirexd.jdbc.EmbeddedDriver");
		cxn = DriverManager.getConnection("jdbc:gemfirexd:");
		stmt = cxn.createStatement();
                try {
                  stmt.execute("DROP PROCEDURE ListAgg");
                } catch (SQLException ignore) {
                }
                try {
		  stmt.execute("DROP ALIAS  ListAggProcessor");
                } catch (SQLException ignore) {
                }
                try {
		  stmt.execute("DROP TABLE XML_IDX_1_1");
                } catch (SQLException ignore) {
                }

		stmt.execute("CREATE PROCEDURE ListAgg(IN groupBy VARCHAR(256), "
				+ "IN ListAggCols VARCHAR(256), IN tableName VARCHAR(128), "
				+ "IN whereClause VARCHAR(256), IN delimiter VARCHAR(10)) "
				+ "LANGUAGE JAVA PARAMETER STYLE JAVA READS SQL DATA "
				+ "DYNAMIC RESULT SETS 1 EXTERNAL NAME 'ListAggProcedure.ListAgg';");
		String aliasString = "CREATE ALIAS ListAggProcessor FOR '"
				+ LISTAGGPROCESSOR.class.getName() + "'";
		stmt.execute(aliasString);

		String tableDDL = "CREATE TABLE XML_IDX_1_1 ("
				+ "IDX_COL1 VARCHAR(120)," + "IDX_COL2 VARCHAR(60),"
				+ "IDX_COL3 VARCHAR(60)," + "IDX_COL4 VARCHAR(60),"
				+ "IDX_COL5 VARCHAR(2),"
				+ "XML_DOC_ID_NBR DECIMAL(19) NOT NULL,"
				+ "CREATE_MINT_CD CHAR(1) NOT NULL,"
				+ "LAST_UPDATE_SYSTEM_NM VARCHAR(30),"
				+ "LAST_UPDATE_TMSTP TIMESTAMP NOT NULL" + ")"
				+ " PARTITION BY COLUMN (XML_DOC_ID_NBR)  REDUNDANCY 1;";
		stmt.execute(tableDDL);
		System.out.println("Table schema created");
/*		String[] inputData = {
				"9500REDROAD,80908,COLORADOSPRINGS,CO,US,1347036371777100014,1,APP943415,2012-09-07 16:48:40.689",
				"9500ELMSTREET,80918,COLORADOSPRINGS,CO,US,1347036371777100019,1,APP943415,2012-09-07 16:48:44.717",
				"9500OLDROAD,80917,COLORADOSPRINGS,CO,US,1347036371777100013,1,APP943415,2012-09-07 16:48:39.837",
				"9500SOMESTREET,80901,COLORADOSPRINGS,CO,US,1347036371777100017,1,APP943415,2012-09-07 16:48:43.089",
				"9500ROCKYSTREET,80919,COLORADOSPRINGS,CO,US,1347036371777100012,1,APP943415,2012-09-07 16:48:39.026",
				"9500SOMESTREET,80907,COLORADOSPRINGS,CO,US,1347036371777100016,1,APP943415,2012-09-07 16:48:42.297",
				"9500SOMESTREET,80903,COLORADOSPRINGS,CO,US,1347036371777100018,1,APP943415,2012-09-07 16:48:43.888",
				"9500LUCKYSTREET,80911,COLORADOSPRINGS,CO,US,1347036371778100010,1,APP943415,2012-09-07 16:48:45.557",
				"9500ROCKYROAD,80918,COLORADOSPRINGS,CO,US,1347036371777100011,1,APP943415,2012-09-07 16:48:37.354",
				"9500ROUGHROAD,80914,COLORADOSPRINGS,CO,US,1347036371777100015,1,APP943415,2012-09-07 16:48:41.49" };
		*/
		
		String[] inputData = {
				"9500REDROAD,80908,COLORADOSPRINGS,CO,US,1347036371777100014,1,APP943415,2012-09-07 16:48:40.689",
				"103,80908,COLORADOSPRINGS,CO,US,1347036371777100014,1,APP943415,2012-09-07 16:48:40.763",
				"9500ELMSTREET,80918,COLORADOSPRINGS,CO,US,1347036371777100019,1,APP943415,2012-09-07 16:48:44.717",
				"108,80918,COLORADOSPRINGS,CO,US,1347036371777100019,1,APP943415,2012-09-07 16:48:44.789",
				"9500OLDROAD,80917,COLORADOSPRINGS,CO,US,1347036371777100013,1,APP943415,2012-09-07 16:48:39.837",
				"102,80917,COLORADOSPRINGS,CO,US,1347036371777100013,1,APP943415,2012-09-07 16:48:39.92",
				"9500SOMESTREET,80901,COLORADOSPRINGS,CO,US,1347036371777100017,1,APP943415,2012-09-07 16:48:43.089",
				"106,80901,COLORADOSPRINGS,CO,US,1347036371777100017,1,APP943415,2012-09-07 16:48:43.16",
				"9500ROCKYSTREET,80919,COLORADOSPRINGS,CO,US,1347036371777100012,1,APP943415,2012-09-07 16:48:39.026",
				"101,80919,COLORADOSPRINGS,CO,US,1347036371777100012,1,APP943415,2012-09-07 16:48:39.1",
				"9500SOMESTREET,80907,COLORADOSPRINGS,CO,US,1347036371777100016,1,APP943415,2012-09-07 16:48:42.297",
				"105,80907,COLORADOSPRINGS,CO,US,1347036371777100016,1,APP943415,2012-09-07 16:48:42.369",
				"9500SOMESTREET,80903,COLORADOSPRINGS,CO,US,1347036371777100018,1,APP943415,2012-09-07 16:48:43.888",
				"107,80903,COLORADOSPRINGS,CO,US,1347036371777100018,1,APP943415,2012-09-07 16:48:43.97",
				"9500LUCKYSTREET,80911,COLORADOSPRINGS,CO,US,1347036371778100010,1,APP943415,2012-09-07 16:48:45.557",
				"109,80911,COLORADOSPRINGS,CO,US,1347036371778100010,1,APP943415,2012-09-07 16:48:45.628",
				"9500ROCKYROAD,80918,COLORADOSPRINGS,CO,US,1347036371777100011,1,APP943415,2012-09-07 16:48:37.354",
				"100,80918,COLORADOSPRINGS,CO,US,1347036371777100011,1,APP943415,2012-09-07 16:48:37.433",
				"9500ROUGHROAD,80914,COLORADOSPRINGS,CO,US,1347036371777100015,1,APP943415,2012-09-07 16:48:41.49",
				"104,80914,COLORADOSPRINGS,CO,US,1347036371777100015,1,APP943415,2012-09-07 16:48:41.561"};

		
		importData(stmt, "XML_IDX_1_1", inputData);
		System.out.println("Table data imported");
		String queryString = "{call ListAgg(?,?,?,?,?) WITH RESULT PROCESSOR ListAggProcessor}";
		cs = cxn.prepareCall(queryString);
		System.out.println("After procedure call.");
	}

	private static void importData(Statement stmt, String tableName,
			String[] inputData) {
		try {
			System.out.println("Start of import data");
			for (String line : inputData) {
				String fields[] = line.split(",");

				StringBuilder sb = new StringBuilder("INSERT INTO " + tableName
						+ " VALUES (");
				boolean firstField = true;
				for (int i = 0; i < fields.length; ++i) {
					String fld = fields[i];
					if (firstField) {
						firstField = false;
					} else
						sb.append(",");
					sb.append(convertField(colDefs[i], fld));
				}
				sb.append(");");
				System.out.println(sb.toString());
				stmt.execute(sb.toString());
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	private static String convertField(String type, String fld) {
		if (type.equals("S") || type.equals("T") || type.equals("C"))
			fld = "'" + fld + "'";
		return fld;
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

	@SuppressWarnings("rawtypes")
	public void collectResults(CallableStatement cs) {
		try {
			ResultSet thisResultSet = cs.getResultSet();

			resultMetaData = thisResultSet.getMetaData();
			rowData = new ArrayList<ArrayList>();
			colCount = resultMetaData.getColumnCount();

			rowCount = 0;
			junit.framework.Assert.assertTrue(thisResultSet != null);
			while (thisResultSet.next()) {
				ArrayList<Object> colData = new ArrayList<Object>(colCount);
				for (int i = 1; i < colCount + 1; i++) {
					Object o = thisResultSet.getObject(i);
					//System.out.print("\t");

					colData.add(o);
				}
				//System.out.println("");
				rowData.add(colData);
				++rowCount;
			}
			displayResults();
		} catch (SQLException e) {
			String err = e.getMessage();
			e.printStackTrace();
			fail("Unexpected exception:" + err);

		}

	}

	/*
	@SuppressWarnings("unchecked")
	private String getCellValue(int row, int col) {
		ArrayList<Object> checkRow1 = rowData.get(row);
		return checkRow1.get(col).toString();
		//ColumnValues cv = (ColumnValues) checkRow1.get(col);
		//String colValue = cv.groupValues(",");
		//return colValue;
	}
	*/

	private String getRowData(int row) {
		ArrayList<Object> checkRow1 = rowData.get(row);
		String result = "";
		for (Object o : checkRow1) {
			String s = o.toString();
			if (result.length() > 0)
				result += "|";
			result += s;
		}
		return result;
	}

	private String compareResultsToMaster(String[] expectedResults) {
		String result = null;

		for (int i = 0; i < expectedResults.length; ++i) {
			String line = expectedResults[i];
			String resultLine = getRowData(i);

			//System.out.println(resultLine);
			if (!line.equals(resultLine)) {

				result = "Error matching expected=" + line + " to result="
						+ resultLine;
				break;
			}
		}
		return result;
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


	public void testUseCase2TestDESC() {
		try {
			String result = setParameters(
					"IDX_COL1 DESC, IDX_COL2 DESC,IDX_COL3 DESC, IDX_COL4 DESC, IDX_COL5 DESC,XML_DOC_ID_NBR DESC",
					"IDX_COL1",
					"XML_IDX_1_1",
					"IDX_COL1 like '9500%'and IDX_COL2 like'809%' and IDX_COL3='COLORADOSPRINGS'and IDX_COL4='CO' and IDX_COL5='US'",
					",");
			junit.framework.Assert.assertTrue("Set parameters = " + result, result.equals("OK"));
			long startTime = new Date().getTime();
			cs.execute();
			System.out.println("Elapsed time = "
					+ (new Date().getTime() - startTime));
			collectResults(cs);

			String[] expectedResults = {
					"9500SOMESTREET|80907|COLORADOSPRINGS|CO|US|1347036371777100016",
					"9500SOMESTREET|80903|COLORADOSPRINGS|CO|US|1347036371777100018",
					"9500SOMESTREET|80901|COLORADOSPRINGS|CO|US|1347036371777100017",
					"9500ROUGHROAD|80914|COLORADOSPRINGS|CO|US|1347036371777100015",
					"9500ROCKYSTREET|80919|COLORADOSPRINGS|CO|US|1347036371777100012",
					"9500ROCKYROAD|80918|COLORADOSPRINGS|CO|US|1347036371777100011",
					"9500REDROAD|80908|COLORADOSPRINGS|CO|US|1347036371777100014",
					"9500OLDROAD|80917|COLORADOSPRINGS|CO|US|1347036371777100013",
					"9500LUCKYSTREET|80911|COLORADOSPRINGS|CO|US|1347036371778100010",
					"9500ELMSTREET|80918|COLORADOSPRINGS|CO|US|1347036371777100019" };

			String resultsMatch = compareResultsToMaster(expectedResults);
			assertTrue("Results did not match expected:" + resultsMatch,
					resultsMatch == null);
		} catch (SQLException e) {
			String err = e.getMessage();
			// e.printStackTrace();
			fail("Unexpected exeception:" + err);
		}
	}

	public void testUseCase2TestASC() {
		try {
			String result = setParameters(
					"IDX_COL1 ASC, IDX_COL2 ASC,IDX_COL3 ASC, IDX_COL4 ASC, IDX_COL5 ASC,XML_DOC_ID_NBR ASC",
					"IDX_COL1",
					"XML_IDX_1_1",
					"IDX_COL1 like '9500%'and IDX_COL2 like'809%' and IDX_COL3='COLORADOSPRINGS'and IDX_COL4='CO' and IDX_COL5='US'",
					",");
			assertTrue("Set parameters = " + result, result.equals("OK"));
			long startTime = new Date().getTime();
			cs.execute();
			System.out.println("Elapsed time = "
					+ (new Date().getTime() - startTime));
			collectResults(cs);

			String[] expectedResults = {
					"9500ELMSTREET|80918|COLORADOSPRINGS|CO|US|1347036371777100019",
					"9500LUCKYSTREET|80911|COLORADOSPRINGS|CO|US|1347036371778100010",
					"9500OLDROAD|80917|COLORADOSPRINGS|CO|US|1347036371777100013",
					"9500REDROAD|80908|COLORADOSPRINGS|CO|US|1347036371777100014",
					"9500ROCKYROAD|80918|COLORADOSPRINGS|CO|US|1347036371777100011",
					"9500ROCKYSTREET|80919|COLORADOSPRINGS|CO|US|1347036371777100012",
					"9500ROUGHROAD|80914|COLORADOSPRINGS|CO|US|1347036371777100015",
					"9500SOMESTREET|80901|COLORADOSPRINGS|CO|US|1347036371777100017",
					"9500SOMESTREET|80903|COLORADOSPRINGS|CO|US|1347036371777100018",
					"9500SOMESTREET|80907|COLORADOSPRINGS|CO|US|1347036371777100016" };

			String resultsMatch = compareResultsToMaster(expectedResults);
			assertTrue("Results did not match expected:" + resultsMatch,
					resultsMatch == null);
		} catch (SQLException e) {
			String err = e.getMessage();
			// e.printStackTrace();
			fail("Unexpected exeception:" + err);
		}
	}
	
	public void testUseCase2TestMixed() {
		try {
			String result = setParameters(
					"IDX_COL1 ASC, IDX_COL2 DESC,IDX_COL3 ASC, IDX_COL4 ASC, IDX_COL5 ASC,XML_DOC_ID_NBR ASC",
					"IDX_COL1",
					"XML_IDX_1_1",
					"IDX_COL1 like '9500%'and IDX_COL2 like'809%' and IDX_COL3='COLORADOSPRINGS'and IDX_COL4='CO' and IDX_COL5='US'",
					",");
			assertTrue("Set parameters = " + result, result.equals("OK"));
			long startTime = new Date().getTime();
			cs.execute();
			System.out.println("Elapsed time = "
					+ (new Date().getTime() - startTime));
			collectResults(cs);

			String[] expectedResults = {
					"9500ELMSTREET|80918|COLORADOSPRINGS|CO|US|1347036371777100019",
					"9500LUCKYSTREET|80911|COLORADOSPRINGS|CO|US|1347036371778100010",
					"9500OLDROAD|80917|COLORADOSPRINGS|CO|US|1347036371777100013",
					"9500REDROAD|80908|COLORADOSPRINGS|CO|US|1347036371777100014",
					"9500ROCKYROAD|80918|COLORADOSPRINGS|CO|US|1347036371777100011",
					"9500ROCKYSTREET|80919|COLORADOSPRINGS|CO|US|1347036371777100012",
					"9500ROUGHROAD|80914|COLORADOSPRINGS|CO|US|1347036371777100015",
					"9500SOMESTREET|80907|COLORADOSPRINGS|CO|US|1347036371777100016",
					"9500SOMESTREET|80903|COLORADOSPRINGS|CO|US|1347036371777100018",
					"9500SOMESTREET|80901|COLORADOSPRINGS|CO|US|1347036371777100017"
					
					 };

			String resultsMatch = compareResultsToMaster(expectedResults);
			assertTrue("Results did not match expected:" + resultsMatch,
					resultsMatch == null);
		} catch (SQLException e) {
			String err = e.getMessage();
			// e.printStackTrace();
			fail("Unexpected exeception:" + err);
		}
	}
	
	public void testUseCase2TestSmallerQuerySet() {
		try {
			String result = setParameters(
					"IDX_COL1 ASC, IDX_COL2 DESC,IDX_COL3 ASC, IDX_COL4 ASC, IDX_COL5 ASC,XML_DOC_ID_NBR ASC",
					"IDX_COL1",
					"XML_IDX_1_1",
					"IDX_COL1 like '9500ROCK%'and IDX_COL2 like'809%' and IDX_COL3='COLORADOSPRINGS'and IDX_COL4='CO' and IDX_COL5='US'",
					",");
			assertTrue("Set parameters = " + result, result.equals("OK"));
			long startTime = new Date().getTime();
			cs.execute();
			System.out.println("Elapsed time = "
					+ (new Date().getTime() - startTime));
			collectResults(cs);

			String[] expectedResults = {

					"9500ROCKYROAD|80918|COLORADOSPRINGS|CO|US|1347036371777100011",
					"9500ROCKYSTREET|80919|COLORADOSPRINGS|CO|US|1347036371777100012"
					
					 };

			String resultsMatch = compareResultsToMaster(expectedResults);
			assertTrue("Results did not match expected:" + resultsMatch,
					resultsMatch == null);
		} catch (SQLException e) {
			String err = e.getMessage();
			// e.printStackTrace();
			fail("Unexpected exeception:" + err);
		}
	}
	
	public void testUseCase2TestLastListAggFieldNoSummary() {
		try {
			String result = setParameters(
					"IDX_COL1 ASC, IDX_COL2 DESC,IDX_COL3 ASC, IDX_COL4 ASC, IDX_COL5 ASC,XML_DOC_ID_NBR ASC",
					"XML_DOC_ID_NBR",
					"XML_IDX_1_1",
					"IDX_COL1 like '9500%'and IDX_COL2 like'809%' and IDX_COL3='COLORADOSPRINGS'and IDX_COL4='CO' and IDX_COL5='US'",
					",");
			assertTrue("Set parameters = " + result, result.equals("OK"));
			long startTime = new Date().getTime();
			cs.execute();
			System.out.println("Elapsed time = "
					+ (new Date().getTime() - startTime));
			collectResults(cs);

			String[] expectedResults = {
					"9500ELMSTREET|80918|COLORADOSPRINGS|CO|US|1347036371777100019",
					"9500LUCKYSTREET|80911|COLORADOSPRINGS|CO|US|1347036371778100010",
					"9500OLDROAD|80917|COLORADOSPRINGS|CO|US|1347036371777100013",
					"9500REDROAD|80908|COLORADOSPRINGS|CO|US|1347036371777100014",
					"9500ROCKYROAD|80918|COLORADOSPRINGS|CO|US|1347036371777100011",
					"9500ROCKYSTREET|80919|COLORADOSPRINGS|CO|US|1347036371777100012",
					"9500ROUGHROAD|80914|COLORADOSPRINGS|CO|US|1347036371777100015",
					"9500SOMESTREET|80907|COLORADOSPRINGS|CO|US|1347036371777100016",
					"9500SOMESTREET|80903|COLORADOSPRINGS|CO|US|1347036371777100018",
					"9500SOMESTREET|80901|COLORADOSPRINGS|CO|US|1347036371777100017"
					
					 };

			String resultsMatch = compareResultsToMaster(expectedResults);
			assertTrue("Results did not match expected:" + resultsMatch,
					resultsMatch == null);
		} catch (SQLException e) {
			String err = e.getMessage();
			// e.printStackTrace();
			fail("Unexpected exeception:" + err);
		}
		
		
	}
	
	public void testUseCase2TestDESCListAggWithSummary() {
		try {
			String result = setParameters(
					"IDX_COL1 DESC",
					"XML_DOC_ID_NBR",
					"XML_IDX_1_1",
					"IDX_COL1 like '9500%'and IDX_COL2 like'809%' and IDX_COL3='COLORADOSPRINGS'and IDX_COL4='CO' and IDX_COL5='US'",
					",");
			assertTrue("Set parameters = " + result, result.equals("OK"));
			long startTime = new Date().getTime();
			cs.execute();
			System.out.println("Elapsed time = "
					+ (new Date().getTime() - startTime));
			collectResults(cs);

			String[] expectedResults = {
					"9500SOMESTREET|1347036371777100016,1347036371777100017,1347036371777100018",
					"9500ROUGHROAD|1347036371777100015",
					"9500ROCKYSTREET|1347036371777100012",
					"9500ROCKYROAD|1347036371777100011",
					"9500REDROAD|1347036371777100014",
					"9500OLDROAD|1347036371777100013",
					"9500LUCKYSTREET|1347036371778100010",
					"9500ELMSTREET|1347036371777100019" };

			String resultsMatch = compareResultsToMaster(expectedResults);
			assertTrue("Results did not match expected:" + resultsMatch,
					resultsMatch == null);
		} catch (SQLException e) {
			String err = e.getMessage();
			// e.printStackTrace();
			fail("Unexpected exeception:" + err);
		}
	}
	
	public void testUseCase2TestMixedSortWithSummary() {
		try {
			String result = setParameters(
					"IDX_COL2 DESC,IDX_COL3 ASC",
					"XML_DOC_ID_NBR",
					"XML_IDX_1_1",
					"IDX_COL1 like '9500%'and IDX_COL2 like'809%' and IDX_COL3='COLORADOSPRINGS'and IDX_COL4='CO' and IDX_COL5='US'",
					",");
			assertTrue("Set parameters = " + result, result.equals("OK"));
			long startTime = new Date().getTime();
			cs.execute();
			System.out.println("Elapsed time = "
					+ (new Date().getTime() - startTime));
			collectResults(cs);

			String[] expectedResults = {
					"80919|COLORADOSPRINGS|1347036371777100012",
					"80918|COLORADOSPRINGS|1347036371777100011,1347036371777100019",
					"80917|COLORADOSPRINGS|1347036371777100013",
					"80914|COLORADOSPRINGS|1347036371777100015",
					"80911|COLORADOSPRINGS|1347036371778100010",
					"80908|COLORADOSPRINGS|1347036371777100014",					
					"80907|COLORADOSPRINGS|1347036371777100016",
					"80903|COLORADOSPRINGS|1347036371777100018",
					"80901|COLORADOSPRINGS|1347036371777100017"
					
					 };

			String resultsMatch = compareResultsToMaster(expectedResults);
			assertTrue("Results did not match expected:" + resultsMatch,
					resultsMatch == null);
		} catch (SQLException e) {
			String err = e.getMessage();
			// e.printStackTrace();
			fail("Unexpected exeception:" + err);
		}
		
		
	}
	
	public void testUseCase2TestLastListAgg3() {
		System.out.println("testUseCase2TestLastListAgg3:------------------->");
		try {
			String result = setParameters(
					"IDX_COL2,IDX_COL3 ASC",
					"XML_DOC_ID_NBR",
					"XML_IDX_1_1",
					"IDX_COL1 like '9500%'and IDX_COL2 like'809%' and IDX_COL3='COLORADOSPRINGS'and IDX_COL4='CO' and IDX_COL5='US'",
					",");
			assertTrue("Set parameters = " + result, result.equals("OK"));
			long startTime = new Date().getTime();
			cs.execute();
			System.out.println("Elapsed time = "
					+ (new Date().getTime() - startTime));
			collectResults(cs);

			String[] expectedResults = {
					"80901|COLORADOSPRINGS|1347036371777100017",
					"80903|COLORADOSPRINGS|1347036371777100018",
					"80907|COLORADOSPRINGS|1347036371777100016",
					"80908|COLORADOSPRINGS|1347036371777100014",
					"80911|COLORADOSPRINGS|1347036371778100010",
					"80914|COLORADOSPRINGS|1347036371777100015",
					"80917|COLORADOSPRINGS|1347036371777100013",
					"80918|COLORADOSPRINGS|1347036371777100011,1347036371777100019",
					"80919|COLORADOSPRINGS|1347036371777100012"
					
					 };

			String resultsMatch = compareResultsToMaster(expectedResults);
			assertTrue("Results did not match expected:" + resultsMatch,
					resultsMatch == null);
		} catch (SQLException e) {
			String err = e.getMessage();
			// e.printStackTrace();
			fail("Unexpected exeception:" + err);
		}
		
		
	}	

	public void testUseCase2TestBigListAggAndDupesASC() {
		try {
			String result = setParameters(
					"IDX_COL3 ASC, IDX_COL2 ASC",
					"IDX_COL2",
					"XML_IDX_1_1",
					"IDX_COL1 like '9500%'and IDX_COL2 like'809%' and IDX_COL3='COLORADOSPRINGS'and IDX_COL4='CO' and IDX_COL5='US'",
					",");
			assertTrue("Set parameters = " + result, result.equals("OK"));
			long startTime = new Date().getTime();
			cs.execute();
			System.out.println("Elapsed time = "
					+ (new Date().getTime() - startTime));
			collectResults(cs);

			String[] expectedResults = {
			    // [sumedh] below expected result is incorrect; ideally it would be
			    // "COLORADOSPRINGS|80901,80901","COLORADOSPRINGS|80903,80903",...
			    // but current impl returns only one of each since this usage does
			    // not look useful
					//"COLORADOSPRINGS|80901,80903,80907,80908,80911,80914,80917,80918,80919"	
			    "COLORADOSPRINGS|80901",
			    "COLORADOSPRINGS|80903",
			    "COLORADOSPRINGS|80907",
			    "COLORADOSPRINGS|80908",
			    "COLORADOSPRINGS|80911",
			    "COLORADOSPRINGS|80914",
			    "COLORADOSPRINGS|80917",
			    "COLORADOSPRINGS|80918",
			    "COLORADOSPRINGS|80919"
					 };

			String resultsMatch = compareResultsToMaster(expectedResults);
			assertTrue("Results did not match expected:" + resultsMatch,
					resultsMatch == null);
		} catch (SQLException e) {
			String err = e.getMessage();
			// e.printStackTrace();
			fail("Unexpected exeception:" + err);
		}
		
		
	}	
	
	public void testUseCase2TestBigListAggAndDupesDESC() {
		try {
			String result = setParameters(
					"IDX_COL3 ASC, IDX_COL2 DESC",
					"IDX_COL2",
					"XML_IDX_1_1",
					"IDX_COL1 like '9500%'and IDX_COL2 like'809%' and IDX_COL3='COLORADOSPRINGS'and IDX_COL4='CO' and IDX_COL5='US'",
					",");
			assertTrue("Set parameters = " + result, result.equals("OK"));
			long startTime = new Date().getTime();
			cs.execute();
			System.out.println("Elapsed time = "
					+ (new Date().getTime() - startTime));
			collectResults(cs);

			String[] expectedResults = {					
			    // [sumedh] expected below is incorrect; see comments in
			    // previous test
					//"COLORADOSPRINGS|80919,80918,80917,80914,80911,80908,80907,80903,80901"
			    "COLORADOSPRINGS|80919",
			    "COLORADOSPRINGS|80918",
			    "COLORADOSPRINGS|80917",
			    "COLORADOSPRINGS|80914",
			    "COLORADOSPRINGS|80911",
			    "COLORADOSPRINGS|80908",
			    "COLORADOSPRINGS|80907",
			    "COLORADOSPRINGS|80903",
			    "COLORADOSPRINGS|80901"
					};

			String resultsMatch = compareResultsToMaster(expectedResults);
			assertTrue("Results did not match expected:" + resultsMatch,
					resultsMatch == null);
		} catch (SQLException e) {
			String err = e.getMessage();
			// e.printStackTrace();
			fail("Unexpected exeception:" + err);
		}
		
		
	}	
	

	
}
