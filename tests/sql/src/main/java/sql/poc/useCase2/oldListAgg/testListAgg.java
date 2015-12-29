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

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

public class testListAgg {

	public testListAgg() {

		try {
			Connection cxn = DriverManager.getConnection("jdbc:gemfirexd:");
			Statement stmt = cxn.createStatement();

			stmt.execute("DROP TABLE XML_DOC_1");
			stmt.execute("DROP PROCEDURE ListAgg");
			stmt.execute("DROP ALIAS  ListAggProcessor");

			stmt.execute("CREATE PROCEDURE ListAgg(IN groupBy VARCHAR(256), "
					+ "IN ListAggCols VARCHAR(256), IN tableName VARCHAR(128), "
					+ "IN whereClause VARCHAR(256), IN delimiter VARCHAR(10)) "
					+ "LANGUAGE JAVA PARAMETER STYLE JAVA READS SQL DATA "
					+ "DYNAMIC RESULT SETS 1 EXTERNAL NAME 'ListAggProcedure.ListAgg';");
			String aliasString = "CREATE ALIAS ListAggProcessor FOR '"
					+ LISTAGGPROCESSOR.class.getName() + "'";
			System.out.println(aliasString);
			stmt.execute(aliasString);

			String tableDDL = "create table APP.XML_DOC_1 (ID int NOT NULL,"
					+ " SECONDID int not null, THIRDID varchar(10) not null) PARTITION BY COLUMN (ID)";

			stmt.execute(tableDDL);
			DatabaseMetaData dbmd = cxn.getMetaData();
			ResultSet resultSet = dbmd.getColumns(null, "CDSDBA", "XML_DOC_1",
					null);

			while (resultSet.next()) {
				String strTableName = resultSet.getString("COLUMN_NAME");
				System.out.println("TABLE_NAME is " + strTableName);
			}
			
			  stmt.execute("INSERT INTO APP.XML_DOC_1  VALUES (2, 1, '3'); ");
			 stmt.execute("INSERT INTO APP.XML_DOC_1  VALUES (3, 3, '3'); " );
			 stmt.execute("INSERT INTO APP.XML_DOC_1  VALUES (4, 4, '3'); ");
			 stmt.execute("INSERT INTO APP.XML_DOC_1  VALUES (5, 5, '3'); ");
			 stmt.execute("INSERT INTO APP.XML_DOC_1  VALUES (2, 1, '9'); ");
			 stmt.execute("INSERT INTO APP.XML_DOC_1  VALUES (2, 3, '4'); ");
			 stmt.execute("INSERT INTO APP.XML_DOC_1  VALUES (3, 3, '4'); "); 
			 stmt.execute("INSERT INTO APP.XML_DOC_1  VALUES (5, 3, '4'); "); 
			 stmt.execute("select count(*) from APP.XML_DOC_1 ");
			 

			String queryString = "{CALL APP.ListAgg(?,?,?,?,?) WITH RESULT PROCESSOR ListAggProcessor }";
			// queryString =
			// "{CALL CDSDBA.ListAgg('structure_id_nbr DESC','create_mint_cd','CDSDBA.XML_DOC_1','MSG_PAYLOAD_QTY=5',',') WITH RESULT PROCESSOR ListAggProcessor ;}";
			CallableStatement cs = cxn.prepareCall(queryString);

			String groupBy = "ID";
			String listAgg = "THIRDID";
			String table = "XML_DOC_1";
			String whereClause = "";
			String delimiter = ",";

			cs = cxn.prepareCall(queryString);
			cs.setString(1, groupBy);
			cs.setString(2, listAgg);
			cs.setString(3, table);
			cs.setString(4, whereClause);
			cs.setString(5, delimiter);

			long startTime = new Date().getTime();
			cs.execute();

			long endTime = new Date().getTime();
			System.out.println("Duration = " + (endTime - startTime));
			ResultSet thisResultSet;
			boolean moreResults = true;
			int cnt = 0;

			do {
				thisResultSet = cs.getResultSet();
				ResultSetMetaData rMeta = thisResultSet.getMetaData();
				int colCnt = rMeta.getColumnCount();
				for (int i = 1; i < colCnt + 1; i++) {
					System.out.print(rMeta.getColumnName(i));
					System.out.print("\t");
				}
				System.out.println("");

				if (cnt == 0) {
					while (thisResultSet.next()) {
						for (int i = 1; i < colCnt + 1; i++) {
							System.out.print(thisResultSet.getObject(i));
							System.out.print("\t");
						}
						System.out.println("");
					}
					System.out.println("ResultSet 1 ends\n");
					cnt++;
				}
				moreResults = cs.getMoreResults();
			} while (moreResults);

		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new testListAgg();

	}

}
