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
package sql.poc.useCase2;

import hydra.Log;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

import sql.SQLHelper;

public class PerfTestListAgg extends UseCase2Test {
  public static PerfTestListAgg pt;
  
  public static void HydraTask_perfTestListAgg() {
    if (pt == null) {
      pt = new PerfTestListAgg();
    }
    pt.perfTestListAgg();
  }
  
  protected void perfTestListAgg() {
    Connection conn = getGFEConnection();
    perfTestListAgg(conn);
    closeGFEConnection(conn);
  }
  
	protected void perfTestListAgg(Connection cxn) {

		try {
			Statement stmt = cxn.createStatement();

/*			stmt.execute("DROP PROCEDURE ListAgg");
			stmt.execute("DROP ALIAS  ListAggProcessor");
			stmt.execute("DROP TABLE XML_DOC_1");
			stmt.execute("CREATE PROCEDURE ListAgg(IN groupBy VARCHAR(256), "
					+ "IN ListAggCols VARCHAR(256), IN tableName VARCHAR(128), "
					+ "IN whereClause VARCHAR(256), IN delimiter VARCHAR(10)) "
					+ "LANGUAGE JAVA PARAMETER STYLE JAVA READS SQL DATA "
					+ "DYNAMIC RESULT SETS 1 EXTERNAL NAME 'ListAggProcedure.ListAgg';");
			String aliasString = "CREATE ALIAS ListAggProcessor FOR '"
					+ LISTAGGPROCESSOR.class.getName() + "'";
			stmt.execute(aliasString);
*/
			stmt.execute("select count(*) from CDSDBA.XML_DOC_1 ");

			String queryString = "{CALL CDSDBA.ListAgg(?,?,?,?,?) WITH RESULT PROCESSOR ListAggProcessor }";
			
			CallableStatement cs = cxn.prepareCall(queryString);
			String qs = "{CALL CDSDBA.ListAgg('structure_id_nbr','xml_doc_id_nbr','CDSDBA.XML_DOC_1','create_mint_cd=~x~',',') WITH RESULT PROCESSOR ListAggProcessor}";
			String groupBy = "structure_id_nbr";
			String listAgg = "xml_doc_id_nbr";
			String table = "CDSDBA.XML_DOC_1";
			String whereClause = "";
			String delimiter = ",";
			//System.out.println(queryString);
			//System.out.println("Table="+table);
			Log.getLogWriter().info(queryString);
			Log.getLogWriter().info("Table="+table);
			cs = cxn.prepareCall(queryString);

			for (int i = 0; i < 1000; ++i) {
				cs.setString(1, groupBy);
				cs.setString(2, listAgg);
				cs.setString(3, table);

				char value = (char) ('a' + (char) ((Math.random() * 20)));
				whereClause = "create_mint_cd='" + value + "'";
				cs.setString(4, whereClause);
				cs.setString(5, delimiter);
				long startTime = new Date().getTime();
				cs.execute();
				long endTime = new Date().getTime();
				Log.getLogWriter().info("Where:"+whereClause+"   Duration = " + (endTime - startTime));
			}

		} catch (SQLException e) {
			SQLHelper.handleSQLException(e);
		}

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new PerfTestListAgg();

	}

}
