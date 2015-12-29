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
package sql.dbCache;

import java.sql.*;
import java.util.*;

import util.*;
import hydra.*;
import hydra.gemfirexd.GfxdHelper;
import hydra.gemfirexd.GfxdHelperPrms;
import sql.sqlutil.*;
import sql.ddlStatements.*;
import sql.dmlStatements.*;
import sql.SQLHelper;
import sql.SQLPrms;
import sql.SQLBB;
import sql.SQLTest;
import sql.DBType;
import sql.alterTable.*;

public class DBCacheTest extends SQLTest {
	protected static DBCacheTest dbCacheTest;   
	int lruCount = TestConfig.tab().intAt(DBCachePrms.lruCount,100);
	
	public static synchronized void HydraTask_initialize() {
		if (dbCacheTest == null){
			dbCacheTest = new DBCacheTest();    
			dbCacheTest.initialize();	
		}		
	  }

	public static synchronized void HydraTask_verifyLRUCount() {
		if (dbCacheTest == null){
			dbCacheTest = new DBCacheTest();    
		}	
		dbCacheTest.verifyLRUCount();
	}
	
	//only in init task after populate table to avoid delete operation 
	protected void verifyLRUCount(){
		Connection gConn = getGFEConnection();
		String query = "SELECT COUNT(*) FROM trade.sellorders";
		try{	
			PreparedStatement stmt = gConn.prepareStatement(query);
			ResultSet rs = stmt.executeQuery();
			rs.next();
			int num_row = rs.getInt(1);
			Log.getLogWriter().info("Number of rows in sellorders : " + num_row);
			if(num_row != lruCount && SQLBB.getBB().getSharedCounters().read(SQLBB.tradeSellOrdersPrimary) > 2 * lruCount)
				throw new TestException("Number of rows: " + num_row + 
				    " in sellorders table is not equal to LRUCount: " + lruCount );
			rs.close();
			stmt.close();	
		}catch (SQLException se){
			SQLHelper.handleSQLException(se);
		}
		closeGFEConnection(gConn);
	}
 }
