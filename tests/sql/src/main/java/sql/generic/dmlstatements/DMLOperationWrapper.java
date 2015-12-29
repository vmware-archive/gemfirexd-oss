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
package sql.generic.dmlstatements;

import hydra.Log;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * DMLOperationWrapper
 * 
 * @author Namrata Thanvi
 */

public class DMLOperationWrapper {

	private DMLOperation operation;
	private Connection conn;
        public static final int MSG_START =0;
        public static final int MSG_END =1;
        
	public DMLOperationWrapper(DMLOperation operation, Connection conn)  {
		this.operation = operation;
		this.conn = conn;					
	}

	public PreparedStatement getPreparedStatement() throws SQLException {	        
	        operation.createPrepareStatement(conn);
		if (isDerby()) {
			return operation.getDMLPreparedStatement().getDerbyPreparedStmt();
		}
		return operation.getDMLPreparedStatement().getGemXDPreparedStmt();
	}

	public Connection getConnection() {
		return conn;
	}
       
	
	public DMLOperation getOperation (){
	  return operation;
	}
	public void executionMsg (int message){
	  String database = isDerby() ? "Derby - " : "Gemxd - ";
	  switch (message) {
	    case MSG_START:
	        Log.getLogWriter().info(database + "Started execution of " + operation.getOperation().getValue() + operation.getPreparedStmtForLogging());
	        break;
	    case MSG_END:
	                Log.getLogWriter().info(database + "completed execution of " + operation.getOperation().getValue()  + operation.getPreparedStmtForLogging());
	                break   ; 
	  }
	}
	
	public boolean isDerby(){
	   return GenericDMLHelper.isDerby(conn);
	}

}