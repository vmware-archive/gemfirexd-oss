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
package sql.sqlCallback.listener;

import hydra.TestConfig;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

import sql.SQLPrms;
import util.TestException;

import com.pivotal.gemfirexd.callbacks.Event;
import com.pivotal.gemfirexd.callbacks.EventCallback;

public abstract class AbstractListener implements EventCallback {
	protected static String initStr = TestConfig.tab().stringAt(SQLPrms.backendDB_url, 
	"jdbc:derby:test");
	
	public void close() throws SQLException {}
	public void init(String initStr) throws SQLException {
		if (!this.initStr.equals(initStr)) 
			throw new TestException ("init in Listener() does not get the initStr.");
	}
	
	public abstract void onEvent(Event event) throws SQLException;
	
	
	//the sql string does not have the where condition yet
	protected String getSql(Event event) throws SQLException {
		ResultSetMetaData meta = event.getResultSetMetaData();
		List<Object> newRow = event.getNewRow();
		StringBuffer str = new StringBuffer();
		if (event.getModifiedColumns() == null) throw new TestException("event.getModifiedColumns " +
				"return null on AFTER_UPDATE");
		
		str.append("update " + meta.getSchemaName(1) + "." 
				+ meta.getTableName(1) + " set ");		
		for (int i=0; i<event.getModifiedColumns().length; i++) {
                        str.append(" " + meta.getColumnName(event.getModifiedColumns()[i]) + "=");
                        appendValue(str, newRow.get(event.getModifiedColumns()[i]-1),
                                                meta.getColumnType(event.getModifiedColumns()[i]));
		}
		str.delete(str.length() -1 , str.length());
		return str.toString();
	}

	//how to append using Statement
	protected void appendValue(StringBuffer str, Object value, int type) {
		switch (type){
		case Types.BIGINT:
		case Types.DECIMAL:
		case Types.NUMERIC:
		case Types.SMALLINT:
		case Types.TINYINT:
		case Types.INTEGER:
		case Types.FLOAT:
		case Types.DOUBLE:
		case Types.REAL:
		case Types.TIMESTAMP:
			str.append(value + ",");
			break;
		default:
			str.append("'" + value +"',");
		}
	}
}
