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

import java.util.ArrayList;

public class ColumnDef {
	static final int ASC = 1;
	static final int DESC = 2;
	static final int NONE = 0;
	public static final int LISTAGG=1;
	public static final int GROUPBY=0;
	String columnName;
	String delimiter = null;
	int sortOrder = ASC;
	int colNumber = -1;
	int columnType = -1;
	public int aggType=GROUPBY;

	public ColumnDef(String colName, int c) {
		String temp[] = colName.trim().split(" ");
		columnName = temp[0];
		if (temp.length > 1) {
			setSortOrder(temp[1]);
		} 
		delimiter = null;
		colNumber = c;
		columnType = -1;
		
	}

	/*
	 * public ColumnDef(String colName, int t, int c, String d) { columnName =
	 * colName.trim(); listAggCol = true; delimiter = d; colNumber = c;
	 * columnType = t; }
	 */

	private void setSortOrder(String orderDef) {
		if (orderDef != null && orderDef.toUpperCase().equals("DESC")){
			sortOrder = ColumnDef.DESC;
		} else {
			sortOrder = ColumnDef.ASC;
		}
		
	}

	public ColumnDef(String colName, int c, String d) {
		String temp[] = colName.trim().split(" ");
		columnName = temp[0];
		if (temp.length > 1) {
			setSortOrder(temp[1]);
		} 

		aggType = LISTAGG;
		delimiter = d;
		colNumber = c;
		columnType = -1;
	}

	public String toString() {
		return "ColumnDef: columnName=" + columnName + " number=" + colNumber
				+ " aggregate=" + aggType + " delim=" + delimiter+" sortOrder="+sortOrder;
	}

	public String getColumnName() {
		return columnName;
	}

	/*
	 * public void setColumnName(String columnName) { this.columnName =
	 * columnName; }
	 */

	public boolean isListAggCol() {
		return (aggType == LISTAGG);
	}
	

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((columnName == null) ? 0 : columnName.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ColumnDef other = (ColumnDef) obj;
		if (columnName == null) {
			if (other.columnName != null)
				return false;
		} else if (!columnName.equals(other.columnName))
			return false;
		return true;
	}


	public Object getOrderBy() {
		StringBuilder result = new StringBuilder(columnName);
		if (sortOrder != NONE) {
			result.append(" ");
			if (sortOrder == ASC){
				result.append("ASC");
			} else if (sortOrder == DESC) {
				result.append("DESC");
			}
		}
				
		return result.toString();
	}
	
}
