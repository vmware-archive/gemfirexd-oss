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


public class FieldGroup implements java.io.Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -1949584832665912553L;
	ArrayList<ColumnValues> fg = new ArrayList<ColumnValues>();

/*	public FieldGroup(ColumnValues cv) {
		fg.add(cv);
	}
	
	public FieldGroup(FieldGroup oldFg) {
		fg = new ArrayList<ColumnValues>(oldFg.fg);
	}*/
	
	public FieldGroup() {
		
	}
	
/*	public FieldGroup(ArrayList<ColumnValues> cList) {
		fg.addAll(cList);
	}
	
	public ArrayList<ColumnValues> add(ArrayList<ColumnValues> cList) {
		fg.addAll(cList);
		return fg;
	}


	public Object getKey() {
		StringBuilder sb = new StringBuilder();
		for(ColumnValues cv: fg) {
			sb.append(cv.groupValues(""));
		}
		return sb.toString();
	}
	*/
	public int getColumnCount() {
		return fg.size();
	}
	public ArrayList<ColumnValues> getColumns() {
		return fg;
	}

	public void add(ColumnDef c) {
		ColumnValues cv = new ColumnValues(c.colNumber, c.columnName, c.columnType, null);
		fg.add(cv);
		
	}
	
	public void add(ColumnValues cv) {
		fg.add(cv);
	}
	
	public String toString() {
		return fg.toString();
	}

}
