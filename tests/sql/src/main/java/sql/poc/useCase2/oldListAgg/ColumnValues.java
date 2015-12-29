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
import java.util.Collections;

public class ColumnValues implements Cloneable, java.io.Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1857796487844499493L;

	public int colNumber;
	public String columnName;
	public int colType;
	public int sortOrder = ColumnDef.ASC; 
	public int aggType = ColumnDef.GROUPBY;
	public ArrayList<Object> values = new ArrayList<Object>();

	public ColumnValues(int num, String name, int t, Object v) {
		colNumber = num;
		columnName = new String(name);
		colType = t;
		if (v != null)
			values.add(v);
	}
	
	public ColumnValues(ColumnDef cd) {
		colNumber = cd.colNumber;
		columnName = new String(cd.columnName);
		colType = cd.columnType;
		sortOrder = cd.sortOrder;
		aggType = cd.aggType;
	}

	public ColumnValues(int num, String name, int t, int a) {
		colNumber = num;
		columnName = new String(name);
		colType = t;
		aggType = a;
	}

	public String toString() {
//		return "#" + colNumber + ": Name:" + columnName + " Col type=" + colType+ "AggType="+aggType
//				+ " values=" + groupValues(",");
		return groupValues(",");
	}

	public static ArrayList<Object> listAggValuesBuilderFactory(
			ArrayList<ColumnDef> cols) {
		ArrayList<Object> listAgg = new ArrayList<Object>(cols.size());
		for (ColumnDef c : cols) {
			listAgg.add(new ColumnValues(c));
		}
		return listAgg;
	}

	/*
	 * Convert value list into a comma delimited list.
	 * 
	 * NOTE: This method drops field values which are null
	 */
	public String groupValues(String delim) {
		StringBuilder sb = new StringBuilder();
		boolean firstValue = true;
		Collections.sort((ArrayList)values);
		for (Object o : values) {
			if (!firstValue && o != null) {
				sb.append(delim).append(o.toString());
			} else {
				if (o != null) {
					sb.append(o.toString());
				} 
				firstValue = false;
			}
		}
		return sb.toString();
	}

	public ArrayList<Object> getValues() {
		return values;
	}



	public void appendValue(Object o) {
		if (o != null && !values.contains(o))
			values.add(o);
	}

	public void mergeValues(ColumnValues cv) {
		// this.values.addAll(cv.getValues());
		for (Object o : cv.getValues()) {
			if (!values.contains(o)) {
				values.add(o);
			}
		}
	}

	public ColumnValues clone() {
		try {
			return (ColumnValues) super.clone();
		} catch (CloneNotSupportedException e) {
			return null;
		}

	}

/*	@Override
	public int compareTo(ColumnValues o) {
		Object baseValue = values.get(0);
		Object newValue = o.values.get(0);
		int result = 0;
		//System.out.println("base="+baseValue+"  new="+newValue);
		//int result = baseValue.compareTo(newValue);
		return result;
	}*/

}
