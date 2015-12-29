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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Logger;

public class ResultRow implements Cloneable, Comparable<ResultRow> {
	Logger logger = Logger.getLogger("com.pivotal.gemfirexd");
	ArrayList<Object> row = new ArrayList<Object>();
	String rowKeyStr = "";

	public ResultRow(List<Object> nextRow) {
		for (Object o : nextRow) {
			ColumnValues cv = (ColumnValues) o;
			if (cv.aggType == ColumnDef.GROUPBY) {
				rowKeyStr += cv.groupValues("");
			}
			row.add(o);
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((row == null) ? 0 : row.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object newO) {
		ResultRow newRR = (ResultRow)newO;
		int result = this.compareTo(newRR);
		return (result == 0);
	}

	public void merge(ResultRow nextRow) {
		for (Object o : row) {
			ColumnValues cv = (ColumnValues) o;
			if (cv.aggType == ColumnDef.LISTAGG || cv.getValues().size() == 0) {
				int col = cv.colNumber - 1;
				ColumnValues newCv = (ColumnValues) nextRow.row.get(col);

				cv.mergeValues(newCv);
			}
		}
	}

	public ArrayList<Object> getValues() {
		return this.row;
	}

	/*
	 * public boolean haveDoneGroupByFieldValues() { return groupByColsSet; }
	 */
	public String toString() {
		StringBuilder sb = new StringBuilder("---------RR----------\n");
		for (Object o : row) {
			ColumnValues cv = (ColumnValues) o;
			sb.append(cv.columnName).append("-----AggType:").append(cv.aggType);
			sb.append("   ");
			sb.append(cv.toString()).append("\n");
		}
		sb.append("---------RR----------\n");

		return sb.toString();
	}

	@Override
	public int compareTo(ResultRow rr) {
		int result = 0;

		for (Object o : row) {
			ColumnValues cv = (ColumnValues) o;

			if (cv.aggType == ColumnDef.GROUPBY) {
				ColumnValues cvRR = (ColumnValues) rr.row.get(cv.colNumber - 1);
				String tmp = "";
				String tmpCvRR = "";
				if (cv.values.size() > 0) {
					Object cvVal = cv.values.get(0);
					if (cvVal != null)
						tmp = cvVal.toString();
				}

				if (cvRR.values.size() > 0) {
					Object cvRRval = cvRR.values.get(0);
					if (cvRRval != null)
						tmpCvRR = cvRRval.toString();
				}

				result = tmp.compareTo(tmpCvRR.toString());
				if (result != 0) {
					if (cv.sortOrder == ColumnDef.DESC) {
						result = result * -1;
					}
					break;
				}
			}
		}

		return result;
	}

	public String getKey() {
		return rowKeyStr;
	}

}
