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

//import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
/*
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
*/
public class ListAggProcedureTest {
/*
	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}


	
	@Test 
	public void testQueryBuilder() {
		ArrayList<ColumnDef> colList = new ArrayList<ColumnDef>();
		int i = 1;
		colList.add(new ColumnDef("Fld1", i));
		colList.add(new ColumnDef("Agg1 ASC", ++i, ","));
		colList.add(new ColumnDef("Agg2", ++i,","));
		String query = ListAggProcedure.buildQueryString(colList, "MyTable", "");
		//System.out.println(query);
		assertTrue(query, query.equals("<local> SELECT Fld1,Agg1,Agg2 FROM MyTable ORDER BY Fld1 ASC,Agg1 ASC,Agg2 ASC"));
		
	}

	@Test 
	public void testQueryDesc1Builder() {
		ArrayList<ColumnDef> colList = new ArrayList<ColumnDef>();
		int i = 1;

		colList.add(new ColumnDef("Fld1", i));
		colList.add(new ColumnDef("Field2 DESC", ++i));
		colList.add(new ColumnDef("Agg1 ASC", ++i,","));
		colList.add(new ColumnDef("Field3 ASC", ++i));
		colList.add(new ColumnDef("Agg2 DESC", ++i,","));

		String query = ListAggProcedure.buildQueryString(colList, "MyTable", "");
		//System.out.println(query);
		assertTrue(query, query.equals("<local> SELECT Fld1,Field2,Agg1,Field3,Agg2 FROM MyTable ORDER BY Fld1 ASC,Field2 DESC,Agg1 ASC,Field3 ASC,Agg2 DESC"));
		
	}
	
	@Test 
	public void testQueryDesc1BuilderWithWhereCaluse() {
		ArrayList<ColumnDef> colList = new ArrayList<ColumnDef>();
		int i = 1;

		colList.add(new ColumnDef("Fld1", i));
		colList.add(new ColumnDef("Field2 DESC", ++i));
		colList.add(new ColumnDef("Agg1 ASC", ++i,","));
		colList.add(new ColumnDef("Field3 ASC", ++i));
		colList.add(new ColumnDef("Agg2 DESC", ++i,","));

		String query = ListAggProcedure.buildQueryString(colList, "MyTable", "Fld1='abc'");
		//System.out.println(query);
		assertTrue(query, query.equals("<local> SELECT Fld1,Field2,Agg1,Field3,Agg2 FROM MyTable WHERE Fld1='abc' ORDER BY Fld1 ASC,Field2 DESC,Agg1 ASC,Field3 ASC,Agg2 DESC"));
		
	}


	@Test void testProcessInputParametersUseCase2WithWhere() {
		String listAggCols = "Agg1, Agg2";
		String groupByCols = "Fld1,Fld2 ASC,   Fld3 DESC";
		String delimiter = ",";
		try {
			ArrayList<ColumnDef> cols = ListAggProcedure.processInputParameters(listAggCols,
						groupByCols, delimiter);
			assertTrue("Col Count="+cols.size(), cols.size() == 5);
			int i=1;
			
			validateColumn(cols, i, new ColumnDef("Fld1",++i,","),ColumnDef.ASC);
			validateColumn(cols, i, new ColumnDef("Fld2",++i,","),ColumnDef.ASC);
			validateColumn(cols, i, new ColumnDef("Fld3",++i,","),ColumnDef.DESC);
			validateColumn(cols, i, new ColumnDef("Agg1",++i,","),ColumnDef.ASC);
			validateColumn(cols, i, new ColumnDef("Agg2",++i,","),ColumnDef.ASC);
			

		} catch (Exception e) {
			String err = e.getMessage();
			e.printStackTrace();
		}
		
	}
	@Test
	public void testProcessInputParameters1() {
		String listAggCols = "Agg1, Agg2";
		String groupByCols = "Fld1,Fld2 ASC,   Fld3 DESC";
		String delimiter = ",";
		try {
			ArrayList<ColumnDef> cols = ListAggProcedure.processInputParameters(listAggCols,
						groupByCols, delimiter);
			assertTrue("Col Count="+cols.size(), cols.size() == 5);
			int i=1;
			
			validateColumn(cols, i, new ColumnDef("Fld1",++i,","),ColumnDef.ASC);
			validateColumn(cols, i, new ColumnDef("Fld2",++i,","),ColumnDef.ASC);
			validateColumn(cols, i, new ColumnDef("Fld3",++i,","),ColumnDef.DESC);
			validateColumn(cols, i, new ColumnDef("Agg1",++i,","),ColumnDef.ASC);
			validateColumn(cols, i, new ColumnDef("Agg2",++i,","),ColumnDef.ASC);
			

		} catch (Exception e) {
			String err = e.getMessage();
			e.printStackTrace();
		}
		
	}
	
	
	@Test
	public void testProcessInputParametersAggListOverlapCols() {
		String listAggCols = "Agg1, Agg2";
		String groupByCols = "Fld1,Agg2 DESC, Fld2 ASC,Agg1 ,  Fld3 DESC";
		String delimiter = ",";
		try {
			ArrayList<ColumnDef> cols = ListAggProcedure.processInputParameters(listAggCols,
						groupByCols, delimiter);
			assertTrue("Col Count="+cols.size(), cols.size() == 5);
			int i=1;
			
			validateColumn(cols, i, new ColumnDef("Fld1",++i,","),ColumnDef.ASC);
			validateColumn(cols, i, new ColumnDef("Agg2 DESC",++i,","),ColumnDef.DESC);
			validateColumn(cols, i, new ColumnDef("Fld2",++i,","),ColumnDef.ASC);
			validateColumn(cols, i, new ColumnDef("Agg1",++i,","),ColumnDef.ASC);
			validateColumn(cols, i, new ColumnDef("Fld3 DESC",++i,","),ColumnDef.DESC);
			

		} catch (Exception e) {
			String err = e.getMessage();
			e.printStackTrace();
		}
		
	}

	private ColumnDef validateColumn(ArrayList<ColumnDef> cols, int i, ColumnDef columnDef, int sortOrder) {
		ColumnDef cd = cols.get(i-1);
		assertTrue(cd != null);
		assertTrue("Column Number for "+cd.columnName+" was "+cd.colNumber, cd.colNumber==i);
		assertTrue("Validate columnname = "+cd.columnName,cd.columnName.equals(columnDef.columnName));
		assertTrue("Sort order for "+cd.columnName+" was "+cd.sortOrder,cd.sortOrder == sortOrder);
		return cd;
		
	}

*/

	
}
