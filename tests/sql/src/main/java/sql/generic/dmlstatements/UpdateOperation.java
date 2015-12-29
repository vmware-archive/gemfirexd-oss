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

import java.util.List;
import java.util.Map;

/**
 * UpdateOperation
 * 
 * @author Namrata Thanvi
 */

public class UpdateOperation extends AbstractDMLOperation implements
		DMLOperation {
        
	public UpdateOperation(DMLExecutor executor, String statement) {
		super(executor, statement,Operation.UPDATE);
	}

        public UpdateOperation(DMLExecutor executor, String statement, DBRow preparedColumnMap ,   Map<String, DBRow> dataPopulatedForTheTable)   {
          super(executor, statement,preparedColumnMap,dataPopulatedForTheTable ,Operation.UPDATE);
        }
	@Override
	public void generateData(List<String> columnList) {
		int columnIndex = getIndexForSetColumns(getPreparedStmt());				
		// if there are parameterized update then set values from datagen
		if ( columnIndex > 0  &&  GenericDML.rand.nextInt() % 10 != 1  ) {
		getRowFromDataGeneratorAndLoadColumnValues(columnList.subList(0, columnIndex ));
		} 
		else 
		{
		  columnIndex = 0;
		}
		getColumnValuesFromDataBase(columnList.subList(columnIndex, columnList.size()));

	}

	public int getIndexForSetColumns(String stmt) {	  
		return ((stmt.substring(0, stmt.indexOf(" WHERE ")).length() - stmt
				.substring(0, stmt.indexOf(" WHERE ")).replaceAll("\\?", "")
				.length()) );

	}	
	
	 void setTableNameFromStatement()
	    {
	   int lastIndex = statement.trim().toLowerCase().indexOf("set" , 7);
           tableName = statement.trim().substring(7 , lastIndex).trim();
	    }

}
