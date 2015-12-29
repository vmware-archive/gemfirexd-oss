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

/**
 * DMLExecutor
 * 
 * @author Namrata Thanvi
 */

public interface DMLExecutor {

	DBRow getRandomRowForDml(String tableName) ;

	List<DBRow>  getRandomRowsForDml(String query , Object value, int columnType, int NumberOfRequiredRows , String tableName) ;
	
	void executeStatement(DMLOperation abstractDMLOperation) ;

	DBRow.Column  getRandomColumnForDML(String query, Object value , int columnType) ;

	void query(DMLOperation operation) ;

	void executeStatements(List<DMLOperation> operationsMap) ;
	
	void commitAll();
	
}       

