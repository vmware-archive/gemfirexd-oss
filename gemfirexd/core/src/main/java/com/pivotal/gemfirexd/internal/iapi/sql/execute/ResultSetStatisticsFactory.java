/*

   Derby - Class com.pivotal.gemfirexd.internal.iapi.sql.execute.ResultSetStatisticsFactory

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

/*
 * Changes for GemFireXD distributed data platform (some marked by "GemStone changes")
 *
 * Portions Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
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

package com.pivotal.gemfirexd.internal.iapi.sql.execute;




import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultSet;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.RunTimeStatistics;
import com.pivotal.gemfirexd.internal.impl.sql.execute.rts.ResultSetStatistics;

/**
 * ResultSetStatisticsFactory provides a wrapper around all of
 * the result sets statistics objects needed in building the run time statistics.
 *
 */
public interface ResultSetStatisticsFactory 
{
	/**
		Module name for the monitor's module locating system.
	 */
	String MODULE = "com.pivotal.gemfirexd.internal.iapi.sql.execute.ResultSetStatisticsFactory";

	//
	// RunTimeStatistics Object
	//

	/**
	 * RunTimeStatistics creation.
	 *
	 * @param activation	The Activation we are generating the statistics for
	 * @param rs			The top ResultSet for the ResultSet tree
	 * @param subqueryTrackingArray	Array of subqueries, used for finding
	 *								materialized subqueries.
	 *
	 * @exception StandardException on error
	 */
	RunTimeStatistics getRunTimeStatistics(Activation activation, ResultSet rs,
										   NoPutResultSet[] subqueryTrackingArray)
		throws StandardException;


	//
	// ResultSetStatistics Objects
	//

	/**
		Get the matching ResultSetStatistics for the specified ResultSet.
	 */
	public ResultSetStatistics getResultSetStatistics(ResultSet rs);

	public ResultSetStatistics getResultSetStatistics(NoPutResultSet rs);

	public ResultSetStatistics getNoRowsResultSetStatistics(ResultSet rs);
	
	//GemStone changes BEGIN
	/* If we choose to have this method in ResultSet interface instead we
	 * can do so. Just that statistics is not inherent part of a ResultSet
	 * and hence this Mediator is used. 
	 */
	public long getResultSetMemoryUsage(ResultSet rs) throws StandardException;
	//GemStone changes END
}
