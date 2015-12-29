/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.DMLVTIResultSet

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

package com.pivotal.gemfirexd.internal.impl.sql.execute;

import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.NoPutResultSet;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;
import com.pivotal.gemfirexd.internal.impl.sql.execute.xplain.XPLAINUtil;


/**
 * Base class for Insert, Delete & UpdateVTIResultSet
 */
abstract class DMLVTIResultSet extends DMLWriteResultSet
{

	// passed in at construction time

	NoPutResultSet sourceResultSet;
	NoPutResultSet savedSource;
	UpdatableVTIConstantAction	constants;
	TransactionController 	tc;

	private int						numOpens;
	boolean				firstExecute;

    /**
	 *
	 * @exception StandardException		Thrown on error
     */
    DMLVTIResultSet(NoPutResultSet source, 
						   Activation activation)
		throws StandardException
    {
		super(activation);
		sourceResultSet = source;
		constants = (UpdatableVTIConstantAction) constantAction;

        tc = activation.getTransactionController();
	}
	
	/**
		@exception StandardException Standard Derby error policy
	*/
	public void open() throws StandardException
	{
		setup();
		// Remember if this is the 1st execution
		firstExecute = (numOpens == 0);

		rowCount = 0;

		if (numOpens++ == 0)
		{
			sourceResultSet.openCore();
		}
		else
		{
			sourceResultSet.reopenCore();
		}

        openCore();
       
		/* Cache query plan text for source, before it gets blown away */
		if (lcc.getRunTimeStatisticsMode())
		{
			/* savedSource nulled after run time statistics generation */
			savedSource = sourceResultSet;
		}

		cleanUp(false);

		endTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
	} // end of open()

    protected abstract void openCore() throws StandardException;

	/**
	 * @see com.pivotal.gemfirexd.internal.iapi.sql.ResultSet#cleanUp
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void	cleanUp(boolean cleanupOnError) throws StandardException
	{
		/* Close down the source ResultSet tree */
        if( null != sourceResultSet)
            sourceResultSet.close(cleanupOnError);
		numOpens = 0;
		super.close(cleanupOnError);
	} // end of cleanUp

	public void finish() throws StandardException
    {

		sourceResultSet.finish();
		super.finish();
	} // end of finish
}
