/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.NoRowsResultSetImpl

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

package com.pivotal.gemfirexd.internal.impl.sql.execute;

import java.sql.Timestamp;

import com.pivotal.gemfirexd.internal.catalog.UUID;
import com.pivotal.gemfirexd.internal.engine.access.GemFireTransaction;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.jdbc.GemFireXDRuntimeException;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.i18n.MessageService;
import com.pivotal.gemfirexd.internal.iapi.services.loader.GeneratedMethod;
import com.pivotal.gemfirexd.internal.iapi.services.monitor.Monitor;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.services.stream.HeaderPrintWriter;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultDescription;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultSet;
import com.pivotal.gemfirexd.internal.iapi.sql.Row;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.StatementContext;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.ConglomerateDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDictionary;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.TableDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecutionFactory;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.NoPutResultSet;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ResultSetStatisticsFactory;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.impl.sql.execute.xplain.XPLAINUtil;

/**
 * Abstract ResultSet for implementations that do not return rows.
 * Examples are DDL statements, CALL statements and DML.
 * <P>
 * An implementation must provide a ResultSet.open() method
 * that performs the required action. 
 * <P>
 * ResultSet.returnsRows() returns false and any method
 * that fetches a row will throw an exception.
 *
 */
public abstract class NoRowsResultSetImpl implements ResultSet
{
//GemStone changes BEGIN
	protected final Activation    activation;
	/* final Activation    activation; */
	private UUID executionPlanID;
// GemStone changes END
	private boolean dumpedStats;
	NoPutResultSet[]	subqueryTrackingArray;

	protected boolean statisticsTimingOn;
	//GemStone changes BEGIN
        protected boolean                               runtimeStatisticsOn;
        protected boolean                               explainConnection;
        protected boolean                               statsEnabled;
	//GemStone changes END
	/** True if the result set has been opened, and not yet closed. */
	private boolean isOpen;

	/* Run time statistics variables */
	final LanguageConnectionContext lcc;
	protected long beginTime;
	protected long endTime;
	public long beginExecutionTime;
	public long endExecutionTime;
  
	NoRowsResultSetImpl(Activation activation)
		throws StandardException
	{
		this.activation = activation;

		if (SanityManager.DEBUG) {
			if (activation == null)
				SanityManager.THROWASSERT("activation is null in result set " + getClass());
		}

		lcc = activation.getLanguageConnectionContext();
		statisticsTimingOn = lcc.getStatisticsTiming();
	        //GemStone changes BEGIN
                runtimeStatisticsOn = lcc.getRunTimeStatisticsMode();
                explainConnection = lcc.explainConnection();
                statsEnabled = lcc.statsEnabled();
	        //GemStone changes END

		/* NOTE - We can't get the current time until after setting up the
		 * activation, as we end up using the activation to get the 
		 * LanguageConnectionContext.
		 */
		if (statisticsTimingOn) {
		  beginTime = XPLAINUtil.nanoTime();
		  beginExecutionTime = XPLAINUtil.currentTimeMillis();
		}

		StatementContext sc = lcc.getStatementContext();
		sc.setTopResultSet(this, (NoPutResultSet[]) null);

		// Pick up any materialized subqueries
		subqueryTrackingArray = sc.getSubqueryTrackingArray();
	}

	/**
	 * Set up the result set for use. Should always be called from
	 * <code>open()</code>.
	 *
	 * @exception StandardException thrown on error
	 */
// GemStone changes BEGIN
	protected void setup() throws StandardException {
	/* void  setup() throws StandardException */
            statisticsTimingOn = lcc.getStatisticsTiming();
            if (statisticsTimingOn && beginExecutionTime == 0) {
              beginTime = XPLAINUtil.nanoTime();
              beginExecutionTime = XPLAINUtil.currentTimeMillis();
            }
            runtimeStatisticsOn = lcc.getRunTimeStatisticsMode();
            explainConnection = lcc.explainConnection();
            statsEnabled = lcc.statsEnabled();
// GemStone changes END
		isOpen = true;
	}

    /**
	 * Returns FALSE
	 */
	 public final boolean	returnsRows() { return false; }

	/**
	 * Returns zero.
	 */
	public int	modifiedRowCount() { return 0; }
	
	public final Activation getActivation()
	{
		return activation;
	}

	/**
	 * Returns the row at the absolute position from the query, 
	 * and returns NULL when there is no such position.
	 * (Negative position means from the end of the result set.)
	 * Moving the cursor to an invalid position leaves the cursor
	 * positioned either before the first row (negative position)
	 * or after the last row (positive position).
	 * NOTE: An exception will be thrown on 0.
	 *
	 * @param row	The position.
	 * @return	The row at the absolute position, or NULL if no such position.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	public final ExecRow	getAbsoluteRow(int row) throws StandardException
	{
		/*
			The JDBC use of this class will never call here.
			Only the DB API used directly can get this exception.
		 */
		throw StandardException.newException(SQLState.LANG_DOES_NOT_RETURN_ROWS, "absolute");
	}

	/**
	 * Returns the row at the relative position from the current
	 * cursor position, and returns NULL when there is no such position.
	 * (Negative position means toward the beginning of the result set.)
	 * Moving the cursor to an invalid position leaves the cursor
	 * positioned either before the first row (negative position)
	 * or after the last row (positive position).
	 * NOTE: 0 is valid.
	 * NOTE: An exception is thrown if the cursor is not currently
	 * positioned on a row.
	 *
	 * @param row	The position.
	 * @return	The row at the relative position, or NULL if no such position.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	public final ExecRow	getRelativeRow(int row) throws StandardException
	{
		/*
			The JDBC use of this class will never call here.
			Only the DB API used directly can get this exception.
		 */
		throw StandardException.newException(SQLState.LANG_DOES_NOT_RETURN_ROWS, "relative");
	}

	/**
	 * Sets the current position to before the first row and returns NULL
	 * because there is no current row.
	 *
	 * @return	NULL.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	public final ExecRow	setBeforeFirstRow() 
		throws StandardException
	{
		/*
			The JDBC use of this class will never call here.
			Only the DB API used directly can get this exception.
		 */
		throw StandardException.newException(SQLState.LANG_DOES_NOT_RETURN_ROWS, "beforeFirst");
	}

	/**
	 * Returns the first row from the query, and returns NULL when there
	 * are no rows.
	 *
	 * @return	The first row, or NULL if no rows.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	public final ExecRow	getFirstRow() 
		throws StandardException
	{
		/*
			The JDBC use of this class will never call here.
			Only the DB API used directly can get this exception.
		 */
		throw StandardException.newException(SQLState.LANG_DOES_NOT_RETURN_ROWS, "first");
	}

	/**
     * No rows to return, so throw an exception.
	 *
	 * @exception StandardException		Always throws a
	 *									StandardException to indicate
	 *									that this method is not intended to
	 *									be used.
	 */
	public final ExecRow	getNextRow() throws StandardException
	{
		/*
			The JDBC use of this class will never call here.
			Only the DB API used directly can get this exception.
		 */
		throw StandardException.newException(SQLState.LANG_DOES_NOT_RETURN_ROWS, "next");
	}

	/**
	 * Returns the previous row from the query, and returns NULL when there
	 * are no more previous rows.
	 *
	 * @return	The previous row, or NULL if no more previous rows.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	public final ExecRow	getPreviousRow() 
		throws StandardException
	{
		/*
			The JDBC use of this class will never call here.
			Only the DB API used directly can get this exception.
		 */
		throw StandardException.newException(SQLState.LANG_DOES_NOT_RETURN_ROWS, "previous");
	}

	/**
	 * Returns the last row from the query, and returns NULL when there
	 * are no rows.
	 *
	 * @return	The last row, or NULL if no rows.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	public final ExecRow	getLastRow()
		throws StandardException
	{
		/*
			The JDBC use of this class will never call here.
			Only the DB API used directly can get this exception.
		 */
		throw StandardException.newException(SQLState.LANG_DOES_NOT_RETURN_ROWS, "last");
	}

	/**
	 * Sets the current position to after the last row and returns NULL
	 * because there is no current row.
	 *
	 * @return	NULL.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	public final ExecRow	setAfterLastRow() 
		throws StandardException
	{
		/*
			The JDBC use of this class will never call here.
			Only the DB API used directly can get this exception.
		 */
		throw StandardException.newException(SQLState.LANG_DOES_NOT_RETURN_ROWS, "afterLast");
	}

	/**
	 * Clear the current row. This is done after a commit on holdable
	 * result sets.
	 * This is a no-op on result set which do not provide rows.
	 */
	public final void clearCurrentRow() 
	{
		
	}

    /**
     * Determine if the cursor is before the first row in the result 
     * set.   
     *
     * @return true if before the first row, false otherwise. Returns
     * false when the result set contains no rows.
     */
    public final boolean checkRowPosition(int isType)
	{
		return false;
	}

	/**
	 * Returns the row number of the current row.  Row
	 * numbers start from 1 and go to 'n'.  Corresponds
	 * to row numbering used to position current row
	 * in the result set (as per JDBC).
	 *
	 * @return	the row number, or 0 if not on a row
	 *
	 */
	public final int getRowNumber()
	{
		return 0;
	}

	/**
     * Dump the stat if not already done so. Close all of the open subqueries.
	 *
	 * @exception StandardException thrown on error
	 */
	public void	close(final boolean cleanupOnError) throws StandardException
	{ 
		if (!isOpen)
			return;

		GemFireTransaction gft = (GemFireTransaction) this.lcc.getTransactionExecute();
		if(gft != null) {
		  gft.release();
		}
		
		if (! dumpedStats)
		{
			/*
			** If run time statistics tracing is turned on, then now is the
			** time to dump out the information.
			** NOTE - We make a special exception for commit.  If autocommit
			** is on, then the run time statistics from the autocommit is the
			** only one that the user would ever see.  So, we don't overwrite
			** the run time statistics object for a commit.
			*/
                  //GemStone changes BEGIN
			/*if (lcc.getRunTimeStatisticsMode() &&
				! doesCommit() &&*/
                          if (runtimeStatisticsOn && !doesCommit() && !lcc.isConnectionForRemote()
		  //GemStone changes END
				)
			{
				endExecutionTime = statisticsTimingOn ? XPLAINUtil.currentTimeMillis() : 0;

                ExecutionFactory ef = lcc.getLanguageConnectionFactory()
                    .getExecutionFactory();
                ResultSetStatisticsFactory rssf = ef.getResultSetStatisticsFactory();

                                // GemStone changes BEGIN
				/*(original code) lcc.setRunTimeStatisticsObject(
					rssf.getRunTimeStatistics(activation, this, subqueryTrackingArray));

				HeaderPrintWriter istream = lcc.getLogQueryPlan() ? Monitor.getStream() : null;*/
                                final HeaderPrintWriter istream;
                                final boolean logPlan = lcc.getLogQueryPlan();
                                if (logPlan || lcc.getRunTimeStatisticsModeExplicit()) {
                                  
                                  lcc.setRunTimeStatisticsObject(rssf.getRunTimeStatistics(activation,
                                      this, subqueryTrackingArray));
                        
                                  istream = logPlan ? Monitor.getStream() : null;
                                }
                                else {
                                  istream = null;
                                }
                                // GemStone changes END
				if (istream != null)
				{
					istream.printlnWithHeader(LanguageConnectionContext.xidStr + 
											  lcc.getTransactionExecute().getTransactionIdString() +
											  "), " +
											  LanguageConnectionContext.lccStr +
											  lcc.getInstanceNumber() +
											  "), " +
											  lcc.getRunTimeStatisticsObject().getStatementText() + " ******* " +
											  lcc.getRunTimeStatisticsObject().getStatementExecutionPlanText());
				}
				
				if (!cleanupOnError) {
				  // now explain gathered statistics, using an appropriate visitor
				  ResultSetStatisticsVisitor visitor = ef.getXPLAINFactory()
				      .getXPLAINVisitor(lcc, statsEnabled, explainConnection);
				  visitor.doXPLAIN(this, activation, true, statisticsTimingOn, false);
				  // GemStone changes BEGIN
				  executionPlanID = visitor.getStatementUUID();
				  // GemStone changes END
				}
			}
			dumpedStats = true;
		}

		/* This is the top ResultSet, 
		 * close all of the open subqueries.
		 */
		int staLength = (subqueryTrackingArray == null) ? 0 :
							subqueryTrackingArray.length;

		for (int index = 0; index < staLength; index++)
		{
			if (subqueryTrackingArray[index] == null)
			{
				continue;
			}
			if (subqueryTrackingArray[index].isClosed())
			{
				continue;
			}
			subqueryTrackingArray[index].close(cleanupOnError);
		}

                // GemStone changes BEGIN
                // reset the statistics variables
                if (!lcc.isConnectionForRemote()) {
                  resetStatistics();
                }
                // GemStone changes END
		isOpen = false;

		if (activation.isSingleExecution())
			activation.close();
	}

	/**
	 * Find out if the <code>ResultSet</code> is closed.
	 *
	 * @return <code>true</code> if closed, <code>false</code> otherwise
	 */
	public boolean isClosed() {
		return !isOpen;
	}

	public void	finish() throws StandardException
	{
	}

	/**
	 * Get the execution time in milliseconds.
	 *
	 * @return long		The execution time in milliseconds.
	 */
	public long getExecuteTime()
	{
		return endTime - beginTime;
	}

	/**
	 * Get the Timestamp for the beginning of execution.
	 *
	 * @return Timestamp		The Timestamp for the beginning of execution.
	 */
	public Timestamp getBeginExecutionTimestamp()
	{
		if (beginExecutionTime == 0)
		{
			return null;
		}
		else
		{
			return new Timestamp(beginExecutionTime);
		}
	}

	/**
	 * Get the Timestamp for the end of execution.
	 *
	 * @return Timestamp		The Timestamp for the end of execution.
	 */
	public Timestamp getEndExecutionTimestamp()
	{
		if (endExecutionTime == 0)
		{
			return null;
		}
		else
		{
			return new Timestamp(endExecutionTime);
		}
	}

	/**
	 * RESOLVE - This method will go away once it is overloaded in all subclasses.
	 * Return the query plan as a String.
	 *
	 * @param depth	Indentation level.
	 *
	 * @return String	The query plan as a String.
	 */
	public String getQueryPlanText(int depth)
	{
		return MessageService.getTextMessage(
				SQLState.LANG_GQPT_NOT_SUPPORTED,
				getClass().getName());
	}

	/**
	 * Return the total amount of time spent in this ResultSet
	 *
	 * @param type	CURRENT_RESULTSET_ONLY - time spent only in this ResultSet
	 *				ENTIRE_RESULTSET_TREE  - time spent in this ResultSet and below.
	 *
	 * @return long		The total amount of time spent (in milliseconds).
	 */
	public final long getTimeSpent(int type, int timeType)
	{

                // GemStone changes BEGIN
	        if(GemFireXDUtils.TracePlanGeneration) {
	          throw new AssertionError("should be overridden by subclasses ");
	        }
                // GemStone changes END
		/* RESOLVE - this should be overloaded in all subclasses */
		return 0;
	}

	/**
	 * @see ResultSet#getSubqueryTrackingArray
	 */
	public final NoPutResultSet[] getSubqueryTrackingArray(int numSubqueries)
	{
		if (subqueryTrackingArray == null)
		{
			subqueryTrackingArray = new NoPutResultSet[numSubqueries];
		}

		return subqueryTrackingArray;
	}

	/**
	 * @see ResultSet#getAutoGeneratedKeysResultset
	 */
	public ResultSet getAutoGeneratedKeysResultset()
	{
		//A non-null resultset would be returned only for an insert statement 
		return (ResultSet)null;
	}

	/**
		Return the cursor name, null in this case.

		@see ResultSet#getCursorName
	*/
	public String getCursorName() {
		return null;
	}

	// class implementation

// GemStone changes BEGIN
	/* * [sb: stopping virtual call, instead making XPLAIN static calls.
	 * Return the current time in milliseconds, if DEBUG and RunTimeStats is
	 * on, else return 0.  (Only pay price of system call if need to.)
	 *
	 * @return long		Current time in milliseconds.
	 * /
	protected final long getCurrentTimeMillis()
	{
		if (statisticsTimingOn)
		{
			return XPLAINUtil.nanoTime();
		}
		else
		{
			return 0;
		}
	}*/

	/**
	 * @see ResultSet#hasAutoGeneratedKeysResultSet
	 */
	@Override
	public boolean hasAutoGeneratedKeysResultSet() {
	  return false;
	}

	/**
	 * @see ResultSet#flushBatch
	 */
	@Override
	public void flushBatch() throws StandardException {
	}
	
	/**
         * @see ResultSet#closeBatch
         */
        @Override
        public void closeBatch() throws StandardException {
        }
// GemStone changes END

	/**
	  *	Run a check constraint against the current row. Raise an error if
	  * the check constraint is violated.
	  *
	  *	@param	checkGM			Generated code to run the check constraint.
	  * @param	checkName		Name of the constraint to check.
	  *	@param	heapConglom		Number of heap conglomerate.
	  *	@param	activation		Class in which checkGM lives.
	  *
	  * @exception StandardException thrown on error
	  */
	public	static	void	evaluateACheckConstraint
	(
	  GeneratedMethod checkGM,
	  String checkName,
	  long heapConglom,
	  Activation activation
	)
		throws StandardException
	{
		if (checkGM != null)
		{
			DataValueDescriptor checkBoolean;

			checkBoolean = (DataValueDescriptor) checkGM.invoke(activation);

			/* Throw exception if check constraint is violated.
			 * (Only if check constraint evaluates to false.)
			 */ 
			if ((checkBoolean != null) &&
				(! checkBoolean.isNull()) &&
				(! checkBoolean.getBoolean()))
			{
				/* Now we have a lot of painful work to get the
				 * table name for the error message.  All we have 
				 * is the conglomerate number to work with.
				 */
				DataDictionary dd = activation.getLanguageConnectionContext().getDataDictionary();
				ConglomerateDescriptor cd = dd.getConglomerateDescriptor( heapConglom );
				TableDescriptor td = dd.getTableDescriptor(cd.getTableID());

				StandardException se = StandardException.newException(SQLState.LANG_CHECK_CONSTRAINT_VIOLATED, 
					td.getQualifiedName(), checkName);

				throw se;
			}
		}

	}

	/**
	  *	Run check constraints against the current row. Raise an error if
	  * a check constraint is violated.
	  *
	  *	@param	checkGM			Generated code to run the check constraint.
	  *	@param	activation		Class in which checkGM lives.
	  *
	  * @exception StandardException thrown on error
	  */
	public	static	void	evaluateCheckConstraints
	(
	  GeneratedMethod checkGM,
	  Activation activation
	)
		throws StandardException
	{
		if (checkGM != null)
		{
			// Evaluate the expression containing the check constraints.
			// This expression will throw an exception if there is a
			// violation, so there is no need to check the result.
			checkGM.invoke(activation);
		}

	}
	  
	/**
	 * Does this ResultSet cause a commit or rollback.
	 *
	 * @return Whether or not this ResultSet cause a commit or rollback.
	 */
	public boolean doesCommit()
	{
		return false;
	}

	public java.sql.SQLWarning getWarnings() {
		return null;
	}

  // GemStone changes BEGIN
  public UUID getExecutionPlanID() {
    return executionPlanID;
  }
  public void markLocallyExecuted() {
    // nothing to do.
  }
  public void resetStatistics() {
    beginExecutionTime = 0;
    endExecutionTime = 0;
  }

  /**
   * {@inheritDoc}
   */
  public boolean isDistributedResultSet() {
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean addLockReference(GemFireTransaction tran) {
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean releaseLocks(GemFireTransaction tran) {
    return false;
  }
  
  @Override
  public final void checkCancellationFlag() throws StandardException {
    if (activation.isQueryCancelled()) {
      activation.checkCancellationFlag();
    }
  }
// GemStone changes END
}
