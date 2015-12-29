/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.RealResultSetStatisticsFactory

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

import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.sql.execute.GfxdSubqueryResultSet;
import com.pivotal.gemfirexd.internal.engine.store.AbstractCompactExecRow;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.i18n.MessageService;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatIdUtil;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.PreparedStatement;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultSet;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.NoPutResultSet;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ResultSetFactory;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ResultSetStatisticsFactory;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.RunTimeStatistics;
import com.pivotal.gemfirexd.internal.iapi.store.access.SortController;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;
import com.pivotal.gemfirexd.internal.impl.sql.execute.AnyResultSet;
import com.pivotal.gemfirexd.internal.impl.sql.execute.CurrentOfResultSet;
import com.pivotal.gemfirexd.internal.impl.sql.execute.DeleteCascadeResultSet;
import com.pivotal.gemfirexd.internal.impl.sql.execute.DeleteResultSet;
import com.pivotal.gemfirexd.internal.impl.sql.execute.DeleteVTIResultSet;
import com.pivotal.gemfirexd.internal.impl.sql.execute.DependentResultSet;
import com.pivotal.gemfirexd.internal.impl.sql.execute.DistinctScalarAggregateResultSet;
import com.pivotal.gemfirexd.internal.impl.sql.execute.DistinctScanResultSet;
import com.pivotal.gemfirexd.internal.impl.sql.execute.GroupedAggregateResultSet;
import com.pivotal.gemfirexd.internal.impl.sql.execute.HashJoinResultSet;
import com.pivotal.gemfirexd.internal.impl.sql.execute.HashLeftOuterJoinResultSet;
import com.pivotal.gemfirexd.internal.impl.sql.execute.HashScanResultSet;
import com.pivotal.gemfirexd.internal.impl.sql.execute.HashTableResultSet;
import com.pivotal.gemfirexd.internal.impl.sql.execute.IndexRowToBaseRowResultSet;
import com.pivotal.gemfirexd.internal.impl.sql.execute.InsertResultSet;
import com.pivotal.gemfirexd.internal.impl.sql.execute.InsertVTIResultSet;
import com.pivotal.gemfirexd.internal.impl.sql.execute.LastIndexKeyResultSet;
import com.pivotal.gemfirexd.internal.impl.sql.execute.MaterializedResultSet;
import com.pivotal.gemfirexd.internal.impl.sql.execute.NestedLoopJoinResultSet;
import com.pivotal.gemfirexd.internal.impl.sql.execute.NestedLoopLeftOuterJoinResultSet;
import com.pivotal.gemfirexd.internal.impl.sql.execute.NormalizeResultSet;
import com.pivotal.gemfirexd.internal.impl.sql.execute.OnceResultSet;
import com.pivotal.gemfirexd.internal.impl.sql.execute.ProjectRestrictResultSet;
import com.pivotal.gemfirexd.internal.impl.sql.execute.RowResultSet;
import com.pivotal.gemfirexd.internal.impl.sql.execute.ScalarAggregateResultSet;
import com.pivotal.gemfirexd.internal.impl.sql.execute.ScrollInsensitiveResultSet;
import com.pivotal.gemfirexd.internal.impl.sql.execute.SetOpResultSet;
import com.pivotal.gemfirexd.internal.impl.sql.execute.SortResultSet;
import com.pivotal.gemfirexd.internal.impl.sql.execute.TableScanResultSet;
import com.pivotal.gemfirexd.internal.impl.sql.execute.UnionResultSet;
import com.pivotal.gemfirexd.internal.impl.sql.execute.UpdateResultSet;
import com.pivotal.gemfirexd.internal.impl.sql.execute.VTIResultSet;
import com.pivotal.gemfirexd.internal.impl.sql.execute.WindowResultSet;
import com.pivotal.gemfirexd.internal.impl.sql.execute.rts.RealAnyResultSetStatistics;
import com.pivotal.gemfirexd.internal.impl.sql.execute.rts.RealCurrentOfStatistics;
import com.pivotal.gemfirexd.internal.impl.sql.execute.rts.RealDeleteCascadeResultSetStatistics;
import com.pivotal.gemfirexd.internal.impl.sql.execute.rts.RealDeleteResultSetStatistics;
import com.pivotal.gemfirexd.internal.impl.sql.execute.rts.RealDeleteVTIResultSetStatistics;
import com.pivotal.gemfirexd.internal.impl.sql.execute.rts.RealDistinctScalarAggregateStatistics;
import com.pivotal.gemfirexd.internal.impl.sql.execute.rts.RealDistinctScanStatistics;
import com.pivotal.gemfirexd.internal.impl.sql.execute.rts.RealGroupedAggregateStatistics;
import com.pivotal.gemfirexd.internal.impl.sql.execute.rts.RealHashJoinStatistics;
import com.pivotal.gemfirexd.internal.impl.sql.execute.rts.RealHashLeftOuterJoinStatistics;
import com.pivotal.gemfirexd.internal.impl.sql.execute.rts.RealHashScanStatistics;
import com.pivotal.gemfirexd.internal.impl.sql.execute.rts.RealHashTableStatistics;
import com.pivotal.gemfirexd.internal.impl.sql.execute.rts.RealIndexRowToBaseRowStatistics;
import com.pivotal.gemfirexd.internal.impl.sql.execute.rts.RealInsertResultSetStatistics;
import com.pivotal.gemfirexd.internal.impl.sql.execute.rts.RealInsertVTIResultSetStatistics;
import com.pivotal.gemfirexd.internal.impl.sql.execute.rts.RealJoinResultSetStatistics;
import com.pivotal.gemfirexd.internal.impl.sql.execute.rts.RealLastIndexKeyScanStatistics;
import com.pivotal.gemfirexd.internal.impl.sql.execute.rts.RealMaterializedResultSetStatistics;
import com.pivotal.gemfirexd.internal.impl.sql.execute.rts.RealNestedLoopJoinStatistics;
import com.pivotal.gemfirexd.internal.impl.sql.execute.rts.RealNestedLoopLeftOuterJoinStatistics;
import com.pivotal.gemfirexd.internal.impl.sql.execute.rts.RealNormalizeResultSetStatistics;
import com.pivotal.gemfirexd.internal.impl.sql.execute.rts.RealOnceResultSetStatistics;
import com.pivotal.gemfirexd.internal.impl.sql.execute.rts.RealProjectRestrictStatistics;
import com.pivotal.gemfirexd.internal.impl.sql.execute.rts.RealRowCountStatistics;
import com.pivotal.gemfirexd.internal.impl.sql.execute.rts.RealRowResultSetStatistics;
import com.pivotal.gemfirexd.internal.impl.sql.execute.rts.RealScalarAggregateStatistics;
import com.pivotal.gemfirexd.internal.impl.sql.execute.rts.RealScrollInsensitiveResultSetStatistics;
import com.pivotal.gemfirexd.internal.impl.sql.execute.rts.RealSetOpResultSetStatistics;
import com.pivotal.gemfirexd.internal.impl.sql.execute.rts.RealSortStatistics;
import com.pivotal.gemfirexd.internal.impl.sql.execute.rts.RealTableScanStatistics;
import com.pivotal.gemfirexd.internal.impl.sql.execute.rts.RealUnionResultSetStatistics;
import com.pivotal.gemfirexd.internal.impl.sql.execute.rts.RealUpdateResultSetStatistics;
import com.pivotal.gemfirexd.internal.impl.sql.execute.rts.RealVTIStatistics;
import com.pivotal.gemfirexd.internal.impl.sql.execute.rts.RealWindowResultSetStatistics;
import com.pivotal.gemfirexd.internal.impl.sql.execute.rts.ResultSetStatistics;
import com.pivotal.gemfirexd.internal.impl.sql.execute.rts.RunTimeStatisticsImpl;

import java.util.Properties;
import java.util.Vector;

/**
 * ResultSetStatisticsFactory provides a wrapper around all of
 * objects associated with run time statistics.
 * <p>
 * This implementation of the protocol is for returning the "real"
 * run time statistics.  We have modularized this so that we could
 * have an implementation that just returns null for each of the
 * objects should we decided to provide a configuration without
 * the run time statistics feature.
 *
 */
public class RealResultSetStatisticsFactory 
		implements ResultSetStatisticsFactory
{

	//
	// ExecutionFactory interface
	//
	//
	// ResultSetStatisticsFactory interface
	//

        private final boolean TraceMemEstimation = SanityManager.DEBUG_ON("TraceMemoryEstimation");
	/**
		@see ResultSetStatisticsFactory#getRunTimeStatistics
	 */
	public RunTimeStatistics getRunTimeStatistics(
			Activation activation, 
			ResultSet rs,
			NoPutResultSet[] subqueryTrackingArray)
		throws StandardException
	{
		PreparedStatement preStmt = activation.getPreparedStatement();

		// If the prepared statement is null then the result set is being
		// finished as a result of a activation being closed during a recompile.
		// In this case statistics should not be generated.
		if (preStmt == null)
			return null;




		ResultSetStatistics topResultSetStatistics;

		if (rs instanceof NoPutResultSet)
		{
			topResultSetStatistics =
									getResultSetStatistics((NoPutResultSet) rs);
		}
		else
		{
			topResultSetStatistics = getResultSetStatistics(rs);
		}

		/* Build up the info on the materialized subqueries */
		int subqueryTrackingArrayLength =
				(subqueryTrackingArray == null) ? 0 :
					subqueryTrackingArray.length;
		ResultSetStatistics[] subqueryRSS =
				new ResultSetStatistics[subqueryTrackingArrayLength];
		boolean anyAttached = false;
		for (int index = 0; index < subqueryTrackingArrayLength; index++)
		{
			if (subqueryTrackingArray[index] != null &&
				subqueryTrackingArray[index].getPointOfAttachment() == -1)
			{
				subqueryRSS[index] =
						getResultSetStatistics(subqueryTrackingArray[index]);
				anyAttached = true;
			}
		}
		if (anyAttached == false)
		{
			subqueryRSS = null;
		}

		// Get the info on all of the materialized subqueries (attachment point = -1)
		return new RunTimeStatisticsImpl(
								preStmt.getSPSName(),
								activation.getCursorName(),
								preStmt.getUserQueryString(activation.getLanguageConnectionContext()),
								preStmt.getCompileTimeInMillis(),
								preStmt.getParseTimeInMillis(),
								preStmt.getBindTimeInMillis(),
								preStmt.getOptimizeTimeInMillis(),
								preStmt.getGenerateTimeInMillis(),
								rs.getExecuteTime(),
								preStmt.getBeginCompileTimestamp(),
								preStmt.getEndCompileTimestamp(),
								rs.getBeginExecutionTimestamp(),
								rs.getEndExecutionTimestamp(),
								subqueryRSS,
								topResultSetStatistics);
	}

	/**
		@see ResultSetStatisticsFactory#getResultSetStatistics
	 */
	public ResultSetStatistics getResultSetStatistics(ResultSet rs)
	{
		if (!rs.returnsRows())
		{
			return getNoRowsResultSetStatistics(rs);
		}
		else if (rs instanceof NoPutResultSet)
		{
			return getResultSetStatistics((NoPutResultSet) rs);
		}
		else
		{
			return null;
		}
	}

	public ResultSetStatistics getNoRowsResultSetStatistics(ResultSet rs)
	{
		ResultSetStatistics retval = null;

		/* We need to differentiate based on instanceof in order
		 * to find the right constructor to call.  This is ugly,
		 * but if we don't do instanceof then rs is always seen as an
		 * interface instead of a class when we try to overload 
		 * a method with both.
		 */
		if( rs instanceof InsertResultSet)
		{
			InsertResultSet irs = (InsertResultSet) rs;

			retval = new RealInsertResultSetStatistics(
									irs.rowCount,
									irs.constants.deferred,
									irs.constants.irgs.length,
									irs.userSpecifiedBulkInsert,
									irs.bulkInsertPerformed,
									irs.constants.lockMode ==
										TransactionController.MODE_TABLE,
									irs.getExecuteTime(), 
									getResultSetStatistics(irs.savedSource)
									);

			irs.savedSource = null;
		}
		else if( rs instanceof InsertVTIResultSet)
		{
			InsertVTIResultSet iVTIrs = (InsertVTIResultSet) rs;

			retval = new RealInsertVTIResultSetStatistics(
									iVTIrs.rowCount,
									iVTIrs.constants.deferred,
									iVTIrs.getExecuteTime(), 
									getResultSetStatistics(iVTIrs.savedSource)
									);

			iVTIrs.savedSource = null;
		}
		else if( rs instanceof UpdateResultSet)
		{
			UpdateResultSet urs = (UpdateResultSet) rs;

			retval = new RealUpdateResultSetStatistics(
									urs.rowCount,
									urs.constants.deferred,
									urs.constants.irgs.length,
									urs.constants.lockMode ==
										TransactionController.MODE_TABLE,
									urs.getExecuteTime(),
									getResultSetStatistics(urs.savedSource)
									);

			urs.savedSource = null;
		}
		else if( rs instanceof DeleteCascadeResultSet)
		{
			DeleteCascadeResultSet dcrs = (DeleteCascadeResultSet) rs;
			int dependentTrackingArrayLength =
				(dcrs.dependentResultSets == null) ? 0 :
					dcrs.dependentResultSets.length;
			ResultSetStatistics[] dependentTrackingArray =
				new ResultSetStatistics[dependentTrackingArrayLength];
			boolean anyAttached = false;
			for (int index = 0; index < dependentTrackingArrayLength; index++)
			{
				if (dcrs.dependentResultSets[index] != null)
				{
					dependentTrackingArray[index] =
										getResultSetStatistics(
											dcrs.dependentResultSets[index]);
					anyAttached = true;
				}
			}
			if (! anyAttached)
			{
				dependentTrackingArray = null;
			}

			retval = new RealDeleteCascadeResultSetStatistics(
									dcrs.rowCount,
									dcrs.constants.deferred,
									dcrs.constants.irgs.length,
									dcrs.constants.lockMode ==
										TransactionController.MODE_TABLE,
									dcrs.getExecuteTime(),
									getResultSetStatistics(dcrs.savedSource),
									dependentTrackingArray
									);

			dcrs.savedSource = null;
		}
		else if( rs instanceof DeleteResultSet)
		{
			DeleteResultSet drs = (DeleteResultSet) rs;

			retval = new RealDeleteResultSetStatistics(
									drs.rowCount,
									drs.constants.deferred,
									drs.constants.irgs.length,
									drs.constants.lockMode ==
										TransactionController.MODE_TABLE,
									drs.getExecuteTime(),
									getResultSetStatistics(drs.savedSource)
									);

			drs.savedSource = null;
		}
		else if( rs instanceof DeleteVTIResultSet)
		{
			DeleteVTIResultSet dVTIrs = (DeleteVTIResultSet) rs;

			retval = new RealDeleteVTIResultSetStatistics(
									dVTIrs.rowCount,
									dVTIrs.getExecuteTime(), 
									getResultSetStatistics(dVTIrs.savedSource)
									);

			dVTIrs.savedSource = null;
		}


		return retval;
	}

	public ResultSetStatistics getResultSetStatistics(NoPutResultSet rs)
	{
		/* We need to differentiate based on instanceof in order
		 * to find the right constructor to call.  This is ugly,
		 * but if we don't do instanceof then rs is always seen as an
		 * interface instead of a class when we try to overload 
		 * a method with both.
		 */
		if( rs instanceof ProjectRestrictResultSet)
		{
			ProjectRestrictResultSet prrs = (ProjectRestrictResultSet) rs;
			int subqueryTrackingArrayLength =
				(prrs.subqueryTrackingArray == null) ? 0 :
					prrs.subqueryTrackingArray.length;
			ResultSetStatistics[] subqueryTrackingArray =
				new ResultSetStatistics[subqueryTrackingArrayLength];
			boolean anyAttached = false;
			for (int index = 0; index < subqueryTrackingArrayLength; index++)
			{
				if (prrs.subqueryTrackingArray[index] != null &&
					prrs.subqueryTrackingArray[index].getPointOfAttachment() ==
						prrs.resultSetNumber)
				{
					subqueryTrackingArray[index] =
										getResultSetStatistics(
											prrs.subqueryTrackingArray[index]);
					anyAttached = true;
				}
			}
			if (! anyAttached)
			{
				subqueryTrackingArray = null;
			}

			return new RealProjectRestrictStatistics(
											prrs.numOpens,
											prrs.rowsSeen,
											prrs.rowsFiltered,
											prrs.constructorTime,
											prrs.openTime,
											prrs.nextTime,
											prrs.closeTime,
											prrs.resultSetNumber,
											prrs.restrictionTime,
											prrs.projectionTime,
											subqueryTrackingArray,
											(prrs.restriction != null),
											prrs.doesProjection,
											prrs.optimizerEstimatedRowCount,
											prrs.optimizerEstimatedCost,
											getResultSetStatistics(prrs.source)
											);
		}
		else if (rs instanceof RowCountResultSet)
		{
			RowCountResultSet rcrs = (RowCountResultSet) rs;

			return new RealRowCountStatistics(
				rcrs.numOpens,
				rcrs.rowsSeen,
				rcrs.rowsFiltered,
				rcrs.constructorTime,
				rcrs.openTime,
				rcrs.nextTime,
				rcrs.closeTime,
				rcrs.resultSetNumber,
				rcrs.optimizerEstimatedRowCount,
				rcrs.optimizerEstimatedCost,
				getResultSetStatistics(rcrs.source) );
		}
		else if (rs instanceof SortResultSet)
		{
			SortResultSet srs = (SortResultSet) rs;

			return new RealSortStatistics(
											srs.numOpens,
											srs.rowsSeen,
											srs.rowsFiltered,
											srs.constructorTime,
											srs.openTime,
											srs.nextTime,
											srs.closeTime,
											srs.resultSetNumber,
											srs.rowsInput,
											srs.rowsReturned,
											srs.distinct,
											srs.isInSortedOrder,
											srs.sortProperties,
											srs.optimizerEstimatedRowCount,
											srs.optimizerEstimatedCost,
											getResultSetStatistics(srs.getSource())
										);
		}
		else if (rs instanceof DistinctScalarAggregateResultSet)
		{
			DistinctScalarAggregateResultSet dsars = (DistinctScalarAggregateResultSet) rs;

			return new RealDistinctScalarAggregateStatistics(
											dsars.numOpens,
											dsars.rowsSeen,
											dsars.rowsFiltered,
											dsars.constructorTime,
											dsars.openTime,
											dsars.nextTime,
											dsars.closeTime,
											dsars.resultSetNumber,
											dsars.rowsInput,
											dsars.optimizerEstimatedRowCount,
											dsars.optimizerEstimatedCost,
											getResultSetStatistics(dsars.source)
										);
		}
		else if (rs instanceof ScalarAggregateResultSet)
		{
			ScalarAggregateResultSet sars = (ScalarAggregateResultSet) rs;

			return new RealScalarAggregateStatistics(
											sars.numOpens,
											sars.rowsSeen,
											sars.rowsFiltered,
											sars.constructorTime,
											sars.openTime,
											sars.nextTime,
											sars.closeTime,
											sars.resultSetNumber,
											sars.singleInputRow,
											sars.rowsInput,
											sars.optimizerEstimatedRowCount,
											sars.optimizerEstimatedCost,
											getResultSetStatistics(sars.source)
										);
		}
		else if (rs instanceof GroupedAggregateResultSet)
		{
			GroupedAggregateResultSet gars = (GroupedAggregateResultSet) rs;

			return new RealGroupedAggregateStatistics(
											gars.numOpens,
											gars.rowsSeen,
											gars.rowsFiltered,
											gars.constructorTime,
											gars.openTime,
											gars.nextTime,
											gars.closeTime,
											gars.resultSetNumber,
											gars.rowsInput,
											gars.hasDistinctAggregate,
											gars.isInSortedOrder,
											gars.sortProperties,
											gars.optimizerEstimatedRowCount,
											gars.optimizerEstimatedCost,
											getResultSetStatistics(gars.source)
										);
		}
		else if (rs instanceof TableScanResultSet)
		{
			boolean instantaneousLocks = false;
			TableScanResultSet tsrs = (TableScanResultSet) rs;
			String startPosition = null;
			String stopPosition = null;
			String isolationLevel =  null;
			String lockString = null;
			String lockRequestString = null;

			switch (tsrs.isolationLevel)
			{
				case TransactionController.ISOLATION_SERIALIZABLE:
					isolationLevel = 
                        MessageService.getTextMessage(
                            SQLState.LANG_SERIALIZABLE);
					break;

				case TransactionController.ISOLATION_REPEATABLE_READ:
					isolationLevel = 
                        MessageService.getTextMessage(
                            SQLState.LANG_REPEATABLE_READ);
					break;

				case TransactionController.ISOLATION_READ_COMMITTED_NOHOLDLOCK:
					instantaneousLocks = true;
					//fall through
				case TransactionController.ISOLATION_READ_COMMITTED:
					isolationLevel = MessageService.getTextMessage(
												SQLState.LANG_READ_COMMITTED);
					break;

				case TransactionController.ISOLATION_READ_UNCOMMITTED:
					isolationLevel = 
                        MessageService.getTextMessage(
                            SQLState.LANG_READ_UNCOMMITTED);
					break;
			}

			if (tsrs.forUpdate())
			{
				lockString = MessageService.getTextMessage(
												SQLState.LANG_EXCLUSIVE);
			}
			else
			{
				if (instantaneousLocks)
				{
					lockString = MessageService.getTextMessage(
											SQLState.LANG_INSTANTANEOUS_SHARE);
				}
				else
				{
					lockString = MessageService.getTextMessage(
														SQLState.LANG_SHARE);
				}
			}

			switch (tsrs.lockMode)
			{
				case TransactionController.MODE_TABLE:
					// RESOLVE: Not sure this will really work, as we
					// are tacking together English words to make a phrase.
					// Will this work in other languages?
					lockRequestString = lockString + " " +
										MessageService.getTextMessage(
											SQLState.LANG_TABLE);
					break;

				case TransactionController.MODE_RECORD:
					// RESOLVE: Not sure this will really work, as we
					// are tacking together English words to make a phrase.
					// Will this work in other languages?
					lockRequestString = lockString + " " +
										MessageService.getTextMessage(
											SQLState.LANG_ROW);
					break;
			}

			if (tsrs.indexName != null)
			{
				/* Start and stop position strings will be non-null
			 	* if the TSRS has been closed.  Otherwise, we go off
			 	* and build the strings now.
			 	*/
				startPosition = tsrs.startPositionString;
				if (startPosition == null)
				{
					startPosition = tsrs.printStartPosition();
				}
				stopPosition = tsrs.stopPositionString;
				if (stopPosition == null)
				{
					stopPosition = tsrs.printStopPosition();
				}
			}

			return new 
                RealTableScanStatistics(
                    tsrs.numOpens,
                    tsrs.rowsSeen,
                    tsrs.rowsFiltered,
                    tsrs.constructorTime,
                    tsrs.openTime,
                    tsrs.nextTime,
                    tsrs.closeTime,
                    tsrs.resultSetNumber,
                    tsrs.tableName,
					tsrs.userSuppliedOptimizerOverrides,
                    tsrs.indexName,
                    tsrs.isConstraint(),
                    tsrs.printQualifiers(tsrs.qualifiers, true),
                    tsrs.getScanProperties(),
                    startPosition,
                    stopPosition,
                    isolationLevel,
                    lockRequestString,
                    tsrs.rowsPerRead,
                    tsrs.coarserLock(),
                    tsrs.optimizerEstimatedRowCount,
                    tsrs.optimizerEstimatedCost);
		}

		else if (rs instanceof LastIndexKeyResultSet )
		{
			LastIndexKeyResultSet lrs = (LastIndexKeyResultSet) rs;
			String isolationLevel =  null;
			String lockRequestString = null;

			switch (lrs.isolationLevel)
			{
				case TransactionController.ISOLATION_SERIALIZABLE:
					isolationLevel = 
                        MessageService.getTextMessage(
                            SQLState.LANG_SERIALIZABLE);
					break;

				case TransactionController.ISOLATION_REPEATABLE_READ:
					isolationLevel = 
                        MessageService.getTextMessage(
                            SQLState.LANG_REPEATABLE_READ);
					break;

				case TransactionController.ISOLATION_READ_COMMITTED_NOHOLDLOCK:
				case TransactionController.ISOLATION_READ_COMMITTED:
					isolationLevel = MessageService.getTextMessage(
												SQLState.LANG_READ_COMMITTED);
					break;

				case TransactionController.ISOLATION_READ_UNCOMMITTED:
					isolationLevel = MessageService.getTextMessage(
												SQLState.LANG_READ_UNCOMMITTED);
                    break;
			}

			switch (lrs.lockMode)
			{
				case TransactionController.MODE_TABLE:
					lockRequestString = MessageService.getTextMessage(
													SQLState.LANG_SHARE_TABLE);
					break;

				case TransactionController.MODE_RECORD:
					lockRequestString = MessageService.getTextMessage(
													SQLState.LANG_SHARE_ROW);
					break;
			}

			return new RealLastIndexKeyScanStatistics(
											lrs.numOpens,
											lrs.constructorTime,
											lrs.openTime,
											lrs.nextTime,
											lrs.closeTime,
											lrs.resultSetNumber,
											lrs.tableName,
											lrs.indexName,
											isolationLevel,
											lockRequestString,
											lrs.optimizerEstimatedRowCount,
											lrs.optimizerEstimatedCost);
		}
		else if (rs instanceof HashLeftOuterJoinResultSet)
		{
			HashLeftOuterJoinResultSet hlojrs =
				(HashLeftOuterJoinResultSet) rs;

			return new RealHashLeftOuterJoinStatistics(
											hlojrs.numOpens,
											hlojrs.rowsSeen,
											hlojrs.rowsFiltered,
											hlojrs.constructorTime,
											hlojrs.openTime,
											hlojrs.nextTime,
											hlojrs.closeTime,
											hlojrs.resultSetNumber,
											hlojrs.rowsSeenLeft,
											hlojrs.rowsSeenRight,
											hlojrs.rowsReturned,
											hlojrs.restrictionTime,
											hlojrs.optimizerEstimatedRowCount,
											hlojrs.optimizerEstimatedCost,
											hlojrs.userSuppliedOptimizerOverrides,
											getResultSetStatistics(
												hlojrs.leftResultSet),
											getResultSetStatistics(
												hlojrs.rightResultSet),
											hlojrs.emptyRightRowsReturned);
		}
		else if (rs instanceof NestedLoopLeftOuterJoinResultSet)
		{
			NestedLoopLeftOuterJoinResultSet nllojrs =
				(NestedLoopLeftOuterJoinResultSet) rs;

			return new RealNestedLoopLeftOuterJoinStatistics(
											nllojrs.numOpens,
											nllojrs.rowsSeen,
											nllojrs.rowsFiltered,
											nllojrs.constructorTime,
											nllojrs.openTime,
											nllojrs.nextTime,
											nllojrs.closeTime,
											nllojrs.resultSetNumber,
											nllojrs.rowsSeenLeft,
											nllojrs.rowsSeenRight,
											nllojrs.rowsReturned,
											nllojrs.restrictionTime,
											nllojrs.optimizerEstimatedRowCount,
											nllojrs.optimizerEstimatedCost,
											nllojrs.userSuppliedOptimizerOverrides,
											getResultSetStatistics(
												nllojrs.leftResultSet),
											getResultSetStatistics(
												nllojrs.rightResultSet),
											nllojrs.emptyRightRowsReturned);
		}
		else if (rs instanceof HashJoinResultSet)
		{
			HashJoinResultSet hjrs = (HashJoinResultSet) rs;

			return new RealHashJoinStatistics(
											hjrs.numOpens,
											hjrs.rowsSeen,
											hjrs.rowsFiltered,
											hjrs.constructorTime,
											hjrs.openTime,
											hjrs.nextTime,
											hjrs.closeTime,
											hjrs.resultSetNumber,
											hjrs.rowsSeenLeft,
											hjrs.rowsSeenRight,
											hjrs.rowsReturned,
											hjrs.restrictionTime,
											hjrs.oneRowRightSide,
											hjrs.optimizerEstimatedRowCount,
											hjrs.optimizerEstimatedCost,
											hjrs.userSuppliedOptimizerOverrides,
											getResultSetStatistics(
												hjrs.leftResultSet),
											getResultSetStatistics(
												hjrs.rightResultSet)
											);
		}
		else if (rs instanceof NestedLoopJoinResultSet)
		{
			NestedLoopJoinResultSet nljrs = (NestedLoopJoinResultSet) rs;

			return new RealNestedLoopJoinStatistics(
											nljrs.numOpens,
											nljrs.rowsSeen,
											nljrs.rowsFiltered,
											nljrs.constructorTime,
											nljrs.openTime,
											nljrs.nextTime,
											nljrs.closeTime,
											nljrs.resultSetNumber,
											nljrs.rowsSeenLeft,
											nljrs.rowsSeenRight,
											nljrs.rowsReturned,
											nljrs.restrictionTime,
											nljrs.oneRowRightSide,
											nljrs.optimizerEstimatedRowCount,
											nljrs.optimizerEstimatedCost,
											nljrs.userSuppliedOptimizerOverrides,
											getResultSetStatistics(
												nljrs.leftResultSet),
											getResultSetStatistics(
												nljrs.rightResultSet)
											);
		}
		else if (rs instanceof IndexRowToBaseRowResultSet)
		{
			IndexRowToBaseRowResultSet irtbrrs =
											(IndexRowToBaseRowResultSet) rs;

			return new RealIndexRowToBaseRowStatistics(
											irtbrrs.numOpens,
											irtbrrs.rowsSeen,
											irtbrrs.rowsFiltered,
											irtbrrs.constructorTime,
											irtbrrs.openTime,
											irtbrrs.nextTime,
											irtbrrs.closeTime,
											irtbrrs.resultSetNumber,
											irtbrrs.indexName,
											irtbrrs.accessedHeapCols,
											irtbrrs.optimizerEstimatedRowCount,
											irtbrrs.optimizerEstimatedCost,
											getResultSetStatistics(
																irtbrrs.source)
											);
		}
		else if (rs instanceof WindowResultSet)
		{
			WindowResultSet wrs = (WindowResultSet) rs;

			return new RealWindowResultSetStatistics(											
											wrs.numOpens,
											wrs.rowsSeen,
											wrs.rowsFiltered,
											wrs.constructorTime,
											wrs.openTime,
											wrs.nextTime,
											wrs.closeTime,
											wrs.resultSetNumber,											
											wrs.optimizerEstimatedRowCount,
											wrs.optimizerEstimatedCost,
											getResultSetStatistics(wrs.source)		
											);
		}
		else if (rs instanceof RowResultSet)
		{
			RowResultSet rrs = (RowResultSet) rs;

			return new RealRowResultSetStatistics(
											rrs.numOpens,
											rrs.rowsSeen,
											rrs.rowsFiltered,
											rrs.constructorTime,
											rrs.openTime,
											rrs.nextTime,
											rrs.closeTime,
											rrs.resultSetNumber,
											rrs.rowsReturned,
											rrs.optimizerEstimatedRowCount,
											rrs.optimizerEstimatedCost);
		}
		else if (rs instanceof SetOpResultSet)
		{
			SetOpResultSet srs = (SetOpResultSet) rs;

			return new RealSetOpResultSetStatistics(
											srs.getOpType(),
											srs.numOpens,
											srs.rowsSeen,
											srs.rowsFiltered,
											srs.constructorTime,
											srs.openTime,
											srs.nextTime,
											srs.closeTime,
											srs.getResultSetNumber(),
											srs.getRowsSeenLeft(),
											srs.getRowsSeenRight(),
											srs.getRowsReturned(),
											srs.optimizerEstimatedRowCount,
											srs.optimizerEstimatedCost,
											getResultSetStatistics(srs.getLeftSourceInput()),
											getResultSetStatistics(srs.getRightSourceInput())
											);
		}
		else if (rs instanceof UnionResultSet)
		{
			UnionResultSet urs = (UnionResultSet)rs;

			return new RealUnionResultSetStatistics(
											urs.numOpens,
											urs.rowsSeen,
											urs.rowsFiltered,
											urs.constructorTime,
											urs.openTime,
											urs.nextTime,
											urs.closeTime,
											urs.resultSetNumber,
											urs.rowsSeenLeft,
											urs.rowsSeenRight,
											urs.rowsReturned,
											urs.optimizerEstimatedRowCount,
											urs.optimizerEstimatedCost,
											getResultSetStatistics(urs.source1),
											getResultSetStatistics(urs.source2)
											);
		}
		else if (rs instanceof AnyResultSet)
		{
			AnyResultSet ars = (AnyResultSet) rs;

			return new RealAnyResultSetStatistics(
											ars.numOpens,
											ars.rowsSeen,
											ars.rowsFiltered,
											ars.constructorTime,
											ars.openTime,
											ars.nextTime,
											ars.closeTime,
											ars.resultSetNumber,
											ars.subqueryNumber,
											ars.pointOfAttachment,
											ars.optimizerEstimatedRowCount,
											ars.optimizerEstimatedCost,
											getResultSetStatistics(ars.source)
											);
		}
		else if (rs instanceof OnceResultSet)
		{
			OnceResultSet ors = (OnceResultSet) rs;

			return new RealOnceResultSetStatistics(
											ors.numOpens,
											ors.rowsSeen,
											ors.rowsFiltered,
											ors.constructorTime,
											ors.openTime,
											ors.nextTime,
											ors.closeTime,
											ors.resultSetNumber,
											ors.subqueryNumber,
											ors.pointOfAttachment,
											ors.optimizerEstimatedRowCount,
											ors.optimizerEstimatedCost,
											getResultSetStatistics(ors.source)
											);
		}
		else if (rs instanceof NormalizeResultSet)
		{
			NormalizeResultSet nrs = (NormalizeResultSet) rs;

			return new RealNormalizeResultSetStatistics(
											nrs.numOpens,
											nrs.rowsSeen,
											nrs.rowsFiltered,
											nrs.constructorTime,
											nrs.openTime,
											nrs.nextTime,
											nrs.closeTime,
											nrs.resultSetNumber,
											nrs.optimizerEstimatedRowCount,
											nrs.optimizerEstimatedCost,
											getResultSetStatistics(nrs.source)
											);
		}
		else if (rs instanceof MaterializedResultSet)
		{
			MaterializedResultSet mrs = (MaterializedResultSet) rs;

			return new RealMaterializedResultSetStatistics(
											mrs.numOpens,
											mrs.rowsSeen,
											mrs.rowsFiltered,
											mrs.constructorTime,
											mrs.openTime,
											mrs.nextTime,
											mrs.closeTime,
											mrs.createTCTime,
											mrs.fetchTCTime,
											mrs.resultSetNumber,
											mrs.optimizerEstimatedRowCount,
											mrs.optimizerEstimatedCost,
											getResultSetStatistics(mrs.source)
											);
		}
		else if (rs instanceof ScrollInsensitiveResultSet)
		{
			ScrollInsensitiveResultSet sirs = (ScrollInsensitiveResultSet) rs;

			return new RealScrollInsensitiveResultSetStatistics(
											sirs.numOpens,
											sirs.rowsSeen,
											sirs.rowsFiltered,
											sirs.constructorTime,
											sirs.openTime,
											sirs.nextTime,
											sirs.closeTime,
											sirs.numFromHashTable,
											sirs.numToHashTable,
											sirs.resultSetNumber,
											sirs.optimizerEstimatedRowCount,
											sirs.optimizerEstimatedCost,
											getResultSetStatistics(sirs.source)
											);
		}
		else if (rs instanceof CurrentOfResultSet)
		{
			CurrentOfResultSet cors = (CurrentOfResultSet) rs;

			return new RealCurrentOfStatistics(
											cors.numOpens,
											cors.rowsSeen,
											cors.rowsFiltered,
											cors.constructorTime,
											cors.openTime,
											cors.nextTime,
											cors.closeTime,
											cors.resultSetNumber
											);
		}
		else if (rs instanceof HashScanResultSet)
		{
			boolean instantaneousLocks = false;
			HashScanResultSet hsrs = (HashScanResultSet) rs;
			String startPosition = null;
			String stopPosition = null;
			String isolationLevel =  null;
			String lockString = null;

			switch (hsrs.isolationLevel)
			{
				case TransactionController.ISOLATION_SERIALIZABLE:
					isolationLevel = 
                        MessageService.getTextMessage(
                            SQLState.LANG_SERIALIZABLE);
					break;

				case TransactionController.ISOLATION_REPEATABLE_READ:
					isolationLevel = 
                        MessageService.getTextMessage(
                            SQLState.LANG_REPEATABLE_READ);
					break;

				case TransactionController.ISOLATION_READ_COMMITTED_NOHOLDLOCK:
					instantaneousLocks = true;
					//fall through
				case TransactionController.ISOLATION_READ_COMMITTED:
					isolationLevel = MessageService.getTextMessage(
												SQLState.LANG_READ_COMMITTED);
					break;

			}

			if (hsrs.forUpdate)
			{
				lockString = MessageService.getTextMessage(
													SQLState.LANG_EXCLUSIVE);
			}
			else
			{
				if (instantaneousLocks)
				{
					lockString = MessageService.getTextMessage(
											SQLState.LANG_INSTANTANEOUS_SHARE);
				}
				else
				{
					lockString = MessageService.getTextMessage(
														SQLState.LANG_SHARE);
				}
			}

			switch (hsrs.lockMode)
			{
				case TransactionController.MODE_TABLE:
					// RESOLVE: Not sure this will really work, as we
					// are tacking together English words to make a phrase.
					// Will this work in other languages?
					lockString = lockString + " " +
									MessageService.getTextMessage(
														SQLState.LANG_TABLE);
					break;

				case TransactionController.MODE_RECORD:
					// RESOLVE: Not sure this will really work, as we
					// are tacking together English words to make a phrase.
					// Will this work in other languages?
					lockString = lockString + " " +
									MessageService.getTextMessage(
															SQLState.LANG_ROW);
					break;
			}

			if (hsrs.indexName != null)
			{
				/* Start and stop position strings will be non-null
			 	* if the HSRS has been closed.  Otherwise, we go off
			 	* and build the strings now.
			 	*/
				startPosition = hsrs.startPositionString;
				if (startPosition == null)
				{
					startPosition = hsrs.printStartPosition();
				}
				stopPosition = hsrs.stopPositionString;
				if (stopPosition == null)
				{
					stopPosition = hsrs.printStopPosition();
				}
			}

			// DistinctScanResultSet is simple sub-class of
			// HashScanResultSet
			if (rs instanceof DistinctScanResultSet)
			{
				return new RealDistinctScanStatistics(
											hsrs.numOpens,
											hsrs.rowsSeen,
											hsrs.rowsFiltered,
											hsrs.constructorTime,
											hsrs.openTime,
											hsrs.nextTime,
											hsrs.closeTime,
											hsrs.resultSetNumber,
											hsrs.tableName,
											hsrs.indexName,
											hsrs.isConstraint,
											hsrs.hashtableSize,
											hsrs.keyColumns,
											hsrs.printQualifiers(
												hsrs.scanQualifiers, true),
											hsrs.printQualifiers(
												hsrs.nextQualifiers, true),
											hsrs.getScanProperties(),
											startPosition,
											stopPosition,
											isolationLevel,
											lockString,
											hsrs.optimizerEstimatedRowCount,
											hsrs.optimizerEstimatedCost
											);
			}
			else
			{
				return new RealHashScanStatistics(
											hsrs.numOpens,
											hsrs.rowsSeen,
											hsrs.rowsFiltered,
											hsrs.constructorTime,
											hsrs.openTime,
											hsrs.nextTime,
											hsrs.closeTime,
											hsrs.resultSetNumber,
											hsrs.tableName,
											hsrs.indexName,
											hsrs.isConstraint,
											hsrs.hashtableSize,
											hsrs.keyColumns,
											hsrs.printQualifiers(
												hsrs.scanQualifiers, true),
											hsrs.printQualifiers(
												hsrs.nextQualifiers, true),
											hsrs.getScanProperties(),
											startPosition,
											stopPosition,
											isolationLevel,
											lockString,
											hsrs.optimizerEstimatedRowCount,
											hsrs.optimizerEstimatedCost
											);
			}
		}
		else if (rs instanceof HashTableResultSet)
		{
			HashTableResultSet htrs = (HashTableResultSet) rs;
			int subqueryTrackingArrayLength =
				(htrs.subqueryTrackingArray == null) ? 0 :
					htrs.subqueryTrackingArray.length;
			ResultSetStatistics[] subqueryTrackingArray =
				new ResultSetStatistics[subqueryTrackingArrayLength];
			boolean anyAttached = false;
			for (int index = 0; index < subqueryTrackingArrayLength; index++)
			{
				if (htrs.subqueryTrackingArray[index] != null &&
					htrs.subqueryTrackingArray[index].getPointOfAttachment() ==
						htrs.resultSetNumber)
				{
					subqueryTrackingArray[index] =
										getResultSetStatistics(
											htrs.subqueryTrackingArray[index]);
					anyAttached = true;
				}
			}
			if (! anyAttached)
			{
				subqueryTrackingArray = null;
			}

			return new 
                RealHashTableStatistics(
                    htrs.numOpens,
                    htrs.rowsSeen,
                    htrs.rowsFiltered,
                    htrs.constructorTime,
                    htrs.openTime,
                    htrs.nextTime,
                    htrs.closeTime,
                    htrs.resultSetNumber,
                    htrs.hashtableSize,
                    htrs.keyColumns,
                    HashScanResultSet.printQualifiers(
                        htrs.nextQualifiers, true),
                    htrs.scanProperties,
                    htrs.optimizerEstimatedRowCount,
                    htrs.optimizerEstimatedCost,
                    subqueryTrackingArray,
                    getResultSetStatistics(htrs.source)
                    );
		}
		else if (rs instanceof VTIResultSet)
		{
			VTIResultSet vtirs = (VTIResultSet) rs;

			return new RealVTIStatistics(
										vtirs.numOpens,
										vtirs.rowsSeen,
										vtirs.rowsFiltered,
										vtirs.constructorTime,
										vtirs.openTime,
										vtirs.nextTime,
										vtirs.closeTime,
										vtirs.resultSetNumber,
										vtirs.javaClassName,
										vtirs.optimizerEstimatedRowCount,
										vtirs.optimizerEstimatedCost
										);
		}

		else if (rs instanceof DependentResultSet)
		{
			boolean instantaneousLocks = false;
			DependentResultSet dsrs = (DependentResultSet) rs;
			String startPosition = null;
			String stopPosition = null;
			String isolationLevel =  null;
			String lockString = null;
			String lockRequestString = null;

			switch (dsrs.isolationLevel)
			{
				case TransactionController.ISOLATION_SERIALIZABLE:
					isolationLevel = 
                        MessageService.getTextMessage(
                            SQLState.LANG_SERIALIZABLE);
					break;

				case TransactionController.ISOLATION_REPEATABLE_READ:
					isolationLevel = 
                        MessageService.getTextMessage(
                            SQLState.LANG_REPEATABLE_READ);
					break;

				case TransactionController.ISOLATION_READ_COMMITTED_NOHOLDLOCK:
					instantaneousLocks = true;
					//fall through
				case TransactionController.ISOLATION_READ_COMMITTED:
					isolationLevel = MessageService.getTextMessage(
												SQLState.LANG_READ_COMMITTED);
					break;

				case TransactionController.ISOLATION_READ_UNCOMMITTED:
					isolationLevel = 
                        MessageService.getTextMessage(
                            SQLState.LANG_READ_UNCOMMITTED);
					break;
			}

			if (dsrs.forUpdate)
			{
				lockString = MessageService.getTextMessage(
												SQLState.LANG_EXCLUSIVE);
			}
			else
			{
				if (instantaneousLocks)
				{
					lockString = MessageService.getTextMessage(
											SQLState.LANG_INSTANTANEOUS_SHARE);
				}
				else
				{
					lockString = MessageService.getTextMessage(
														SQLState.LANG_SHARE);
				}
			}

			switch (dsrs.lockMode)
			{
				case TransactionController.MODE_TABLE:
					// RESOLVE: Not sure this will really work, as we
					// are tacking together English words to make a phrase.
					// Will this work in other languages?
					lockRequestString = lockString + " " +
										MessageService.getTextMessage(
											SQLState.LANG_TABLE);
					break;

				case TransactionController.MODE_RECORD:
					// RESOLVE: Not sure this will really work, as we
					// are tacking together English words to make a phrase.
					// Will this work in other languages?
					lockRequestString = lockString + " " +
										MessageService.getTextMessage(
											SQLState.LANG_ROW);
					break;
			}

			/* Start and stop position strings will be non-null
			 * if the dSRS has been closed.  Otherwise, we go off
			 * and build the strings now.
			 */
			startPosition = dsrs.startPositionString;
			if (startPosition == null)
			{
				startPosition = dsrs.printStartPosition();
			}
			stopPosition = dsrs.stopPositionString;
			if (stopPosition == null)
			{
				stopPosition = dsrs.printStopPosition();
			}
		
			return new 
                RealTableScanStatistics(
                    dsrs.numOpens,
                    dsrs.rowsSeen,
                    dsrs.rowsFiltered,
                    dsrs.constructorTime,
                    dsrs.openTime,
                    dsrs.nextTime,
                    dsrs.closeTime,
                    dsrs.resultSetNumber,
                    dsrs.tableName,
					null,
                    dsrs.indexName,
                    dsrs.isConstraint,
                    dsrs.printQualifiers(),
                    dsrs.getScanProperties(),
                    startPosition,
                    stopPosition,
                    isolationLevel,
                    lockRequestString,
                    dsrs.rowsPerRead,
                    dsrs.coarserLock,
                    dsrs.optimizerEstimatedRowCount,
                    dsrs.optimizerEstimatedCost);
		}
		else
		{
			return null;
		}
	}

	//
	// class interface
	//
	public RealResultSetStatisticsFactory() 
	{
	}

	//GemStone changes BEGIN
        public long getResultSetMemoryUsage(ResultSet rs) throws StandardException {
            if (rs == null) {
                    return 0;
            }
            else if (rs instanceof CallStatementResultSet) {
              if(((CallStatementResultSet)rs).isSysProc) {
                return 0;
              }
              return 2;
            }
            else if (!rs.returnsRows())
            {
                    if (rs instanceof DeleteResultSet) {
                      return 0;
                    }
                    return 1;
            }
            else if (rs instanceof NoPutResultSet)
            {
                    return getResultSetMemoryUsage((NoPutResultSet) rs, 0);
            }
            else
            {
                    return 0;
            }
        }

        /**
	 * soubhik: This method estimates memory consumption of each query 
	 * based on the runtimeStatistics getting collected during .getNextCore()
	 * of ResultSets. 
	 * 
	 * TableScan and IndexScan currently adds up returned rows * per row size.
	 * This should be removed once we optimize temporary object creations of
	 * CompactRow and SortedIndexScanController iterators.
         * @throws StandardException 
	 */
        public long getResultSetMemoryUsage(NoPutResultSet rs, int depth) throws StandardException
        {
                 long totalRowsMemory = 0;
                 
                 StringBuilder logbuffer = null;
                 if(SanityManager.DEBUG) {
                   if(TraceMemEstimation) {
                     logbuffer = new StringBuilder(100);
                     for(int i = depth; i > 0; i--) 
                        logbuffer.append("\t");
                     SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_ESTIMATION,
                         logbuffer.toString() + "estimating " + rs.getClass().getSimpleName());
                   }
                 }
          
                /* We might hit the ResultSet interface with a method instead of 
                 * instanceof and that will take memory usage code inside each RS
                 * instead of one place and might be easier to keep in sync.
                 * 
                 * TODO soubhik: consider moving this logic to respective ResultSet classes after finishing this method.
                 */
                if( rs instanceof ProjectRestrictResultSet)
                {
                        ProjectRestrictResultSet prrs = (ProjectRestrictResultSet) rs;
                        String indent = null;
                        if(SanityManager.DEBUG) {
                          if(TraceMemEstimation) {
                            indent = logbuffer.toString();
                          }
                        }
                        
                        //we clone in this condition alone and hence should be considered under temporary memory creation
                        if(prrs.isTopResultSet && !prrs.doesProjection && (prrs.currentRow instanceof AbstractCompactExecRow) ) {
                          totalRowsMemory += (prrs.rowsSeen - prrs.rowsFiltered) * 
                                                (prrs.currentRow!=null ? prrs.currentRow.estimateRowSize() : 1);
                        }
                        
                        int subqueryTrackingArrayLength =
                                (prrs.subqueryTrackingArray == null) ? 0 :
                                        prrs.subqueryTrackingArray.length;
                        for (int index = 0; index < subqueryTrackingArrayLength; index++)
                        {
                                if (prrs.subqueryTrackingArray[index] != null &&
                                        prrs.subqueryTrackingArray[index].getPointOfAttachment() ==
                                                prrs.resultSetNumber)
                                {
                                  totalRowsMemory += getResultSetMemoryUsage(prrs.subqueryTrackingArray[index], depth+1);
                                  if(SanityManager.DEBUG) {
                                    if(TraceMemEstimation) {
                                      indent += "\nsubquery[" + index + "] " + 
                                                 prrs.subqueryTrackingArray[index].getClass().getSimpleName() + "  " + 
                                                 totalRowsMemory + "\n";
                                    }
                                  }
                                }
                        }
                        
                        totalRowsMemory += getResultSetMemoryUsage(prrs.source, depth+1 ); 
                        
                        if(SanityManager.DEBUG) {
                          if(TraceMemEstimation) {
                            indent += prrs.getClass().getSimpleName() + "  " +
                                      totalRowsMemory + "\n";
                            SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_ESTIMATION, indent);
                          }
                        }
                        // RealProjectRestrictStatistics;
                }
                else if (rs instanceof SortResultSet)
                {
                        SortResultSet srs = (SortResultSet) rs;

                        /*
                         * Does source's memory consumption estimation if underlying is already
                         * sorted, otherwise try to get estimated memory of the sorted if RS.open 
                         * is through else make a guess based on input rows because sorting might
                         * be happening at this time.
                         */
                        if(srs.isInSortedOrder && srs.distinct) {
                          totalRowsMemory = getResultSetMemoryUsage(srs.getSource(), depth+1);
                        }
                        else {
                          SortController sorter = srs.sorter;
                          if (sorter != null) {
                            totalRowsMemory = sorter.estimateMemoryUsage(
                                srs.sortResultRow);
                          }
                          else {
                             totalRowsMemory = (srs.rowsInput * 
                                                 (srs.sortResultRow!=null ? srs.sortResultRow.estimateRowSize() : 1) );
                          }
                        }
                        
                        if(SanityManager.DEBUG) {
                          if(TraceMemEstimation) {
                            String indent = logbuffer.toString();
                            indent += srs.getClass().getSimpleName() + "  " + 
                                       totalRowsMemory ;
                            SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_ESTIMATION, indent);
                          }
                        }
                        //RealSortStatistics
                }
                else if (rs instanceof DistinctScalarAggregateResultSet)
                {
                        DistinctScalarAggregateResultSet dsars = (DistinctScalarAggregateResultSet) rs;

                        totalRowsMemory = dsars.rowsInput * 
                                             (dsars.sortResultRow!=null ? dsars.sortResultRow.estimateRowSize() : 1);
                        
                        totalRowsMemory += getResultSetMemoryUsage(dsars.source, depth+1);
                        
                        //RealDistinctScalarAggregateStatistics
                }
                else if (rs instanceof ScalarAggregateResultSet)
                {
//                        ScalarAggregateResultSet sars = (ScalarAggregateResultSet) rs;
//
//                        return new RealScalarAggregateStatistics(
//                                                                                        sars.numOpens,
//                                                                                        sars.rowsSeen,
//                                                                                        sars.rowsFiltered,
//                                                                                        sars.constructorTime,
//                                                                                        sars.openTime,
//                                                                                        sars.nextTime,
//                                                                                        sars.closeTime,
//                                                                                        sars.resultSetNumber,
//                                                                                        sars.singleInputRow,
//                                                                                        sars.rowsInput,
//                                                                                        sars.optimizerEstimatedRowCount,
//                                                                                        sars.optimizerEstimatedCost,
//                                                                                        getResultSetMemoryUsage(sars.source)
//                                                                                );
                }
                else if (rs instanceof GroupedAggregateResultSet)
                {
                        GroupedAggregateResultSet gars = (GroupedAggregateResultSet) rs;

                        totalRowsMemory = gars.rowsInput * gars.estimateMemoryUsage();
                        
                        totalRowsMemory += getResultSetMemoryUsage(gars.source, depth+1);
                        
                        //RealGroupedAggregateStatistics
                }
                else if (rs instanceof TableScanResultSet)
                {
                        TableScanResultSet tsrs = (TableScanResultSet) rs;

                        /* During table scan memory doesn't gets allocated other than
                         * we creating temporary objects (new CompactRow).
                         *
                         * During local index scan we create lots of iterator garbage.
                         * So that also gets accomodated here. 
                         */

                        long estimatedRowCount = 1L, estimatedRowSize = 0L, stat_numRows = 1L;
                        try {
                          if(tsrs.scanController != null) {
                            estimatedRowCount = tsrs.scanController.getEstimatedRowCount() ;
                          }
                          if(tsrs.candidate != null) {
                            estimatedRowSize = tsrs.candidate.estimateRowSize();
                          }
                          totalRowsMemory = estimatedRowCount * estimatedRowSize;
                        }
                        catch (StandardException ignore) {
                          stat_numRows = (tsrs.rowsSeen - tsrs.rowsFiltered) ;
                          if(tsrs.candidate != null) {
                            estimatedRowSize = tsrs.candidate.estimateRowSize();
                          }
                          totalRowsMemory = stat_numRows * estimatedRowSize;
                        }
                        
                        
                        if(SanityManager.DEBUG) {
                          if(TraceMemEstimation) {
                            String indent = logbuffer.toString();
                            indent += tsrs.getClass().getSimpleName() + "  " +
                                      " estimatedRowCount=" + estimatedRowCount +
                                      " stat_numRows="      + stat_numRows      +
                                      " esimatedRowSize="   + estimatedRowSize  +
                                      " totalRowsMemory="   + totalRowsMemory   ;
                            SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_ESTIMATION, indent);
                          }
                        }
                        //RealTableScanStatistics

                }

                else if (rs instanceof LastIndexKeyResultSet )
                {
                        LastIndexKeyResultSet lrs = (LastIndexKeyResultSet) rs;
                }
                else if (rs instanceof HashLeftOuterJoinResultSet)
                {
                        HashLeftOuterJoinResultSet hlojrs =
                                (HashLeftOuterJoinResultSet) rs;
                        /*
                         * A join typically will be more CPU intensive than memory.
                         * But, the sheer number of times underlying table scans happen
                         * (specially rightResultSet due to reopencore()) and as a result
                         * create too many temporary objects can raise old generation usage.
                         * 
                         * rowsReturned * mergedRow should give us a fair estimation of memory
                         * used, but going ahead with left/right RS memory addition to it because
                         * we have no idea about the underlying RS. E.g. n-way joins will
                         * recursively compute underlying RS memory usage.
                         */
                        totalRowsMemory = getResultSetMemoryUsage(hlojrs.leftResultSet, depth+1) + 
                                          getResultSetMemoryUsage(hlojrs.rightResultSet, depth+1) +
                                          (hlojrs.rowsReturned * 
                                              (hlojrs.mergedRow!=null ? hlojrs.mergedRow.estimateRowSize() : 1)
                                           );
                        
                        // RealHashLeftOuterJoinStatistics
                        
                }
                else if (rs instanceof NestedLoopLeftOuterJoinResultSet)
                {
                        NestedLoopLeftOuterJoinResultSet nllojrs =
                                (NestedLoopLeftOuterJoinResultSet) rs;

                        totalRowsMemory = getResultSetMemoryUsage(nllojrs.leftResultSet, depth+1) + 
                                          getResultSetMemoryUsage(nllojrs.rightResultSet, depth+1) +
                                          (nllojrs.rowsReturned * 
                                              (nllojrs.mergedRow!=null ? nllojrs.mergedRow.estimateRowSize() : 1)
                                           );
                        
                        // RealNestedLoopLeftOuterJoinStatistics
                }
                else if (rs instanceof NestedLoopJoinResultSet)
                {
                        NestedLoopJoinResultSet nljrs = (NestedLoopJoinResultSet) rs;
                        totalRowsMemory = getResultSetMemoryUsage(nljrs.leftResultSet, depth+1) + 
                                          getResultSetMemoryUsage(nljrs.rightResultSet, depth+1) +
                                          (nljrs.rowsReturned * 
                                              ( nljrs.mergedRow!=null ? nljrs.mergedRow.estimateRowSize() : 1)
                                           );
                        
                        // RealNestedLoopLeftOuterJoinStatistics

                }
                else if (rs instanceof IndexRowToBaseRowResultSet)
                {
                        IndexRowToBaseRowResultSet irtbrrs =
                                                            (IndexRowToBaseRowResultSet) rs;

                        /*
                         * the temporary object(s) getting created during index scan
                         * is accounted from .source.
                         * 
                         * here lets just add underlying TABLE's rows fetched.
                         * 
                         * This gives an idea of IndexScan.
                         */
                        totalRowsMemory = getResultSetMemoryUsage(irtbrrs.source, depth+1) +
                                          ((irtbrrs.rowsSeen - irtbrrs.rowsFiltered) * 
                                               (irtbrrs.currentRow!=null ? irtbrrs.currentRow.estimateRowSize() : 1) );
                        
                        if(SanityManager.DEBUG) {
                          if(TraceMemEstimation) {
                            String indent = logbuffer.toString();
                            indent += irtbrrs.getClass().getSimpleName() + "  " +
                                      "totalMemoryUsage=" + totalRowsMemory ;
                            SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_ESTIMATION, indent);
                          }
                        }
                        //RealIndexRowToBaseRowStatistics
                }
                else if (rs instanceof WindowResultSet)
                {
                        WindowResultSet wrs = (WindowResultSet) rs;
                }
                else if (rs instanceof RowResultSet)
                {
                        RowResultSet rrs = (RowResultSet) rs;
                }
                else if (rs instanceof SetOpResultSet)
                {
                        SetOpResultSet srs = (SetOpResultSet) rs;
                }
                else if (rs instanceof UnionResultSet)
                {
                      UnionResultSet urs = (UnionResultSet)rs;
                      totalRowsMemory = getResultSetMemoryUsage(urs.source1, depth + 1)
                          + getResultSetMemoryUsage(urs.source2, depth + 1)
                          + (urs.rowsReturned * (urs.currentRow != null ? urs.currentRow
                              .estimateRowSize()
                              : 1));
                
                      if (SanityManager.DEBUG) {
                        if (TraceMemEstimation) {
                          String indent = logbuffer.toString();
                          indent += urs.getClass().getSimpleName() + "  " + "totalMemoryUsage="
                              + totalRowsMemory;
                          SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_ESTIMATION, indent);
                        }
                      }
                }
                else if (rs instanceof AnyResultSet)
                {
                        AnyResultSet ars = (AnyResultSet) rs;
                }
                else if (rs instanceof OnceResultSet)
                {
                        OnceResultSet ors = (OnceResultSet) rs;
                }
                else if (rs instanceof NormalizeResultSet)
                {
                        NormalizeResultSet nrs = (NormalizeResultSet) rs;
                }
                else if (rs instanceof MaterializedResultSet)
                {
                        MaterializedResultSet mrs = (MaterializedResultSet) rs;

//                        return new RealMaterializedResultSetStatistics(
//                                                                                        mrs.numOpens,
//                                                                                        mrs.rowsSeen,
//                                                                                        mrs.rowsFiltered,
//                                                                                        mrs.constructorTime,
//                                                                                        mrs.openTime,
//                                                                                        mrs.nextTime,
//                                                                                        mrs.closeTime,
//                                                                                        mrs.createTCTime,
//                                                                                        mrs.fetchTCTime,
//                                                                                        mrs.resultSetNumber,
//                                                                                        mrs.optimizerEstimatedRowCount,
//                                                                                        mrs.optimizerEstimatedCost,
//                                                                                        getResultSetMemoryUsage(mrs.source)
//                                                                                        );
                }
                else if (rs instanceof ScrollInsensitiveResultSet)
                {
                        ScrollInsensitiveResultSet sirs = (ScrollInsensitiveResultSet) rs;
                }
                else if (rs instanceof CurrentOfResultSet)
                {
                        CurrentOfResultSet cors = (CurrentOfResultSet) rs;
                }
                else if (rs instanceof HashScanResultSet)
                {
                        HashScanResultSet hsrs = (HashScanResultSet) rs;
                        String startPosition = null;
                        String stopPosition = null;

                        if (hsrs.indexName != null)
                        {
                                /* Start and stop position strings will be non-null
                                * if the HSRS has been closed.  Otherwise, we go off
                                * and build the strings now.
                                */
                                startPosition = hsrs.startPositionString;
                                if (startPosition == null)
                                {
                                        startPosition = hsrs.printStartPosition();
                                }
                                stopPosition = hsrs.stopPositionString;
                                if (stopPosition == null)
                                {
                                        stopPosition = hsrs.printStopPosition();
                                }
                        }
                        try {
                          totalRowsMemory = ( hsrs.hashtable!=null ? hsrs.hashtable.size() : 1 ) *
                                            ( hsrs.compactRow!=null ? hsrs.compactRow.estimateRowSize() : 1);
                        }
                        catch (StandardException ignore) {
                        }

                        if(SanityManager.DEBUG) {
                          if(TraceMemEstimation) {
                            String indent = logbuffer.toString();
                            indent += hsrs.getClass().getSimpleName() + "  " +
                                      "totalMemoryUsage=" + totalRowsMemory ;
                            SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_ESTIMATION, indent);
                          }
                        }
                        // DistinctScanResultSet is simple sub-class of
                        // HashScanResultSet
//                        if (rs instanceof DistinctScanResultSet)
//                        {
//                                return new RealDistinctScanStatistics(
//                                                                                        hsrs.numOpens,
//                                                                                        hsrs.rowsSeen,
//                                                                                        hsrs.rowsFiltered,
//                                                                                        hsrs.constructorTime,
//                                                                                        hsrs.openTime,
//                                                                                        hsrs.nextTime,
//                                                                                        hsrs.closeTime,
//                                                                                        hsrs.resultSetNumber,
//                                                                                        hsrs.tableName,
//                                                                                        hsrs.indexName,
//                                                                                        hsrs.isConstraint,
//                                                                                        hsrs.hashtableSize,
//                                                                                        hsrs.keyColumns,
//                                                                                        hsrs.printQualifiers(
//                                                                                                hsrs.scanQualifiers),
//                                                                                        hsrs.printQualifiers(
//                                                                                                hsrs.nextQualifiers),
//                                                                                        hsrs.getScanProperties(),
//                                                                                        startPosition,
//                                                                                        stopPosition,
//                                                                                        isolationLevel,
//                                                                                        lockString,
//                                                                                        hsrs.optimizerEstimatedRowCount,
//                                                                                        hsrs.optimizerEstimatedCost
//                                                                                        );
//                        }
//                        else
//                        {
//                                return new RealHashScanStatistics(
//                                                                                        hsrs.numOpens,
//                                                                                        hsrs.rowsSeen,
//                                                                                        hsrs.rowsFiltered,
//                                                                                        hsrs.constructorTime,
//                                                                                        hsrs.openTime,
//                                                                                        hsrs.nextTime,
//                                                                                        hsrs.closeTime,
//                                                                                        hsrs.resultSetNumber,
//                                                                                        hsrs.tableName,
//                                                                                        hsrs.indexName,
//                                                                                        hsrs.isConstraint,
//                                                                                        hsrs.hashtableSize,
//                                                                                        hsrs.keyColumns,
//                                                                                        hsrs.printQualifiers(
//                                                                                                hsrs.scanQualifiers),
//                                                                                        hsrs.printQualifiers(
//                                                                                                hsrs.nextQualifiers),
//                                                                                        hsrs.getScanProperties(),
//                                                                                        startPosition,
//                                                                                        stopPosition,
//                                                                                        isolationLevel,
//                                                                                        lockString,
//                                                                                        hsrs.optimizerEstimatedRowCount,
//                                                                                        hsrs.optimizerEstimatedCost
//                                                                                        );
//                        }
                }
                else if (rs instanceof HashTableResultSet)
                {
                        HashTableResultSet htrs = (HashTableResultSet) rs;
                        int subqueryTrackingArrayLength =
                                (htrs.subqueryTrackingArray == null) ? 0 :
                                        htrs.subqueryTrackingArray.length;
                        for (int index = 0; index < subqueryTrackingArrayLength; index++)
                        {
                                if (htrs.subqueryTrackingArray[index] != null &&
                                        htrs.subqueryTrackingArray[index].getPointOfAttachment() ==
                                                htrs.resultSetNumber)
                                {
                                        totalRowsMemory += getResultSetMemoryUsage(htrs.subqueryTrackingArray[index], depth+1);
                                }
                        }

//                        return new RealHashTableStatistics(
//                                        htrs.numOpens,
//                                        htrs.rowsSeen,
//                                        htrs.rowsFiltered,
//                                        htrs.constructorTime,
//                                        htrs.openTime,
//                                        htrs.nextTime,
//                                        htrs.closeTime,
//                                        htrs.resultSetNumber,
//                                        htrs.hashtableSize,
//                                        htrs.keyColumns,
//                                        HashScanResultSet.printQualifiers(
//                                            htrs.nextQualifiers),
//                                        htrs.scanProperties,
//                                        htrs.optimizerEstimatedRowCount,
//                                        htrs.optimizerEstimatedCost,
//                                        subqueryTrackingArray,
//                                        getResultSetMemoryUsage(htrs.source)
//                                        );
                }
                else if (rs instanceof VTIResultSet)
                {
                        VTIResultSet vtirs = (VTIResultSet) rs;
                }

                else if (rs instanceof DependentResultSet)
                {
                        boolean instantaneousLocks = false;
                        DependentResultSet dsrs = (DependentResultSet) rs;
                        String startPosition = null;
                        String stopPosition = null;
                        String isolationLevel =  null;
                        String lockString = null;
                        String lockRequestString = null;

                        /* Start and stop position strings will be non-null
                         * if the dSRS has been closed.  Otherwise, we go off
                         * and build the strings now.
                         */
                        startPosition = dsrs.startPositionString;
                        if (startPosition == null)
                        {
                                startPosition = dsrs.printStartPosition();
                        }
                        stopPosition = dsrs.stopPositionString;
                        if (stopPosition == null)
                        {
                                stopPosition = dsrs.printStopPosition();
                        }
                
//                        return new 
//                RealTableScanStatistics(
//                    dsrs.numOpens,
//                    dsrs.rowsSeen,
//                    dsrs.rowsFiltered,
//                    dsrs.constructorTime,
//                    dsrs.openTime,
//                    dsrs.nextTime,
//                    dsrs.closeTime,
//                    dsrs.resultSetNumber,
//                    dsrs.tableName,
//                                        null,
//                    dsrs.indexName,
//                    dsrs.isConstraint,
//                    dsrs.printQualifiers(),
//                    dsrs.getScanProperties(),
//                    startPosition,
//                    stopPosition,
//                    isolationLevel,
//                    lockRequestString,
//                    dsrs.rowsPerRead,
//                    dsrs.coarserLock,
//                    dsrs.optimizerEstimatedRowCount,
//                    dsrs.optimizerEstimatedCost);
                }
                else if (rs instanceof GfxdSubqueryResultSet) {
                  GfxdSubqueryResultSet ssrs = (GfxdSubqueryResultSet)rs;
                  totalRowsMemory += ssrs.estimateMemoryUsage();
                }
                
          return totalRowsMemory;
        }
	//GemStone changes END
}
