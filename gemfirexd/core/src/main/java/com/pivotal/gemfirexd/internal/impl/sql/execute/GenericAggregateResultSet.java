/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.GenericAggregateResultSet

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

import java.util.ArrayList;

import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.loader.ClassFactory;
import com.pivotal.gemfirexd.internal.iapi.services.loader.GeneratedMethod;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecAggregator;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.NoPutResultSet;

/**
 * Generic aggregation utilities.
 *
 */
public abstract class GenericAggregateResultSet extends NoPutResultSetImpl
{
	protected GenericAggregator[]		aggregates;	
	protected GeneratedMethod			rowAllocator;
	protected AggregatorInfoList	aggInfoList;
      
        // GemStone changes BEGIN
        /* (original code)
                public NoPutResultSet source;
                protected       NoPutResultSet  originalSource; // used for run time stats only
         */
        public final NoPutResultSet source;
        // GemStone changes END

	/**
	 * Constructor
	 *
	 * @param a activation
	 * @param ra row allocator generated method
	 * @param resultSetNumber result set number
	 * @param optimizerEstimatedRowCount optimizer estimated row count
	 * @param optimizerEstimatedCost optimizer estimated cost
	 *
	 * @exception StandardException Thrown on error
	 */
	GenericAggregateResultSet
	(
		NoPutResultSet s,
		int	aggregateItem,
		Activation 	a,
		GeneratedMethod	ra,
		int 			resultSetNumber,
		double 			optimizerEstimatedRowCount,
		double 			optimizerEstimatedCost
// GemStone changes BEGIN
		, boolean createAggregates
// GemStone changes END
	) 
		throws StandardException 
	{
		super(a, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
		source = s;
	        // GemStone changes BEGIN
		/*(original code) originalSource = s;*/
	        // GemStone changes END


		rowAllocator = ra;

		aggInfoList = (AggregatorInfoList) (a.getSavedObject(aggregateItem));
// GemStone changes BEGIN
		if (createAggregates)
// GemStone changes END
		aggregates = getSortAggregators(aggInfoList, false, 
				a.getLanguageConnectionContext(), s);
		
                // GemStone changes BEGIN
		printResultSetHierarchy();
                // GemStone changes END
	}

	/**
	 * For each AggregatorInfo in the list, generate a
	 * GenericAggregator and stick it in an array of
	 * GenericAggregators.
	 *
	 * @param list 	the list of aggregators to set up	
	 * @param eliminateDistincts	should distincts be ignored.  
	 *		Used to toss out distinct aggregates for a prelim
	 *		sort.
	 * @param lcc the lcc
	 * @param inputResultSet the incoming result set
	 *
	 * @return the array of GenericAggregators
	 * 
	 * @exception StandardException on error
	 */	
	protected final GenericAggregator[] getSortAggregators
	(
		AggregatorInfoList 			list,
		boolean 					eliminateDistincts,
		LanguageConnectionContext	lcc,
		NoPutResultSet				inputResultSet	
	) throws StandardException
	{
		GenericAggregator 	aggregators[]; 
		ArrayList<GenericAggregator> tmpAggregators;
		ClassFactory		cf = lcc.getLanguageConnectionFactory().getClassFactory();

		int count = list.size();
		tmpAggregators = new ArrayList<GenericAggregator>(count);
		for (int i = 0; i < count; i++)
		{
			AggregatorInfo aggInfo = (AggregatorInfo) list.elementAt(i);
			if (! (eliminateDistincts && aggInfo.isDistinct()))
			// if (eliminateDistincts == aggInfo.isDistinct())
			{
				tmpAggregators.add(new GenericAggregator(aggInfo, cf));
			}
		}



		aggregators = new GenericAggregator[tmpAggregators.size()];
		tmpAggregators.toArray(aggregators);
		//tmpAggregators.copyInto(aggregators);
		// System.out.println("size of sort aggregates " + tmpAggregators.size());

		return aggregators;
	}

	/**
	 * Finish the aggregation for the current row.  
	 * Basically call finish() on each aggregator on
	 * this row.  Called once per grouping on a vector
	 * aggregate or once per table on a scalar aggregate.
	 *
	 * If the input row is null, then rowAllocator is
	 * invoked to create a new row.  That row is then
	 * initialized and used for the output of the aggregation.
	 *
	 * @param 	row	the row to finish aggregation
	 *
	 * @return	the result row.  If the input row != null, then
	 *	the result row == input row
	 *
	 * @exception StandardException Thrown on error
	 */
	protected final ExecRow finishAggregation(ExecRow row)
		throws StandardException
	{
		int	size = aggregates.length;

		/*
		** If the row in which we are to place the aggregate
		** result is null, then we have an empty input set.
		** So we'll have to create our own row and set it
		** up.  Note: we needn't initialize in this case,
		** finish() will take care of it for us.
		*/ 
		if (row == null)
		{
			row = getExecutionFactory().getIndexableRow((ExecRow) rowAllocator.invoke(activation));
		}

		setCurrentRow(row);

		boolean eliminatedNulls = false;
		for (int i = 0; i < size; i++)
		{
			GenericAggregator currAggregate = aggregates[i];
			if (currAggregate.finish(row))
				eliminatedNulls = true;
		}

		if (eliminatedNulls)
// GemStone changes BEGIN
		  this.activation.addNullEliminatedWarning();
		  /* (original code)
			addWarning(SQLWarningFactory.newSQLWarning(SQLState.LANG_NULL_ELIMINATED_IN_SET_FUNCTION));
		  */
// GemStone changes END
	
		return row;
	}

	public void finish() throws StandardException {
		source.finish();
		super.finish();
	}

// GemStone changes BEGIN

  /**
   * For each AggregatorInfo in the list, generate an ExecAggregator and stick
   * it in an array of ExecAggregators.
   * 
   * @param list
   *          the list of aggregators to set up
   * @param eliminateDistincts
   *          should distincts be ignored. Used to toss out distinct aggregates
   *          for a prelim sort.
   * @param lcc
   *          the lcc
   * 
   * @return the array of ExecAggregators
   * 
   * @exception StandardException
   *              on error
   */
  protected final ExecAggregator[] getExecAggregators(AggregatorInfoList list,
      boolean eliminateDistincts, LanguageConnectionContext lcc)
      throws StandardException {
   final ArrayList<ExecAggregator> aggregatorList =
        new ArrayList<ExecAggregator>();
    ClassFactory cf = lcc.getLanguageConnectionFactory().getClassFactory();

    int count = list.size();
    for (int i = 0; i < count; i++) {
      AggregatorInfo aggInfo = (AggregatorInfo)list.elementAt(i);
      if (!(eliminateDistincts && aggInfo.isDistinct())) {
        try {
          Class<?> aggregatorClass = cf.loadApplicationClass(aggInfo
              .getAggregatorClassName());
          ExecAggregator agg = (ExecAggregator)aggregatorClass.newInstance();
          agg.setup(aggInfo.getAggregateName(), aggInfo);

          aggregatorList.add(agg);
        } catch (Exception e) {
          throw StandardException.unexpectedUserException(e);
        }
      }
    }

    // System.out.println("size of sort aggregates " + aggregatorList.size());
    return aggregatorList.toArray(new ExecAggregator[aggregatorList.size()]);
  }

  public void resetStatistics() {
    source.resetStatistics();
    super.resetStatistics();
  }
  
  @Override
  public void printResultSetHierarchy() {
    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TraceNCJ) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_NON_COLLOCATED_JOIN,
            "ResultSet Created: "
                + this.getClass().getSimpleName()
                + " with resultSetNumber="
                + resultSetNumber
                + " with source = "
                + (this.source != null ? this.source.getClass().getSimpleName()
                    : null) + " and source ResultSetNumber = "
                + (this.source != null ? this.source.resultSetNumber() : -1));
      }
    }
  }
// GemStone changes END
}
