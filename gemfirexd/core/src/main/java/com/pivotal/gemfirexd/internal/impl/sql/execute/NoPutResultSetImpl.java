/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.NoPutResultSetImpl

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
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.store.RowFormatter;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.i18n.MessageService;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableBitSet;
import com.pivotal.gemfirexd.internal.iapi.services.monitor.Monitor;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.services.stream.HeaderPrintWriter;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecIndexRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecutionFactory;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.NoPutResultSet;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.TargetResultSet;
import com.pivotal.gemfirexd.internal.iapi.store.access.Qualifier;
import com.pivotal.gemfirexd.internal.iapi.store.access.RowLocationRetRowSource;
import com.pivotal.gemfirexd.internal.iapi.store.access.RowSource;
import com.pivotal.gemfirexd.internal.iapi.types.BinarySQLHybridType;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.Orderable;
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation;
import com.pivotal.gemfirexd.internal.iapi.types.SQLChar;
import com.pivotal.gemfirexd.internal.impl.sql.execute.xplain.XPLAINUtil;


/**
 * Abstract ResultSet with built in Activation support for operations that
 * return rows but do not allow the caller to put data on output pipes. This
 * implementation of ResultSet is meant to be overridden by subtypes in the
 * execution engine. Its primary users will be DML operations that do not put
 * data on output pipes, but simply return it due to being result sets
 * themselves.
 * <p>
 * This abstract class does not define the entire ResultSet
 * interface, but leaves the 'get' half of the interface
 * for subtypes to implement. It is package-visible only,
 * with its methods being public for exposure by its subtypes.
 * <p>
 */
// GemStone changes BEGIN
// changed to public
public abstract class NoPutResultSetImpl
// GemStone changes END
extends BasicNoPutResultSetImpl
{
	/* Set in constructor and not modified */
	public final int				resultSetNumber;

	// fields used when being called as a RowSource
	private boolean needsRowLocation;
	protected ExecRow clonedExecRow;
	protected TargetResultSet	targetResultSet;

	/* beetle 4464. compact flags into array of key column positions that we do check/skip nulls,
	 * so that we burn less cycles for each row, column.
	 */
	private int[] checkNullCols;
	private int cncLen;
	

	/**
	 *  Constructor
	 *
	 *	@param	activation			The activation
	 *	@param	resultSetNumber		The resultSetNumber
	 *  @param	optimizerEstimatedRowCount	The optimizer's estimated number
	 *										of rows.
	 *  @param	optimizerEstimatedCost		The optimizer's estimated cost
	 */
// GemStone changes BEGIN
// changed to public
	public NoPutResultSetImpl(Activation activation,
// GemStone changes END
						int resultSetNumber,
						double optimizerEstimatedRowCount,
						double optimizerEstimatedCost)
	{
		super(activation,
				optimizerEstimatedRowCount,
				optimizerEstimatedCost);

		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(activation!=null, "activation expected to be non-null");
			SanityManager.ASSERT(resultSetNumber >= 0, "resultSetNumber expected to be >= 0");
		}
		this.resultSetNumber = resultSetNumber;
	}

	// NoPutResultSet interface

	/**
		Return my cursor name for JDBC. Can be null.
	*/
	public String getCursorName() {

		String cursorName = activation.getCursorName();
		if ((cursorName == null) && isForUpdate()) {

			activation.setCursorName(activation.getLanguageConnectionContext().getUniqueCursorName());

			cursorName = activation.getCursorName();
		}

		return cursorName;
	}

	/** @see NoPutResultSet#resultSetNumber() */
	public int resultSetNumber() {
		return resultSetNumber;
	}

	/**
		Close needs to invalidate any dependent statements, if this is a cursor.
		Must be called by any subclasses that override close().
		@exception StandardException on error
	*/
	public void close(final boolean cleanupOnError) throws StandardException
	{
		if (!isOpen)
			return;

		/* If this is the top ResultSet then we must
		 * close all of the open subqueries for the
		 * entire query.
		 */
		if (isTopResultSet)
		{
		  closeTopResultSet(cleanupOnError);
		}

// GemStone changes BEGIN
		this.localTXState = null;
		this.localTXStateSet = false;

// GemStone changes END
		isOpen = false;

	}
	
	private final void closeTopResultSet(final boolean cleanupOnError) throws StandardException {
          /*
          ** If run time statistics tracing is turned on, then now is the
          ** time to dump out the information.
          */
          LanguageConnectionContext lcc = getLanguageConnectionContext();
          // GemStone changes BEGIN
          /*if (lcc.getRunTimeStatisticsMode()*/
          if (runtimeStatisticsOn
              && (!lcc.isConnectionForRemote() || isLocallyExecuted)
          // GemStone changes END
              )
          {
                  endExecutionTime = statisticsTimingOn ? XPLAINUtil.currentTimeMillis() : 0;
  

                  // GemStone changes BEGIN
                  if(statisticsTimingOn) closeTime = getElapsedNanos(beginTime); 
                  // GemStone changes END
                  
                  // get the ResultSetStatisticsFactory, which gathers RuntimeStatistics
                  final ExecutionFactory ef = lcc.getLanguageConnectionFactory()
                      .getExecutionFactory();
                  
                  // GemStone changes BEGIN
                  if (lcc.getLogQueryPlan() || lcc.getRunTimeStatisticsModeExplicit()) {
                    logRuntimeStatistics(lcc.getLogQueryPlan(), ef);
                  }
                  // GemStone changes END
                  if (!cleanupOnError) {
                    invokeQueryPlan(ef);
                  }
                  else {
                    if (SanityManager.DEBUG) {
                      if (GemFireXDUtils.TracePlanGeneration) {
                        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_PLAN_GENERATION, "Invoking query plan capture.");
                      }
                    }
                  }
          }

          // GemStone changes BEGIN
          if (subqueryTrackingArray != null && subqueryTrackingArray.length > 0) {
            closeSubqueryTrackingArray(cleanupOnError);
          }
          // reset the statistics variables
          if (!lcc.isConnectionForRemote()) {
            resetStatistics();
          }
          // GemStone changes END
	}
	
	private final void logRuntimeStatistics(final boolean logPlan, final ExecutionFactory ef) throws StandardException {
          // GemStone changes BEGIN
          /*(original code) lcc.setRunTimeStatisticsObject(
          ef.getResultSetStatisticsFactory().getRunTimeStatistics(activation, this, subqueryTrackingArray));
      
          HeaderPrintWriter istream = lcc.getLogQueryPlan() ? Monitor.getStream() : null;*/
	  lcc.setRunTimeStatisticsObject(ef.getResultSetStatisticsFactory()
	      .getRunTimeStatistics(activation, this, subqueryTrackingArray));
	  
          if (logPlan) {
            final HeaderPrintWriter istream = Monitor.getStream();
            if (istream != null) {
              istream.printlnWithHeader(LanguageConnectionContext.xidStr
                  + lcc.getTransactionExecute().getTransactionIdString() + "), "
                  + LanguageConnectionContext.lccStr + lcc.getInstanceNumber() + "), "
                  + lcc.getRunTimeStatisticsObject().getStatementText() + " ******* "
                  + lcc.getRunTimeStatisticsObject().getStatementExecutionPlanText());
            }
          }
          // GemStone changes END
	}
	
	private final void invokeQueryPlan(final ExecutionFactory ef) throws StandardException {
          // now explain gathered statistics, using an appropriate visitor
          ResultSetStatisticsVisitor visitor = ef.getXPLAINFactory()
              .getXPLAINVisitor(lcc, statsEnabled, explainConnection);
          visitor.doXPLAIN(this, activation, true, statisticsTimingOn, isLocallyExecuted);
          
          // GemStone changes BEGIN
          executionPlanID = visitor.getStatementUUID();
          
          if (observer != null) {
            observer.afterQueryPlanGeneration();
          }
          // GemStone changes END
	}
	
	private final void closeSubqueryTrackingArray(final boolean cleanupOnError) throws StandardException {
            final int staLength = subqueryTrackingArray.length;
        
            for (int index = 0; index < staLength; index++) {
              if (subqueryTrackingArray[index] == null) {
                continue;
              }
              if (subqueryTrackingArray[index].isClosed()) {
                continue;
              }
              subqueryTrackingArray[index].close(cleanupOnError);
            }
	}

	/** @see NoPutResultSet#setTargetResultSet */
	public void setTargetResultSet(TargetResultSet trs)
	{
		targetResultSet = trs;
	}

	/** @see NoPutResultSet#setNeedsRowLocation */
	public void setNeedsRowLocation(boolean needsRowLocation)
	{
		this.needsRowLocation = needsRowLocation;
	}

	// RowSource interface
	
	/** 
	 * @see RowSource#getValidColumns
	 */
	public FormatableBitSet getValidColumns()
	{
		// All columns are valid
		return null;
	}
	
	/** 
	 * @see RowSource#getNextRowFromRowSource
	 * @exception StandardException on error
	 */
	public ExecRow getNextRowFromRowSource()
		throws StandardException
	{
		ExecRow execRow = getNextRowCore();
 		if (execRow != null)
		{
			/* Let the target preprocess the row.  For now, this
			 * means doing an in place clone on any indexed columns
			 * to optimize cloning and so that we don't try to drain
			 * a stream multiple times.  This is where we also
			 * enforce any check constraints.
			 */
			clonedExecRow = targetResultSet.preprocessSourceRow(execRow);

			return execRow;
		}

		return null;
	}

	/**
	 * @see RowSource#needsToClone
	 */
	public boolean needsToClone()
	{
		return(true);
	}

	/** 
	 * @see RowSource#closeRowSource
	 */
	public void closeRowSource()
	{
		// Do nothing here - actual work will be done in close()
	}


	// RowLocationRetRowSource interface

	/**
	 * @see RowLocationRetRowSource#needsRowLocation
	 */
	public boolean needsRowLocation()
	{
		return needsRowLocation;
	}

	/**
	 * @see RowLocationRetRowSource#rowLocation
	 * @exception StandardException on error
	 */
	public void rowLocation(RowLocation rl)
		throws StandardException
	{
		targetResultSet.changedRow(clonedExecRow, rl);
	}


	// class implementation

	/**
	 * Clear the Orderable cache for each qualifier.
	 * (This should be done each time a scan/conglomerate with
	 * qualifiers is reopened.)
	 *
	 * @param qualifiers	The Qualifiers to clear
	 */
	protected void clearOrderableCache(Qualifier[][] qualifiers) throws StandardException
	{
		// Clear the Qualifiers's Orderable cache 
		if (qualifiers != null)
		{
			Qualifier qual;
			for (int term = 0; term < qualifiers.length; term++)
			{
				for (int index = 0; index < qualifiers[term].length; index++)
				{
					qual = qualifiers[term][index];
					qual.clearOrderableCache();
					/* beetle 4880 performance enhancement and avoid deadlock while pushing
					 * down method call to store: pre-evaluate.
					 */
					if (((GenericQualifier) qual).variantType != Qualifier.VARIANT)
						qual.getOrderable();		// ignore return value
				}
			}
		}
	}

	/**
	 * Set the current row to the row passed in.
	 *
	 * @param row the new current row
	 *
	 */
	public final void setCurrentRow(ExecRow row)
	{
		activation.setCurrentRow(row, resultSetNumber);
		currentRow = row;
	}

	/**
	 * Clear the current row
	 *
	 */
	public void clearCurrentRow()
	{
	  
		currentRow = null;
		activation.clearCurrentRow(resultSetNumber);
	}

	/**
	 * Is this ResultSet or it's source result set for update
	 * This method will be overriden in the inherited Classes
	 * if it is true
	 * @return Whether or not the result set is for update.
	 */
	public boolean isForUpdate()
	{
		return false;
	}

	/**
	 * Return true if we should skip the scan due to nulls in the start
	 * or stop position when the predicate on the column(s) in question
	 * do not implement ordered null semantics. beetle 4464, we also compact
	 * the areNullsOrdered flags into checkNullCols here.
	 *
	 * @param startPosition	An index row for the start position
	 * @param stopPosition	An index row for the stop position
	 *
	 * @return	true means not to do the scan
	 */
	protected boolean skipScan(ExecIndexRow startPosition, ExecIndexRow stopPosition)
		throws StandardException
	{
		int nStartCols = (startPosition == null) ? 0 : startPosition.nColumns();
		int nStopCols = (stopPosition == null) ? 0 : stopPosition.nColumns();

		/* Two facts 1) for start and stop key column positions, one has to be the prefix
		 * of the other, 2) startPosition.areNullsOrdered(i) can't be different from
		 * stopPosition.areNullsOrdered(i) unless the case "c > null and c < 5", (where c is
		 * non-nullable), in which we skip the scan anyway.
		 * So we can just use the longer one to get checkNullCols.
		 */
		boolean startKeyLonger = false;
		int size = nStopCols;
		if (nStartCols > nStopCols)
		{
			startKeyLonger = true;
			size = nStartCols;
		}
		if (size == 0)
			return false;
		if ((checkNullCols == null) || (checkNullCols.length < size))
			checkNullCols = new int[size];
		cncLen = 0;

		boolean returnValue = false;
		for (int position = 0; position < nStartCols; position++)
		{
			if ( ! startPosition.areNullsOrdered(position))
			{
				if (startKeyLonger)
					checkNullCols[cncLen++] = position + 1;
				if (startPosition.getColumn(position + 1).isNull())
				{
					returnValue =  true;
					if (! startKeyLonger)
						break;
				}
			}
		}
		if (startKeyLonger && returnValue)
			return true;
		for (int position = 0; position < nStopCols; position++)
		{
			if ( ! stopPosition.areNullsOrdered(position))
			{
				if (! startKeyLonger)
					checkNullCols[cncLen++] = position + 1;
				if (returnValue)
					continue;
				if (stopPosition.getColumn(position + 1).isNull())
				{
					returnValue =  true;
					if (startKeyLonger)
						break;
				}
			}
		}

		return returnValue;
	}

	/**
	 * Return true if we should skip the scan due to nulls in the row
	 * when the start or stop positioners on the columns containing
	 * null do not implement ordered null semantics.
	 *
	 * @param row	An index row
	 *
	 * @return	true means skip the row because it has null
	 */
	protected final boolean skipRow(ExecRow row)  throws StandardException
	{
	  final int cncLen = this.cncLen;
	  if (cncLen == 0) {
	    return false;
	  }
		for (int i = 0; i < cncLen; i++)
		{
			if (row.isNull(checkNullCols[i]) ==
			    RowFormatter.OFFSET_AND_WIDTH_IS_NULL)
				return true;
		}

		return false;
	}

	/**
	 * Return a 2-d array of Qualifiers as a String
	 */
	public static String printQualifiers(Qualifier[][] qualifiers, boolean addNewLine)
	{
		String idt = "";

		String output = "";
		if (qualifiers == null)
		{
			return idt + MessageService.getTextMessage(SQLState.LANG_NONE);
		}

        for (int term = 0; term < qualifiers.length; term++)
        {
            for (int i = 0; i < qualifiers[term].length; i++)
            {
                //Gemstone changes BEGIN
                Qualifier qual = qualifiers[term][i];
                boolean negateOp = qual.negateCompareResult();
                
                // Print the name of the column if it exists here, else
                // print number
                output = idt + output +
                    MessageService.getTextMessage(
                        SQLState.LANG_COLUMN_ID_ARRAY,
                            String.valueOf(term), String.valueOf(i)) +
                        ": " + ((qual.getColumnName()!=null)?
                              qual.getColumnName():
                              qual.getColumnId()) + (addNewLine ? " \n" : "  ");
                    
                int operator = qual.getOperator();
                String opString = null;
                switch (operator)
                {
                  // Print proper operator if negation is turned on
                  case Orderable.ORDER_OP_EQUALS:
                    opString = negateOp?"<>":"=";
                    break;

                  case Orderable.ORDER_OP_LESSOREQUALS:
                    opString = negateOp?">":"<=";
                    break;

                  case Orderable.ORDER_OP_LESSTHAN:
                    opString = negateOp?">=":"<";
                    break;

                  default:
                    if (SanityManager.DEBUG)
                    {
                        SanityManager.THROWASSERT("Unknown operator " + operator);
                    }

                    // NOTE: This does not have to be internationalized, because
                    // this code should never be reached.
                    opString = "unknown value (" + operator + ")";
                    break;
                }

                // If the operand is a number or date, print it out
                // (Strings/binaries are sometimes unprintable characters in situations like
                //    "where col1 like '%'"
                //  this can be included with escaping for unprintable characters at a later time)
                try {
                  DataValueDescriptor desc = qual.getOrderable();
                  boolean printValue = false;           // Whether or not the value is printable
                  // If orderableCache is null, then the value is only known at runtime via
                  //  direct call to generated class - do not print a value in this case
                  //  as it varies per execution
                  if (desc != null)
                  {
                    if (desc instanceof BinarySQLHybridType)
                    {
                      desc = ((BinarySQLHybridType)desc).getSQLValue();
                    }
                    if ((!BinarySQLHybridType.isStringType(desc.getTypeFormatId()))
                      &&
                      (!BinarySQLHybridType.isBinaryType(desc.getTypeFormatId())))
                    {
                      printValue = true;
                    }
                    else
                    {
                      // It's a string-type. Special-case here for LIKE with wildcards
                      // Which produces unprintable characters. If the first bytechar is unprintable,
                      // then do not print the string at all
                      // Wildcards use >0x0000 and <=0xFFFF predicates, ignore these
                      // Wildcards are also always SQLChar, not SQLVarchar
                      if (desc instanceof SQLChar)
                      {
                        char[] descChars = ((SQLChar)desc).getCharArray();
                        if (descChars != null)
                        {
                          final int firstChar = ((SQLChar)desc).getCharArray()[0];
                          // Printable - not a control character and not 0xFFFF
                          if ((firstChar >= 0x0020) && (firstChar < 0xFFFF))
                          {
                            printValue = true;
                          }
                        }
                      }
                      else
                      {
                        printValue = true;
                      }
                    }
                  }
                  if (printValue)
                  {
                    opString = opString + " " + desc.getString();
                  }
                }
                catch(StandardException e)
                {
                  e.printStackTrace();
                }
                
                output = output +
                    idt + MessageService.getTextMessage(SQLState.LANG_OPERATOR) +
                            ": " + opString + (addNewLine ? "\n" : "  ") +
                    idt +
                        MessageService.getTextMessage(
                            SQLState.LANG_ORDERED_NULLS) +
                        ": " + qual.getOrderedNulls() + (addNewLine ? "\n" : "  ") +
                    idt +
                        MessageService.getTextMessage(
                            SQLState.LANG_UNKNOWN_RETURN_VALUE) +
                        ": " + qual.getUnknownRV() + (addNewLine ? "\n" : "  ");
                    // Don't print out negation, we already reversed the operator above
                    // + idt +
                    //    MessageService.getTextMessage(
                    //        SQLState.LANG_NEGATE_COMPARISON_RESULT) +
                    //    ": " + qual.negateCompareResult() + "\n";
                    //GemStone changes END
            }
        }

		return output;
	}

	/**
	 * @see NoPutResultSet#updateRow
	 *
	 * This method is result sets used for scroll insensitive updatable 
	 * result sets for other result set it is a no-op.
	 */
	public void updateRow(ExecRow row) throws StandardException {
		// Only ResultSets of type Scroll Insensitive implement
		// detectability, so for other result sets this method
		// is a no-op
	}

	/**
	 * @see NoPutResultSet#markRowAsDeleted
	 *
	 * This method is result sets used for scroll insensitive updatable 
	 * result sets for other result set it is a no-op.
	 */
	public void markRowAsDeleted() throws StandardException {
		// Only ResultSets of type Scroll Insensitive implement
		// detectability, so for other result sets this method
		// is a no-op
	}

	/**
	 * @see NoPutResultSet#positionScanAtRowLocation
	 *
	 * This method is result sets used for scroll insensitive updatable 
	 * result sets for other result set it is a no-op.
	 */
	public void positionScanAtRowLocation(RowLocation rl) 
		throws StandardException 
	{
		// Only ResultSets of type Scroll Insensitive implement
		// detectability, so for other result sets this method
		// is a no-op
	}
	
	/*
	 * Must be implemented by each derived class, 
	 * and should be called by derived class constructor 
	 */
	public abstract void printResultSetHierarchy();
}
