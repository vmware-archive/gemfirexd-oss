/*

   Derby - Class com.pivotal.gemfirexd.internal.iapi.sql.Activation

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

package com.pivotal.gemfirexd.internal.iapi.sql;

// GemStone changes BEGIN
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.pivotal.gemfirexd.internal.engine.access.GemFireTransaction;
import com.pivotal.gemfirexd.internal.engine.procedure.cohort.ProcedureSender;
import com.pivotal.gemfirexd.internal.iapi.services.context.ContextManager;
// GemStone changes END

import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.TableDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ConstantAction;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.CursorResultSet;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecPreparedStatement;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecutionFactory;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.NoPutResultSet;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.TemporaryRowHolder;
import com.pivotal.gemfirexd.internal.iapi.store.access.ConglomerateController;
import com.pivotal.gemfirexd.internal.iapi.store.access.ScanController;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;
import com.pivotal.gemfirexd.internal.iapi.types.DataTypeDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueFactory;
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation;

import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.Vector;
import java.util.Hashtable;


/**
 * An activation contains all the local state information necessary
 * to execute a re-entrant PreparedStatement. The way it will actually work
 * is that a PreparedStatement will have an executable plan, which will be
 * a generated class. All of the local state will be variables in the class.
 * Creating a new instance of the executable plan will create the local state
 * variables. This means that an executable plan must implement this interface,
 * and that the PreparedStatement.getActivation() method will do a
 * "new" operation on the executable plan.
 * <p>
 * The fixed implementations of Activation in the Execution impl
 * package are used as skeletons for the classes generated for statements
 * when they are compiled.
 * <p>
 * There are no fixed implementations of Activation for statements;
 * a statement has an activation generated for it when it is compiled.
 *
 */

public interface Activation
{
	/**
	 * Resets the activation to the "pre-execution" state -
	 * that is, the state where it can be used to begin a new execution.
	 * Frees local buffers, stops scans, resets counters to zero, sets
	 * current date and time to an unitialized state, etc.
	 * @param cleanupOnError TODO
	 *
	 * @exception StandardException thrown on failure
	 */
	void	reset(boolean cleanupOnError) throws StandardException;

	/**
	 * JDBC requires that all select statements be converted into cursors,
	 * and that the cursor name be settable for each execution of a select
	 * statement. The Language Module will support this, so that the JDBC
	 * driver will not have to parse JSQL text. This method will have no
	 * effect when called on non-select statements.
	 * <p>
	 * There will be a JSQL statement to disable the "cursorization" of
	 * all select statements. For non-cursorized select statements, this
	 * method will have no effect.
	 * <p>
	 * This has no effect if the activation has been closed.
	 * <p>
	 * @param cursorName  The cursor name to use.
	 */
	void	setCursorName(String cursorName);

	/**
	 * Temporary tables can be declared with ON COMMIT DELETE ROWS. But if the table has a held curosr open at
	 * commit time, data should not be deleted from the table. This method, (gets called at commit time) checks if this
	 * activation held cursor and if so, does that cursor reference the passed temp table name.
	 *
	 * @return	true if this activation has held cursor and if it references the passed temp table name
	 */
	public boolean checkIfThisActivationHasHoldCursor(String tableName);

	/**
	 * Gets the ParameterValueSet for this execution of the statement.
	 *
	 * @return	The ParameterValueSet for this execution of the
	 *		statement. Returns NULL if there are no parameters.
	 */
	ParameterValueSet	getParameterValueSet();

	/**
	 * Sets the parameter values for this execution of the statement.
	 * <p>
	 * Has no effect if the activation has been closed.
	 *
	 * <p>
	 * NOTE: The setParameters() method is currently unimplemented. 
	 * A statement with parameters will generate its own ParameterValueSet,
	 * which can be gotten with the getParameterValueSet() method (above).
	 * The idea behind setParameters() is to improve performance when
	 * operating across a network by allowing all the parameters to be set
	 * in one call, as opposed to one call per parameter.
	 *
	 * @param parameterValues	The values of the parameters.
	 */
	void	setParameters(ParameterValueSet parameterValues, DataTypeDescriptor[] parameterTypes) throws StandardException;

	/**
	 * When the prepared statement is executed, it passes
	 * execution on to the activation execution was requested for.
	 *
	 * @return the ResultSet for further manipulation, if any.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	ResultSet execute() throws StandardException;

	/**
		Closing an activation statement marks it as unusable. Any other
		requests made on it will fail.  An activation should be
		marked closed when it is expected to not be used any longer,
		i.e. when the connection for it is closed, or it has suffered some
		sort of severe error. This will also close its result set and
		release any resources it holds e.g. for parameters.
		<P>
		Any class that implements this must be prepared to be executed
		from garbage collection, ie. there is no matching context stack.

		@exception StandardException		Thrown on failure
	 */
	void close() throws StandardException;

	/**
		Find out if the activation is closed or not.

		@return true if the Activation has been closed.
	 */
	boolean isClosed();

	/**
		Set this Activation for a single execution.
		E.g. a java.sql.Statement execution.
	*/
	void setSingleExecution();

	/**
		Returns true if this Activation is only going to be used for
		one execution.
	*/
	boolean isSingleExecution();

	/**
	  Returns the chained list of warnings. Returns null
	  if there are no warnings.
	  */
	//SQLWarning getWarnings();
	SQLWarning getResultsetWarnings();
	SQLWarning getWarnings();
	/**
	  Add a warning to the activation
	  */
	//void addWarning(SQLWarning w);
	void addResultsetWarning(SQLWarning w);
	void addWarning(SQLWarning w);
	/**
	  Clear the activation's warnings.
	  */
	void clearResultsetWarnings();
	void clearWarnings();
	/**
	 * Get the language connection context associated with this activation
     */
	public	LanguageConnectionContext	getLanguageConnectionContext();

	/**
	 * Get the Execution TransactionController associated with this 
	 * activation/lcc.
	 */
	TransactionController getTransactionController();

	/**
	 * Returns the current result set for this activation, i.e.
	 * the one returned by the last execute() call.  If there has
	 * been no execute call or the activation has been reset or closed,
	 * a null is returned.
	 *
	 * @return the current ResultSet of this activation.
	 */
	ResultSet getResultSet();

	/**
	 * Generated plans have a current row field for ease in defining
	 * the methods and finding them dynamically. The interface is
	 * used to set the row before a dynamic method that uses it is
	 * invoked.
	 * <p>
	 * When all processing on the currentRow has been completed,
	 * callers should call activation.clearCurrentRow(resultSetNumber)
	 * to ensure that no unnecessary references are retained to rows.
	 * This will allow the rows no longer in use to be collected by
	 * the garbage collecter.
	 *
	 * @param currentRow		The row to be operated upon.
	 * @param resultSetNumber	The resultSetNumber for the current ResultSet
	 */
	void setCurrentRow(ExecRow currentRow, int resultSetNumber);

	/**
	 * Generated plans have a current row field for ease in defining
	 * the methods and finding them dynamically. The interface is
	 * used to set the row before a dynamic method that uses it is
	 * invoked.
	 * <p>
	 * When all processing on the currentRow has been completed,
	 * callers should call activation.clearCurrentRow(resultSetNumber)
	 * to ensure that no unnecessary references are retained to rows.
	 * This will allow the rows no longer in use to be collected by
	 * the garbage collecter.
	 *
	 * @param resultSetNumber	The resultSetNumber for the current ResultSet
	 */
	/* RESOLVE - this method belongs on an internal, not external, interface */
	void clearCurrentRow(int resultSetNumber);

	/**
	 * Get the prepared statement that this activation is for.
	 *
	 * @return the prepared statement this activation is for.
	 *
	 */
	ExecPreparedStatement getPreparedStatement();

	/**
		Check the validity of the current executing statement. Needs to be
		called after a statement has obtained the relevant table locks on
		the 
	*/
	public void checkStatementValidity() throws StandardException;

	/**
	 * Get the result description for this activation, if it has one.
	 *
	 * @return result description for this activation, if it has one;
	 * otherwise, null.
	 */
	ResultDescription getResultDescription();

	/**
	 * Get the DataValueFactory
	 *
	 * @return DataValueFactory
	 */
	DataValueFactory getDataValueFactory();

	/**
	 * Get the ExecutionFactory
	 *
	 * @return ExecutionFactory
	 */
	ExecutionFactory getExecutionFactory();

	/**
		Get the saved RowLocation.

		@param itemNumber	The saved item number.

		@return	A RowLocation template for the conglomerate
	 */
	public RowLocation getRowLocationTemplate(int itemNumber);

	/**
		Get the number of subqueries in the entire query.
		@return int	 The number of subqueries in the entire query.
	 */
	public int getNumSubqueries();

	/**
	 * Return the cursor name of this activation. This will differ
	 * from its ResultSet's cursor name if it has been
	 * altered with setCursorName. Thus this always returns the cursor
	 * name of the next execution of this activation. The cursor name
	 * of the current execution must be obtained from the ResultSet.
	 * or this.getResultSet.getCursorName() [with null checking].
	 * <p>
	 * Statements that do not support cursors will return a null.
	 * <p>
	 * @return The cursor name.
	 */
	public String	getCursorName();

	/**
	 * Return the holdability of this activation.
	 * <p>
	 * @return The holdability of this activation.
	 */
	public boolean	getResultSetHoldability();

	/**
	 * Set current resultset holdability.
	 *
	 * @param resultSetHoldability	The new resultset holdability.
	 */
	public void setResultSetHoldability(boolean resultSetHoldability);

	/**
	 * Set the auto-generated keys resultset mode to true for this activation.
	 *
	 * The specific columns for auto-generated keys resultset can be requested by
	 * passing column positions array
	 *
	 * The specific columns for auto-generated keys resultset can be requested by
	 * passing column names array
	 *
	 * Both the parameters would be null if user didn't request specific keys.
	 * Otherwise, the user could request specific columns by passing column positions
	 * or names array but not both.
	 *
	 * @param columnIndexes Request specific columns in auto-generated keys
	 * resultset by passing column positions. null means no specific columns
	 * requested by position
	 *
	 * @param columnNames Request specific columns in auto-generated keys
	 * resultset by passing column names.  null means no specific columns
	 * requested by position
	 */
	public void setAutoGeneratedKeysResultsetInfo(int[] columnIndexes, String[] columnNames);

	/**
	 * Returns true if auto-generated keys resultset request was made for this
	 * avtivation.
	 * <p>
	 * @return auto-generated keys resultset mode for this activation.
	 */
	public boolean	getAutoGeneratedKeysResultsetMode();

	/**
	 * Returns the column positions array of columns requested in auto-generated
	 * keys resultset for this avtivation. Returns null if no specific column
	 * requested by positions
	 * <p>
	 * @return column positions array of columns requested.
	 */
	public int[] getAutoGeneratedKeysColumnIndexes();

	/**
	 * Returns the column names array of columns requested in auto-generated
	 * keys resultset for this avtivation. Returns null if no specific column
	 * requested by names
	 * <p>
	 * @return column names array of columns requested.
	 */
	public String[] getAutoGeneratedKeysColumnNames();

	/**
	 * Mark the activation as unused.  
	 */
	public void markUnused();

	/**
	 * Is the activation in use?
	 *
	 * @return true/false
	 */
	public boolean isInUse();

	/**
	 * Tell this activation that the given ResultSet was found to have
	 * the given number of rows.  This is used during execution to determine
	 * whether a table has grown or shrunk.  If a table's size changes
	 * significantly, the activation may invalidate its PreparedStatement
	 * to force recompilation.
	 *
	 * Note that the association of row counts with ResultSets is kept
	 * in the activation class, not in the activation itself.  This
	 * means that this method must be synchronized.
	 *
	 * This method is not required to check the number of rows on each
	 * call.  Because of synchronization, this check is likely to be
	 * expensive, so it may only check every hundred calls or so.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void informOfRowCount(NoPutResultSet resultSet, long rowCount)
					throws StandardException;

	/**
	 * Get the ConglomerateController, if any, that has already
	 * been opened for the heap when scaning for an update or delete.
	 * (Saves opening the ConglomerateController twice.)
	 *
	 * @return The ConglomerateController, if available, to use for the update.
	 */
	public ConglomerateController getHeapConglomerateController();

	/**
	 * Set the ConglomerateController to be used for an update or delete.
	 * (Saves opening the ConglomerateController twice.)
	 *
	 * @param updateHeapCC	The ConglomerateController to reuse for the update or delete.
	 */
	public void setHeapConglomerateController(ConglomerateController updateHeapCC);

	/**
	 * Clear the ConglomerateController to be used for an update or delete.
	 * (Saves opening the ConglomerateController twice.)
	 */
	public void clearHeapConglomerateController();

	/**
	 * Get the ScanController, if any, that has already
	 * been opened for the index when scaning for an update or delete.
	 * (Saves opening the ScanController twice.)
	 *
	 * @return The ScanController, if available, to use for the update.
	 */
	public ScanController getIndexScanController();

	/**
	 * Set the ScanController to be used for an update or delete,
	 * when scanning an index that will also be updated
	 * (Saves opening the ScanController twice.)
	 *
	 * @param indexSC	The ScanController to reuse for the update or delete.
	 */
	public void setIndexScanController(ScanController indexSC);

	/**
	 * Get the conglomerate number of the index, if any, that has already
	 * been opened for scaning for an update or delete.
	 * (Saves opening the ScanController twice.)
	 *
	 * @return The conglomerate number, if available, to use for the update.
	 */
	public long getIndexConglomerateNumber();

	/**
	 * Set the conglomerate number of the index to be used for an update or delete,
	 * when scanning an index that will also be updated
	 * (Saves opening the ScanController twice.)
	 *
	 * @param indexConglomerateNumber The conglomerate number of the index to reuse for the update or delete.
	 */
	public void setIndexConglomerateNumber(long indexConglomerateNumber);

	/**
	 * Clear the info for the index to be re-used for update/delete.
	 * (ScanController and conglomerate number.)
	 */
	public void clearIndexScanInfo();

	/**
	 * Mark the Activation as being for create table.
	 * (NOTE: We can do certain optimizations for
	 * create table that we can't do for other DDL.)
	 */
	public void setForCreateTable();

	/**
	 * Get whether or not this activation is for
	 * create table.
	 * (NOTE: We can do certain optimizations for
	 * create table that we can't do for other DDL.)
	 *
	 * @return Whether or not this activation is for
	 *		   create table.
	 */
	public boolean getForCreateTable();

	/**
	 * Save the TableDescriptor for the target of 
	 * DDL so that it can be passed between the
	 * various ConstantActions during execution.
	 */
	public void setDDLTableDescriptor(TableDescriptor td);

	/**
	 * Get the TableDescriptor for the target of
	 * DDL.
	 *
	 * @return The TableDescriptor for the target of
	 * DDL.
	 */
	public TableDescriptor getDDLTableDescriptor();

	/**
	 * Set the maximum # of rows.  (# of rows that can
	 * be returned by a ResultSet.  0 means no limit.)
	 *
	 * @param maxRows Maximum # of rows. (0 means no limit.)
	 */
	public void setMaxRows(int maxRows);

	/**
	 * Get the maximum # of rows.  (# of rows that can
	 * be returned by a ResultSet.  0 means no limit.)
	 *
	 * @return Maximum # of rows.  (0 means no limit.)
	 */
	public int getMaxRows();

	/**
	 * Is this Activation for a cursor?
	 *
	 * @return Whether or not this Activation is for a cursor.
	 */
	public boolean isCursorActivation();

	/**
	 * Save the ResultSet for the target
	 * of an update/delete to a VTI.
	 */
	public void setTargetVTI(java.sql.ResultSet targetVTI);

	/**
	 * Get the ResultSet for the target
	 * of an update/delete to a VTI.
	 *
	 * @return The ResultSet for the target
	 * of an update/delete to a VTI.
	 */
	public java.sql.ResultSet getTargetVTI();

	public ConstantAction	getConstantAction();

	//store a reference to the parent table result sets
	public void setParentResultSet(TemporaryRowHolder rs, String resultSetId);

	/**
	 * get the reference to parent table ResultSets, that will be needed by the 
	 * referential action dependent table scans.
	 */
	public Vector getParentResultSet(String resultSetId);
	
	//clear the parent resultset hash table;
	public void clearParentResultSets();

	public Hashtable getParentResultSets();

	/**
	 * beetle 3865: updateable cursor using index.  A way of communication
	 * between cursor activation and update activation.
	 */
	public void setForUpdateIndexScan(CursorResultSet forUpdateResultSet);

	public CursorResultSet getForUpdateIndexScan();

	/**
		Return the set of dynamical created result sets, for procedures.
		Base implementation returns null, a generated class for a procedure overwrites
		this with a real implementation.
		@return null if no dynamic results exists. Otherwise an array of ResultSet
		arrays, each of length one containing null or a reference to a ResultSet.
	*/
	public java.sql.ResultSet[][] getDynamicResults();

	/**
		Return the maximum number of dynamical created result sets from the procedure definition.
		Base implementation returns 0, a generated class for a procedure overwrites
		this with a real implementation.
	*/
	public int getMaxDynamicResults();

	/**
	 * Set the current role name of the dynamic call context stemming
	 * from this activation (which must be a stored
	 * procedure/function) call.
	 *
	 * @param role The name of the current role
	 */
	public void setNestedCurrentRole(String role);

	/**
	 * Get the current role name of the dynamic call context stemming
	 * from this activation (which must be a stored
	 * procedure/function) call.
	 *
	 * @return The name of the current role
	 */
    public String getNestedCurrentRole();

	/**
	 * This activation is created in a dynamic call context, remember
	 * its caller's activation.
	 *
	 * @param a The caller's activation
	 */
	public void setCallActivation(Activation a);

	/**
	 * This activation is created in a dynamic call context, get its
	 * caller's activation.
	 *
	 * @return The caller's activation
	 */
	public Activation getCallActivation();

// GemStone changes BEGIN

  public final int SPECIAL_OUTER_JOIN = 0x1;

  public final int NEED_KEY_FOR_SELECT_FOR_UPDATE = 0x2;

  public final int NEED_KEY_FOR_SET_OPERATORS = 0x4;

  public final int IS_PREPARED_BATCH = 0x8;

  public final int INSERT_AS_SUBSELECT = 0x10;

  public final int IS_POSSIBLE_DUPLICATE = 0x20;

  public final int HAS_NULL_ELIMINATED_WARNING = 0x40;

  /** flags that cannot be changed via {@link #setFlags(int)} */
  public final int FLAGS_IMMUTABLE = IS_PREPARED_BATCH | INSERT_AS_SUBSELECT;

  /** flags that need primary keys to be shipped back */
  public final int FLAGS_NEED_KEYS = SPECIAL_OUTER_JOIN
      | NEED_KEY_FOR_SELECT_FOR_UPDATE | NEED_KEY_FOR_SET_OPERATORS;

  /**
   * set a bunch of flags exception for {@link #FLAGS_IMMUTABLE}
   */
  public void setFlags(int flags);

  public String getObjectName();

  public void setObjectName(String name);

  /**
   * This is for an optimization for NULLS_ELIMINATED warnings for aggregates on
   * null column values. Previously a warning was added for every output row and
   * for large number of rows (e.g. > 1000) adding all these warnings and then
   * transporting them all was a huge performance problem. Now this ensures that
   * a warning is added only once per query.
   */
  public void addNullEliminatedWarning();

  /**
   * Get the current column name set by a previous call to
   * {@link #setCurrentColumnName(String)}.
   */
  public String getCurrentColumnName();

  /**
   * Name of the current column (primarily used for exception messages)
   */
  public void setCurrentColumnName(String name);

  public long getConnectionID();

  public ContextManager getContextManager();

  public long getStatementID();
  
  public int getExecutionID();
  
  public long getRootID();
  
  public int getStatementLevel();

  public boolean getIsPrepStmntQuery();

  /**
   *unique identifier of the EmbedConnection in the distributed system
   * 
   * @param id
   *          long
   */
  public void setConnectionID(long id);

  /**
   * unique identifier of the EmbedStatement in the distributed system
   * 
   * @param id
   *          long
   */
  public void setStatementID(long id);

  public void setExecutionID(int id);
  
  public void setRootID(long id);
  
  public void setStatementLevel(int value);
  
  /**
   * Identifies if the query is being executed via a PreparedStatement or
   * Statement
   * 
   * @param flag
   *          true if the query is being executed via a PreparedStatement
   */
  public void setIsPrepStmntQuery(boolean flag);

  public void distributeClose();

  /**
   * checks whether this query needs to be cancelled due to 
   * timeout or low memory. If yes, throw an appropriate exception
   * @throws StandardException 
   */
  public void checkCancellationFlag() throws StandardException;
  
  /**
   * determines whether this query is supposed to be terminated abruptly due to
   * external events like memory critical limit breach or query time out.
   * 
   * @return true if this query is marked for cancellation
   */
  public boolean isQueryCancelled();

  /**
   * Cancel a statement execution if consuming too much memory and critical
   * threshold is up.
   */
  public void cancelOnLowMemory();
  
  /**
   * Cancel a statement on query time out
   */
  public void cancelOnTimeOut();
  
  /**
   * Cancel a statement on user request
   */
  public void cancelOnUserRequest();
  
  /**
   * set the time out for query
   * @param timeOutMillis
   */
  public void setTimeOutMillis(long timeOutMillis);
  
  /**
   * returns the time out set for the query
   * @return timeOutMillis
   */
  public long getTimeOutMillis();

  /**
   * Returns estimated working memory of the statement i.e. resultSet.
   * 
   * @see RealResultSetStatisticsFactory#getResultSetMemoryUsage(NoPutResultSet rs)
   *  
   * @return long value for comparing between multiple statements. This doesn't gives
   * actual memory used.
   *  
   * @throws StandardException
   */
  public long estimateMemoryUsage() throws StandardException;

  /**
   * Distributed a bulk DML operation to DBSynchronizer or WAN.
   */
  public void distributeBulkOpToDBSynchronizer(LocalRegion rgn,
      boolean isDynamic, GemFireTransaction tran, boolean skipListeners,
      ArrayList<Object> bulkInsertRows) throws StandardException;

  /** get the FunctionContext to be used by scans etc. for this execution */
  public FunctionContext getFunctionContext();

  /**
   * set the FunctionContext to be used by scans etc. for this execution and
   * return the old context, if any
   */
  public void setFunctionContext(FunctionContext fc);

  /**
   * Get whether this execution is for the outer join case involving replicated
   * table on LHS and partitioned table on RHS.
   */
  public boolean isSpecialCaseOuterJoin();

  /**
   * Set to true when this execution is for the outer join case involving
   * replicated table on LHS and partitioned table on RHS.
   */
  public void setSpecialCaseOuterJoin(boolean flag);

  /**
   * Returns true when primary key columns are required in the result for
   * "SELECT FOR UPDATE" statement.
   */
  public boolean needKeysForSelectForUpdate();

  /**
   * Set to true when primary key columns are required in the result for
   * "SELECT FOR UPDATE" statement.
   */
  public void setNeedKeysForSelectForUpdate(boolean flag);

  /**
   * Return whether the current execution is for a prepared batch statement or
   * not.
   */
  public boolean isPreparedBatch();

  /**
   * Set whether the current execution is for a prepared batch statement or not.
   */
  public void setPreparedBatch(boolean flag);

  /**
   * Return whether the statement is a insert ... select ... statement.
   */
  public boolean getInsertAsSubselect();

  /**
   * Set whether the statement is a insert ... select ... statement.
   */
  public void setInsertAsSubselect(boolean flag);

  /**
   * Get the {@link ProcedureSender} for this execution.
   */
  public ProcedureSender getProcedureSender();

  /**
   * Set the {@link ProcedureSender} for this execution.
   */
  public void setProcedureSender(ProcedureSender sender);

  /**
   * Get whether this execution is a possible duplicate.
   */
  public boolean isPossibleDuplicate();

  /**
   * Set whether this execution is a possible duplicate.
   */
  public void setPossibleDuplicate(boolean flag);

  /**
   * Generated plans have a current row field for ease in defining
   * the methods and finding them dynamically. The interface is
   * used to get the row after a dynamic method that uses it is
   * invoked.
   * <p>
   * When all processing on the currentRow has been completed,
   * activation.clearCurrentRow(resultSetNumber) gets called and 
   * this method might return null indicating the same.
   *
   * @param resultSetNumber       The resultSetNumber for the current ResultSet
   * 
   * @returns currentRow            The row that is operated upon.
   */
  ExecRow getCurrentRow(int resultSetNumber);
  
  void setUseOnlyPrimaryBuckets(boolean b);
  
  public boolean getUseOnlyPrimaryBuckets(); 
  
  public void setQueryHDFS(boolean val);
  
  public boolean getQueryHDFS();
  
  public boolean isPutDML();  
  
  public void setHasQueryHDFS(boolean val);
  
  public boolean getHasQueryHDFS();

  /**
   *  Get the specified saved object.
   *
   *  @param  objectNum The object to get.
   *  @return the requested saved object.
   */
  public  Object getSavedObject(int objectNum);
// GemStone changes END

  Object[] getSavedObjects();

}
