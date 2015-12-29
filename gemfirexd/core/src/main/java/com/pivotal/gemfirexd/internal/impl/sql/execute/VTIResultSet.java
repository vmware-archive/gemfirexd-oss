/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.VTIResultSet

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

import com.gemstone.gemfire.internal.cache.TXState;
import com.pivotal.gemfirexd.internal.catalog.TypeDescriptor;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.UpdateVTITemplate;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatIdInputStream;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatIdUtil;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableBitSet;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableHashtable;
import com.pivotal.gemfirexd.internal.iapi.services.loader.ClassInspector;
import com.pivotal.gemfirexd.internal.iapi.services.loader.GeneratedMethod;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.CursorResultSet;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecutionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.NoPutResultSet;
import com.pivotal.gemfirexd.internal.iapi.store.access.Qualifier;
import com.pivotal.gemfirexd.internal.iapi.types.DataTypeDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation;
import com.pivotal.gemfirexd.internal.iapi.types.TypeId;
import com.pivotal.gemfirexd.internal.iapi.types.VariableSizeDataValue;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedPreparedStatement;
import com.pivotal.gemfirexd.internal.impl.sql.GenericPreparedStatement;
import com.pivotal.gemfirexd.internal.impl.sql.execute.xplain.XPLAINUtil;
import com.pivotal.gemfirexd.internal.vti.DeferModification;
import com.pivotal.gemfirexd.internal.vti.IFastPath;
import com.pivotal.gemfirexd.internal.vti.VTIEnvironment;

import java.io.ByteArrayInputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.ResultSetMetaData;


/**
 */
public class VTIResultSet extends NoPutResultSetImpl
	implements CursorResultSet, VTIEnvironment {

	/* Run time statistics variables */
	public int rowsReturned;
	public String javaClassName;

    private boolean next;
	private ClassInspector classInspector;
    private GeneratedMethod row;
    private GeneratedMethod constructor;
	private PreparedStatement userPS;
	private ResultSet userVTI;
	private ExecRow allocatedRow;
	private FormatableBitSet referencedColumns;
	private boolean version2;
	private boolean reuseablePs;
	private boolean isTarget;
	private FormatableHashtable compileTimeConstants;
	private int ctcNumber;

	private boolean pushedProjection;
	private IFastPath	fastPath;

	private Qualifier[][]	pushedQualifiers;

	private boolean[] runtimeNullableColumn;

	private boolean isDerbyStyleTableFunction;

    private String  returnType;

    private DataTypeDescriptor[]    returnColumnTypes;

	/**
		Specified isolation level of SELECT (scan). If not set or
		not application, it will be set to ExecutionContext.UNSPECIFIED_ISOLATION_LEVEL
	*/
	private int scanIsolationLevel = ExecutionContext.UNSPECIFIED_ISOLATION_LEVEL;
	
    //
    // class interface
    //
    VTIResultSet(Activation activation, GeneratedMethod row, int resultSetNumber,
				 GeneratedMethod constructor,
				 String javaClassName,
				 Qualifier[][] pushedQualifiers,
				 int erdNumber,
				 boolean version2, boolean reuseablePs,
				 int ctcNumber,
				 boolean isTarget,
				 int scanIsolationLevel,
			     double optimizerEstimatedRowCount,
				 double optimizerEstimatedCost,
				 boolean isDerbyStyleTableFunction,
                 String returnType
                 ) 
		throws StandardException
	{
		super(activation, resultSetNumber, 
			  optimizerEstimatedRowCount, optimizerEstimatedCost);
        this.row = row;
		this.constructor = constructor;
		this.javaClassName = javaClassName;
		this.version2 = version2;
		this.reuseablePs = reuseablePs;
		this.isTarget = isTarget;
		this.pushedQualifiers = pushedQualifiers;
		this.scanIsolationLevel = scanIsolationLevel;
		this.isDerbyStyleTableFunction = isDerbyStyleTableFunction;
        this.returnType = returnType;

		if (erdNumber != -1)
		{
			this.referencedColumns = (FormatableBitSet)(activation.
								getSavedObject(erdNumber));
		}

		this.ctcNumber = ctcNumber;
		compileTimeConstants = (FormatableHashtable) (activation.
								getSavedObject(ctcNumber));

		recordConstructorTime();
		
                // GemStone changes BEGIN
		printResultSetHierarchy();
                // GemStone changes END
    }

	//
	// ResultSet interface (leftover from NoPutResultSet)
	//


	/**
     * Sets state to 'open'.
	 *
	 * @exception StandardException thrown if activation closed.
     */
	public void	openCore() throws StandardException 
	{
		beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
		if (SanityManager.DEBUG)
		    SanityManager.ASSERT( ! isOpen, "VTIResultSet already open");

	    isOpen = true;
		numOpens++;

		/* We need to Instantiate the user's ResultSet on the each open since
		 * there is no way to close and then reopen a java.sql.ResultSet.
		 * For Version 2 VTIs, we may be able to skip instantiated their
		 * PreparedStatement here.
		 */
		try {
			if (version2)
			{
				userPS = (PreparedStatement) constructor.invoke(activation);

				if (userPS instanceof com.pivotal.gemfirexd.internal.vti.Pushable) {
					com.pivotal.gemfirexd.internal.vti.Pushable p = (com.pivotal.gemfirexd.internal.vti.Pushable) userPS;
					if (referencedColumns != null) {
						pushedProjection = p.pushProjection(this, getProjectedColList());
					}
				}

				if (userPS instanceof com.pivotal.gemfirexd.internal.vti.IQualifyable) {
					com.pivotal.gemfirexd.internal.vti.IQualifyable q = (com.pivotal.gemfirexd.internal.vti.IQualifyable) userPS;

					q.setQualifiers(this, pushedQualifiers);
				}
				fastPath = userPS instanceof IFastPath ? (IFastPath) userPS : null;

                if( isTarget
                    && userPS instanceof DeferModification
                    && activation.getConstantAction() instanceof UpdatableVTIConstantAction)
                {
                    UpdatableVTIConstantAction constants = (UpdatableVTIConstantAction) activation.getConstantAction();
                    ((DeferModification) userPS).modificationNotify( constants.statementType, constants.deferred);
                }
                
				if ((fastPath != null) && fastPath.executeAsFastPath())
					;
				else
					userVTI = userPS.executeQuery();

				/* Save off the target VTI */
				if (isTarget)
				{
					activation.setTargetVTI(userVTI);
				}

			}
			else
			{
				userVTI = (ResultSet) constructor.invoke(activation);
				fastPath = userVTI instanceof IFastPath ? (IFastPath) userVTI : null;
			}

			// Set up the nullablity of the runtime columns, may be delayed
			setNullableColumnList();

// GemStone changes BEGIN
			if (this.userVTI instanceof UpdateVTITemplate) {
			  ((UpdateVTITemplate)this.userVTI).setActivation(
			      this.activation);
			}
			setSharedStateInUserVTI();
// GemStone changes END
		}
		catch (Throwable t)
		{
			throw StandardException.unexpectedUserException(t);
		}


		if (statisticsTimingOn) openTime += getElapsedNanos(beginTime);
	}

	private boolean[] setNullableColumnList() throws SQLException, StandardException {

		if (runtimeNullableColumn != null)
			return runtimeNullableColumn;

		// Derby-style table functions return SQL rows which don't have not-null
		// constraints bound to them
		if ( isDerbyStyleTableFunction )
		{
		    int         count = getAllocatedRow().nColumns() + 1;
            
		    runtimeNullableColumn = new boolean[ count ];
		    for ( int i = 0; i < count; i++ )   { runtimeNullableColumn[ i ] = true; }
            
		    return runtimeNullableColumn;
		}

		if (userVTI == null)
			return null;

		ResultSetMetaData rsmd = userVTI.getMetaData();
		boolean[] nullableColumn = new boolean[rsmd.getColumnCount() + 1];
		for (int i = 1; i <  nullableColumn.length; i++) {
			nullableColumn[i] = rsmd.isNullable(i) != ResultSetMetaData.columnNoNulls;
		}

		return runtimeNullableColumn = nullableColumn;
	}

	/**
	 * If the VTI is a version2 vti that does not
	 * need to be instantiated multiple times then
	 * we simply close the current ResultSet and 
	 * create a new one via a call to 
	 * PreparedStatement.executeQuery().
	 *
	 * @see NoPutResultSet#openCore
	 * @exception StandardException thrown if cursor finished.
	 */
	public void reopenCore() throws StandardException
	{
		if (reuseablePs)
		{
			/* close the user ResultSet.
			 */
			if (userVTI != null)
			{
				try
				{
					userVTI.close();
					userVTI = userPS.executeQuery();

					/* Save off the target VTI */
					if (isTarget)
					{
						activation.setTargetVTI(userVTI);
					}
				} catch (SQLException se)
				{
					throw StandardException.unexpectedUserException(se);
				}
			}
		}
		else
		{
			close(false);
			openCore();	
		}
	}

	/**
     * If open and not returned yet, returns the row
     * after plugging the parameters into the expressions.
	 *
	 * @exception StandardException thrown on failure.
     */
	public ExecRow	getNextRowCore() throws StandardException 
	{
	    ExecRow result = null;

		beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
		
		if ( isOpen ) 
		{
			try
			{
				if ((userVTI == null || userVTI instanceof IFastPath) && (fastPath != null)) {
					result = getAllocatedRow();
					int action = fastPath.nextRow(result, this);
					if (action == IFastPath.GOT_ROW)
						;
					else if (action == IFastPath.SCAN_COMPLETED)
						result = null;
					else if (action == IFastPath.NEED_RS) {
						userVTI = userPS.executeQuery();
					}
				}
				if ((userVTI != null) && !(userVTI instanceof IFastPath))
                {
                    if( ! userVTI.next())
                    {
                        if( null != fastPath)
                            fastPath.rowsDone();
                        result = null;
                    }
                    else
                    {
                        // Get the cached row and fill it up
                        result = getAllocatedRow();
                        populateFromResultSet(result);
                        if (fastPath != null)
                            fastPath.currentRow(userVTI, result.getRowArray());
                    }
				}
			}
			catch (Throwable t)
			{
				throw StandardException.unexpectedUserException(t);
			}

		}

		setCurrentRow(result);
		if (result != null)
		{
			rowsReturned++;
			rowsSeen++;
		}

		if (statisticsTimingOn) nextTime += getElapsedNanos(beginTime);
	    return result;
	}

	

	/**
     * @see com.pivotal.gemfirexd.internal.iapi.sql.ResultSet#close
	 *
	 * @exception StandardException thrown on error
	 */
	public void	close(boolean cleanupOnError) throws StandardException
	{
		beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
		if (isOpen) {

			// we don't want to keep around a pointer to the
			// row ... so it can be thrown away.
			// REVISIT: does this need to be in a finally
			// block, to ensure that it is executed?
	    	clearCurrentRow();
	    	next = false;

			/* close the user ResultSet.  We have to eat any exception here
			 * since our close() method cannot throw an exception.
			 */
			if (userVTI != null)
			{
				try
				{
					userVTI.close();
				} catch (SQLException se)
				{
					throw StandardException.unexpectedUserException(se);
				}
				finally {
					userVTI = null;
				}
			}
			if ((userPS != null) && !reuseablePs)
			{
				try
				{
					userPS.close();
				} catch (SQLException se)
				{
					throw StandardException.unexpectedUserException(se);
				}
				finally {
					userPS = null;
				}
			}
			super.close(cleanupOnError);
		}
		else
			if (SanityManager.DEBUG)
				SanityManager.DEBUG("CloseRepeatInfo","Close of VTIResultSet repeated");

		if (statisticsTimingOn) closeTime += getElapsedNanos(beginTime);
	}

	public void finish() throws StandardException {

		// for a reusablePS it will be closed by the activation
		// when it is closed.
		if ((userPS != null) && !reuseablePs)
		{
			try
			{
				userPS.close();
				userPS = null;
			} catch (SQLException se)
			{
				throw StandardException.unexpectedUserException(se);
			}
		}

		finishAndRTS();

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
	        final long time = PlanUtils.getTimeSpent(constructorTime, openTime, nextTime, closeTime, timeType);
		return time;
	}

	//
	// CursorResultSet interface
	//

	/**
	 * This is not operating against a stored table,
	 * so it has no row location to report.
	 *
	 * @see CursorResultSet
	 *
	 * @return a null.
	 */
	public RowLocation getRowLocation() {
		if (SanityManager.DEBUG)
			SanityManager.THROWASSERT("RowResultSet used in positioned update/delete");
		return null;
	}

	/**
	 * This is not used in positioned update and delete,
	 * so just return a null.
	 *
	 * @see CursorResultSet
	 *
	 * @return a null.
	 */
	public ExecRow getCurrentRow() {
		if (SanityManager.DEBUG)
			SanityManager.THROWASSERT("RowResultSet used in positioned update/delete");
		return null;
	}

	// Class implementation

	/**
	 * Return the GeneratedMethod for instantiating the VTI.
	 *
	 * @return The  GeneratedMethod for instantiating the VTI.
	 */
	GeneratedMethod getVTIConstructor()
	{
		return constructor;
	}

	boolean isReuseablePs() {
		return reuseablePs;
	}


	/**
	 * Cache the ExecRow for this result set.
	 *
	 * @return The cached ExecRow for this ResultSet
	 *
	 * @exception StandardException thrown on failure.
	 */
	private ExecRow getAllocatedRow()
		throws StandardException
	{
		if (allocatedRow == null)
		{
			allocatedRow = (ExecRow) row.invoke(activation);
		}

		return allocatedRow;
	}

	private int[] getProjectedColList() {

		FormatableBitSet refs = referencedColumns;
		int size = refs.getLength();
		int arrayLen = 0;
		for (int i = 0; i < size; i++) {
			if (refs.isSet(i))
				arrayLen++;
		}

		int[] colList = new int[arrayLen];
		int offset = 0;
		for (int i = 0; i < size; i++) {
			if (refs.isSet(i))
				colList[offset++] = i + 1;
		}

		return colList;
	}
	/**
	 * @exception StandardException thrown on failure to open
	 */
	public void populateFromResultSet(ExecRow row)
		throws StandardException
	{
		try
		{
            DataTypeDescriptor[]    columnTypes = null;
            if ( isDerbyStyleTableFunction )
            {
                    columnTypes = getReturnColumnTypes();
            }

			boolean[] nullableColumn = setNullableColumnList();
			DataValueDescriptor[] columns = row.getRowArray();
			// ExecRows are 0-based, ResultSets are 1-based
			int rsColNumber = 1;
			for (int index = 0; index < columns.length; index++)
			{
				// Skip over unreferenced columns
				if (referencedColumns != null && (! referencedColumns.get(index)))
				{
					if (!pushedProjection)
						rsColNumber++;

					continue;
				}

				columns[index].setValueFromResultSet(
									userVTI, rsColNumber, 
									/* last parameter is whether or
									 * not the column is nullable
									 */
									nullableColumn[rsColNumber]);
				rsColNumber++;

                // for Derby-style table functions, coerce the value coming out
                // of the ResultSet to the declared SQL type of the return
                // column
                if ( isDerbyStyleTableFunction )
                {
                    DataTypeDescriptor  dtd = columnTypes[ index ];
                    DataValueDescriptor dvd = columns[ index ];

                    cast( dtd, dvd );
                }

            }

		} catch (StandardException se) {
			throw se;
		}
		catch (Throwable t)
		{
			throw StandardException.unexpectedUserException(t);
		}
	}

	public final int getScanIsolationLevel() {
		return scanIsolationLevel;
	}

	/*
	** VTIEnvironment
	*/
	public final boolean isCompileTime() {
		return false;
	}

	public final String getOriginalSQL() {
		return activation.getPreparedStatement().getUserQueryString(activation.getLanguageConnectionContext());
	}

	public final int getStatementIsolationLevel() {
		return ExecutionContext.CS_TO_JDBC_ISOLATION_LEVEL_MAP[getScanIsolationLevel()];
	}


	public final void setSharedState(String key, java.io.Serializable value) {
		if (key == null)
			return;

		if (compileTimeConstants == null) {

			Object[] savedObjects = activation.getPreparedStatement().getSavedObjects();

			synchronized (savedObjects) {

				compileTimeConstants = (FormatableHashtable) savedObjects[ctcNumber];
				if (compileTimeConstants == null) {
					compileTimeConstants = new FormatableHashtable();
					savedObjects[ctcNumber] = compileTimeConstants;
				}
			}
		}

		if (value == null)
			compileTimeConstants.remove(key);
		else
			compileTimeConstants.put(key, value);


	}

	public Object getSharedState(String key) {
		if ((key == null) || (compileTimeConstants == null))
			return null;

		return compileTimeConstants.get(key);
	}

    /**
     * <p>
     * Get the types of the columns returned by a Derby-style table function.
     * </p>
     */
    private DataTypeDescriptor[]    getReturnColumnTypes()
        throws StandardException
    {
        if ( returnColumnTypes == null )
        {
            TypeDescriptor      td = thawReturnType( returnType );
            TypeDescriptor[]    columnTypes = td.getRowTypes();
            int                         count = columnTypes.length;

            returnColumnTypes = new DataTypeDescriptor[ count ];
            for ( int i = 0; i < count; i++ )
            {
                returnColumnTypes[ i ] = DataTypeDescriptor.getType( columnTypes[ i ] );
            }
        }

        return returnColumnTypes;
    }

    /**
     * <p>
     * Deserialize a type descriptor from a string.
     * </p>
     */
    private TypeDescriptor  thawReturnType( String ice )
        throws StandardException
    {
        try {
            byte[]                                          bytes = FormatIdUtil.fromString( ice );
            ByteArrayInputStream                    bais = new ByteArrayInputStream( bytes );
            FormatIdInputStream                     fiis = new FormatIdInputStream( bais );
            TypeDescriptor                              td = (TypeDescriptor) fiis.readObject();

            return td;
            
        } catch (Throwable t)
        {
            throw StandardException.unexpectedUserException( t );
        }
    }
    
    /**
     * <p>
     * Cast the value coming out of the user-coded ResultSet. The
     * rules are described in CastNode.getDataValueConversion().
     * </p>
     */
    private void    cast( DataTypeDescriptor dtd, DataValueDescriptor dvd )
        throws StandardException
    {
        TypeId      typeID = dtd.getTypeId();

        if ( !typeID.isBlobTypeId() && !typeID.isClobTypeId() )
        {
            if ( typeID.isLongVarcharTypeId() ) { castLongvarchar( dtd, dvd ); }
            else if ( typeID.isLongVarbinaryTypeId() ) { castLongvarbinary( dtd, dvd ); }
            else if ( typeID.isDecimalTypeId() ) { castDecimal( dtd, dvd ); }
            else
            {
                Object      o = dvd.getObject();

                dvd.setObjectForCast( o, true, typeID.getCorrespondingJavaTypeName() );

                if ( typeID.variableLength() )
                {
                    VariableSizeDataValue   vsdv = (VariableSizeDataValue) dvd;
                    int                                 width;
                    if ( typeID.isNumericTypeId() ) { width = dtd.getPrecision(); }
                    else { width = dtd.getMaximumWidth(); }
            
                    vsdv.setWidth( width, dtd.getScale(), false );
                }
            }

        }

    }

    /**
     * <p>
     * Truncate long varchars to the legal maximum.
     * </p>
     */
    private void    castLongvarchar( DataTypeDescriptor dtd, DataValueDescriptor dvd )
        throws StandardException
    {
        if ( dvd.getLength() > TypeId.LONGVARCHAR_MAXWIDTH )
        {
            dvd.setValue( dvd.getString().substring( 0, TypeId.LONGVARCHAR_MAXWIDTH ) );
        }
    }
    
    /**
     * <p>
     * Truncate long varbinary values to the legal maximum.
     * </p>
     */
    private void    castLongvarbinary( DataTypeDescriptor dtd, DataValueDescriptor dvd )
        throws StandardException
    {
        if ( dvd.getLength() > TypeId.LONGVARBIT_MAXWIDTH )
        {
            byte[]  original = dvd.getBytes();
            byte[]  result = new byte[ TypeId.LONGVARBIT_MAXWIDTH ];

            System.arraycopy( original, 0, result, 0, TypeId.LONGVARBIT_MAXWIDTH );
            
            dvd.setValue( result );
        }
    }
    
    /**
     * <p>
     * Set the correct precision and scale for a decimal value.
     * </p>
     */
    private void    castDecimal( DataTypeDescriptor dtd, DataValueDescriptor dvd )
        throws StandardException
    {
        VariableSizeDataValue   vsdv = (VariableSizeDataValue) dvd;
            
        vsdv.setWidth( dtd.getPrecision(), dtd.getScale(), false );
    }

// GemStone changes BEGIN
    @Override
  public boolean isForUpdate() {
    if (this.userVTI instanceof com.pivotal.gemfirexd.internal.engine.sql.execute.UpdatableResultSet) {
      return ((com.pivotal.gemfirexd.internal.engine.sql.execute.UpdatableResultSet)
          this.userVTI).isForUpdate();
    }
    return super.isForUpdate();
  }

  @Override
  public boolean canUpdateInPlace() {
    // for VTITemplates it has to be an in-place update if required
    if (this.userVTI instanceof com.pivotal.gemfirexd.internal.engine.sql.execute.UpdatableResultSet) {
      return ((com.pivotal.gemfirexd.internal.engine.sql.execute.UpdatableResultSet)
          this.userVTI).canUpdateInPlace();
    }
    return super.canUpdateInPlace();
  }

  @Override
  public void updateRow(ExecRow row) throws StandardException {
    if (this.userVTI instanceof UpdateVTITemplate) {
      ((UpdateVTITemplate)this.userVTI).updateRow(row);
    }
    else {
      super.updateRow(row);
    }
  }

  @Override
  public void updateRowLocationPostRead() throws StandardException {
  }

  @Override
  public void filteredRowLocationPostRead(TXState localTXState) {
  }

  @Override
  public void accept(ResultSetStatisticsVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public void resetStatistics() {
    rowsReturned = 0;
    super.resetStatistics();
  }

  private void setSharedStateInUserVTI() throws SQLException {
    if (this.userVTI instanceof com.pivotal.gemfirexd.internal.engine.GfxdVTITemplate) {
      ((com.pivotal.gemfirexd.internal.engine.GfxdVTITemplate)userVTI)
          .setSharedState(compileTimeConstants);
    }
    
    if (userVTI instanceof com.pivotal.gemfirexd.internal.vti.Pushable) {
      com.pivotal.gemfirexd.internal.vti.Pushable p = (com.pivotal.gemfirexd.internal.vti.Pushable)userVTI;
      if (referencedColumns != null) {
        pushedProjection = p.pushProjection(this, getProjectedColList());
      }
    }

    if (userVTI instanceof com.pivotal.gemfirexd.internal.vti.IQualifyable) {
      com.pivotal.gemfirexd.internal.vti.IQualifyable q = (com.pivotal.gemfirexd.internal.vti.IQualifyable)userVTI;

      q.setQualifiers(this, pushedQualifiers);
    }
  }
  
  @Override
  public StringBuilder buildQueryPlan(StringBuilder builder, PlanUtils.Context context) {
    super.buildQueryPlan(builder, context);
    
    if (userVTI != null)
      PlanUtils.xmlAttribute(builder, "userVTI", this.userVTI.getClass().getSimpleName());
    
    if (userPS != null)
    {
      final GenericPreparedStatement gps;
      if(this.userPS instanceof EmbedPreparedStatement) {
        gps = ((EmbedPreparedStatement)this.userPS).getGPS();
      } else if (this.userPS instanceof GenericPreparedStatement) {
        gps = ((GenericPreparedStatement)this.userPS);
      }
      else {
        gps = null;
      }

      if (gps != null)
        PlanUtils.xmlAttribute(builder, "userPS", gps.getUserQueryString(lcc));
    }
    
    PlanUtils.xmlTermTag(builder, context, PlanUtils.OP_VTI);
    
    PlanUtils.xmlCloseTag(builder, context, this);
    
    return builder;
    
  }
// GemStone changes END

  /**
   * Test method.
   * @return
   */
  public IFastPath getFastPath() {
    return fastPath;
  }

  /**
   * Test method.
   */
  public ResultSet getUserVTI() {
    return userVTI;
  }
  
  @Override
  public void printResultSetHierarchy() {
    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TraceNCJ) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_NON_COLLOCATED_JOIN,
            "ResultSet Created: " + this.getClass().getSimpleName()
                + " with resultSetNumber=" + resultSetNumber);
      }
    }
  }
}
