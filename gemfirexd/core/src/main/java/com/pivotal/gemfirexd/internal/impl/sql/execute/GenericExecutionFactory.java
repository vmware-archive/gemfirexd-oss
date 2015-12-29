/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.GenericExecutionFactory

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

import com.pivotal.gemfirexd.internal.catalog.TypeDescriptor;
import com.pivotal.gemfirexd.internal.catalog.UUID;
import com.pivotal.gemfirexd.internal.engine.procedure.cohort.ProcedureExecutionContextImpl;
import com.pivotal.gemfirexd.internal.engine.procedure.coordinate.DefaultProcedureResultProcessor;
import com.pivotal.gemfirexd.internal.engine.procedure.coordinate.ProcedureProxy;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.jdbc.ConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.reference.EngineType;
import com.pivotal.gemfirexd.internal.iapi.services.context.ContextManager;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatIdUtil;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableArrayHolder;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableBitSet;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableHashtable;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableIntHolder;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableProperties;
import com.pivotal.gemfirexd.internal.iapi.services.loader.GeneratedMethod;
import com.pivotal.gemfirexd.internal.iapi.services.monitor.ModuleControl;
import com.pivotal.gemfirexd.internal.iapi.services.monitor.ModuleSupportable;
import com.pivotal.gemfirexd.internal.iapi.services.monitor.Monitor;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.ParameterValueSet;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultColumnDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultDescription;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.IndexRowGenerator;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecIndexRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecutionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecutionFactory;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ResultSetFactory;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ResultSetStatisticsFactory;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.RowChanger;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ScanQualifier;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.xplain.XPLAINFactoryIF;
import com.pivotal.gemfirexd.internal.iapi.store.access.DynamicCompiledOpenConglomInfo;
import com.pivotal.gemfirexd.internal.iapi.store.access.Qualifier;
import com.pivotal.gemfirexd.internal.iapi.store.access.StaticCompiledOpenConglomInfo;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueFactory;
import com.pivotal.gemfirexd.internal.impl.sql.GenericColumnDescriptor;
import com.pivotal.gemfirexd.internal.impl.sql.GenericParameterValueSet;
import com.pivotal.gemfirexd.internal.impl.sql.GenericResultDescription;
import com.pivotal.gemfirexd.procedure.ProcedureExecutionContext;
import com.pivotal.gemfirexd.procedure.ProcedureResultProcessor;

import java.sql.ResultSet;
import java.util.Properties;
import java.util.Vector;

/**
	This Factory is for creating the execution items needed
	by a connection for a given database.  Once created for
	the connection, they should be pushed onto the execution context
	so that they can be found again by subsequent actions during the session.

 */
public class GenericExecutionFactory
	implements ModuleControl, ModuleSupportable, ExecutionFactory {
    
    /**
     * Statistics factory for this factory.
     */
    private ResultSetStatisticsFactory rssFactory;

	//
	// ModuleControl interface
	//
	public boolean canSupport(String identifier, Properties startParams)
	{
        return Monitor.isDesiredType(startParams,
                EngineType.STANDALONE_DB | EngineType.STORELESS_ENGINE);
	}

	/**
		This Factory is expected to be booted relative to a
		LanguageConnectionFactory.

		@see com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionFactory
	 * @exception StandardException Thrown on error
	 */
	public void boot(boolean create, Properties startParams)
		throws StandardException
	{
		// do we need to/ is there some way to check that
		// we are configured per database?

		/* Creation of the connection execution factories 
		 * for this database deferred until needed to reduce
		 * boot time.
		 */

		// REMIND: removed boot of LanguageFactory because
		// that is done in BasicDatabase.
	}

	public void stop() {
	}

	//
	// ExecutionFactory interface
	//
	/**
	 * Factories are generic and can be used by all connections.
	 * We defer instantiation until needed to reduce boot time.
	 * We may instantiate too many instances in rare multi-user
	 * situation, but consistency will be maintained and at some
	 * point, usually always, we will have 1 and only 1 instance
	 * of each factory because assignment is atomic.
	 */
	public ResultSetFactory getResultSetFactory() 
	{
		if (rsFactory == null)
		{
			rsFactory = new GenericResultSetFactory();
		}
		return rsFactory;
	}

	/**
	  *	Get the factory for constant actions.
	  *
	  *	@return	the factory for constant actions.
	  */
	public	GenericConstantActionFactory	getConstantActionFactory() 
	{ 
		if (genericConstantActionFactory == null)
		{
			genericConstantActionFactory = new GenericConstantActionFactory();
		}
		return genericConstantActionFactory; 
	}
    
    /**
     * Get the ResultSetStatisticsFactory from this ExecutionFactory.
     *
     * @return  The result set statistics factory associated with this
     *      ExecutionFactory
     *
     * @exception StandardException     Thrown on error
     */
    public ResultSetStatisticsFactory getResultSetStatisticsFactory()
                    throws StandardException {
        if (rssFactory == null) {
            rssFactory = (ResultSetStatisticsFactory)
                Monitor.bootServiceModule(
                                    false,
                                    this,
                                    ResultSetStatisticsFactory.MODULE,
                                    (Properties) null);
        }

        return rssFactory;
    }

	/**
		We want a dependency context so that we can push it onto
		the stack.  We could instead require the implementation
		push it onto the stack for us, but this way we know
		which context object exactly was pushed onto the stack.
	 */
	public ExecutionContext newExecutionContext(ContextManager cm)
	{
		/* Pass in nulls for execution factories.  GEC
		 * will call back to get factories when needed.
		 * This allows us to reduce boot time class loading.
		 * (Replication currently instantiates factories
		 * at boot time.)
		 */
		return new GenericExecutionContext(
							cm, this);
	}

	/*
	 * @see ExecutionFactory#getScanQualifier
	 */
	public ScanQualifier[][] getScanQualifier(int numQualifiers)
	{
		ScanQualifier[] sqArray = new GenericScanQualifier[numQualifiers];

		for (int ictr = 0; ictr < numQualifiers; ictr++)
		{
			sqArray[ictr] = new GenericScanQualifier();
		}

        ScanQualifier[][] ret_sqArray = { sqArray };

		return(ret_sqArray);
	}

	/**
		Make a result description
	 */
	public ResultDescription getResultDescription(
		ResultColumnDescriptor[] columns, String statementType) {
		return new GenericResultDescription(columns, statementType);
	}

	/**
	 * Create an execution time ResultColumnDescriptor from a 
	 * compile time RCD.
	 *
	 * @param compileRCD	The compile time RCD.
	 *
	 * @return The execution time ResultColumnDescriptor
	 */
	public ResultColumnDescriptor getResultColumnDescriptor(ResultColumnDescriptor compileRCD)
	{
		return new GenericColumnDescriptor(compileRCD);
	}

	/**
	 * @see ExecutionFactory#releaseScanQualifier
	 */
	public void releaseScanQualifier(ScanQualifier[][] qualifiers)
	{
	}

	/**
	 * @see ExecutionFactory#getQualifier
	 */
	public Qualifier getQualifier(
							int columnId,
							//Gemstone changes BEGIN
							String columnName,
							//Gemstone changes END
							int operator,
							GeneratedMethod orderableGetter,
							Activation activation,
							boolean orderedNulls,
							boolean unknownRV,
							boolean negateCompareResult,
							int variantType)
	{
	  //Gemstone changes BEGIN
	  // pass columnName
		return new GenericQualifier(columnId, columnName, operator, orderableGetter,
									activation, orderedNulls, unknownRV,
									negateCompareResult, variantType);
	  //Gemstone changes END
	}

	/**
	  @exception StandardException		Thrown on error
	  @see ExecutionFactory#getRowChanger
	  */
	public RowChanger
	getRowChanger(long heapConglom,
				  StaticCompiledOpenConglomInfo heapSCOCI,
				  DynamicCompiledOpenConglomInfo heapDCOCI,
				  IndexRowGenerator[] irgs,
				  long[] indexCIDS,
				  StaticCompiledOpenConglomInfo[] indexSCOCIs,
				  DynamicCompiledOpenConglomInfo[] indexDCOCIs,
				  int numberOfColumns,
				  TransactionController tc,
				  int[] changedColumnIds,
				  int[] streamStorableHeapColIds,
				  Activation activation) throws StandardException
	{
		return new RowChangerImpl( heapConglom, 
								   heapSCOCI, heapDCOCI, 
								   irgs, indexCIDS, indexSCOCIs, indexDCOCIs,
								   numberOfColumns, 
								   changedColumnIds, tc, null,
								   streamStorableHeapColIds, activation );
	}

	/**
	  @exception StandardException		Thrown on error
	  @see ExecutionFactory#getRowChanger
	  */
	public RowChanger getRowChanger(
			   long heapConglom,
			   StaticCompiledOpenConglomInfo heapSCOCI,
			   DynamicCompiledOpenConglomInfo heapDCOCI,
			   IndexRowGenerator[] irgs,
			   long[] indexCIDS,
			   StaticCompiledOpenConglomInfo[] indexSCOCIs,
			   DynamicCompiledOpenConglomInfo[] indexDCOCIs,
			   int numberOfColumns,
			   TransactionController tc,
			   int[] changedColumnIds,
			   FormatableBitSet	baseRowReadList,
			   int[] baseRowReadMap,
			   int[] streamStorableColIds,
			   Activation activation
			   )
		 throws StandardException
	{
		return new RowChangerImpl( heapConglom,
								   heapSCOCI, heapDCOCI, 
								   irgs, indexCIDS, indexSCOCIs, indexDCOCIs,
								   numberOfColumns, 
								   changedColumnIds, tc, baseRowReadList,
								   baseRowReadMap, activation );
	}


	/**
	 * Get a trigger execution context
	 *
	 * @exception StandardException		Thrown on error
	 */
	public InternalTriggerExecutionContext getTriggerExecutionContext
	(
		LanguageConnectionContext	lcc,
		ConnectionContext			cc,
		String 						statementText,
		int 						dmlType,
		int[]						changedColIds,
		String[]					changedColNames,
		UUID						targetTableId,
		String						targetTableName,
		Vector						aiCounters
	) throws StandardException
	{
		return new InternalTriggerExecutionContext(lcc, cc,
												   statementText, dmlType,
												   changedColIds,
												   changedColNames,
												   targetTableId,
												   targetTableName, 
												   aiCounters);
	}

	/*
		Old RowFactory interface
	 */

	public ExecRow getValueRow(int numColumns) {
		return new ValueRow(numColumns);
	}

	public ExecIndexRow getIndexableRow(int numColumns) {
		return new IndexRow(numColumns);
	}

	public ExecIndexRow getIndexableRow(ExecRow valueRow) {
		if (valueRow instanceof ExecIndexRow)
			return (ExecIndexRow)valueRow;
		return new IndexValueRow(valueRow);
	}

	//
	// class interface
	//
	public GenericExecutionFactory() {
	}

	//
	// fields
	//
	private ResultSetFactory rsFactory;
    private GenericConstantActionFactory	genericConstantActionFactory;
    private XPLAINFactoryIF xplainFactory; 
    
    /**
     * Get the XPLAINFactory from this ExecutionContext.
     *
     * @return  The XPLAINFactory associated with this
     *      ExecutionContext
     *
     * @exception StandardException     Thrown on error
     */
    public final XPLAINFactoryIF getXPLAINFactory()
                    throws StandardException {
        if (xplainFactory == null) {
            xplainFactory = (XPLAINFactoryIF)
                Monitor.bootServiceModule(
                                    false,
                                    this,
                                    XPLAINFactoryIF.MODULE,
                                    (Properties) null);
        }
        return xplainFactory;
    }

    //Gemstone changes Begin 
    public ProcedureProxy getDistributedProcedureCallProxy(
        Activation activation, 
        int tableIdIndex, 
        long indexId,
        int numColumns,
        GeneratedMethod startKeyGetter,
        int startSearchOperator,
        GeneratedMethod stopKeyGetter, 
        int stopSearchOperator,
        boolean sameStartStopPosition,
        DataValueDescriptor[] probeValues,
        int ordering, boolean all,
        int serverGroupIdx,
        int distributedProcNodeIdx,
        boolean nowait,
        ProcedureResultProcessor prp,
       /* DataValueDescriptor[] parameterSet,*/
        ResultSet[][] dynamicResultSets) {
      // TODO Auto-generated method stub
      return new ProcedureProxy(activation, 
      tableIdIndex,
      indexId,
      numColumns,
      startKeyGetter,
      startSearchOperator,
      stopKeyGetter,
      stopSearchOperator,
      sameStartStopPosition,
      probeValues,
      ordering,
      all,
      serverGroupIdx,
      distributedProcNodeIdx,
      nowait,
      prp,
    /*  parameterSet,*/
      dynamicResultSets);     
    }

    public ProcedureResultProcessor getDefaultProcedureResultProcessor() {
      
      return ( new DefaultProcedureResultProcessor());
    }

  public ProcedureExecutionContext getProcedureExecutionContext(
      Activation activation, ResultSet[][] dynamicResultSets,
      String procedureName, String tableName) {
    GenericParameterValueSet pvs = (GenericParameterValueSet)activation
        .getParameterValueSet();

    return (new ProcedureExecutionContextImpl(activation,
        activation.getProcedureSender(), pvs.getWhereClause(),
        activation.isPossibleDuplicate(), dynamicResultSets,
        pvs.getTableName(), procedureName));

  }

  //  public ProcedureSender getProcedureSender(Activation activation, ResultSet[][] dynamicResultSets) {
      
 //      ParameterValueSet pvs=activation.getParameterValueSet();
 //      ResultSender sender=pvs.getResultSender();
 //      ProcedureSender procedureSender=new ProcedureSender(activation, sender, dynamicResultSets );
 //      return procedureSender;
     
     
  //  }
    
    
    
    //Gemstone changes End
}
