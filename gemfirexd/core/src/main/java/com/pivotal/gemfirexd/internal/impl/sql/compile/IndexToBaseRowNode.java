/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.compile.IndexToBaseRowNode

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

package	com.pivotal.gemfirexd.internal.impl.sql.compile;













import java.util.Vector;
// GemStone changes BEGIN



import com.gemstone.gnu.trove.TIntArrayList;
import com.pivotal.gemfirexd.internal.catalog.types.ReferencedColumnsDescriptorImpl;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.engine.store.RowFormatter;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.ClassName;
import com.pivotal.gemfirexd.internal.iapi.services.classfile.VMOpcode;
import com.pivotal.gemfirexd.internal.iapi.services.compiler.MethodBuilder;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableBitSet;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.AccessPath;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.CostEstimate;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.Optimizable;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.RequiredRowOrdering;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.ConglomerateDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.TableDescriptor;
import com.pivotal.gemfirexd.internal.iapi.store.access.StaticCompiledOpenConglomInfo;
import com.pivotal.gemfirexd.internal.iapi.store.raw.ContainerHandle;
import com.pivotal.gemfirexd.internal.iapi.store.raw.ContainerKey;
import com.pivotal.gemfirexd.internal.impl.sql.compile.ActivationClassBuilder;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;

/**
 * This node type translates an index row to a base row.  It takes a
 * FromBaseTable as its source ResultSetNode, and generates an
 * IndexRowToBaseRowResultSet that takes a TableScanResultSet on an
 * index conglomerate as its source.
 */
public class IndexToBaseRowNode extends FromTable
{
	protected FromBaseTable	source;
	protected ConglomerateDescriptor	baseCD;
	protected boolean	cursorTargetTable;
	protected PredicateList restrictionList;
	protected boolean	forUpdate;
	private FormatableBitSet	heapReferencedCols;
	private FormatableBitSet	indexReferencedCols;
	private FormatableBitSet	allReferencedCols;
	private FormatableBitSet	heapOnlyReferencedCols;
	//GemStone changes BEGIN
        private boolean delayScanOpening = false;
        //GemStone changes END
	public void init(
			Object	source,
			Object	baseCD,
			Object	resultColumns,
			Object	cursorTargetTable,
			Object heapReferencedCols,
			Object indexReferencedCols,
			Object restrictionList,
			Object forUpdate,
			Object tableProperties)
	{
		super.init(null, tableProperties);
		this.source = (FromBaseTable) source;
		this.baseCD = (ConglomerateDescriptor) baseCD;
		this.resultColumns = (ResultColumnList) resultColumns;
		this.cursorTargetTable = ((Boolean) cursorTargetTable).booleanValue();
		this.restrictionList = (PredicateList) restrictionList;
		this.forUpdate = ((Boolean) forUpdate).booleanValue();
		this.heapReferencedCols = (FormatableBitSet) heapReferencedCols;
		this.indexReferencedCols = (FormatableBitSet) indexReferencedCols;

		if (this.indexReferencedCols == null) {
			this.allReferencedCols = this.heapReferencedCols;
			heapOnlyReferencedCols = this.heapReferencedCols;
		}
		else {
			this.allReferencedCols =
				new FormatableBitSet(this.heapReferencedCols);
			this.allReferencedCols.or(this.indexReferencedCols);
			heapOnlyReferencedCols =
				new FormatableBitSet(allReferencedCols);
			heapOnlyReferencedCols.xor(this.indexReferencedCols);
		}
	}

	/** @see Optimizable#forUpdate */
	public boolean forUpdate()
	{
		return source.forUpdate();
	}

	/** @see Optimizable#getTrulyTheBestAccessPath */
	public AccessPath getTrulyTheBestAccessPath()
	{
		// Get AccessPath comes from base table.
		return ((Optimizable) source).getTrulyTheBestAccessPath();
	}

	public CostEstimate getCostEstimate()
	{
		return source.getTrulyTheBestAccessPath().getCostEstimate();
	}

	public CostEstimate getFinalCostEstimate()
	{
		return source.getFinalCostEstimate();
	}

	/**
	 * Return whether or not the underlying ResultSet tree
	 * is ordered on the specified columns.
	 * RESOLVE - This method currently only considers the outermost table 
	 * of the query block.
	 *
	 * @param	crs					The specified ColumnReference[]
	 * @param	permuteOrdering		Whether or not the order of the CRs in the array can be permuted
	 * @param	fbtVector			Vector that is to be filled with the FromBaseTable	
	 *
	 * @return	Whether the underlying ResultSet tree
	 * is ordered on the specified column.
	 *
	 * @exception StandardException		Thrown on error
	 */
	boolean isOrderedOn(ColumnReference[] crs, boolean permuteOrdering, Vector fbtVector)
				throws StandardException
	{
		return source.isOrderedOn(crs, permuteOrdering, fbtVector);
	}

	/**
	 * Generation of an IndexToBaseRowNode creates an
	 * IndexRowToBaseRowResultSet, which uses the RowLocation in the last
	 * column of an index row to get the row from the base conglomerate (heap).
	 *
	 * @param acb	The ActivationClassBuilder for the class being built
	 * @param mb the method  for the method to be built
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void generate(ActivationClassBuilder acb,
								MethodBuilder mb)
							throws StandardException
	{
          /*
                ** Get the next ResultSet #, so that we can number this ResultSetNode,
                ** its ResultColumnList and ResultSet.
                */
                assignResultSetNumber();
                int heapColRefItem = -1;
                if (heapReferencedCols != null)
                {
                        heapColRefItem = acb.addItem(heapReferencedCols);
                }
                int allColRefItem = -1;
                if (allReferencedCols != null)
                {
                        allColRefItem = acb.addItem(allReferencedCols);
                }
                int heapOnlyColRefItem = -1;
                if (heapOnlyReferencedCols != null)
                {
                        heapOnlyColRefItem = acb.addItem(heapOnlyReferencedCols);
                }
// GemStone changes BEGIN
    if (this.source.isGFEResultSet()) {
      acb.pushGetResultSetFactoryExpression(mb);
      acb.pushThisAsActivation(mb);
      final MethodBuilder resultRowAllocator = this.resultColumns
          .generateHolderMethod(acb, this.allReferencedCols, null);
      acb.pushMethodReference(mb, resultRowAllocator);
      mb.push(this.resultSetNumber);
      int rowFormatterItem = -1;
      int fixedColsItem = -1;
      int varColsItem = -1;
      int lobColsItem = -1;
      int allColsItem = -1;
      int allColsWithLobsItem = -1;
      if (this.allReferencedCols != null) {
        // create the RowFormatter and push the projection fixed/var/LOB columns
        final GemFireContainer container = Misc.getMemStore().getContainer(
            ContainerKey.valueOf(ContainerHandle.TABLE_SEGMENT,
                this.baseCD.getConglomerateNumber()));
        final RowFormatter rf = container
            .getRowFormatter(this.allReferencedCols);
        if (rf != null) {
          final TIntArrayList fixedCols = new TIntArrayList();
          final TIntArrayList varCols = new TIntArrayList();
          final TIntArrayList lobCols = new TIntArrayList();
          final TIntArrayList allCols = new TIntArrayList();
          final TIntArrayList allColsWithLobs = new TIntArrayList();
          rf.getColumnPositions(fixedCols, varCols, lobCols, allCols,
              allColsWithLobs);
          rowFormatterItem = acb.addItem(rf);
          if (fixedCols.size() > 0) {
            fixedColsItem = acb.addItem(fixedCols.toNativeArray());
          }
          if (varCols.size() > 0) {
            varColsItem = acb.addItem(varCols.toNativeArray());
          }
          if (lobCols.size() > 0) {
            lobColsItem = acb.addItem(lobCols.toNativeArray());
          }
          if (allCols.size() > 0) {
            allColsItem = acb.addItem(allCols.toNativeArray());
          }
          assert allColsWithLobs.size() > 0;
          allColsWithLobsItem = acb.addItem(allColsWithLobs.toNativeArray());
        }
      }
      mb.push(rowFormatterItem);
      mb.push(fixedColsItem);
      mb.push(varColsItem);
      mb.push(lobColsItem);
      mb.push(allColsItem);
      mb.push(allColsWithLobsItem);
      mb.push(getCompilerContext().getScanIsolationLevel());
      mb.push(forUpdate);
      mb.push(acb.addItem(this.source.getQueryInfo()));
      mb.callMethod(VMOpcode.INVOKEINTERFACE, (String)null, "getGFEResultSet",
          ClassName.NoPutResultSet, 12);
      return;
    }
// GemStone changes END
		ValueNode		restriction = null;	

		// Get the CostEstimate info for the underlying scan
		costEstimate = getFinalCostEstimate();

		/* Put the predicates back into the tree */
		if (restrictionList != null)
		{
			restriction = restrictionList.restorePredicates();
			/* Allow the restrictionList to get garbage collected now
			 * that we're done with it.
			 */
			restrictionList = null;
		}

		// for the restriction, we generate an exprFun
		// that evaluates the expression of the clause
		// against the current row of the child's result.
		// if the restriction is empty, simply pass null
		// to optimize for run time performance.

   		// generate the function and initializer:
   		// Note: Boolean lets us return nulls (boolean would not)
   		// private Boolean exprN()
   		// {
   		//   return <<restriction.generate(ps)>>;
   		// }
   		// static Method exprN = method pointer to exprN;

		/* Create the ReferencedColumnsDescriptorImpl which tells which columns
		 * come from the index.
		 */
		int indexColMapItem = acb.addItem(new ReferencedColumnsDescriptorImpl(getIndexColMapping()));
		long heapConglomNumber = baseCD.getConglomerateNumber();
		StaticCompiledOpenConglomInfo scoci = getLanguageConnectionContext().
												getTransactionCompile().
													getStaticCompiledConglomInfo(heapConglomNumber);

		acb.pushGetResultSetFactoryExpression(mb);

		mb.push(heapConglomNumber);
		mb.push(acb.addItem(scoci));
		source.generate(acb, mb);
		
		mb.upCast(ClassName.NoPutResultSet);

		resultColumns.generateHolder(acb, mb,  heapReferencedCols, indexReferencedCols);
		mb.push(resultSetNumber);
		mb.push(source.getBaseTableName());
		mb.push(heapColRefItem);

		mb.push(allColRefItem);
		mb.push(heapOnlyColRefItem);

		mb.push(indexColMapItem);

		// if there is no restriction, we just want to pass null.
		if (restriction == null)
		{
		   	mb.pushNull(ClassName.GeneratedMethod);
		}
		else
		{
			// this sets up the method and the static field.
			// generates:
			// 	Object userExprFun { }
			MethodBuilder userExprFun = acb.newUserExprFun();

			// restriction knows it is returning its value;
	
			/* generates:
			 *    return <restriction.generate(acb)>;
			 * and adds it to userExprFun
			 * NOTE: The explicit cast to DataValueDescriptor is required
			 * since the restriction may simply be a boolean column or subquery
			 * which returns a boolean.  For example:
			 *		where booleanColumn
			 */
			restriction.generate(acb, userExprFun);
			userExprFun.methodReturn();

			// we are done modifying userExprFun, complete it.
			userExprFun.complete();

	   		// restriction is used in the final result set as an access of the new static
   			// field holding a reference to this new method.
			// generates:
			//	ActivationClass.userExprFun
			// which is the static field that "points" to the userExprFun
			// that evaluates the where clause.
   			acb.pushMethodReference(mb, userExprFun);
		}

		mb.push(forUpdate);
		mb.push(costEstimate.rowCount());
		mb.push(costEstimate.getEstimatedCost());
                //GemStone changes BEGIN
		mb.push(this.delayScanOpening);
		mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getIndexRowToBaseRowResultSet",
						ClassName.NoPutResultSet, 15  /*14*/);
		//GemStone changes END
		/* The IndexRowToBaseRowResultSet generator is what we return */

		/*
		** Remember if this result set is the cursor target table, so we
		** can know which table to use when doing positioned update and delete.
		*/
		if (cursorTargetTable)
		{
			acb.rememberCursorTarget(mb);
		}
	}

	/**
	 * Return whether or not the underlying ResultSet tree will return
	 * a single row, at most.
	 * This is important for join nodes where we can save the extra next
	 * on the right side if we know that it will return at most 1 row.
	 *
	 * @return Whether or not the underlying ResultSet tree will return a single row.
	 * @exception StandardException		Thrown on error
	 */
	public boolean isOneRowResultSet()	throws StandardException
	{
		// Default is false
		return source.isOneRowResultSet();
	}

	/**
	 * Return whether or not the underlying FBT is for NOT EXISTS.
	 *
	 * @return Whether or not the underlying FBT is for NOT EXISTS.
	 */
	public boolean isNotExists()
	{
		return source.isNotExists();
	}

	/**
	 * Decrement (query block) level (0-based) for this FromTable.
	 * This is useful when flattening a subquery.
	 *
	 * @param decrement	The amount to decrement by.
	 */
	void decrementLevel(int decrement)
	{
		source.decrementLevel(decrement);
	}

	/**
	 * Get the lock mode for the target of an update statement
	 * (a delete or update).  The update mode will always be row for
	 * CurrentOfNodes.  It will be table if there is no where clause.
	 *
	 * @return	The lock mode
	 */
	public int updateTargetLockMode()
	{
		return source.updateTargetLockMode();
	}

	/**
	 * @see ResultSetNode#adjustForSortElimination
	 */
	void adjustForSortElimination()
	{
		/* NOTE: We use a different method to tell a FBT that
		 * it cannot do a bulk fetch as the ordering issues are
		 * specific to a FBT being under an IRTBR as opposed to a
		 * FBT being under a PRN, etc.
		 */
		source.disableBulkFetch();
	}

	/**
	 * @see ResultSetNode#adjustForSortElimination
	 */
	void adjustForSortElimination(RequiredRowOrdering rowOrdering)
		throws StandardException
	{
		/* rowOrdering is not important to this specific node, so
		 * just call the no-arg version of the method.
		 */
		adjustForSortElimination();

		/* Now pass the rowOrdering down to source, which may
		 * need to do additional work. DERBY-3279.
		 */
		source.adjustForSortElimination(rowOrdering);
	}

	/** 
	 * Fill in the column mapping for those columns coming from the index.
	 *
	 * @return The int[] with the mapping.
	 */
	private int[] getIndexColMapping()
	{
		int		rclSize = resultColumns.size();
		int[]	indexColMapping = new int[rclSize];

		for (int index = 0; index < rclSize; index++)
		{
			ResultColumn rc = (ResultColumn) resultColumns.elementAt(index);
			if (indexReferencedCols != null && rc.getExpression() instanceof VirtualColumnNode)
			{
				// Column is coming from index
				VirtualColumnNode vcn = (VirtualColumnNode) rc.getExpression();
				indexColMapping[index] =
					vcn.getSourceColumn().getVirtualColumnId() - 1;
			}
			else
			{
				// Column is not coming from index
				indexColMapping[index] = -1;
			}
		}

		return indexColMapping;
	}
// GemStone changes BEGIN
        public FromBaseTable getSource() {
          return this.source;
        }
        
        public ReferencedColumnsDescriptorImpl getRefColDescImpl() {
          return new ReferencedColumnsDescriptorImpl(getIndexColMapping());
        }
        
        public void printSubNodes(int depth)
        {
                if (SanityManager.DEBUG)
                {
                        super.printSubNodes(depth);
                        
                        if(source != null) {
                          printLabel(depth, "source: \n");
                          source.printSubNodes(depth + 1);
                        }

                        if (restrictionList != null)
                        {
                                printLabel(depth, "restrictionList: ");
                                restrictionList.treePrint(depth);
                        }
                }
        }
        
        @Override
        public void delayScanOpening(boolean delay) {
          this.delayScanOpening = delay;
        }

      //This is appropriately overriden in SingleChildResultSetNode 
        //and Join node classes
        protected void optimizeForOffHeap( boolean shouldOptimize) {
           shouldOptimize = shouldOptimize || !this.isWholeRowNeeded();
           this.source.optimizeForOffHeap(shouldOptimize); 
        }

  private boolean isWholeRowNeeded() {
    TableDescriptor td = source.tableDescriptor;
    boolean getWholeRow = false;
    if (td != null) {
      int numCols = td.getNumberOfColumns();
      getWholeRow = true;
      if (this.allReferencedCols != null) {
        for (int i = 0; i < numCols; i++) {
          if (!this.allReferencedCols.isSet(i)) {
            getWholeRow = false;
            break;
          }
        }
      }
    }

    // also, gets whole row only if accessedHeapCols is equal to
    // accessedAllCols.
    // This should be the case if this is a SELECT, but may not be the case
    // for other cases such as DELETE
    if (getWholeRow) {
      getWholeRow = this.allReferencedCols == null
          && this.heapReferencedCols == null || this.allReferencedCols != null
          && this.allReferencedCols.equals(this.heapReferencedCols);
    }
    return getWholeRow;
  }
  
  @Override
  protected FromBaseTable ncjGetOnlyOneFBTNode() throws StandardException {
    return this.getSource();
  }
// GemStone changes END
}
