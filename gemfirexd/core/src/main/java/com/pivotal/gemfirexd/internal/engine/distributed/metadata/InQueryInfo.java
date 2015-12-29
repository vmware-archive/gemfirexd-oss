/*
 * Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
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
package com.pivotal.gemfirexd.internal.engine.distributed.metadata;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.gemstone.gemfire.LogWriter;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.access.index.GlobalRowLocation;
import com.pivotal.gemfirexd.internal.engine.ddl.resolver.GfxdPartitionResolver;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.jdbc.GemFireXDRuntimeException;
import com.pivotal.gemfirexd.internal.engine.sql.compile.types.DVDSet;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableBitSet;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.ConglomerateDescriptor;
import com.pivotal.gemfirexd.internal.iapi.store.access.ScanController;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.impl.sql.compile.BinaryRelationalOperatorNode;
import com.pivotal.gemfirexd.internal.impl.sql.compile.RelationalOperator;
import com.pivotal.gemfirexd.internal.shared.common.ColumnRoutingObjectInfo;
import com.pivotal.gemfirexd.internal.shared.common.ListRoutingObjectInfo;
import com.pivotal.gemfirexd.internal.shared.common.ResolverUtils;

//TODO:Asif: Should it extend AbstractConditionQueryInfo  or ComparisonQueryInfo.
//If it extends ComparisonQueryInfo we will get the behaviour of merging of operands
// though the rightOperand field of ComparisonQueryInfo will not make sense.
//  Presently extending ComparisonQueryInfo , but later if we decide to break 
//InQueryInfo into a JunctionQueryInfo , we should extend AbstractConditionueryInfo
/**
 * The instance of this class represents an In operator in the where clause.
 * An instance of this class necessarily means more than one values present
 * in the IN list. At this point assuming that it will not be converted into 
 * Region.Get. Later intelligence needs to be put in this class to identify 
 * if it can be converted into multiple Gets. Till then presence of this 
 * structure will imply not convertible to Get
 *   
 * @since GemFireXD
 * @author Asif
 * @see BinaryRelationalOperatorNode
 */
public class InQueryInfo extends ComparisonQueryInfo {  

  //private final QueryInfo[] listOperandsInfo;
 // private final boolean isListOpDynamic;
  private QueryInfo runTimePruner = null;
  private boolean rightOperandIsArray;
  
  public InQueryInfo(QueryInfo leftOp, boolean isListDynamic,
      ValueQueryInfo[] rhsOps) throws StandardException {
    super(leftOp, new ValueListQueryInfo(rhsOps, isListDynamic),
        RelationalOperator.EQUALS_RELOP);

    // [sumedh] adjust the type of constants (bug #41196)
    if (leftOp instanceof ColumnQueryInfo) {
      ColumnQueryInfo left = (ColumnQueryInfo)leftOp;
      for (ValueQueryInfo right : rhsOps) {
        adjustConstantType(left, right);
      }
    }
    // this.isListOpDynamic = isListDynamic;
  }

  @Override
  Object[] isConvertibleToGet(int pkColumn, TableQueryInfo tqi)
      throws StandardException {

    // TODO:Asif: Would it be better if we try to identify the column node
    // and the value node once & for all?
    Object[] pks = null;
    assert this.opType == RelationalOperator.EQUALS_RELOP;
    if (this.leftOperand instanceof ColumnQueryInfo
        && (this.rightOperand instanceof ValueQueryInfo)) {
      ColumnQueryInfo cqi = (ColumnQueryInfo)leftOperand;
      if (cqi.getActualColumnPosition() == pkColumn
          && cqi.getTableNumber() == tqi.getTableNumber()) {
        pks = (Object[])this.rightOperand.getPrimaryKey();
        /* Note:
         * Allow GetAll only for Partitioned Tables
         */
        if (pks.length > 1
            && tqi.getRegion().getPartitionAttributes() == null) {
          return null;
        }
      }
    }
    return pks;
  }

  @Override
  Object[] isConvertibleToGet(int[][] pkColumns, TableQueryInfo tqi)
      throws StandardException {
    if (pkColumns.length == 1) {
      Object[] pks = this.isConvertibleToGet(pkColumns[0][0], tqi);
      // If the primary key is not null and also it is not a Primary dynamic
      // key, then it needs to be coverted into GemFireKey here.
      if (pks != null) {
        final GemFireContainer container = (GemFireContainer)tqi.getRegion()
            .getUserAttribute();
        int len = pks.length;
        for (int i = 0; i < len; ++i) {
          Object pk = pks[i];
          // TODO:Asif: is there a better way
          if (pk instanceof DataValueDescriptor) {
            pks[i] = GemFireXDUtils.convertIntoGemfireRegionKey(
                (DataValueDescriptor)pk, container, false);
          }
        }
      }
      return pks;
    }
    else {
      return null;
    }
  }
  
  /*
   * (non-Javadoc)
   * @seeisConvertibleToGet(...)
   */
  @Override
  Object[] isConvertibleToGetOnLocalIndex(int ikColumn, TableQueryInfo tqi)
      throws StandardException {
    Object[] fks = null;
    assert this.opType == RelationalOperator.EQUALS_RELOP;
    if (this.leftOperand instanceof ColumnQueryInfo
        && (this.rightOperand instanceof ValueQueryInfo)) {
      ColumnQueryInfo cqi = (ColumnQueryInfo)leftOperand;
      if (cqi.getActualColumnPosition() == ikColumn
          && cqi.getTableNumber() == tqi.getTableNumber()) {
        fks = (Object[])this.rightOperand.getIndexKey();
        /* Note:
         * Allow GetAll only for Partitioned Tables
         */
        if (fks.length > 1 && tqi.getRegion().getPartitionAttributes() == null) {
          return null;
        }
      }
    }
    return fks;
  }
  
  /*
   * (non-Javadoc)
   * @seeisConvertibleToGet(...)
   */
  @Override
  Object[] isConvertibleToGetOnLocalIndex(int[][] ikColumns, TableQueryInfo tqi)
      throws StandardException {
    if (ikColumns.length == 1) {
      Object[] fks = this.isConvertibleToGetOnLocalIndex(ikColumns[0][0], tqi);
      return fks;
    }
    else {
      return null;
    }
  }

  /**
   * Test API
   * 
   * @return Returns an arary containing the left  operannd as the first element
   *  (0 th index ) & the rest  of the elements as the operands of the Lis
   */
  @Override
  QueryInfo[] getOperands() {    
    QueryInfo [] temp = new QueryInfo[1 + ((ValueListQueryInfo)this.rightOperand).getSize()];
    temp[0] = this.leftOperand;
    ValueQueryInfo vqiArr[] = ((ValueListQueryInfo)this.rightOperand).getOperands();
    for(int i=1; i < temp.length ; ++i) {
      temp [i] = vqiArr[i-1];
    }
    return temp;
  }
  
  @Override
  public boolean isWhereClauseDynamic() {
    return this.rightOperand.isDynamic() || this.leftOperand.isDynamic();
  }

  @Override
  public boolean isEquiJoinCondition() {
    // always return true since this may be a valid equijoin condition where
    // some tables are replicated
    return true;
  }

  @Override
  public String isEquiJoinColocationCriteriaFullfilled(TableQueryInfo ncjTqi) {
    // if the number of PR is <= 1 then no join required so is possible
    return (this.colocationMatrixPRTableCount <= 1) ? null
        : "more than one partitioned tables with an IN clause";
  }

  @Override
  AbstractConditionQueryInfo createOrAddToGroup(
      AbstractConditionQueryInfo operand,
      boolean createConstantConditionsWrapper, Activation activation, boolean forSingleHopPreparePhase) throws StandardException {
    AbstractConditionQueryInfo retVal = null;
    if (activation == null && (this.isWhereClauseDynamic() || operand.isWhereClauseDynamic())) {
      retVal = new ParameterizedConditionsWrapperQueryInfo(this, operand);
    }
    else if(createConstantConditionsWrapper){
      retVal = new ConstantConditionsWrapperQueryInfo(this, operand);
    }
    return retVal;
  }  
  
  
  @Override
  QueryInfo getRuntimeNodesPruner(boolean forSingleHopPreparePhase)  {
    if(this.runTimePruner == null) {
      this.runTimePruner = createRuntimeNodesPruner(forSingleHopPreparePhase);
    }
    return this.runTimePruner;
  }

  private QueryInfo createRuntimeNodesPruner(boolean forSingleHopPreparePhase) {
    QueryInfo pruner = null;

    if (this.leftOperand instanceof ColumnQueryInfo) {
      ColumnQueryInfo cqi = (ColumnQueryInfo)this.leftOperand;

      //PartitionedRegion pr = (PartitionedRegion)cqi.getRegion();
      final GfxdPartitionResolver rslvr = cqi.getResolverIfSingleColumnPartition();
      if (rslvr != null) {
        pruner = InQueryInfo.getResolverBasedPruner(rslvr,
            (ValueListQueryInfo)this.rightOperand, forSingleHopPreparePhase,
            this.rightOperandIsArray);
      }
      else {
        if (!forSingleHopPreparePhase) {
          pruner = InQueryInfo.attemptToGetGlobalIndexBasedPruner(
              (ValueListQueryInfo)this.rightOperand, cqi,
              this.rightOperandIsArray);
        }
        else {
          pruner = QueryInfoConstants.NON_PRUNABLE;
        }
      }
    }
    else {
      pruner = QueryInfoConstants.NON_PRUNABLE;
    }

    return pruner;
  }

  private static QueryInfo getResolverBasedPruner(
      final GfxdPartitionResolver rslvr, final ValueListQueryInfo vlqi,
      boolean forSingleHopPreparePhase, final boolean rightOperandIsArray) {

    final LogWriter logger = Misc.getCacheLogWriter();

    QueryInfo pruner = new AbstractQueryInfo() {
      @Override
      public void computeNodes(Set<Object> routingKeysToExecute,
          Activation activation, boolean forSingleHopPreparePhase) throws StandardException
      {
        assert routingKeysToExecute.size() == 1;
        assert routingKeysToExecute.contains(ResolverUtils.TOK_ALL_NODES);
        if (logger.fineEnabled()) {
          logger.fine("InQueryInfo::computeNodes: Resolver being used = "
              + rslvr);
        }
        if (!forSingleHopPreparePhase) {
          DataValueDescriptor[] keys = vlqi
              .evaluateToGetDataValueDescriptorArray(activation);
          if (logger.fineEnabled()) {
            for (Object key : keys) {
              logger.fine("InQueryInfo::computeNodes: Key of In predicate  = "
                  + key);
            }
          }
          final Object[] routingObjects;
          if (rightOperandIsArray) {
            SanityManager.ASSERT(keys.length == 1);
            SanityManager.ASSERT(keys[0] instanceof DVDSet);
            keys = ((DVDSet)keys[0]).getValues();
          }
          routingObjects = rslvr.getRoutingObjectsForList(keys);
          assert routingObjects != null;
          // Get DistributedMembers
          routingKeysToExecute.remove(ResolverUtils.TOK_ALL_NODES);

          for (Object key : routingObjects) {
            if (logger.fineEnabled()) {
              logger
                  .fine("InQueryInfo::computeNodes: Routing Key for In predicate  = "
                      + key);
            }
            routingKeysToExecute.add(key);
          }
        }
        else {
          int size = vlqi.getSize();
          ValueQueryInfo[] operands = vlqi.getOperands();
          ColumnRoutingObjectInfo[] colRobjInfo = new ColumnRoutingObjectInfo[size];
          for (int i = 0; i < size; i++) {
            ValueQueryInfo vqi = operands[i];
            int type = vqi.typeOfValue();
            switch (type) {
              case ValueQueryInfo.CONSTANT_VALUE:
              case ValueQueryInfo.PARAMETERIZED_CONSTANT_VALUE:
                colRobjInfo[i] = getAppropriateRoutingObjectInfo(activation, vqi, rslvr);
                break;

              case ValueQueryInfo.PARAMETER_VALUE:
                colRobjInfo[i] = getAppropriateRoutingObjectInfo(activation, vqi, rslvr);
                break;

              default:
                throw new GemFireXDRuntimeException("unexpected type: " + type
                    + " encountered");
            }
          }
          ListRoutingObjectInfo lrobjInfo = new ListRoutingObjectInfo(
              colRobjInfo, rslvr);
          routingKeysToExecute.remove(ResolverUtils.TOK_ALL_NODES);
          routingKeysToExecute.add(lrobjInfo);
        }
      }
    };
    return pruner;

  }
  
  private static QueryInfo attemptToGetGlobalIndexBasedPruner(
      final ValueListQueryInfo vlqi, final ColumnQueryInfo cqi,
      final boolean rightOperandIsArray)
  {

    final LogWriter logger = Misc.getCacheLogWriter();
    List<ConglomerateDescriptor> globalIndexes = cqi
        .getAvailableGlobalHashIndexForColumn();
    // Pick only the index whose num cols is 1
    ConglomerateDescriptor cd = null;
    Iterator<ConglomerateDescriptor> itr = globalIndexes.iterator();
    QueryInfo pruner = null;
    while (itr.hasNext()) {
      ConglomerateDescriptor temp = itr.next();
      if (temp.getIndexDescriptor().baseColumnPositions().length == 1) {
        cd = temp;
        break;
      }
    }
    if (cd != null) {
      final long conglomID = cd.getConglomerateNumber();
      pruner = new AbstractQueryInfo() {
        @Override
        public void computeNodes(Set<Object> routingKeysToExecute,
            Activation activation, boolean forSingleHopPreparePhase) throws StandardException
        {
          assert routingKeysToExecute.size() == 1;
          assert routingKeysToExecute.contains(ResolverUtils.TOK_ALL_NODES);
          if (logger.fineEnabled()) {
            logger
                .fine("InQueryInfo::computeNodes: Conglom ID of the Global Index  being used = "
                    + conglomID);
          }
          TransactionController tc = activation.getTransactionController();
          DataValueDescriptor[] keys = vlqi
              .evaluateToGetDataValueDescriptorArray(activation);
          if (logger.fineEnabled()) {
            for (Object key : keys) {
              logger.fine("InQueryInfo::computeNodes: Key of In predicate  = "
                  + key);
            }
          }
          if (rightOperandIsArray) {
            SanityManager.ASSERT(keys.length == 1);
            SanityManager.ASSERT(keys[0] instanceof DVDSet);
            keys = ((DVDSet)keys[0]).getValues();
          }
          // By default clear the map as either no rows are inserted or atleast
          // one row would be inserted . If no row is inserted it implies no
          // fetch
          routingKeysToExecute.clear();
          for (DataValueDescriptor keyDVD : keys) {

            DataValueDescriptor[] dvdKeyArr = new DataValueDescriptor[] { keyDVD };
            ScanController scan = tc.openScan(
                conglomID,
                false, // hold
                0, // open read only
                TransactionController.MODE_TABLE,
                TransactionController.ISOLATION_SERIALIZABLE,
                (FormatableBitSet)null, dvdKeyArr, // start key value
                ScanController.GE, // start operator
                null, // qualifier
                dvdKeyArr, // stop key value
                ScanController.GE, null);
            if (scan.next()) {
              // Remove TOKEN_ALL_NODES

              final DataValueDescriptor[] fetchKey = new DataValueDescriptor[2];
              fetchKey[0] = keyDVD;
              while (scan.next()) {
                GlobalRowLocation grl = new GlobalRowLocation();
                fetchKey[1] = grl;
                scan.fetch(new DataValueDescriptor[] { keyDVD, grl });
                Object routingObject = grl.getRoutingObject();
                if (logger.fineEnabled()) {
                  logger.fine("ComparisonQueryInfo::pruneUsingGlobalIndex: "
                      + "Scan of Global index found a row = " + grl);
                  logger.fine("ComparisonQueryInfo::pruneUsingGlobalIndex: "
                      + "Scan of Global index found routing object = "
                      + routingObject);
                }

                assert routingObject != null;
                routingKeysToExecute.add(routingObject);
              }
            }
            else {
              if (logger.fineEnabled()) {
                logger.fine("ComparisonQueryInfo::pruneUsingGlobalIndex: "
                    + "Scan of Global index found NO row");
              }
            }
            scan.close();
          }
        }
      };
    }
    else {
      pruner = QueryInfoConstants.NON_PRUNABLE;
    }
    return pruner;
  }
  
  /*
   * NCJ Purpose
   */
  public void setRightOperandArray() {
    this.rightOperandIsArray = true;
  }
}
