
/*

 Derived from source files from the Derby project.

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
 * Changes for GemFireXD distributed data platform.
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

package com.pivotal.gemfirexd.internal.engine.distributed.metadata;

import java.util.ArrayList;
import java.util.Vector;

import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.expression.ExpressionBuilderVisitor;
import com.pivotal.gemfirexd.internal.engine.jdbc.GemFireXDRuntimeException;
import com.pivotal.gemfirexd.internal.engine.sql.compile.CollectExpressionOperandsVisitor;
import com.pivotal.gemfirexd.internal.engine.sql.compile.CollectParameterNodeVisitor;
import com.pivotal.gemfirexd.internal.engine.sql.execute.GemFireDistributedResultSet;
import com.pivotal.gemfirexd.internal.engine.store.RowFormatter;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.context.ContextManager;
import com.pivotal.gemfirexd.internal.iapi.services.loader.GeneratedClass;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.C_NodeTypes;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.store.access.ColumnOrdering;
import com.pivotal.gemfirexd.internal.iapi.types.DataTypeDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.TypeId;
import com.pivotal.gemfirexd.internal.impl.sql.compile.GroupByColumn;
import com.pivotal.gemfirexd.internal.impl.sql.compile.GroupByList;
import com.pivotal.gemfirexd.internal.impl.sql.compile.GroupByNode;
import com.pivotal.gemfirexd.internal.impl.sql.compile.ResultColumn;
import com.pivotal.gemfirexd.internal.impl.sql.compile.ResultColumnList;
import com.pivotal.gemfirexd.internal.impl.sql.compile.ValueNode;
import com.pivotal.gemfirexd.internal.impl.sql.compile.ValueNodeList;
import com.pivotal.gemfirexd.internal.impl.sql.compile.VirtualColumnNode;
import com.pivotal.gemfirexd.internal.impl.sql.execute.AggregatorInfo;
import com.pivotal.gemfirexd.internal.impl.sql.execute.AggregatorInfoList;
import com.pivotal.gemfirexd.internal.impl.sql.execute.BaseActivation;

/**
 * This query info class compresses the GroupBy query tree.
 * 
 * e.g. "select x, avg(y) from T group by z,x" query
 *  will have derby generated GroupBy portion of tree as
 *    
 *      PRN (ResultColumList - x, avg(y) )
 *       |
 *      GBN (ResultColumList - x, outputCol(y), inputCol(y), avgAggregator(y), z)
 *       |
 *      PRN (ResultColumList - x,y,z)
 *       
 *  and in order to split/merge the average aggregate we have converted the derby tree
 *  to  
 *  
 *   in QueryNode:
 *   
 *      PRN (ResultColumList - x, sum(y)/count(y) )
 *       |
 *      GBN (ResultColumList - x, outputCol(y), inputCol(y), SUMAggregator(y), outputCol(y), inputCol(y), COUNTAggregator(y), z)
 *       |
 *      PRN (ResultColumList - x,y,z)
 *      
 *  and in DataNode:
 * 
 *      PRN (ResultColumList - x, sum(y), count(y), z )
 *       |
 *      GBN (ResultColumList - x, outputCol(y), inputCol(y), SUMAggregator(y), outputCol(y), inputCol(y), COUNTAggregator(y), z)
 *       |
 *      PRN (ResultColumList - x,y,z)
 *      
 * so, now to merge the remote nodes we will need three row formats
 * a) PRN as asked by the user.
 * b) GBN to re-aggregate the results
 * c) DataNode PRN required to read the remote rows (sum,count) format 
 * 
 *  Remote data rows are expected in DataNode PRN form and GBN row format is only for re-aggregation.
 *  So, GroupBy columnOrdering is based on DataNode PRN whereas aggInfo is based on GBN row.
 *   
 *  Lastly, to return the user expected row does a 'doProjection' is applied to DataNode PRN row.  
 * 
 * @author Soubhik
 * @since GemFireXD
 *
 */
@SuppressWarnings("serial")
public class GroupByQueryInfo extends AbstractQueryInfo implements SecondaryClauseQueryInfo{

  protected AggregatorInfoList aggInfo;
  private ColumnOrdering[] columnOrdering;
  private final boolean isInSortedOrder;
  private boolean isGroupingRequired;

  /**
   * This row format represents post GroupByNode#addAggregateColumns()
   * i.e. for each aggregate we will have 3 columns "OutputCol, InputCol, Aggregator"
   * 
   * This row is used to accumulate the aggregates without copying the underlying data
   * from incoming ExecRow created in ResultHolder#getNext().
   * 
   * @see GemFireDistributedResultSet.GroupedIterator#next()
   */
  private ExecRow groupByExecRow;
  
  /**
   * This row format will be one-to-one stripped down version of resultColumns
   * with which the Data Store node will return rows to query nodes.
   * 
   * e.g. "Select count(1) from Table Group By Col1" 
   * Data Store node will ship the group by column (Col1) in the resultSet. 
   */
  private ExecRow expectedRemoteExecRow;
  private RowFormatter rowFormatter;

  private int[] projectMapping ;
  
  private GeneratedClass exprClass;
  private ArrayList<String> exprMethodList;
  
  /**
   * This is to capture the underlying type of a DVDSet. 
   * If not stored here, we  have to loop back through 
   * generated class as shown in following stack.
   * ...impl.sql.compile.UserDefinedTypeCompiler.nullMethodName(UserDefinedTypeCompiler.java:121)
   * ...impl.sql.compile.BaseTypeCompiler.generateNull(BaseTypeCompiler.java:117)
   * ...impl.sql.compile.UserDefinedTypeCompiler.generateNull(UserDefinedTypeCompiler.java:43)
   * ...impl.sql.compile.ExpressionClassBuilder.generateNullWithExpress(ExpressionClassBuilder.java:914)
   * ...impl.sql.compile.ResultColumnList.generateCore(ResultColumnList.java:1198)
   * 
   * A class generation taking object reference is non-trivial and also, keep such a compilation artifact
   * referenced risks memory leak in the sense restricts the entire from GC'ng. Although leak as of now 
   * can't because DTDs are referenced by the tree and not the other way but later if DTDs does loop back,
   * introducing a reference here in this class is too subtle to catch until we mark this in BOLD. 
   * 
   * #41492
   * 
   * Its only one variable because only one distinct can be there in a query.
   */
  private DataTypeDescriptor distinctAggUnderlyingType = null;
  
  public GroupByQueryInfo(QueryInfoContext qic,
                          GroupByNode groupByNode,
                          AggregatorInfoList _aggInfo, 
                          GroupByList _groupinglist, 
                          ResultColumnList resultColumns, 
                          ResultColumnList parentRCL,
                          ValueNode havingClause,
                          boolean isInSortedOrder) throws StandardException {
   
    this.aggInfo = _aggInfo;
    this.isInSortedOrder = isInSortedOrder;

    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TraceGroupByQI) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_GROUPBYQI,
            "GroupByQI::(): received parentRCL " + parentRCL);
      }
    }
    final LanguageConnectionContext lcc = groupByNode
        .getLanguageConnectionContext();
    CollectExpressionOperandsVisitor exprSubstitutor = new CollectExpressionOperandsVisitor(
                                                         lcc, null, false);
    parentRCL.accept(exprSubstitutor);
    
    ResultColumnList expandedRCL = exprSubstitutor.getResultColumns();

    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TraceGroupByQI) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_GROUPBYQI,
            "GroupByQI::(): parentRCL after expression operands expanded "
                + expandedRCL);
      }
    }

    /**
     * re-align group by column(s) to Projection row (parent RS)
     * Because of this, we will avoid un-necessary transformation of
     * Projected out remote entries to AccumulateRow and then back to
     * Projected row.
     */
    int[] origColumnPositions = null;

    // Collection phase I (projected grouped cols)
    if (_groupinglist != null) {
        if (SanityManager.DEBUG) {
          if (GemFireXDUtils.TraceGroupByQI) {
            SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_GROUPBYQI,
                "GroupByQI::(): GroupingList to begin with " + _groupinglist);
          }
        }

        int groupingCount = _groupinglist.size();
        origColumnPositions = new int[groupingCount];
        for( int i = 0; i < groupingCount; i++) 
        {
          GroupByColumn gbc = (GroupByColumn) _groupinglist.elementAt(i);
          ResultColumn rc = expandedRCL.findParentResultColumn( 
                                              resultColumns.getResultColumn( 
                                                 gbc.getColumnPosition() ));
          if(rc != null) {
            origColumnPositions[i] = gbc.getColumnPosition();
            gbc.setColumnPosition(rc.getVirtualColumnId());
          }
          else {
            origColumnPositions[i] = -1;
          }
          
          collectParamsIfAny(gbc.getColumnExpression(),qic);
        }

        if (SanityManager.DEBUG) {
          if (GemFireXDUtils.TraceGroupByQI) {
            SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_GROUPBYQI,
                "GroupByQI::(): GroupingList after re-aligning projected " +
                "grouping columns: " + _groupinglist);
          }
        }
    }

    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TraceGroupByQI) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_GROUPBYQI,
            "GroupByQI::(): AggregateList to begin with " + _aggInfo);
      }
    }
    /** re-align Aggregate column(s) ...
     * 
     * a) align aggregates w.r.t outer most parent PRN's aggregate columns as this will
     * be "the order" in which output from remote nodes will be received.
     * 
     */
    ResultColumn[] projectedAggregators = new ResultColumn[_aggInfo.size()];
    //Collection phase II (projected aggregates)
    for( int i = 0; i < _aggInfo.size(); i++) {
      AggregatorInfo aggregate = ((AggregatorInfo)_aggInfo.elementAt(i));
      ResultColumn rc = expandedRCL.findParentResultColumn( 
                                  resultColumns.getResultColumn(
                                       aggregate.getOutputColNum()+1)
                         );
      
      if( rc == null) {
        continue;
      }
      projectedAggregators[i] = rc;
      ResultColumn inputRC = resultColumns.getResultColumn( aggregate.getInputColNum() + 1);
      
      aggregate.setInputColNum(rc.getVirtualColumnId());
      aggregate.setOutputColNum( aggregate.getOutputColNum() + 1 ) ;
      
      handleDistinctAggregate(aggregate, inputRC, rc);
      
      collectParamsIfAny( (inputRC !=null? inputRC.getExpression(): null),qic);
    }

    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TraceGroupByQI) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_GROUPBYQI,
            "GroupByQI::(): AggregateList after re-aligning projected "
                + "aggregates " + _aggInfo);
      }
    }

    final ContextManager cm = lcc.getContextManager();
    //Collection phase III (generated grouped cols)
    if( _groupinglist != null ) 
    {
        for(int i = 0, size = origColumnPositions.length; i < size; i++) 
        {
            if(origColumnPositions[i] != -1) {
              continue; 
            }

            GroupByColumn gbc = (GroupByColumn) _groupinglist.elementAt(i);

            ResultColumn gbRC =  resultColumns.getResultColumn( 
                                                 gbc.getColumnPosition() );

            VirtualColumnNode vc = (VirtualColumnNode)lcc.getLanguageConnectionFactory().
                                       getNodeFactory().getNode(
                                                      C_NodeTypes.VIRTUAL_COLUMN_NODE,
                                                      groupByNode, // source result set.
                                                      gbRC,
                                                      gbRC.getVirtualColumnId(),
                                                      cm);

            ResultColumn newRC = (ResultColumn)lcc.getLanguageConnectionFactory(). 
                                       getNodeFactory().getNode(
                                                      C_NodeTypes.RESULT_COLUMN,
                                                      "##UnaggColumn Generated result",
                                                      vc,
                                                      cm);
            newRC.markGenerated();
            newRC.bindResultColumnToExpression();
            origColumnPositions[i] = gbc.getColumnPosition();

            expandedRCL.addResultColumn(newRC);
            gbc.setColumnPosition(expandedRCL.size());
        }
    }    

    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TraceGroupByQI && _groupinglist != null) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_GROUPBYQI,
            "GroupByQI::(): GroupingList after re-aligning generated "
                + "grouping column " + _groupinglist);
      }
    }

    /*
     * if groupingList is null and havingClause is mentioned
     * a dummy Count(*) gets inserted which should be only 
     * non-projected aggregate post havingClause expansion.
     * 
     * But until havingClause is expanded there is no way
     * to find out which aggregate is truly non-projected.
     */
    int rclSelectListIndex = -1;
    if(havingClause != null) {
      
        if(_groupinglist == null) {
          rclSelectListIndex = exprSubstitutor.getResultColumns().size();          
        }
        
        havingClause.accept(exprSubstitutor);
        collectParamsIfAny(havingClause,qic);

        if (SanityManager.DEBUG) {
          if (GemFireXDUtils.TraceGroupByQI) {
            SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_GROUPBYQI,
                "GroupByQI::(): Expected row after expanding having clause "
                    + expandedRCL);
          }
        }
    }
    
  
    Vector<UpdateAggregatorInfo> updateAgg = new Vector<UpdateAggregatorInfo>();
    //Collection phase IV (generated aggregate cols)
    for( int i = 0, size = projectedAggregators.length; i < size; i++) {
      
        if(projectedAggregators[i] != null) {
          continue;
        }
        
        AggregatorInfo aggregate = ((AggregatorInfo)_aggInfo.elementAt(i));
        ResultColumn inputRC = resultColumns.getResultColumn( aggregate.getInputColNum() + 1);
        
        ResultColumn rc = expandedRCL.findParentResultColumn( 
                                          resultColumns.getResultColumn(
                                               aggregate.getOutputColNum()+1));
        
        if( rc != null) {
          projectedAggregators[i] = rc;
          
          updateAgg.add(new UpdateAggregatorInfo(aggregate, inputRC, rc, this));
          
          continue;
        }
        
        assert rclSelectListIndex != -1 : " non-projected aggregate is expected only when there is no group by" ;
        /*
         * possibility of coming here is of havingClause without a GroupBy adds a dummy count(*)
         * see sqlgrammar.jj#tableExpression
         */
        ResultColumn newRC = (ResultColumn) lcc.getLanguageConnectionFactory(). 
                                    getNodeFactory().getNode(
                                                    C_NodeTypes.RESULT_COLUMN,
                                                    "##aggregate Generated result",
                                                    groupByNode.getNullNode(
                                                           //this will always be 1 column descriptor. see GroupByNode#addAggregateColumns
                                                           aggregate.getResultDescription().getColumnDescriptor(1).getType()
                                                    ),
                                                    cm);
        newRC.markGenerated();
        newRC.bindResultColumnToExpression();
        expandedRCL.insertResultColumnAt(newRC, rclSelectListIndex);
        
        updateAgg.add(new UpdateAggregatorInfo(aggregate, inputRC, newRC, this));
    }
    
    for(UpdateAggregatorInfo ua : updateAgg) {
       ua.apply(); 
    }
    
    if(_groupinglist != null) {
        columnOrdering = _groupinglist.getColumnOrdering();
        isGroupingRequired = true;
    }
    else {
      isGroupingRequired = false;
    }
    
    try {
      
      groupByExecRow = resultColumns.buildEmptyRow();
      expectedRemoteExecRow = expandedRCL.buildEmptyRow();
      rowFormatter = getRowFormatterFromRCL(expandedRCL, lcc);

      if (SanityManager.DEBUG) {
        if (GemFireXDUtils.TraceGroupByQI) {
          SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_GROUPBYQI,
              "GroupByQI::(): finally expanded RCL is " + expandedRCL);
        }
      }
    } catch (StandardException e) {
      throw GemFireXDRuntimeException.newRuntimeException(
          "GroupByQueryInfo: Error generating template row", e);
    }
    generateExpressionClass(parentRCL, exprSubstitutor.getOtherExpressions(),qic);
    generateProjectMapping(parentRCL, exprSubstitutor.getNumOperands(), expandedRCL);
    parentRCL.resetVirtualColumnIds();
  }
  
  public GroupByQueryInfo(boolean isDummy) {
    assert isDummy;
    this.isInSortedOrder = false;
    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TraceGroupByQI) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_GROUPBYQI,
            "GroupByQI::(): Created Dummy ");
      }
    }
  }

  void handleDistinctAggregate(AggregatorInfo aggregate, ResultColumn inputRC, ResultColumn receiveRC) 
     throws StandardException 
  {
    
      if(! aggregate.isDistinct()) {
        return;
      }
      
      distinctAggUnderlyingType = inputRC.getType();
      TypeId cti = TypeId.getUserDefinedTypeId(
          com.pivotal.gemfirexd.internal.engine.sql.compile.types.DVDSet.class.getName(), 
          distinctAggUnderlyingType, false);
      DataTypeDescriptor type = new DataTypeDescriptor(cti, false);
      receiveRC.setType(type);
      inputRC.setType(type);
  }

  /**
   * To handle ParameterNode inside Group By, Having  etc. <..>
   */
  private void collectParamsIfAny(ValueNode expression, QueryInfoContext qic) 
      throws StandardException
  {
    if(expression == null) {
      return;
    }
    
    CollectParameterNodeVisitor collectParams = new CollectParameterNodeVisitor(qic);
    expression.accept(collectParams);
    
  }

  private void generateExpressionClass(ResultColumnList parentRCL,
      ValueNodeList otherExprs, QueryInfoContext qic) throws StandardException {
    ExpressionBuilderVisitor genExpression = new ExpressionBuilderVisitor(
        parentRCL.getLanguageConnectionContext());

    parentRCL.accept(genExpression);

    if(otherExprs != null) {
      genExpression.dontSkipChildren();
      otherExprs.accept(genExpression);
    }
    
    exprClass = genExpression.getExpressionClass();
    if(SanityManager.ASSERT) {
      if (exprClass != null) {
        BaseActivation a = (BaseActivation)exprClass
            .newInstance(parentRCL.getLanguageConnectionContext(), false, null /* no ExecPreparedStatement*/);
        SanityManager.ASSERT(a != null, "Bad expression class generated ");
        a.close();
      }
    }

    
    exprMethodList = genExpression.getMethodsGenerated();
    
  }
  
  private void generateProjectMapping(ResultColumnList parentRCL, ArrayList<Integer> numOperands, ResultColumnList dataNodeRCL)
    throws StandardException
  {
    
    projectMapping = new int[parentRCL.size()];
    
    for(int i = 0, j = 1, size = parentRCL.size(); i < size; i++, j++) {
      projectMapping[i] = -1;
      
      if(exprMethodList.get(i) != null) {
        projectMapping[i] = -(i+1); //negative here means expr method on this offset needs to be applied.
        j += (numOperands.get(i).intValue()) - 1;
        continue;
      }
      
      ResultColumn check;
      while( (check = dataNodeRCL.getResultColumn(j)) !=null && check.isGenerated()) {
        j++;
      }
      
      projectMapping[i] = j;
    }

    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TraceGroupByQI) {

        StringBuilder sb = new StringBuilder();
        int idx = 0;
        for (int i : projectMapping) {
          sb.append(" [" + (idx++) + "] = " + i);
        }
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_GROUPBYQI,
            "GroupByQI::generateProjectMapping(): " + sb);
      }
    }
  }

  public final ExecRow getTemplateRow() {
    return groupByExecRow;
  }
  
  public final AggregatorInfoList getAggInfo() {
    return aggInfo;
  }
  
  public final boolean isInSortedOrder() {
    return isInSortedOrder;
  }
  
  
  public final ColumnOrdering[] getColumnOrdering() {
    return columnOrdering;
  }

  public final ExecRow getInComingProjectionExecRow() {
    return expectedRemoteExecRow;
  }

  public final RowFormatter getRowFormatter() {
    return this.rowFormatter;
  }

  /**
   * Indicates whether re-grouping at the query node is required or not.
   * This is to optimize between a simple aggregate and an
   * aggregate with groupby clause.<br><br>
   * <b>e.g.</b> select count(1) from tab;<br>
   * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
   *             select count(1) from tab group by x;
   * @return boolean (true means group by clause is present).
   */
  public final boolean doReGrouping() {
    return isGroupingRequired;
  }

  public DataTypeDescriptor getDistinctAggregateUnderlyingType() {
    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TraceAggreg) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_AGGREG,
            "returning Distinct Aggregate underlying type from GBQI "
                + distinctAggUnderlyingType);
      }
    }
    return distinctAggUnderlyingType;
  }

  public final int[] getProjectMapping() {
    return null;
  }

  public final int[] getPostGroupingProjectMapping() {
    return projectMapping;
  }

  public final BaseActivation getExpressionEvaluator(
      LanguageConnectionContext lcc) throws StandardException {
    if (exprClass == null) {
      return null;
    }

    return (BaseActivation)exprClass
        .newInstance(lcc, true, null /* no ExecPreparedStatement*/);
  }

  public final ArrayList<String> getExprMethodList() {
    return exprMethodList;
  }
}

class UpdateAggregatorInfo {

  private final AggregatorInfo aggregate;

  private final ResultColumn inputRC;

  private final ResultColumn expandedRC;

  private final GroupByQueryInfo _this;

  UpdateAggregatorInfo(AggregatorInfo _agg, ResultColumn _inRC,
      ResultColumn _expdRC, GroupByQueryInfo _this) {
    this.aggregate = _agg;
    this.inputRC = _inRC;
    this.expandedRC = _expdRC;
    this._this = _this;
  }

  void apply() throws StandardException {

    aggregate.setInputColNum(expandedRC.getVirtualColumnId());
    aggregate.setOutputColNum(aggregate.getOutputColNum() + 1);

    _this.handleDistinctAggregate(aggregate, inputRC, expandedRC);

    if (GemFireXDUtils.TraceGroupByQI) {
      SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_GROUPBYQI,
          "GroupByQI::(): after re-aligning generated aggregate " + aggregate);
    }
  }
}
