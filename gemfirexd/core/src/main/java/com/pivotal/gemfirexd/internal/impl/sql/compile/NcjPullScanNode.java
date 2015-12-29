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
package com.pivotal.gemfirexd.internal.impl.sql.compile;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.NcjHashMapWrapper;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.QueryInfo;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.SelectQueryInfo;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.sql.execute.ExplainResultSet;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.jdbc.ConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.reference.ClassName;
import com.pivotal.gemfirexd.internal.iapi.services.classfile.VMOpcode;
import com.pivotal.gemfirexd.internal.iapi.services.compiler.MethodBuilder;
import com.pivotal.gemfirexd.internal.iapi.services.context.ContextManager;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.CompilerContext;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnection;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnectionContext;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedPreparedStatement;
import com.pivotal.gemfirexd.internal.impl.sql.GenericPreparedStatement;

/**
 * @author vivekb
 * 
 */
public class NcjPullScanNode extends SingleChildResultSetNode {
  private boolean isRemoteScan = true;

  private boolean isAtSecondPosition = false;

  public boolean isAtSecondPosition() {
    return isAtSecondPosition;
  }

  private boolean hasVarLengthInList = true;

  /**
   * Initialiser for a NcjPullScanNode.
   * 
   * @see DistinctNode.init
   * 
   * @param childResult
   *          The child ResultSetNode
   * @param isRemoteScan
   *          Whether or not the child ResultSetNode fetch rows from remote
   * @param isAtSecondPosition
   *          Is at second position in join order
   * @param tableProperties
   *          Properties list associated with the table
   * 
   * @exception StandardException
   *              Thrown on error
   */
  public void init(Object childResult, Object isRemoteScan,
      Object isAtSecondPosition, Object tableProperties)
      throws StandardException {
    super.init(childResult, tableProperties);

    if (SanityManager.DEBUG) {
      if (!(childResult instanceof ProjectRestrictNode)) {
        SanityManager.THROWASSERT("childResult, "
            + childResult.getClass().getName()
            + ", expected to be instanceof ProjectRestrictNode");
      }
    }

    /* We want our own resultColumns, which are virtual columns
     * pointing to the child result's columns.
     * 
     * We get a shallow copy of the ResultColumnList and its 
     * ResultColumns.  (Copy maintains ResultColumn.expression for now.)
     */
    ResultColumnList prRCList = this.childResult.getResultColumns()
        .copyListAndObjects();
    this.resultColumns = this.childResult.getResultColumns();
    this.childResult.setResultColumns(prRCList);

    /* Replace ResultColumn.expression with new VirtualColumnNodes
     * in the DistinctNode's RCL.  (VirtualColumnNodes include
     * pointers to source ResultSetNode, this, and source ResultColumn.)
     * 
     * TODO - should third parameter be false?
     */
    this.resultColumns.genVirtualColumnNodes(this, prRCList);

    this.isRemoteScan = ((Boolean)isRemoteScan).booleanValue();

    // This value can change inside this class
    this.isAtSecondPosition = ((Boolean)isAtSecondPosition).booleanValue();

    if (this.isAtSecondPosition) {
      this.hasVarLengthInList = false;
    }
    else {
      this.hasVarLengthInList = true;
    }
  }

  @Override
  public void generate(ActivationClassBuilder acb, MethodBuilder mb)
      throws StandardException {
    this.childResult.assertProjectRestrictNode();
    ProjectRestrictNode childPrn = (ProjectRestrictNode)this.childResult;
    FromBaseTable fbt = childPrn.ncjGetOnlyOneFBTNode();
    SanityManager.ASSERT(fbt != null);
    if (fbt.resultSetNumber < 1) {
      fbt.resultSetNumber = getCompilerContext().getNextResultSetNumber();
    }
    if (childPrn.getRemoteInListCols() == null) {
      this.hasVarLengthInList = false;
    }
    NcjSQLGeneratorVisitor ncjVisitor = new NcjSQLGeneratorVisitor(
        this.hasVarLengthInList);
    childPrn.accept(ncjVisitor);
    ArrayList<Integer> params = new ArrayList<Integer>();
    String sqlString = ncjVisitor.getGeneratedSql(params);
    boolean generateInList = ncjVisitor.isGenerateInList();
    String corrName = fbt.ncjGetCorrelationName();
    int prID = NcjHashMapWrapper.MINUS_ONE;
    if (fbt.isPartitionedRegion()) {
      prID = ((PartitionedRegion)fbt.getRegion(false)).getPRId();
    }
    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TraceNCJ) {
        SanityManager.DEBUG_PRINT(
            GfxdConstants.TRACE_NON_COLLOCATED_JOIN,
            "NcjPullScanNode::generate. SQL=" + sqlString + " ,params="
                + params.toString() + " ,is-remote-scan=" + isRemoteScan
                + " ,hasVarLengthInList=" + hasVarLengthInList
                + " ,childPrnHasVarLengthInList="
                + (childPrn.getRemoteInListCols() != null)
                + " ,generateInList=" + generateInList + " ,tab-corr-name="
                + corrName + " ,prID=" + prID);
      }
    }
    childPrn.generateNOPProjectRestrict();
    childPrn.assignResultSetNumber(fbt.resultSetNumber);
    fbt.assignResultSetNumber(fbt.resultSetNumber);
    childPrn.childResult.assignResultSetNumber(fbt.resultSetNumber);
    generateRemoteResultSet(acb, mb, fbt.resultSetNumber, sqlString, params,
        getContextManager(), this.isRemoteScan,
        (this.hasVarLengthInList && generateInList), prID);
  }

  /*
   * @see SubqueryNode.generateGFEResultSet
   */
  public static void generateRemoteResultSet(ExpressionClassBuilder acb,
      MethodBuilder mb, int rsNum, String sqlString, List<Integer> params,
      ContextManager ctx, boolean isRemoteScan, boolean hasVarLengthInList,
      int prId) throws StandardException {
    acb.pushGetResultSetFactoryExpression(mb);
    acb.pushThisAsActivation(mb);// 1
    mb.push(sqlString);// 2

    final GenericPreparedStatement gps;
    try {
      // Create a GenericPreparedStatement here itself.
      EmbedConnectionContext eCtx = (EmbedConnectionContext)ctx
          .getContext(ConnectionContext.CONTEXT_ID);
      // Get Nested connection
      final EmbedConnection nestedConn = ExplainResultSet
          .createNestedConnection(eCtx.getEmbedConnection());
      java.sql.PreparedStatement ps = nestedConn.prepareStatement(sqlString,
          isRemoteScan/*create query info*/);
      EmbedPreparedStatement eps = (EmbedPreparedStatement)ps;
      gps = eps.getGPS();
      SanityManager.ASSERT(gps != null);
    } catch (SQLException e) {
      throw StandardException.unexpectedUserException(e);
    }

    QueryInfo qinfo = gps.getQueryInfo();
    if (isRemoteScan) {
      if (qinfo != null) {
        if (qinfo instanceof SelectQueryInfo) {
          if (hasVarLengthInList) {
            ((SelectQueryInfo)qinfo).setNcjLevelTwoQueryWithVarIN(true);
          }
        }
        else {
          SanityManager
              .THROWASSERT("Query Info must be for type SelectQueryInfo, and not "
                  + qinfo.getClass().getSimpleName());
        }
      }
      else {
        SanityManager.THROWASSERT("Query Info must not be Null");
      }
    }
    else {
      if (qinfo != null) {
        SanityManager.THROWASSERT("Query Info must be Null, and not "
            + qinfo.getClass().getSimpleName());
      }
    }

    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TraceNCJ) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_NON_COLLOCATED_JOIN,
            "NcjPullScanNode::generateRemoteResultSet. SQL=" + sqlString
                + " ,is remote scan=" + isRemoteScan + " ,qinfo=" + qinfo
                + " ,hasVarLengthInList=" + hasVarLengthInList);
      }
    }

    mb.push(acb.addItem(params));// 3

    final CompilerContext cc = (CompilerContext)ctx
        .getContext(CompilerContext.CONTEXT_ID);
    gps.setParentPS(cc.getParentPS());
    if (gps.getStatementStats() != null && cc.getStatementStats() != null) {
      assert gps.getStatementStats() == cc.getStatementStats();
    }
    gps.setStatementStats(cc.getStatementStats());
    mb.push(acb.addItem(gps));// 4
    mb.push(rsNum);// 5
    mb.push(isRemoteScan);// 6
    mb.push(hasVarLengthInList);// 7
    mb.push(prId);// 8
    mb.callMethod(VMOpcode.INVOKEINTERFACE, (String)null,
        "getNcjPullResultSet", ClassName.NoPutResultSet, 8);
  }
  
  /*
   * NCJ
   * (non-Javadoc)
   * @see com.pivotal.gemfirexd.internal.impl.sql.compile.JoinNode.convertAbsoluteToRelativeColumnPosition(int, ColumnReference)
   *
   * Note: Mainly called from BinaryRelationalOperatorNode.generateRelativeColumnId
   * Should this method be only used when NcjPullScanNode is at 2nd position as in
   * @see BinaryRelationalOperatorNode.getAbsoluteColumnPosition(Optimizable)
   */
  @Override
  public int convertAbsoluteToRelativeColumnPosition(int absolutePosition,
      ColumnReference ref) {
    if (ref != null) {
      final int colLength = getNumColumnsReturned();
      for (int colCtr = 0; colCtr < colLength; colCtr++) {
        ResultColumn resCol = (ResultColumn)this.resultColumns
            .elementAt(colCtr);
        try {
          if (resCol.getTableNumber() == ref.getTableNumber()
              && resCol.getColumnPosition() == ref.getColumnNumber()) {
            return colCtr;
          }
        } catch (StandardException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    }

    return absolutePosition;
  }
}
