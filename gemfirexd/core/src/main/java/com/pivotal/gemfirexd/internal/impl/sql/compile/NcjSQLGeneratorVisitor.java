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
/**
 * 
 */
package com.pivotal.gemfirexd.internal.impl.sql.compile;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.Visitable;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.VisitorAdaptor;

import static com.pivotal.gemfirexd.internal.impl.sql.compile.ParameterNode.GFXDPARAM;
/**
 * @author vivekb
 * 
 */
public class NcjSQLGeneratorVisitor extends VisitorAdaptor {
  private boolean resultColumnsDone = false;
  
  private String resultColumns = null;

  private int fbtSeenCount = 0;

  private String tableName = null;
  
  private ArrayList<String> predSqlList = null;
  
  private ArrayList<String> remoteInListCols = null;
  
  private boolean generateInList;

  public boolean isGenerateInList() {
    return generateInList;
  }

  public NcjSQLGeneratorVisitor(boolean genInList) {
    this.generateInList = genInList;
    if (GemFireXDUtils.TraceNCJ) {
      SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_NON_COLLOCATED_JOIN,
          "NcjSQLGeneratorVisitor constructor: Created with generateInList = "
              + this.generateInList);
    }
  }
  
  @Override
  public Visitable visit(Visitable node) throws StandardException {
    // 1.start with prn - take projection columns
    // 1a.go for predicates - keep adding list of predicates here
    // 1b. also maintain their index; maintain order - may be just copy the
    // printexplaininfo but in this method only.
    // 1c. with same index maintain their params - remember, params can repeat
    // 2. test this for a query.

    if (node instanceof ProjectRestrictNode) {
      final ProjectRestrictNode prn = (ProjectRestrictNode)node;
      if (!this.resultColumnsDone) {
        this.resultColumnsDone = true;
        StringBuilder projectedColumns = new StringBuilder();
        boolean seenOneResultColumn = false;
        for (int i = 0; i < prn.resultColumns.size(); i++) {
          ResultColumn col = (ResultColumn)prn.resultColumns.elementAt(i);
          if ((col.getActualName() == null)
              || (col.getActualName().startsWith("##"))) {
            // TODO- resolve this. @see ProjectRestrictNode.generateMinion
            SanityManager.THROWASSERT("NCJ: Not yet Supported");
          }
          if (seenOneResultColumn) {
            projectedColumns.append(" ,");
          }
          else {
            seenOneResultColumn = true;
          }
          projectedColumns.append(col.getActualName());
        }
        resultColumns = projectedColumns.toString();
      }
      
      if (prn.getRemoteInListCols() != null) {
        SanityManager.ASSERT(this.remoteInListCols == null,
            "Already got In-List");
        this.remoteInListCols = prn.getRemoteInListCols();
      }
    }
    else if (node instanceof FromBaseTable) {
      SanityManager.ASSERT(this.fbtSeenCount == 0,
          "Till we support remote colocated tables");
      this.fbtSeenCount++;
      TableName tName= (((FromBaseTable)node).getActualTableName());
      this.tableName = tName.getFullTableName();
    }
    else if (node instanceof IndexToBaseRowNode) {
      visit(((IndexToBaseRowNode)node).source);
    }
    else if (node instanceof Predicate) {
      if (predSqlList == null) {
        predSqlList = new ArrayList<String>();
      }
      Predicate pNode = (Predicate)node;
      String predSql = pNode.andNode.ncjGenerateSql();
      if (predSql != null) {
        predSqlList.add(predSql);
      }
      if (this.generateInList) {
        CollectNodesVisitor collectNodesVisitor = new CollectNodesVisitor(
            BinaryRelationalOperatorNode.class);
        pNode.accept(collectNodesVisitor);
        boolean ifAllEquals = true;
        boolean ifAnyOneEqual = false;
        for (Iterator nodesList = collectNodesVisitor.getList().iterator(); nodesList
            .hasNext();) {
          BinaryRelationalOperatorNode brOpNode = (BinaryRelationalOperatorNode)nodesList
              .next();
          if (brOpNode.getOperator() == RelationalOperator.EQUALS_RELOP) {
            ifAnyOneEqual = true;
          }
          else {
            ifAllEquals = false;
          }
        }
        
        if (ifAnyOneEqual && ifAllEquals) {
          this.generateInList = false;
          if (GemFireXDUtils.TraceNCJ) {
            SanityManager
                .DEBUG_PRINT(
                    GfxdConstants.TRACE_NON_COLLOCATED_JOIN,
                    "NcjSQLGeneratorVisitor constructor: Marking generateInList from True to False. ifAnyOneEqual="
                        + ifAnyOneEqual + " ,ifAllEquals=" + ifAllEquals);
          }
        }
      }
    }

    return node;
  }

  @Override
  public boolean stopTraversal() {
    return false;
  }

  @Override
  public boolean skipChildren(Visitable node) throws StandardException {
    return (node instanceof ResultColumnList) || (node instanceof Predicate)
        || (node instanceof JoinNode);
  }

  private String getResultColumns() {
    SanityManager.ASSERT(this.resultColumnsDone,
        "No Project Restriction Node found yet");
    return resultColumns;
  }

  private String getTableName() {
    SanityManager.ASSERT(this.fbtSeenCount == 1, "No of tables seen yet "
        + fbtSeenCount);
    return tableName;
  }
  
  private String getGeneratedWhereClause(ArrayList<Integer> params) {
    StringBuilder resultPred = null;
    if (predSqlList != null && predSqlList.size() > 0) {
      resultPred = new StringBuilder();
      for (String predSql : predSqlList) {
        final String pattern = "\\b" + GFXDPARAM + "\\d" + GFXDPARAM + "\\b";
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(predSql);
        while (m.find()) {
          SanityManager.ASSERT(m.group().length() > 2 * GFXDPARAM.length());
          String param = m.group().substring(GFXDPARAM.length(),
              m.group().length() - GFXDPARAM.length());
          Integer prm = Integer.parseInt(param);
          params.add(prm);
        }
        if (resultPred.length() > 0) {
          resultPred.append(" and ");
        }
        resultPred.append(m.replaceAll("?"));
      }
    }

    return resultPred != null ? resultPred.toString() : null;
  }
  
  private String getGeneratedInListClause() {
    StringBuilder resultPred = new StringBuilder();
    SanityManager.ASSERT(this.remoteInListCols != null, "In-list columns missing ");
    boolean foundRemoteColumn = false;
    for (String colName: this.remoteInListCols) {
      if (colName != null) {
        if (foundRemoteColumn) {
          resultPred.append(" and ");
        }
        resultPred.append(colName);
        resultPred.append(" IN ARRAY(?) ");
        foundRemoteColumn = true;
      }
    }
    
    return resultPred != null ? resultPred.toString() : null;
  }
  
  public String getGeneratedSql(ArrayList<Integer> params) {
    StringBuilder resultPred = new StringBuilder("Select ");
    resultPred.append(getResultColumns());
    resultPred.append(" from ");
    resultPred.append(getTableName());
    resultPred.append(" ");

    boolean whereTagAdded = false;
    {
      String whereClause = getGeneratedWhereClause(params);
      if (whereClause != null) {
        resultPred.append(" where ");
        resultPred.append(whereClause);
        whereTagAdded = true;
      }
    }

    if (this.generateInList) {
      String inlistClause = getGeneratedInListClause();
      if (inlistClause != null) {
        if (!whereTagAdded) {
          resultPred.append(" where ");
        }
        else {
          resultPred.append(" and ");
        }
        resultPred.append(inlistClause);
      }
    }

    return resultPred.toString();
  }
}
