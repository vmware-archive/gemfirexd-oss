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

package com.pivotal.gemfirexd.internal.engine.expression;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import com.gemstone.gemfire.internal.offheap.annotations.Released;
import com.gemstone.gemfire.internal.offheap.annotations.Retained;
import com.gemstone.gemfire.internal.offheap.OffHeapHelper;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.engine.store.RegionEntryUtils;
import com.pivotal.gemfirexd.internal.engine.store.RegionKey;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.loader.GeneratedClass;
import com.pivotal.gemfirexd.internal.iapi.services.loader.GeneratedMethod;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.CompilerContext;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.TableDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation;
import com.pivotal.gemfirexd.internal.impl.sql.compile.FromList;
import com.pivotal.gemfirexd.internal.impl.sql.compile.SelectNode;
import com.pivotal.gemfirexd.internal.impl.sql.compile.SubqueryList;
import com.pivotal.gemfirexd.internal.impl.sql.compile.ValueNode;
import com.pivotal.gemfirexd.internal.impl.sql.execute.BaseActivation;

/**
 * Utility case for compiling and managing activations for individual
 * sub-expressions. Originally most of this code was part of
 * <code>GfxdPartitionByExpressionResolver</code> but has been split out for use
 * in other portions like <code>GfxdEvictionCriteria</code>.
 * 
 * Once {@link #compileExpression} has been invoked, then the
 * {@link #evaluateExpression} can be invoked to get the result of evaluating
 * the expression.
 * 
 * @author swale
 * @since gfxd 1.0
 */
public class ExpressionCompiler {

  private ValueNode exprNode;

  protected GeneratedClass exprGC;

  protected GeneratedMethod exprMethod;

  private final int exprId = getNextExpressionId();

  private final String exprType;

  /**
   * map of column names in the expression to the 0-based index in the
   * expression
   */
  private final Map<String, Integer> columnToIndexMap;

  private String[] columnsOrigOrder;

  private boolean isPrimaryKey;
  private boolean isSubsetOfPrimaryKey;

  /**
   * if expression columns are a subset of primary key, then this gives the
   * 0-based column positions in the key
   */
  private int[] columnPositionsInKey;

  /** this stores the 1-based expression column positions in the row */
  private int[] columnPositionsInRow;

  protected String canonicalizedExpression;

  private static final AtomicInteger allExpressionId = new AtomicInteger(0);

  public ExpressionCompiler(ValueNode exprNode,
      Map<String, Integer> columnToIndexMap, String exprType) {
    this.exprNode = exprNode;
    this.columnToIndexMap = columnToIndexMap;
    this.exprType = exprType;
  }

  public static int getNextExpressionId() {
    return allExpressionId.getAndIncrement();
  }

  public String[] bindExpression(FromList fromList, SubqueryList subqueryList,
      Vector<?> aggregateVector) throws StandardException {
    this.exprNode.bindExpression(fromList, subqueryList, aggregateVector);
    final GfxdExprNodeVisitor exprvistr = new GfxdExprNodeVisitor(
        this.columnToIndexMap);
    this.exprNode.accept(exprvistr);
    List<String> cols = exprvistr.getUsedColumns();
    return (this.columnsOrigOrder = cols.toArray(new String[cols.size()]));
  }

  public void normalizeExpression(boolean forceBoolean)
      throws StandardException {
    /*
     * If predicate is a parameter, (where ?/where -?/where +?), then we
     * should catch it and throw exception
     */
    if (this.exprNode.isParameterNode()) {
      throw StandardException.newException(
          SQLState.LANG_NON_BOOLEAN_WHERE_CLAUSE, "PARAMETER");
    }
    if (forceBoolean) {
      this.exprNode = this.exprNode.checkIsBoolean();
    }
    this.exprNode = SelectNode.normExpressions(this.exprNode);
    this.exprNode = this.exprNode.preprocess(1, null, null, null);
  }

  /**
   * Compile this expression.
   * 
   * @return true if compilation was done, and false if it was already done
   *         previously
   */
  public boolean compileExpression(TableDescriptor td,
      LanguageConnectionContext lcc) throws StandardException {
    // Check exprMethod to make sure that this executed only
    // for the first time. This should not be executed for ALTER
    // TABLE executeConstantAction, for example.
    if (this.exprMethod == null) {
      CompilerContext cc = this.exprNode.getCompilerContext();
      int existingReliability = cc.getReliability();
      cc.setReliability(CompilerContext.INTERNAL_SQL_LEGAL);
      // First set the virtual column ID of the ResultColumns that will be
      // used for looking up the DVD in the getColumnFromCurrRow() call
      GfxdExprNodeVirtualIDVisitor visitor = new GfxdExprNodeVirtualIDVisitor(
          this.columnToIndexMap, null, this.exprType);
      this.exprNode.accept(visitor);
      this.canonicalizedExpression = visitor.getCanonicalizedExpression();

      // Generate code for the expression
      ExpressionBuilderVisitor ebv = new ExpressionBuilderVisitor(lcc);
      ebv.dontSkipChildren();
      this.exprNode.accept(ebv);
      final String methodName = ebv.getMethodsGenerated().get(0);
      this.exprGC = ebv.getExpressionClass();
      this.exprMethod = this.exprGC.getMethod(methodName);
      cc.setReliability(existingReliability);
      checkColumnsPrimaryKey(td, lcc);
      return true;
    }
    else {
      return false;
    }
  }

  /** Returns true if all expression columns are same as primary key columns */
  public final boolean isExpressionPrimaryKey() {
    return this.isPrimaryKey;
  }

  /**
   * Returns true if all expression columns are part of primary key columns.
   * This will also be a case when {@link #isExpressionPrimaryKey()} is true but
   * not the other way round
   */
  public final boolean isExpressionPartOfPrimaryKey() {
    return this.isSubsetOfPrimaryKey;
  }

  private void checkColumnsPrimaryKey(TableDescriptor td,
      LanguageConnectionContext lcc) throws StandardException {
    this.isPrimaryKey = false;
    this.isSubsetOfPrimaryKey = false;
    this.columnPositionsInKey = null;
    this.columnPositionsInRow = null;
    final int numExprColumns = this.columnsOrigOrder.length;
    if (td != null) {
      final Map<String, Integer> pkMap = GemFireXDUtils
          .getPrimaryKeyColumnNamesToIndexMap(td, lcc);
      if (pkMap != null) {
        if ((this.isSubsetOfPrimaryKey = checkColumnsPartOfPrimaryKey(pkMap,
            this.columnsOrigOrder))) {
          this.isPrimaryKey = (pkMap.size() == numExprColumns);
        }

        if (this.isSubsetOfPrimaryKey) {
          this.columnPositionsInKey = new int[numExprColumns];
          for (int index = 0; index < this.columnsOrigOrder.length; index++) {
            String partCol = this.columnsOrigOrder[index];
            int keyIndex = 0;
            for (String indexCol : pkMap.keySet()) {
              if (indexCol.equals(partCol)) {
                break;
              }
              keyIndex++;
            }
            assert keyIndex < pkMap.size(): "keyIndex=" + keyIndex + " pkMap="
                + pkMap;
            this.columnPositionsInKey[index] = keyIndex;
          }
        }
      }

      this.columnPositionsInRow = new int[numExprColumns];
      this.columnToIndexMap.clear();
      HashMap<String, Integer> columnMap = GemFireXDUtils
          .getColumnNamesToIndexMap(td, true);
      for (int index = 0; index < numExprColumns; index++) {
        String columnName = this.columnsOrigOrder[index];
        Integer colIndex = columnMap.get(columnName);
        if (colIndex == null) {
          // should never happen
          throw StandardException.newException(SQLState.LANG_SYNTAX_ERROR,
              "The column (" + columnName
                  + ") does not exist in the table's column list for table "
                  + td.getQualifiedName());
        }
        // set the column to index map
        this.columnToIndexMap.put(columnName, Integer.valueOf(index));
        // set the indexes of partitioning columns
        this.columnPositionsInRow[index] = colIndex.intValue();
      }
    }
  }

  public static boolean checkColumnsPartOfPrimaryKey(
      Map<String, Integer> pkMap, String[] columns) {
    for (String colName : columns) {
      if (!pkMap.containsKey(colName)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Evaluate the expression for given key and row location.
   * 
   * @param key
   *          the GemFire region key
   * @param rl
   *          the RowLocation for which the expression has to be evaluated
   * @param lcc
   *          the {@link LanguageConnectionContext} to be used for execution
   */
  public final DataValueDescriptor evaluateExpression(Object key,
      RowLocation rl, GemFireContainer container, LanguageConnectionContext lcc)
      throws StandardException {
    DataValueDescriptor dvd = null;
    DataValueDescriptor[] dvds = null;
    if (this.columnPositionsInKey != null) {
      final int numCols = this.columnPositionsInKey.length;
      final RegionKey gfKey = (RegionKey)key;
      if (numCols == 1) {
        dvd = gfKey.getKeyColumn(this.columnPositionsInKey[0]);
      }
      else {
        int index = 0;
        dvds = new DataValueDescriptor[numCols];
        for (int keyIndex : this.columnPositionsInKey) {
          dvds[index++] = gfKey.getKeyColumn(keyIndex);
        }
      }
    }
    else {
      @Retained @Released Object value = rl.getValueWithoutFaultIn(container);
      try {
        if (this.columnPositionsInRow.length == 1) {
          dvd = RegionEntryUtils.getDVDFromValue(value,
              this.columnPositionsInRow[0], container);
        } else {
          dvds = RegionEntryUtils.getDVDArrayFromValue(value,
              this.columnPositionsInRow, container);
        }
      } finally {
        OffHeapHelper.release(value);
      }
    }
    return evaluateExpression(dvd, dvds, lcc);
  }

  /**
   * Evaluate the expression for given input value or row of values.
   * 
   * @param dvd
   *          if a single value then can be passed in as a single DVD
   * @param dvds
   *          if multiple values then should be passed as an array of DVDs
   * @param lcc
   *          the {@link LanguageConnectionContext} to be used for execution
   */
  public final DataValueDescriptor evaluateExpression(DataValueDescriptor dvd,
      DataValueDescriptor[] dvds, LanguageConnectionContext lcc)
      throws StandardException {
    final BaseActivation act = lcc.getExpressionActivation(this.exprGC,
        this.exprId);
    act.setExpressionDVDs(dvd, dvds);
    return (DataValueDescriptor)this.exprMethod.invoke(act);
  }

  public final ValueNode getExpressionNode() {
    return this.exprNode;
  }

  public final Map<String, Integer> getColumnToIndexMap() {
    return this.columnToIndexMap;
  }

  public final GeneratedClass getGeneratedClass() {
    return this.exprGC;
  }

  public final GeneratedMethod getGeneratedMethod() {
    return this.exprMethod;
  }

  public final String getCanonicalizedExpression() {
    return this.canonicalizedExpression;
  }
}
