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
import java.util.HashMap;
import java.util.ListIterator;
import java.util.Map;

import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gnu.trove.THashSet;
import com.gemstone.gnu.trove.TIntIntHashMap;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserver;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.ddl.resolver.GfxdPartitionResolver;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.ColumnDescriptor;

/**
 * This class enacpsulates the logic of identifying if a multi table query has
 * colocated equijoin criteria satisfied.
 * 
 * @author Asif
 * @author swale
 */
public final class ColocationCriteria {

  /* Array of Set where each element corresponds to a partitioning column.
   * The Set would contain the Table Position for the partitioning column which acts as
   * a tying node across the equi join conditions.  The number of such unqiue tying nodes
   * will be Number of Tables  - 2.
   * The number of equi join conditions needed will be Number Of Tables -1.
   * 
   * For eg if we have 3 tables  &  3 partitioning cols
   *      T1       T2         T3
   *  a   a1       a2         a3
   *  b   b1       b2         b3
   *  c   c1       c2         c3
   *  
   *  we will need in total  2*3 conditions
   *       a1 = b2           a2 = b3          
   *       b1 = a2           b2 = a3
   *       c1 = c2           c2 = c3
   *       
   *    Here number of tying nodes = 3 -2 = 1
   *       For a , a2 is the tying node ( pos 2)
   *       For b , b2 is the tying node
   *       For C , c2 is the tying node       
   *
   * [sumedh] (Change in implementation):
   * The older technique will not work for many cases like a common
   * node being common to more than 2 nodes, extraneous conditions (repeated
   * conditions are now checked). Updating to a new technique that will handle
   * all such cases as well as replicated tables and constants after minor
   * extensions thus: For each equijoin condition we assign a distinct value
   * to the rhs. This value will then be copied to lhs as the equality says.
   * In the end we need to ensure that all columns of each table have all
   * distinct values. If we encounter a column that already has a value
   * assigned then the two values must be made identical. For implementation
   * we let the values be indices into an array of Integer objects. When
   * this case is encountered then the references of the two Integer objects
   * are made identical.
   */

  /**
   * This list contains the actual values for the indices assigned in the
   * colocation matrix.
   */
  private final ArrayList<Integer> indexValueList;

  /** current value of the index of the indexValueList */
  private int currentIndex;

  /**
   * ArrayList used to store the TableQueryInfo's for each table which is used
   * to skip the {@link #isEquiJoinColocationCriteriaFullfilled} for replicated
   * tables.
   */
  private final ArrayList<TableQueryInfo> tableQueryInfos;

  /*
   * The columns correspond to the number of tables involved
   * The rows correspond to the number of partitioning columns
   * If the partitioning columns are a,b,c
   *      T1   T2   T3   T4
   *   a  
   *   b
   *   c        
   * A value in a cell indicates the index into the indexValueList
   * (starting with 1) that has been assigned to both lhs and rhs
   * of an equality condition.
   */
  private final Map<Object, int[]> colocationMatrix;

  /**
   * Initial size of colocation matrix which is equal to the number of
   * partitioning columns of the PR tables.
   */
  private final int initialMatrixSize;

  private final GemFireXDQueryObserver observer = GemFireXDQueryObserverHolder
      .getInstance();
  
  /**
   * Handle self join on a single table
   */
  private boolean evaluatedSingleTableCase = false;
  private boolean isValidSingleTableJoin = false;
  
  ColocationCriteria(int numPartitioningColumns,
      ArrayList<TableQueryInfo> tables) {
    assert numPartitioningColumns > 0:
      "Expected number of partitioning columns to be > 0";
    assert tables != null: "Expected tables to be non-null";

    this.indexValueList = new ArrayList<Integer>();
    // we start index at 1 since matrix will be initialized to all zeros
    // which we use to indicate uninitialized
    this.indexValueList.add(Integer.valueOf(0));
    this.currentIndex = 1;
    this.colocationMatrix = new HashMap<Object, int[]>();
    this.tableQueryInfos = tables;
    this.initialMatrixSize = numPartitioningColumns;
    // merge all duplicate TableQueryInfo entries
    final int numTQIs = tables.size();
    final THashSet tqis = new THashSet(numTQIs);
    TableQueryInfo tqi, oldTQI;
    tqis.add(tables.get(0));
    for (int index = 1; index < numTQIs; index++) {
      tqi = tables.get(index);
      if ((oldTQI = (TableQueryInfo)tqis.putIfAbsent(tqi)) != null) {
        // duplicate; change the table number in the dup and remove from here
        tqi.setTableNumberforColocationCriteria(oldTQI
            .getTableNumberforColocationCriteria());
        tables.set(index, null);
      }
    }
  }

  public void updateColocationCriteria(ComparisonQueryInfo cqi) {
    // the right operand can be a constant
    int rhsTablePos = -1;
    ColumnQueryInfo lhs = null;
    Object rhsColPos = null;
    boolean sameColumn = false;
    if (cqi.rightOperand instanceof ColumnQueryInfo) {
      ColumnQueryInfo rhs = (ColumnQueryInfo)cqi.rightOperand;
      rhsTablePos = rhs.getTableNumberforColocationCriteria();
      rhsColPos = Integer.valueOf(getColumnPositionForMatrix(rhs));
      // ignore if left operand is the same as right (seen in some complex
      //   views that self-join the table on same column via views)
      final ColumnDescriptor rhsCD = rhs.getColumnDescriptor();
      if (rhsCD != null && cqi.leftOperand instanceof ColumnQueryInfo) {
        lhs = (ColumnQueryInfo)cqi.leftOperand;
        int countTablesUptoOne = 0;
        for (int index = 0; index < this.tableQueryInfos.size()
            && countTablesUptoOne <= 1; index++) {
          TableQueryInfo tqi = this.tableQueryInfos.get(index);
          if (tqi != null) {
            countTablesUptoOne++;
          }
        }
        if (rhsCD.equals(lhs.getColumnDescriptor())) {
          sameColumn = true;
          if (countTablesUptoOne == 1) {
            this.evaluatedSingleTableCase = true;
            this.isValidSingleTableJoin = this.isValidSingleTableJoin
                || rhs.isUsedInPartitioning();
          }
          return;
        }
        else if (countTablesUptoOne == 1) {
          this.evaluatedSingleTableCase = true;
          this.isValidSingleTableJoin = this.isValidSingleTableJoin
              || (rhs.isUsedInPartitioning() && lhs.isUsedInPartitioning());
        }
      }
    }
    else if (cqi.rightOperand instanceof ConstantQueryInfo) {
      // SQLInteger cannot clash with integer for existing columns
      // in the Map so we can use any table
      rhsTablePos = 0;
      rhsColPos = ((ConstantQueryInfo)cqi.rightOperand).getValue();
    }
    else if (cqi.rightOperand instanceof ValueListQueryInfo) {
      // treat a single IN just like constant comparison
      ValueListQueryInfo inList = (ValueListQueryInfo)cqi.rightOperand;
      if (inList.getSize() == 1) {
        ValueQueryInfo rhs = inList.getOperands()[0];
        if (rhs instanceof ConstantQueryInfo) {
          rhsTablePos = 0;
          rhsColPos = ((ConstantQueryInfo)rhs).getValue();
        }
      }
    }
    else if (cqi.rightOperand instanceof ParameterQueryInfo
        || cqi.rightOperand instanceof ParameterizedConstantQueryInfo) {
      // ParameterQuery objects cannot clash with integer for existing columns
      // in the Map so we can use any table
      rhsTablePos = 0;
      rhsColPos = cqi.rightOperand;
    }

    // don't invoke observer for self-join on same column (#49584)
    if (observer != null && !sameColumn) {
      observer.updatingColocationCriteria(cqi);
    }

    if (rhsColPos == null) {
      // ignore other cases
      return;
    }

    if (lhs == null) {
      lhs = (ColumnQueryInfo)cqi.leftOperand;
    }
    int lhsTablePos = lhs.getTableNumberforColocationCriteria();
    Integer lhsColPos = Integer.valueOf(getColumnPositionForMatrix(lhs));

    // check for lhs table column first
    int[] row = getAndExpandColocationMatrix(lhsColPos);
    int existingIndex = row[lhsTablePos];
    int newIndex;
    if (existingIndex == 0) {
      newIndex = this.currentIndex++;
      // no entry in the matrix yet; assign the new index
      row[lhsTablePos] = newIndex;
      // make entry in the indexValueList
      Integer newIndexObj = Integer.valueOf(newIndex);
      this.indexValueList.add(newIndexObj);
    }
    else {
      newIndex = existingIndex;
    }

    // next check for rhs table column
    row = getAndExpandColocationMatrix(rhsColPos);
    existingIndex = row[rhsTablePos];
    if (existingIndex == 0) {
      // no entry in the matrix yet; assign the new index
      row[rhsTablePos] = newIndex;
    }
    else {
      // found two indices to be equivalent; set the object at newIndex
      // in indexValueList to point to the object for existingIndex
      Integer existingIndexObj = this.indexValueList.get(existingIndex);
      Integer oldIndexObj = this.indexValueList.set(newIndex, existingIndexObj);
      // Search the list & replace all the old index object with
      // the existingIndexObj
      ListIterator<Integer> itr = this.indexValueList.listIterator();
      while (itr.hasNext()) {
        Integer val = itr.next();
        if (val.equals(oldIndexObj)) {
          itr.set(existingIndexObj);
        }
      }
    }
  }

  private int getColumnPositionForMatrix(ColumnQueryInfo column) {
    int colPos = column.getPartitionColumnPosition();
    if (colPos < 0) {
      // get the actual column position and add to initial size to avoid clash
      colPos = column.getActualColumnPosition() + this.initialMatrixSize;
    }
    return colPos;
  }

  private int[] getAndExpandColocationMatrix(Object key) {
    int[] row = this.colocationMatrix.get(key);
    if (row == null) {
      row = new int[this.tableQueryInfos.size()];
      this.colocationMatrix.put(key, row);
    }
    return row;
  }
  
  String isEquiJoinColocationCriteriaFullfilled(TableQueryInfo ncjTqi_notUsed) {    
    // for all tables check the frequency of the distinct indices in the
    // colocation matrix
    int[] refTableIndicesFrequency = null;
    TableQueryInfo refTQI = null;
    for (int tablePos = 0; tablePos < this.tableQueryInfos.size(); ++tablePos) {
      // skip the colocation check for replicated tables;
      // these only provide with tying conditions (bug #40307)
      final TableQueryInfo tqi = this.tableQueryInfos.get(tablePos);
      if (tqi == null || !tqi.isPartitionedRegion()) {
        continue;
      }
      if (this.evaluatedSingleTableCase) {
        if (!this.isValidSingleTableJoin) {
          return "Self Join on non partioning column are not supported for table "
              + tqi.getFullTableName();
        }
        return null;
      }
      int[] tableIndicesFrequency = new int[this.currentIndex];
      TIntIntHashMap indexToColumnPosition = new TIntIntHashMap();
      // check for only the initial partitioning columns and ignore the
      // extraneous columns introduced due to replicated tables;
      // similarly constants are only useful as tying conditions
      for (int colPos = 0; colPos < this.initialMatrixSize; ++colPos) {
        int index = getAndExpandColocationMatrix(Integer.valueOf(
            colPos))[tablePos];
        if (index > 0) {
          int indexValue = this.indexValueList.get(index).intValue();
          ++tableIndicesFrequency[indexValue];
          indexToColumnPosition.put(indexValue, colPos);
        }
        else {
          // a zero in the matrix means a missing colocation condition
          return "table " + tqi.getFullTableName()
              + " missing colocation for partitioning column "
              + getPartitioningColumn(tqi, colPos);
        }
      }
      // compare that the column frequencies of all tables should be equal
      // to be sure that hashCode's will return identical values and will
      // thus be colocated
      if (refTableIndicesFrequency == null) {
        refTableIndicesFrequency = tableIndicesFrequency;
        refTQI = tqi;
      }
      else {
        for (int index = 1; index < refTableIndicesFrequency.length; ++index) {
          if (tableIndicesFrequency[index] != refTableIndicesFrequency[index]) {
            return "table " + tqi.getFullTableName()
                + " different number of joins " + tableIndicesFrequency[index]
                + " on column "
                + getPartitioningColumn(tqi, indexToColumnPosition.get(index))
                + " than expected " + refTableIndicesFrequency[index]
                + " as in table " + refTQI.getFullTableName();
          }
        }
      }
    }
    return null;
  }

  private String getPartitioningColumn(final TableQueryInfo tqi,
      final int colPos) {
    final PartitionedRegion pr = (PartitionedRegion)tqi.getRegion();
    final String[] partitioningColumns = ((GfxdPartitionResolver)pr
        .getPartitionResolver()).getColumnNames();
    return colPos < partitioningColumns.length ? partitioningColumns[colPos]
        : ("number " + colPos + '?');
  }
}
