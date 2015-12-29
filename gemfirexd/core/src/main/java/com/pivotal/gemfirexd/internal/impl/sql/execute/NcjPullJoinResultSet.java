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
package com.pivotal.gemfirexd.internal.impl.sql.execute;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.loader.GeneratedMethod;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.NoPutResultSet;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;

/**
 * @author vivekb
 * 
 */
public class NcjPullJoinResultSet extends HashJoinResultSet {

  /**
   * @param leftResultSet
   * @param leftNumCols
   * @param rightResultSet
   * @param rightNumCols
   * @param activation
   * @param restriction
   * @param resultSetNumber
   * @param oneRowRightSide
   * @param notExistsRightSide
   * @param optimizerEstimatedRowCount
   * @param optimizerEstimatedCost
   * @param userSuppliedOptimizerOverrides
   * @param leftResultColumnNames
   * @param rightResultColumnNames
   */
  public NcjPullJoinResultSet(NoPutResultSet leftResultSet, int leftNumCols,
      NoPutResultSet rightResultSet, int rightNumCols, Activation activation,
      GeneratedMethod restriction, int resultSetNumber,
      boolean oneRowRightSide, boolean notExistsRightSide,
      double optimizerEstimatedRowCount, double optimizerEstimatedCost,
      String userSuppliedOptimizerOverrides, int leftResultColumnNames,
      int rightResultColumnNames) {
    super(leftResultSet, leftNumCols, rightResultSet, rightNumCols, activation,
        restriction, resultSetNumber, oneRowRightSide, notExistsRightSide,
        optimizerEstimatedRowCount, optimizerEstimatedCost,
        userSuppliedOptimizerOverrides, leftResultColumnNames,
        rightResultColumnNames);
  }

  @Override
  public ExecRow getNextRowCore() throws StandardException {
    ExecRow nextRow = super.getNextRowCore();
    if (nextRow == null) {
      final HashTableResultSet hashTab = (HashTableResultSet)rightResultSet;
      if (hashTab.moreRowsExpected()) {
        hashTab.purgeHashTable();
        if (hashTab.fillUpHashTable()) {
          this.leftResultSet.forceReOpenCore();
          reopenCore();
          nextRow = super.getNextRowCore();
        }
        if (GemFireXDUtils.TraceNCJ) {
          SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_NON_COLLOCATED_JOIN,
              "NcjPullJoinResultSet::getNextRowCore() moreRowsExpected. nextRow-not-null="
                  + (nextRow != null));
        }
      }
    }
    return nextRow;
  }
  
  @Override
  protected void closeRight() throws StandardException {
    if (SanityManager.DEBUG)
      SanityManager.ASSERT(isRightOpen, "isRightOpen is expected to be true");
    final HashTableResultSet hashTab = (HashTableResultSet)rightResultSet;
    if (hashTab.moreRowsExpected()) {
      if (GemFireXDUtils.TraceNCJ) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_NON_COLLOCATED_JOIN,
            "NcjPullJoinResultSet::closeRight() moreRowsExpected.");
      }
    }
    else {
      super.closeRight();
    }
  }

  private void setKeysFromHashTable(DataValueDescriptor[] row,
      Object[] keysFromRight, int keysFromRightSize, HashTableResultSet hashTab) {
    SanityManager.ASSERT(row != null, "Null row from hash table");
    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TraceNCJIter) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_NCJ_ITER,
            "NcjPullJoinResultSet#ncjOpen row=" + RowUtil.toString(row)
                + " ,first-key=" + row[hashTab.keyColumns[0]] + " ,key-column="
                + Arrays.toString(hashTab.keyColumns));
      }
    }

    for (int index = 0; index < keysFromRightSize; index++) {
      ArrayList<DataValueDescriptor> keysArr = (ArrayList<DataValueDescriptor>)keysFromRight[index];
      keysArr.add(row[hashTab.keyColumns[index]]);
    }
  }
  
  @Override
  public void ncjOpen() throws StandardException {
    /*
     * Collect keys for NonCollocated Join
     */
    SanityManager.ASSERT(this.rightResultSet instanceof HashTableResultSet,
        "HashTableResultSet expected, "
            + this.rightResultSet.getClass().getSimpleName());
    super.openRight();
    final HashTableResultSet hashTab = (HashTableResultSet)rightResultSet;
    Iterator hashIter = hashTab.hashTableIterator();
    SanityManager.ASSERT(hashIter != null, "HashTableResultSet is not open");
    final int keysFromRightSize = hashTab.keyColumns.length;
    final Object[] keysFromRight = new Object[keysFromRightSize];
    for (int index = 0; index < keysFromRightSize; index++) {
      keysFromRight[index] = new ArrayList<DataValueDescriptor>();
    }

    int countKeys = 0;
    while (hashIter.hasNext()) {
      Object objHash = hashIter.next();
      SanityManager.ASSERT(objHash != null, "Null row from hash table");
      if (objHash instanceof ArrayList) {
        // In case of duplicate keys
        for (Object objItem : (ArrayList)objHash) {
          setKeysFromHashTable((DataValueDescriptor[])objItem, keysFromRight,
              keysFromRightSize, hashTab);
        }
      }
      else {
        setKeysFromHashTable((DataValueDescriptor[])objHash, keysFromRight,
            keysFromRightSize, hashTab);
      }

      countKeys++;
    }

    for (int index = 0; index < keysFromRightSize; index++) {
      ArrayList<DataValueDescriptor> keysArr = (ArrayList<DataValueDescriptor>)keysFromRight[index];
      leftResultSet.setGfKeysForNCJoin(keysArr);
    }

    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TraceNCJ) {
        SanityManager.DEBUG_PRINT(
            GfxdConstants.TRACE_NON_COLLOCATED_JOIN,
            "NcjPullJoinResultSet#ncjOpen node=" + this.resultSetNumber()
                + " ,leftResultSet=" + leftResultSet.resultSetNumber()
                + " ,rightResultSet=" + rightResultSet.resultSetNumber()
                + " ,rowCount=" + countKeys + " ,keysFromRight-size="
                + keysFromRightSize + " ,hashtable-keys="
                + Arrays.toString(hashTab.keyColumns));
      }

      if (GemFireXDUtils.TraceNCJDump) {
        StringBuilder strKeysFromRight = new StringBuilder();
        for (int index = 0; index < keysFromRightSize; index++) {
          ArrayList<DataValueDescriptor> keysArr = (ArrayList<DataValueDescriptor>)keysFromRight[index];
          strKeysFromRight.append(" <");
          strKeysFromRight.append(keysArr);
          strKeysFromRight.append("> ");
        }

        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_NCJ_DUMP,
            "NcjPullJoinResultSet#ncjOpen keysFromRight-size="
                + keysFromRightSize + " ,rowCount=" + countKeys
                + " ,keysFromRight=" + strKeysFromRight);
      }
    }
  }

  @Override
  public StringBuilder buildQueryPlan(StringBuilder builder,
      PlanUtils.Context context) {

    boolean isSuccess = context.setNested();

    super.buildQueryPlan(builder, context.pushContext());

    PlanUtils.xmlAttribute(builder, "is_noncolocated_join", true);

    if (isSuccess) {
      PlanUtils.xmlTermTag(builder, context, PlanUtils.OP_JOIN_NCJ);
    }

    return builder;
  }

}
