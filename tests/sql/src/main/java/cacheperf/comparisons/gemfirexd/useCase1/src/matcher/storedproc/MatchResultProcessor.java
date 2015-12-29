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
package cacheperf.comparisons.gemfirexd.useCase1.src.matcher.storedproc;

import hydra.Log;

import java.util.List;

import com.pivotal.gemfirexd.procedure.IncomingResultSet;
import com.pivotal.gemfirexd.procedure.ProcedureProcessorContext;
import com.pivotal.gemfirexd.procedure.ProcedureResultProcessor;

public class MatchResultProcessor implements ProcedureResultProcessor {

  private ProcedureProcessorContext context;
  private IncomingResultSet[] resultSets;
  private int currentIndex = 0;
  private int totalResultSets;
  private int previousResultSetNumber = 0;

  @Override
  public void init(ProcedureProcessorContext context) {
    this.context = context;
  }

  /**
   * This method retrieves the result sets from the multiple members and does
   * basic append and returns the result set to the client. This method will be
   * called for each ResultSet.getNext(), the client calls.
   */
  @Override
  public List<Object> getNextResultRow(int resultSetNumber)
  throws InterruptedException {
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine(
        "MatchResultProcessor-getNextResultRow" +
        " previousResultSetNumber=" + previousResultSetNumber +
        " resultSetNumber=" +  resultSetNumber);
    }
    if (this.resultSets == null || previousResultSetNumber != resultSetNumber) {
      this.resultSets = this.context.getIncomingResultSets(resultSetNumber);
      this.totalResultSets = this.resultSets.length;
      this.currentIndex = 0;
      this.previousResultSetNumber = resultSetNumber;
    }
    for (;;) {
      if (this.currentIndex >= this.totalResultSets) {
        return null;
      }
      if (Log.getLogWriter().fineEnabled()) {
        Log.getLogWriter().fine(
          "MatchResultProcessor-getNextResultRow" +
          " totalResultSets=" + totalResultSets +
          " currentIndex=" + currentIndex);
      }
      IncomingResultSet currResultSet = this.resultSets[currentIndex];
      Object currentRow = currResultSet.peekRow();
      if (Log.getLogWriter().fineEnabled()) {
        Log.getLogWriter().fine(
          "MatchResultProcessor-getNextResultRow" +
          " currentRow=" + currentRow);
      }
      List<Object> row = currResultSet.takeRow();
      if (row != IncomingResultSet.END_OF_RESULTS) {
        if (Log.getLogWriter().fineEnabled()) {
          Log.getLogWriter().fine(
            "MatchResultProcessor-getNextResultRow" +
            " takeRow=" + currentRow);
        }
        return row;
      }
      this.currentIndex++;
    }
  }

  @Override
  public Object[] getOutParameters()
  throws InterruptedException {
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine(
        "MatchResultProcessor-getOutParameters");
    }
    IncomingResultSet[] inSets = context.getIncomingOutParameters();
    Integer errorStateValueObj = 0;
    for (IncomingResultSet inSet : inSets) {
      // Each inset corresponds to output params in each server
      // Each inSet has only one record/row for the outparams returned for
      // a given server
      List<Object> outputParams = inSet.takeRow();
      if (outputParams == IncomingResultSet.END_OF_RESULTS) {
        continue; // no result for a given server
      }
      // assuming only output param - first element should be status out param
      errorStateValueObj = (Integer)outputParams.get(0);
      if (Log.getLogWriter().fineEnabled()) {
        Log.getLogWriter().fine(
          "MatchResultProcessor-getOutParameters" +
          " errorStateValueObj=" + errorStateValueObj.intValue());
      }
      // if one of the servers is returning error status, then error status is
      // true (value 1)
      if (errorStateValueObj.intValue() == 1) {
        break;
      }
    }
    Object[] returnOutParams = new Object[1];
    returnOutParams[0] = errorStateValueObj; // test hard coded match status
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine(
        "MatchResultProcessor-getOutParameters before return" +
        " errorStateValueObj=" + errorStateValueObj.intValue() +
        " returnOutParams=" + returnOutParams);
    }
    return returnOutParams;
  }

  @Override
  public void close() {
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine(
        "MatchResultProcessor-close resets context and resultSets to null");
    }
    this.context = null;
    this.resultSets = null;
  }
}
