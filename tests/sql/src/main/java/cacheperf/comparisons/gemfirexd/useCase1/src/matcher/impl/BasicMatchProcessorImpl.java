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
package cacheperf.comparisons.gemfirexd.useCase1.src.matcher.impl;

import hydra.Log;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import cacheperf.comparisons.gemfirexd.useCase1.src.matcher.ExecutionInfo;
import cacheperf.comparisons.gemfirexd.useCase1.src.matcher.MatchExecutor;
import cacheperf.comparisons.gemfirexd.useCase1.src.matcher.MatchProcessingDecisionMaker;
import cacheperf.comparisons.gemfirexd.useCase1.src.matcher.MatchProcessor;
import cacheperf.comparisons.gemfirexd.useCase1.src.matcher.model.MatchingInfo;
import cacheperf.comparisons.gemfirexd.useCase1.src.matcher.model.MatchResultSet;

import com.pivotal.gemfirexd.procedure.OutgoingResultSet;
import com.pivotal.gemfirexd.procedure.ProcedureExecutionContext;

// Implementation of secondary onwards key match
// Implement new matching logic as per ipay analysis
// Now for NO-Match only subsequent priority keys to be executed
// ITR-8: Null value of key will return UNMATCHED.

public class BasicMatchProcessorImpl implements MatchProcessor {

  private static Map<Integer,MatchExecutor> matchExecutorMap;

  static
  {
    matchExecutorMap = new HashMap<Integer,MatchExecutor>(); // NOPMD
    matchExecutorMap.put(1, new BasicMatchExecutorImpl());
    matchExecutorMap.put(2, new BasicMatchExecutorImpl());
    matchExecutorMap.put(3, new BasicMatchExecutorImpl());
    matchExecutorMap.put(4, new BasicMatchExecutorImpl());
    matchExecutorMap.put(5, new BasicMatchExecutorImpl());
  }

  @Override
  public MatchResultSet match(
         Map<String,Map<String,String>> backOfficeMsg,
         SortedMap<Integer,Set<MatchingInfo>> matchingKeyMap,
         ResultSet[] resultSet,
         ProcedureExecutionContext pCtx)
  throws SQLException{
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine(
        "BasicMatchProcessorImpl-match entering" +
        " backOfficeMsg=" + backOfficeMsg +
        " matchingKeyMap=" + matchingKeyMap);
    }
    List<Object> currMatchResultList = new ArrayList<Object>();;
    int currPriority = -1;
    MatchResultSet returnMatchResultSet = null;

    /*
     * 1. Loop thru the priorities
     * 2.   For each priority, execute corresponding matchexecutor
     * 3.   stop condition for loop: matchDecisionMkr.isStop()==true;
     *      That means: matchEexecutor returns error,
     *                  matchexec returns 1 or more records or
     *                  till all priories are exhausted.
     * 4. BuildOutgoingResultSet
     */

    ExecutionInfo execInfo = null;

    // retrieve the set of priorities
    Set<Integer> prioritySet = matchingKeyMap.keySet();
    MatchProcessingDecisionMaker matchDecisionMkr =
        new BasicMatchProcessingDecisionMakerImpl(prioritySet);
    while (true) {
      execInfo = matchDecisionMkr.getNextExecutionInfo(currMatchResultList,
                                                       currPriority);
      if (execInfo.isStop()) {
        if (Log.getLogWriter().fineEnabled()) {
          Log.getLogWriter().fine(
            "BasicMatchProcessorImpl-match Skipping further processing" +
            " execInfo=" + execInfo);
        }
        currMatchResultList = execInfo.getMatchResultList();
        break;
      }
      currPriority = execInfo.getNextPriority();
      if (Log.getLogWriter().fineEnabled()) {
        Log.getLogWriter().fine(
          "BasicMatchProcessorImpl-match processing" +
          " currPriority=" + currPriority);
      }

      // retrieve the executor for the given priority
      MatchExecutor matchExecutor = getMatchExecutor(currPriority);

      // execute match
      returnMatchResultSet = matchExecutor.match(execInfo.getMatchResultList(),
            backOfficeMsg, matchingKeyMap.get(currPriority), pCtx);
      if (returnMatchResultSet.isErrorState()) {
        if (Log.getLogWriter().fineEnabled()) {
          Log.getLogWriter().fine("BasicMatchProcessorImpl-match" +
           " ERROR, stopping further processing");
        }
        break;
      }

      ResultSet rs = returnMatchResultSet.getResultSet();
      // loop through the resultSet and populate currMatchResultList
      // when any key value is null, we expect resultset null
      while (rs != null && rs.next()) {
        currMatchResultList.add(rs.getString(1));
      }
      if (Log.getLogWriter().fineEnabled()) {
        Log.getLogWriter().fine(
          "BasicMatchProcessorImpl-match" +
          " currMatchResultList=" + currMatchResultList +
          " currMatchResultList.size()=" + currMatchResultList.size() +
          " returnMatchResultSet.isErrorState()=" + returnMatchResultSet.isErrorState());
      }
    }
    buildOutgoingResultSet(currMatchResultList, pCtx);
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine(
        "BasicMatchProcessorImpl-match" +
        " returnMatchResultSet=" + returnMatchResultSet);
    }
    return returnMatchResultSet;
  }

  private static MatchExecutor getMatchExecutor(Integer priority) {
    return matchExecutorMap.get(priority);
  }

  private void buildOutgoingResultSet(List<Object> matchResultList,
                                      ProcedureExecutionContext pCtx) {
    OutgoingResultSet ors = pCtx.getOutgoingResultSet(1);
    ors.addColumn("CHANNEL_TXN_ID");
    for (Object txnId : matchResultList) {
      List<Object> row = new ArrayList<Object>();
      if (Log.getLogWriter().fineEnabled()) {
        Log.getLogWriter().fine(
          "BasicMatchProcessorImpl-match" +
          " forLoop txnId=" + txnId);
      }
      row.add(txnId);
      ors.addRow(row);
    }
    ors.endResults(); //add end of result marker
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine(
        "BasicMatchProcessorImpl-buildOutgoingResultSet" +
        " ors=" + ors);
    }
  }
}
