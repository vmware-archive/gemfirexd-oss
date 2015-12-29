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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import cacheperf.comparisons.gemfirexd.useCase1.src.matcher.LogUtils;
import cacheperf.comparisons.gemfirexd.useCase1.src.matcher.MatchProcessor;
import cacheperf.comparisons.gemfirexd.useCase1.src.matcher.impl.BasicMatchProcessorImpl;
import cacheperf.comparisons.gemfirexd.useCase1.src.matcher.model.MatchingInfo;
import cacheperf.comparisons.gemfirexd.useCase1.src.matcher.model.MatchResultSet;
import cacheperf.comparisons.gemfirexd.useCase1.src.matcher.utils.StoredProcUtils;

import com.pivotal.gemfirexd.procedure.ProcedureExecutionContext;

/**
 * Implementation of Secondary onwards key match.
 *
 * This stored procedure is designed to run only on co-ordinating member
 * without result processor. It now invokes another stored procedure.
 */
public class MatchStoredProc {

  private MatchProcessor matchProcessor;

  public MatchStoredProc() {
    super();
  }

  public MatchProcessor getMatchProcessor() {
    return matchProcessor;
  }

  public void setMatchProcessor(MatchProcessor matchProcessor) {
    this.matchProcessor = matchProcessor;
  }

  // GemFireXD does not support Boolean type, so integer used (1=true, 0=false)
  // GemFireXD does not suport Generic types, so raw types used

  @SuppressWarnings("unchecked")
  public static void match(
      @SuppressWarnings("rawtypes") Map inBackOfficeMsg,
      @SuppressWarnings("rawtypes") SortedMap inMatchingKeyMap,
      int[] errorStateValue,
      ResultSet[] resultSet,
      ProcedureExecutionContext pCtx)
  throws SQLException {
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine(
        "MatchStoredProc-match entering" +
        " inBackOfficeMsg=" + inBackOfficeMsg +
        " inMatchingKeyMap=" + inMatchingKeyMap +
        " errorStateValue=" + LogUtils.getErrorStateValueArrayStr(errorStateValue) +
        " resultSet=" + LogUtils.getResultSetArrayStr(resultSet, 20));
    }
    MatchStoredProc matchStoredProc = new MatchStoredProc();
    MatchProcessor matchProcessor = new BasicMatchProcessorImpl();
    matchStoredProc.setMatchProcessor(matchProcessor);

    MatchResultSet matchResults =
      matchProcessor.match(inBackOfficeMsg, inMatchingKeyMap, resultSet, pCtx);
    errorStateValue[0] = StoredProcUtils.toInt(matchResults.isErrorState());

    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine(
        "MatchStoredProc-match: exiting" +
        " resultSet[0]=" + resultSet[0] +
        " errorStateValue[0]="  + errorStateValue[0]);
    }
  }
}
