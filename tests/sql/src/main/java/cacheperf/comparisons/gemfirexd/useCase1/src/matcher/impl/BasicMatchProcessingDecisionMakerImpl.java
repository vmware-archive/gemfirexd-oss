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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import cacheperf.comparisons.gemfirexd.useCase1.src.matcher.ExecutionInfo;
import cacheperf.comparisons.gemfirexd.useCase1.src.matcher.MatchProcessingDecisionMaker;

public class BasicMatchProcessingDecisionMakerImpl
implements MatchProcessingDecisionMaker {

  /**
   *  In case of no-match, current match result list will be empty.
   *  For match/multi-match scenario, no further processing is needed.
   */

  // we need a list to preserve the order of priority and search in the same
  private List<Integer> priorityList;

  public List<Integer> getPriorityList() {
    return priorityList;
  }

  public BasicMatchProcessingDecisionMakerImpl(Set<Integer> prioritySet) {
    super();
    if (prioritySet == null) {
      this.priorityList = null;
    } else {
      this.priorityList = new ArrayList<Integer> (prioritySet);
    }
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine(
        "BasicMatchProcessingDecisionMakerImpl-constructor" +
        " priorityList=" + priorityList);
    }
  }

  @Override
  public ExecutionInfo getNextExecutionInfo(
      List<Object> currMatchResultList,
      int currPriority) {

    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine(
        "BasicMatchProcessingDecisionMakerImpl-getNextExecutionInfo entering" +
        " currMatchResultList=" + currMatchResultList +
        " currPriority=" + currPriority);
    }

    /**
     * 1. Set MatchResultList=prevMatchResultList
     * 2. If null priority list or no more priority left or last execution
     *    is NOT NO-MATCH:
     *       set stopFlag true; nextPriority=-1
     * 3. Else:
     *     set isStop= false; nextPriority=NextPriority from priorityList
     */
    ExecutionInfo execInfo = new ExecutionInfo();

    /*
     * If null priority list or no more priority left or last execution is
     * NOT NO-MATCH (and also it is NOT first execution):
     *       set stopFlag true;  nextPriority=-1
     * For first execution prevMatchResultList will be null.
     */
    if (priorityList != null) {
      if (Log.getLogWriter().fineEnabled()) {
        Log.getLogWriter().fine(
          "BasicMatchProcessingDecisionMakerImpl-getNextExecutionInfo" +
          " currPriority=" + currPriority +
          " priorityList(currPriority)=" + priorityList.indexOf(currPriority) +
          " priorityList.size()=" + priorityList.size());
      }
    }

    if (priorityList == null ||
        priorityList.size() == priorityList.indexOf(currPriority) + 1 ||
        (currMatchResultList != null && currMatchResultList.size() != 0)) {

      if (Log.getLogWriter().fineEnabled()) {
        Log.getLogWriter().fine(
          "BasicMatchProcessingDecisionMakerImpl-getNextExecutionInfo" +
          " setting stop true");
      }
      execInfo.setStop(true);

      if (currMatchResultList != null) {
        if (Log.getLogWriter().fineEnabled()) {
          Log.getLogWriter().fine(
            "BasicMatchProcessingDecisionMakerImpl-getNextExecutionInfo" +
            " prevMatchResultList.size() " + currMatchResultList.size());
        }
      }
      execInfo.setNextPriority(-1);
      execInfo.setMatchResultList(currMatchResultList);

    } else {
      // set isStop=false; nextPriority=NextPriority from priorityList
      if (Log.getLogWriter().fineEnabled()) {
        Log.getLogWriter().fine(
          "BasicMatchProcessingDecisionMakerImpl-getNextExecutionInfo" +
          " setting stop false");
      }
      execInfo.setStop(false);

      // get index of previous priority
      int prevPriorityIndex = priorityList.indexOf(currPriority);
      // in case of first iteration, prevPriority=-1, prevPriorityIndex=-1
      // since it could not be matched
      // get next priority as next element of array list
      execInfo.setNextPriority(priorityList.get(prevPriorityIndex+1));
      // for first iteration it will be : priorityList.get(-1+1)
      execInfo.setMatchResultList(null);
    }
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine(
        "BasicMatchProcessingDecisionMakerImpl-getNextExecutionInfo" +
        " execInfo=" + execInfo);
    }
    return execInfo;
  }
}
