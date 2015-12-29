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
package cacheperf.comparisons.gemfirexd.useCase1.src.matcher;

import java.io.Serializable;
import java.util.List;

public class ExecutionInfo implements Serializable {

  private int nextPriority;
  private List<Object> matchResultList;
  private boolean stopFlag;

  public ExecutionInfo() {
    this.stopFlag = false;
    this.nextPriority = -1;
    this.matchResultList = null;
  }

  public int getNextPriority() {
    return nextPriority;
  }

  public void setNextPriority(int nextPriority) {
    this.nextPriority = nextPriority;
  }

  public List<Object> getMatchResultList() {
    return matchResultList;
  }

  public void setMatchResultList(List<Object> matchResultList) {
    this.matchResultList = matchResultList;
  }

  public boolean isStop() {
    return stopFlag;
  }

  public void setStop(boolean isStop) {
    this.stopFlag = isStop;
  }

  @Override
  public String toString() {
    return "ExecutionInfo [nextPriority=" + nextPriority
        + ", matchResultList=" + matchResultList + ", stopFlag="
        + stopFlag + "]";
  }
}
