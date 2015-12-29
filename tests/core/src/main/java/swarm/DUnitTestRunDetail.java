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
package swarm;

import java.util.Date;

public class DUnitTestRunDetail {
  
  private final int id;
  private final int classId;
  private final int failCount;
  private final long tookMs;
  private final Date time;
  
  
  public DUnitTestRunDetail(int id, int classId, int failCount,long tookMs,Date time) {
    super();
    this.id = id;
    this.classId = classId;
    this.failCount = failCount;
    this.tookMs = tookMs;
    this.time = time;
  }
  
  
  public Date getTime() {
    return time;
  }
  
  public long getTookMs() {
    return tookMs;
  }
  
  public int getId() {
    return id;
  }
  public int getClassId() {
    return classId;
  }
  public int getFailCount() {
    return failCount;
  }
  

}
