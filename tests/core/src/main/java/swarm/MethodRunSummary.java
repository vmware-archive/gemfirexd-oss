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

public class MethodRunSummary implements Comparable<MethodRunSummary> {

  private final int id;
  private final int methodId;
  private final int classId;
  private final String className;
  private final String methodName;
  private final Date time;
  private final long tookMs;
  
  public MethodRunSummary(int id, int classId,int methodId,String className,String methodName,Date time,long tookMs) {
    this.id = id;
    this.classId = classId;
    this.methodId = methodId;
    this.className = className;
    this.methodName = methodName;
    this.time = time;
    this.tookMs = tookMs;
  }

  public int compareTo(MethodRunSummary o1) {
    return getTime().compareTo(o1.getTime());
  }
  
  
  public int getMethodId() {
    return methodId;
  }

  public int getClassId() {
    return classId;
  }

  public String getShortClassName() {
    return TextHelper.getShortName(getClassName());
  }

  public String getVeryShortClassName() {
    return TextHelper.getVeryShortName(getClassName());
  }

  public String getShortMethodName() {
    return TextHelper.getShortMethodName(getMethodName());
  }

  
  
  public String getClassName() {
    return className;
  }

  public String getMethodName() {
    return methodName;
  }

  public Date getTime() {
    return time;
  }


  public int getId() {
    return id;
  }


  public long getTookMs() {
    return tookMs;
  }
  
  
  
}
