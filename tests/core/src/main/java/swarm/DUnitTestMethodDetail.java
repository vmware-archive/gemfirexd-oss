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

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DUnitTestMethodDetail implements Comparable<DUnitTestMethodDetail> {
  
  private final int id;
  private final int methodId;
  private final String status;
  private final String error;
  private final int runId;
  private final Date time;
  private final long tookMs;
  private final int hashCode;
  
  public boolean equals(Object o) {
    if(!(o instanceof DUnitTestMethodDetail)) {
      return false;
    }
    if(((DUnitTestMethodDetail)o).getId()==getId()) {
      return true;
    } else {
      return false;
    }
  }
  public int hashCode() {
    return hashCode;
  }
  
  public int compareTo(DUnitTestMethodDetail o1) {
    return getTime().compareTo(o1.getTime());
  }
  
  
  public DUnitTestMethodDetail(int id, int methodId, String status,
      String error,int runId,Date time,long tookMs) {
    super();
    this.id = id;
    this.methodId = methodId;
    this.status = status;
    this.error = error;
    this.runId = runId;
    this.time = time;
    this.tookMs = tookMs;
    this.hashCode = new Integer(id).hashCode();
  }
  
  public long getTookMs() {
    return tookMs;
  }
  
  public boolean isFail() {
    return error!=null;
  }
  
  public Date getTime() {
    return time;
  }
  
  public String getTimeString() {
    SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yy HH:mm:ss");
    return sdf.format(getTime());
  }
  
  public int getRunId() {
    return runId;
  }
  
  public DUnitRun getRun() {
    try {
      return Swarm.getDUnitRun(getRunId());
    } catch(Exception e) {
      e.printStackTrace();
    }
    return null;
  }
  
  public int getId() {
    return id;
  }
  public int getMethodId() {
    return methodId;
  }
  public String getStatus() {
    return status;
  }
  public String getError() {
    return error;
  }
  
  public DUnitMethodInfo getMethodInfo() {
    try {
      return Swarm.getDUnitMethodInfo(getMethodId());
    } catch(SQLException se) {
      se.printStackTrace();
    }
    return null;
  }
  
  

}
