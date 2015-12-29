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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class DUnitClassRun implements Comparable<DUnitClassRun> {

  private final int runId;
  private final int classId;
  private final Date time;
  
  public int compareTo(DUnitClassRun du) {
    return getTime().compareTo(du.getTime());
  }
  
  
  
  public DUnitClassRun(int runId, int classId,Date time) {
    super();
    this.runId = runId;
    this.classId = classId;
    this.time = time;
  }
  
  
  public int getClassId() {
    return classId;
  }
  
  public int getRunId() {
    return runId;
  }
  
  public Date getTime() {
    return time;
  }
  
  public String getTimeString() {
    SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yy HH:mm:ss");
    return sdf.format(getTime());
  }
  
  List<DUnitTestMethodDetail> fails = null;
  public List<DUnitTestMethodDetail> getFails() throws SQLException {
    if(fails==null) {
      fails = new ArrayList<DUnitTestMethodDetail>();
      for (DUnitTestMethodDetail du : getAllMethodDetails()) {
        if(du.isFail()) {
          fails.add(du);
        }
      }
    } 
    return fails;
  }

  List<DUnitTestMethodDetail> passes = null;
  public List<DUnitTestMethodDetail> getPasses() throws SQLException {
    if(passes==null) {
      passes = new ArrayList<DUnitTestMethodDetail>();
      for (DUnitTestMethodDetail du : getAllMethodDetails()) {
        if(!du.isFail()) {
          passes.add(du);
        }
      }
    } 
    return passes;
  }

  List<DUnitTestMethodDetail> all = null;
  public List<DUnitTestMethodDetail> getAllMethodDetails() throws SQLException {
    if(all==null) {
      all = Swarm.getAllForClassRun(getRunId(),getClassId());
    } 
    return all;
  }

  
  public int getPassCount() throws SQLException {
    return getPasses().size();
  }
  
  public int getFailCount() throws SQLException {
    return getFails().size();
  }
  
  
}
