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

public class DUnitMethodInfo {

  private final int id;
  private final String name;
  private final int classId;
  
  
  public DUnitMethodInfo(int id, String name, int classId) {
    super();
    this.id = id;
    this.name = name;
    this.classId = classId;
  }
  
  
  public int getId() {
    return id;
  }
  
  
  public String getName() {
    return name;
  }
  
  
  public int getClassId() {
    return classId;
  }
  
  public DUnitClassInfo getClassInfo() {
    try {
       return Swarm.getDUnitClassInfo(getClassId());
    } catch(SQLException se) {
      se.printStackTrace();
    }
    return null;
  }
  
  public int getPassCount() {
    try {
      return Swarm.getPassedDetailsForMethod(getId()).size();
    } catch(SQLException se) {
      se.printStackTrace();
    }
    return -1;
  }
  
  public int getFailCount() {
    try {
      return Swarm.getFailedDetailsForMethod(getId()).size();
    } catch(SQLException se) {
      se.printStackTrace();
    }
    return -1;
  }
  
  public int getPassPercent() {
    float p = getPassCount();
    float f = getFailCount();
    float pct = (p/(p+f))*100;
    return (int)pct;
  }
  
  public DUnitTestMethodDetail getLastRun() {
    try {
      return Swarm.getLastRun(getId());
    } catch(SQLException se) {
      se.printStackTrace();
    }
    return null; 
  }
  
  public DUnitTestMethodDetail getLastPass() {
    try {
      return Swarm.getLastPass(getId());
    } catch(SQLException se) {
      se.printStackTrace();
    }
    return null;
  }
  
  public DUnitTestMethodDetail getLastFail() {
    try {
      return Swarm.getLastFail(getId());
    } catch(SQLException se) {
      se.printStackTrace();
    }
    return null;
  }
  
}
