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
import java.util.List;

public class DUnitClassInfo {

  private final int id;
  private final String name;
  
  public DUnitClassInfo(int id, String name) {
    super();
    this.id = id;
    this.name = name;
  }
  public int getId() {
    return id;
  }
  public String getName() {
    return name;
  }
  
  public String getShortName() {
    return TextHelper.getShortName(name);
  }

  public String getVeryShortName() {
    return TextHelper.getVeryShortName(name);
  }

  public int getMethodCount() {
    try {
      return Swarm.getDUnitMethodInfosForClass(getId()).size();
    } catch(SQLException e) {
      e.printStackTrace();
    }
    return -1;
  }
  
  public List<DUnitClassRun> getLast5Runs() {
    try {
      return Swarm.getLastNDUnitClassRuns(getId(),5);
    } catch(SQLException e) {
      e.printStackTrace();
    }
    return null;
  }

  public List<DUnitClassRun> getLast10Runs() {
    try {
      return Swarm.getLastNDUnitClassRuns(getId(),10);
    } catch(SQLException e) {
      e.printStackTrace();
    }
    return null;
  }

  
  public List<DUnitClassRun> getLast2Runs() {
    try {
      return Swarm.getLastNDUnitClassRuns(getId(),2);
    } catch(SQLException e) {
      e.printStackTrace();
    }
    return null;
  }
  
}
