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

public class FilterCriterion {

  private String userName;

  private String osName;

  private String javaVersion;

  private String branch;
  
  public void setUserName(String userName) {
    this.userName = userName;
  }

  public String getUserName() {
    return userName;
  }

  public void setOsName(String osName) {
    this.osName = osName;
  }

  public String getOsName() {
    return osName;
  }

  public void setJavaVersion(String javaVersion) {
    this.javaVersion = javaVersion;
  }

  public String getJavaVersion() {
    return javaVersion;
  }

  public void setBranch(String branch) {
    this.branch = branch;
  }

  public String getBranch() {
    return branch;
  }
  
  public boolean applyFilter(DUnitRun run) {
    if (userName != null && !userName.equalsIgnoreCase("all")) {
      if (!userName.equalsIgnoreCase(run.getUserName())) {
        return false;
      }
    }
    if (branch != null && !branch.equalsIgnoreCase("all")) {
      if (!branch.equalsIgnoreCase(run.getBranch())) {
        return false;
      }
    }
    if (javaVersion != null && !javaVersion.equalsIgnoreCase("all")) {
      if (!run.getJavaVersion().contains(javaVersion)) {
        return false;
      }
    }
    if (osName != null && !osName.equalsIgnoreCase("all")) {
      if (!osName.equalsIgnoreCase(run.getOsName())) {
        return false;
      }
    }
    return true;
  }
}
