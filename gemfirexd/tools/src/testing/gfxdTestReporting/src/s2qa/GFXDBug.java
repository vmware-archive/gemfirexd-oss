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
package s2qa;

public class GFXDBug {
  private String bugNum;
  private String summary;
  private String status;
  private String severity;
  private String priority;
  private String owner;
  private String creationDate;

  public String getBugNum() {
    return bugNum;
  }
  public void setBugNum(String bugNum) {
    this.bugNum = bugNum;
  }
  public String getSummary() {
    return summary;
  }
  public void setSummary(String summary) {
    this.summary = summary;
  }
  public String getStatus() {
    return status;
  }
  public void setStatus(String status) {
    this.status = status;
  }
  public String getSeverity() {
    return severity;
  }
  public void setSeverity(String severity) {
    this.severity = severity;
  }
  public String getPriority() {
    return priority;
  }
  public void setPriority(String priority) {
    this.priority = priority;
  }
  public String getOwner() {
    return owner;
  }
  public void setOwner(String owner) {
    this.owner = owner;
  }
  public String getCreationDate() {
    return creationDate;
  }
  public void setCreationDate(String creationDate) {
    this.creationDate = creationDate;
  }

}
