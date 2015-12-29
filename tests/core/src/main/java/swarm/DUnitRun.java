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
import java.util.Collections;
import java.util.Date;
import java.util.List;

public class DUnitRun implements Comparable<DUnitRun> {

  private final int id;
  private final String userName;
  private final String path;
  private final int sites;

  private final String revision;
  private final String branch;
  private final String osName;
  private final String osVersion;
  private final String javaVersion;
  private final String javaVmVersion;
  private final String javaVmVendor;
  private final Date time;
  

  public int compareTo(DUnitRun du) {
    return getTime().compareTo(du.getTime());
  }
  
  
  
  public DUnitRun(int id, String userName, String path, int sites,
      String revision, String branch, String osName, String osVersion,
      String javaVersion, String javaVmVersion, String javaVmVendor,Date time) {
    super();
    this.id = id;
    this.userName = userName;
    this.path = path;
    this.sites = sites;
    this.revision = revision;
    this.branch = branch;
    this.osName = osName;
    this.osVersion = osVersion;
    this.javaVersion = javaVersion;
    this.javaVmVersion = javaVmVersion;
    this.javaVmVendor = javaVmVendor;
    this.time = time;
  }
  
  
  public int getId() {
    return id;
  }
  public String getUserName() {
    return userName;
  }
  public String getPath() {
    return path;
  }
  public int getSites() {
    return sites;
  }
  public String getRevision() {
    return revision;
  }
  public String getBranch() {
    return branch;
  }
  
  public String getShortBranch() {
    return branch.replaceAll("gemfire/","").replaceAll("branches/","");
  }
  
  public String getOsName() {
    return osName;
  }
  public String getOsVersion() {
    return osVersion;
  }
  public String getJavaVersion() {
    return javaVersion;
  }
  public String getJavaVmVersion() {
    return javaVmVersion;
  }
  public String getJavaVmVendor() {
    return javaVmVendor;
  }

  public String getShortJavaVmVendor() {
    return javaVmVendor.replaceAll(" Microsystems Inc.","");
  }
  
  public Date getTime() {
    return time;
  }
  
  public String getTimeString() {
    SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yy HH:mm:ss");
    return sdf.format(getTime());
  }

  
  private List<DUnitTestMethodDetail> passAndFail = null;
  public List<DUnitTestMethodDetail> getPassesAndFails() throws SQLException {
    if(passAndFail==null) {
      passAndFail = new ArrayList<DUnitTestMethodDetail>();
      passAndFail.addAll(fails);
      passAndFail.addAll(passes);
      Collections.sort(passAndFail);
      Collections.reverse(passAndFail);
    }
    return passAndFail;
  }

  
  private List<DUnitTestMethodDetail> fails = null;
  public List<DUnitTestMethodDetail> getFails() throws SQLException {
    if(fails==null) {
      fails = Swarm.getFailedForRun(getId());
      Collections.sort(fails);
      Collections.reverse(fails);
    }
    return fails;
  }

  private List<DUnitTestMethodDetail> passes = null;
  public List<DUnitTestMethodDetail> getPasses() throws SQLException {
    if(passes==null) {
      passes = Swarm.getPassedForRun(getId());
      Collections.sort(passes);
      Collections.reverse(passes);
    }
    return passes;
  }
  
  private List<MethodRunSummary> passSummaries = null;
  public List<MethodRunSummary> getPassSummaries() throws SQLException {
    if(passSummaries==null) {
      passSummaries = Swarm.getPassSummariesForRun(getId());
      Collections.sort(passSummaries);
      Collections.reverse(passSummaries);
    }
    return passSummaries;
  }
  
  
  public int getPassCount() throws SQLException {
    return getPasses().size();
  }
  
  public int getFailCount() throws SQLException {
    return getFails().size();
  }
  
  @Override
  public String toString() {    
    return new StringBuilder("DUnitRun@"+System.identityHashCode(this)+":")
      .append("id:"+id)
      .append(" userName:"+userName)
      .append(" path:"+path)
      .append(" sites:"+sites)
      .append(" revision:"+revision)
      .append(" branch:"+branch)
      .append(" osName:"+osName)
      .append(" osVersion:"+osVersion)
      .append(" javaVersion:"+javaVersion)
      .append(" javaVmVersion:"+javaVmVendor)
      .append(" javaVmVendor:"+javaVmVendor)
      .append(" time:"+time)
      .toString();
  }
  
  public String getVerboseHtml() throws SQLException {
    StringBuffer sb = new StringBuffer();
    sb.append("<div class=\"dunitMethodResult\" id=\"run").append(getId()).append("\">");
    sb.append("<div class=\"leftResult\">");
    sb.append("<div class=\"timestamp\">").append(getTimeString()).append("</div>");
    sb.append("<div class=\"runInfo\"><label>user:</label> <a href=\"/shh/user/").append(getUserName()).append("\">").append(getUserName()).append("</a> <label>dunit run:</label> <a href=\"/shh/run/").append(getId()).append("\">#").append(getId()).append("</a></div>");
    sb.append("<div class=\"runInfo\"><label>branch:</label> ").append(getShortBranch()).append(" <label>rev:</label> ").append(getRevision()).append("</div>");
    sb.append("<div class=\"runInfo\"><label>os:</label> ").append(getOsName()).append(" <label>jdk:</label> ").append(getShortJavaVmVendor()).append(": ").append(getJavaVersion()).append("</div>");
    sb.append("</div>");
    sb.append("<div class=\"rightResult\">");
    sb.append("<div class=\"");
    if(getFailCount()>0) {
      sb.append("statusfail"); 
    } else {
      sb.append("statuspass");
    }
    sb.append("\"><a href=\"/shh/run/").append(getId()).append("\">Failures: ").append(getFailCount()).append(" Passes: ").append(getPassCount()).append("</a></div>");
    sb.append("");
    sb.append("<div style=\"padding:10px;\">");
    if(getFailCount()==0) {
      sb.append("<a href=\"/shh/run/").append(getId()).append("\">No failures - Click for details</a>");  
    } else {
      sb.append("<div style=\"text-align:left;\"><strong>Failures:</strong></div>");
      sb.append("<div class=\"errorText\" style=\"padding-left:10px;\">");
      for(int i=0;i<getFails().size();i++) {
        DUnitTestMethodDetail fail = getFails().get(i);
        if(i==3 && i!=getFails().size()-1 && getFails().size()>3) {
          sb.append("<div id=\"showMore").append(getId()).append("\"><a style=\"font-weight:bold;\" href=\"#\" onclick=\"document.getElementById('moreFails").append(getId()).append("').style.display='block';document.getElementById('showMore").append(getId()).append("').style.display='none';return false;\">Click to see ").append(getFailCount()-3).append(" more failures...</a></div>");
          sb.append("<div style=\"display:none;\" id=\"moreFails").append(getId()).append("\">");
        }
        sb.append("<div style=\"border-bottom:1px dashed #776045;padding-bottom:3px;\"><a href=\"/shh/result/").append(fail.getId()).append("\" title=\"").append(fail.getError()).append("\">").append(fail.getMethodInfo().getClassInfo().getVeryShortName()).append(" : <strong>").append(fail.getMethodInfo().getName()).append("()</strong></a></div>");
        if(i!=3 && i==getFails().size()-1 && getFails().size()>3) {
          sb.append("</div>");
        }

      }
      sb.append("</div>");
    }
    
    sb.append("</div>");
    sb.append("</div>");
    sb.append("<div class=\"fs\"></div>");
    sb.append("</div>");
    return sb.toString();
  }
  
}
