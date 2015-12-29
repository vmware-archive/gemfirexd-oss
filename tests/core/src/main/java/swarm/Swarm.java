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

import hydra.TestConfig;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;

public class Swarm {

  private static UnitTestObserverImpl unitTestObserver = null;

  public static UnitTestObserver getUnitTestObserver() {
    synchronized (Swarm.class) {
      if (unitTestObserver == null) {
        unitTestObserver = new UnitTestObserverImpl();
      }
    }
    return unitTestObserver;
  }

  /*
  public static BatteryObserver getBatteryObserver() {
    synchronized (Swarm.class) {
      if (batteryObserver == null) {
        batteryObserver = new WebServer();
      }
    }
    return batteryObserver;
  }
  */


  public static long getCurrentUnitTestCount() {
    return ((UnitTestObserverImpl)getUnitTestObserver()).getCurrentTestCount();
  }

  public static long getTotalUnitTestCount() {
    return ((UnitTestObserverImpl)getUnitTestObserver()).getTotalTestCount();
  }

  public static DUnitClassInfo getDUnitClassInfo(int id) throws SQLException {
    ResultSet rs = Database
        .executeQuery("SELECT ID,NAME from dunit_test_class where id=" + id);
    if (rs.next()) {
      DUnitClassInfo duci = new DUnitClassInfo(rs.getInt(1), rs.getString(2));
      return duci;
    }
    else {
      return null;
    }
  }

  public static DUnitClassInfo getOrCreateDUnitClassInfo(String name)
      throws SQLException {
    PreparedStatement ps = Database
        .prepareStatement("SELECT ID,NAME from dunit_test_class where name=?");
    ps.setString(1, name);
    ResultSet rs = ps.executeQuery();
    if (rs.next()) {
      DUnitClassInfo duci = new DUnitClassInfo(rs.getInt(1), rs.getString(2));
      return duci;
    }
    else {
      PreparedStatement psi = Database
          .prepareStatement("INSERT INTO dunit_test_class(name) values(?);SELECT currval('dunit_test_class_id_seq');");
      psi.setString(1, name);
      psi.execute();
      psi.getMoreResults();
      ResultSet rsi = psi.getResultSet();
      rsi.next();
      System.out.println("XXXXX:" + rsi.getInt(1));
      return new DUnitClassInfo(rsi.getInt(1), name);
    }
  }

  public static DUnitMethodInfo getOrCreateDUnitMethodInfo(String name,
      int class_id) throws SQLException {
    PreparedStatement ps = Database
        .prepareStatement("SELECT id from dunit_test_method where name=? and test_class_id=?");
    ps.setString(1, name);
    ps.setInt(2, class_id);
    ResultSet rs = ps.executeQuery();
    if (rs.next()) {
      DUnitMethodInfo duci = new DUnitMethodInfo(rs.getInt(1), name, class_id);
      return duci;
    }
    else {
      PreparedStatement psi = Database
          .prepareStatement("INSERT INTO dunit_test_method(name,test_class_id) values(?,?);SELECT currval('dunit_test_method_id_seq');");
      psi.setString(1, name);
      psi.setInt(2, class_id);
      psi.execute();
      psi.getMoreResults();
      ResultSet rsi = psi.getResultSet();
      rsi.next();
      System.out.println("2XXXXX:" + rsi.getInt(1));
      return new DUnitMethodInfo(rsi.getInt(1), name, class_id);
    }
  }

  public static DUnitMethodInfo getDUnitMethodInfo(int method_id)
      throws SQLException {
    PreparedStatement ps = Database
        .prepareStatement("SELECT id,name,test_class_id from dunit_test_method where id=?");
    ps.setInt(1, method_id);
    ResultSet rs = ps.executeQuery();
    if (rs.next()) {
      DUnitMethodInfo duci = new DUnitMethodInfo(rs.getInt(1), rs.getString(2),
          rs.getInt(3));
      return duci;
    }
    else {
      return null;
    }
  }

  public static List<DUnitMethodInfo> getDUnitMethodInfosForClass(int class_id)
      throws SQLException {
    ResultSet rs = Database.executeQuery(
        "SELECT id,name,test_class_id from dunit_test_method where test_class_id="
            + class_id, false);
    List<DUnitMethodInfo> list = new ArrayList<DUnitMethodInfo>();
    while (rs.next()) {
      DUnitMethodInfo duci = new DUnitMethodInfo(rs.getInt(1), rs.getString(2),
          rs.getInt(3));
      list.add(duci);
    }
    return list;
  }

  static Map<Integer, DUnitRun> runCache = new HashMap<Integer, DUnitRun>();

  public static DUnitRun getDUnitRun(int run_id) throws SQLException {
    synchronized (Swarm.class) {
      DUnitRun r = runCache.get(run_id);
      if (r != null) {
        return r;
      }
    }
    ResultSet rs = Database
        .executeQuery(
            "SELECT id,user_name,path,sites,revision,branch,os_name,os_version,java_version,java_vm_version,java_vm_vendor,time from dunit_run where id="
                + run_id, false);
    if (rs.next()) {
      DUnitRun du = new DUnitRun(rs.getInt(1), rs.getString(2),
          rs.getString(3), rs.getInt(4), rs.getString(5), rs.getString(6), rs
              .getString(7), rs.getString(8), rs.getString(9),
          rs.getString(10), rs.getString(11), new Date(rs.getTimestamp(12)
              .getTime()));
      synchronized (Swarm.class) {
        runCache.put(run_id, du);
      }
      return du;
    }
    else {
      return null;
    }
  }

  public static DUnitRun getLatestDUnitRun(String user_name)
      throws SQLException {
    PreparedStatement ps = Database
        .prepareStatement(
            "SELECT id,user_name,path,sites,revision,branch,os_name,os_version,java_version,java_vm_version,java_vm_vendor,time from dunit_run where user_name=? ORDER BY ID DESC LIMIT 1",
            false);
    ps.setString(1, user_name);
    ResultSet rs = ps.executeQuery();
    if (rs.next()) {
      DUnitRun du = new DUnitRun(rs.getInt(1), rs.getString(2),
          rs.getString(3), rs.getInt(4), rs.getString(5), rs.getString(6), rs
              .getString(7), rs.getString(8), rs.getString(9),
          rs.getString(10), rs.getString(11), new Date(rs.getTimestamp(12)
              .getTime()));
      return du;
    }
    else {
      return null;
    }
  }

  public static List<DUnitRun> getAllDUnitRuns(FilterCriterion fc)
      throws SQLException {
    PreparedStatement ps = Database
        .prepareStatement("SELECT id,user_name,path,sites,"
            + "revision,branch,os_name,os_version,"
            + "java_version,java_vm_version" + ",java_vm_vendor,time"
            + " from dunit_run");
    ResultSet rs = ps.executeQuery();
    ArrayList<DUnitRun> durs = new ArrayList<DUnitRun>();
    while (rs.next()) {
      DUnitRun du = new DUnitRun(rs.getInt(1), rs.getString(2),
          rs.getString(3), rs.getInt(4), rs.getString(5), rs.getString(6), rs
              .getString(7), rs.getString(8), rs.getString(9),
          rs.getString(10), rs.getString(11), new Date(rs.getTimestamp(12)
              .getTime()));
      if (fc.applyFilter(du)) {
        durs.add(du);
      }
    }
    return durs;
  }

  public static List<DUnitClassRun> getLastNDUnitClassRuns(int classId,
      int howMany) throws SQLException {
    ResultSet rs = Database
        .executeQuery(
            "SELECT run_id FROM dunit_test_method_detail md JOIN dunit_test_method tm ON (md.method_id=tm.id AND tm.test_class_id="
                + classId
                + ") GROUP BY run_id,tm.test_class_id ORDER BY run_id DESC LIMIT "
                + howMany, false);
    ArrayList<DUnitClassRun> ducrs = new ArrayList<DUnitClassRun>();
    while (rs.next()) {
      DUnitRun du = getDUnitRun(rs.getInt(1));
      DUnitClassRun ducr = new DUnitClassRun(du.getId(), classId, du.getTime());
      ducrs.add(ducr);
    }
    return ducrs;
  }

  public static DUnitRun generateNewDUnitRun() throws SQLException, IOException {

    Properties gfvp = new Properties();
    gfvp
        .load(GemFireCacheImpl.class
            .getResourceAsStream("/com/gemstone/gemfire/internal/GemFireVersion.properties"));

    PreparedStatement ps = Database
        .prepareStatement("INSERT INTO dunit_run(user_name,path,sites,revision,branch,os_name,os_version,java_version,java_vm_version,java_vm_vendor) values(?,?,?,?,?,?,?,?,?,?);SELECT currval('dunit_run_id_seq');");
    String user_name = System.getProperty("user.name");
    String path = System.getProperty("user.dir");
    int sites = Integer.parseInt((String)TestConfig.getInstance()
        .getSystemProperties().get("dunitSites"));
    String revision = gfvp.getProperty("Source-Revision");
    String branch = gfvp.getProperty("Source-Repository");
    String os_name = System.getProperty("os.name");
    String os_version = System.getProperty("os.version");
    String java_version = System.getProperty("java.version");
    String java_vm_version = System.getProperty("java.vm.version");
    String java_vm_vendor = System.getProperty("java.vm.vendor");

    ps.setString(1, user_name);
    ps.setString(2, path);
    ps.setInt(3, sites);
    ps.setString(4, revision);
    ps.setString(5, branch);
    ps.setString(6, os_name);
    ps.setString(7, os_version);
    ps.setString(8, java_version);
    ps.setString(9, java_vm_version);
    ps.setString(10, java_vm_vendor);

    ps.execute();
    ps.getMoreResults();
    ResultSet rsi = ps.getResultSet();
    rsi.next();
    DUnitRun du = new DUnitRun(rsi.getInt(1), user_name, path, sites, revision,
        branch, os_name, os_version, java_version, java_vm_version,
        java_vm_vendor, new Date());
    return du;
  }

  public static List<DUnitClassInfo> getAllDUnitClasses() throws SQLException {
    ArrayList<DUnitClassInfo> ducis = new ArrayList<DUnitClassInfo>();
    ResultSet rs = Database
        .executeQuery("SELECT id,name FROM dunit_test_class");
    while (rs.next()) {
      DUnitClassInfo duc = new DUnitClassInfo(rs.getInt(1), rs.getString(2));
      ducis.add(duc);
    }
    return ducis;
  }

  public static List<DUnitMethodInfo> getAllDUnitMethods() throws SQLException {
    ArrayList<DUnitMethodInfo> ducis = new ArrayList<DUnitMethodInfo>();
    ResultSet rs = Database
        .executeQuery("SELECT id,name,test_class_id FROM dunit_test_method");
    while (rs.next()) {
      DUnitMethodInfo duc = new DUnitMethodInfo(rs.getInt(1), rs.getString(2),
          rs.getInt(3));
      ducis.add(duc);
    }
    return ducis;
  }

  public static DUnitTestMethodDetail getDUnitTestMethodDetail(int method_id)
      throws SQLException {
    ResultSet rs = Database
        .executeQuery("SELECT id,method_id,status,error,run_id,time,tookMs FROM dunit_test_method_detail where id="
            + method_id);
    if (rs.next()) {
      DUnitTestMethodDetail duc = generateDUnitTestMethodDetail(rs);
      return duc;
    }
    else {
      return null;
    }
  }

  public static List<DUnitTestMethodDetail> getPassedDetailsForClass(int classId)
      throws SQLException {
    ArrayList<DUnitTestMethodDetail> ducis = new ArrayList<DUnitTestMethodDetail>();
    ResultSet rs = Database
        .executeQuery("SELECT md.id,md.method_id,md.status,md.error,md.run_id,md.time,tookMs FROM dunit_test_method_detail md JOIN dunit_test_method dtm ON dtm.test_class_id="
            + classId + " AND dtm.id=md.method_id AND md.status='PASS'");
    while (rs.next()) {
      DUnitTestMethodDetail duc = generateDUnitTestMethodDetail(rs);
      ducis.add(duc);
    }
    return ducis;
  }

  public static List<DUnitTestMethodDetail> getFailedDetailsForClass(int classId)
      throws SQLException {
    ArrayList<DUnitTestMethodDetail> ducis = new ArrayList<DUnitTestMethodDetail>();
    ResultSet rs = Database
        .executeQuery("SELECT md.id,md.method_id,md.status,md.error,md.run_id,md.time,tookMs FROM dunit_test_method_detail md JOIN dunit_test_method dtm ON dtm.test_class_id="
            + classId + " AND dtm.id=md.method_id AND md.status='FAIL'");
    while (rs.next()) {
      DUnitTestMethodDetail duc = generateDUnitTestMethodDetail(rs);
      ducis.add(duc);
    }
    return ducis;
  }

  public static List<DUnitTestMethodDetail> getDetailsForClassAndRun(
      int classId, int runId) throws SQLException {
    ArrayList<DUnitTestMethodDetail> ducis = new ArrayList<DUnitTestMethodDetail>();
    ResultSet rs = Database
        .executeQuery("SELECT md.id,md.method_id,md.status,md.error,md.run_id,md.time,tookMs FROM dunit_test_method_detail md JOIN dunit_test_method dtm ON dtm.test_class_id="
            + classId
            + " AND dtm.id=md.method_id AND md.run_id="
            + runId);
    while (rs.next()) {
      DUnitTestMethodDetail duc = generateDUnitTestMethodDetail(rs);
      ducis.add(duc);
    }
    return ducis;
  }

  public static List<DUnitTestMethodDetail> getPassedDetailsForMethod(
      int methodId) throws SQLException {
    ArrayList<DUnitTestMethodDetail> ducis = new ArrayList<DUnitTestMethodDetail>();
    ResultSet rs = Database
        .executeQuery("SELECT id,method_id,status,error,run_id,time,tookMs FROM dunit_test_method_detail where method_id="
            + methodId + " AND status='PASS'");
    while (rs.next()) {
      DUnitTestMethodDetail duc = generateDUnitTestMethodDetail(rs);
      ducis.add(duc);
    }
    return ducis;
  }

  public static List<DUnitTestMethodDetail> getFailedDetailsForMethod(
      int methodId) throws SQLException {
    ArrayList<DUnitTestMethodDetail> ducis = new ArrayList<DUnitTestMethodDetail>();
    ResultSet rs = Database
        .executeQuery("SELECT id,method_id,status,error,run_id,time,tookMs FROM dunit_test_method_detail where method_id="
            + methodId + " AND status='FAIL'");
    while (rs.next()) {
      DUnitTestMethodDetail duc = generateDUnitTestMethodDetail(rs);
      ducis.add(duc);
    }
    return ducis;
  }

  public static DUnitTestMethodDetail getLastFail(int methodId)
      throws SQLException {
    ResultSet rs = Database
        .executeQuery("SELECT id,method_id,status,error,run_id,time,tookMs FROM dunit_test_method_detail where method_id="
            + methodId + " AND status='FAIL' ORDER BY ID DESC LIMIT 1");
    if (rs.next()) {
      DUnitTestMethodDetail duc = generateDUnitTestMethodDetail(rs);
      return duc;
    }
    return null;
  }

  public static DUnitTestMethodDetail getLastPass(int methodId)
      throws SQLException {
    ResultSet rs = Database
        .executeQuery("SELECT id,method_id,status,error,run_id,time,tookMs FROM dunit_test_method_detail where method_id="
            + methodId + " AND status='PASS' ORDER BY ID DESC LIMIT 1");
    if (rs.next()) {
      DUnitTestMethodDetail duc = generateDUnitTestMethodDetail(rs);
      return duc;
    }
    return null;
  }

  public static DUnitTestMethodDetail getLastRun(int methodId)
      throws SQLException {
    ResultSet rs = Database
        .executeQuery("SELECT id,method_id,status,error,run_id,time,tookMs FROM dunit_test_method_detail where method_id="
            + methodId + " ORDER BY ID DESC LIMIT 1");
    if (rs.next()) {
      DUnitTestMethodDetail duc = generateDUnitTestMethodDetail(rs);
      return duc;
    }
    return null;
  }

  public static List<DUnitTestMethodDetail> getAllDUnitTestMethodDetail()
      throws SQLException {
    ArrayList<DUnitTestMethodDetail> ducis = new ArrayList<DUnitTestMethodDetail>();
    ResultSet rs = Database
        .executeQuery("SELECT id,method_id,status,error,run_id,time,tookMs FROM dunit_test_method_detail");
    while (rs.next()) {
      DUnitTestMethodDetail duc = generateDUnitTestMethodDetail(rs);
      ducis.add(duc);
    }
    return ducis;
  }
  
  
  public static List<DUnitTestMethodDetail> queryFailures(String query) throws SQLException {
    ArrayList<DUnitTestMethodDetail> ducis = new ArrayList<DUnitTestMethodDetail>();
    ResultSet rs = Database.executeQuery("SELECT id,method_id,status,error,run_id,time,tookMs FROM dunit_test_method_detail where lower(error) LIKE '%"+query.toLowerCase()+"%' and status NOT LIKE 'PASS'");
    while(rs.next()) {
      DUnitTestMethodDetail duc = generateDUnitTestMethodDetail(rs);
      ducis.add(duc);
    }
    return ducis;
  }
  
  
  public static List<DUnitTestMethodDetail> getFailedForRun(int runId)
      throws SQLException {
    ArrayList<DUnitTestMethodDetail> ducis = new ArrayList<DUnitTestMethodDetail>();
    ResultSet rs = Database
        .executeQuery("SELECT id,method_id,status,error,run_id,time,tookMs FROM dunit_test_method_detail where run_id="
            + runId + " and status NOT LIKE 'PASS'");
    while (rs.next()) {
      DUnitTestMethodDetail duc = generateDUnitTestMethodDetail(rs);
      ducis.add(duc);
    }
    return ducis;
  }

  public static List<DUnitTestMethodDetail> getPassedForRun(int runId)
      throws SQLException {
    ArrayList<DUnitTestMethodDetail> ducis = new ArrayList<DUnitTestMethodDetail>();
    ResultSet rs = Database
        .executeQuery("SELECT id,method_id,status,error,run_id,time,tookMs FROM dunit_test_method_detail where run_id="
            + runId + " and status NOT LIKE 'FAIL'");
    while (rs.next()) {
      DUnitTestMethodDetail duc = generateDUnitTestMethodDetail(rs);
      ducis.add(duc);
    }
    return ducis;
  }

  public static List<MethodRunSummary> getPassSummariesForRun(int runId)
      throws SQLException {
    ArrayList<MethodRunSummary> ducis = new ArrayList<MethodRunSummary>();
    ResultSet rs = Database
        .executeQuery("SELECT md.id,mi.test_class_id,mi.id,ci.name,mi.name,md.time,md.tookMs FROM dunit_test_method_detail md JOIN dunit_test_method mi ON (mi.id=md.method_id) JOIN dunit_test_class ci ON (ci.id=mi.test_class_id) where md.run_id="
            + runId + " and md.status!='FAIL'");
    while (rs.next()) {
      MethodRunSummary mrs = new MethodRunSummary(rs.getInt(1), rs.getInt(2),
          rs.getInt(3), rs.getString(4), rs.getString(5), new Date(rs
              .getTimestamp(6).getTime()), rs.getLong(7));
      ducis.add(mrs);
    }
    return ducis;
  }

  public static List<DUnitTestMethodDetail> getFailedForClassRun(int runId,
      int classId) throws SQLException {
    ArrayList<DUnitTestMethodDetail> ducis = new ArrayList<DUnitTestMethodDetail>();
    ResultSet rs = Database
        .executeQuery(
            "SELECT md.id,method_id,status,error,run_id,time,tookMs FROM dunit_test_method_detail md JOIN dunit_test_method tm ON (tm.id=md.method_id AND tm.test_class_id="
                + classId + ") where run_id=" + runId + " and status !='PASS'",
            false);
    while (rs.next()) {
      DUnitTestMethodDetail duc = generateDUnitTestMethodDetail(rs);
      ducis.add(duc);
    }
    return ducis;
  }

  public static List<DUnitTestMethodDetail> getPassedForClassRun(int runId,
      int classId) throws SQLException {
    ArrayList<DUnitTestMethodDetail> ducis = new ArrayList<DUnitTestMethodDetail>();
    ResultSet rs = Database
        .executeQuery(
            "SELECT md.id,method_id,status,error,run_id,time,tookMs FROM dunit_test_method_detail md JOIN dunit_test_method tm ON (tm.id=md.method_id AND tm.test_class_id="
                + classId + ") where run_id=" + runId + " and status !='FAIL'",
            false);
    while (rs.next()) {
      DUnitTestMethodDetail duc = generateDUnitTestMethodDetail(rs);
      ducis.add(duc);
    }
    return ducis;
  }

  public static List<DUnitTestMethodDetail> getAllForClassRun(int runId,
      int classId) throws SQLException {
    ArrayList<DUnitTestMethodDetail> ducis = new ArrayList<DUnitTestMethodDetail>();
    ResultSet rs = Database
        .executeQuery(
            "SELECT md.id,method_id,status,error,run_id,time,tookMs FROM dunit_test_method_detail md JOIN dunit_test_method tm ON (tm.id=md.method_id AND tm.test_class_id="
                + classId + ") where run_id=" + runId, false);
    while (rs.next()) {
      DUnitTestMethodDetail duc = generateDUnitTestMethodDetail(rs);
      ducis.add(duc);
    }
    return ducis;
  }

  public static DUnitTestMethodDetail generateDUnitTestMethodDetail(ResultSet rs)
      throws SQLException {
    return new DUnitTestMethodDetail(rs.getInt(1), rs.getInt(2), rs
        .getString(3), rs.getString(4), rs.getInt(5), new Date(rs.getTimestamp(
        6).getTime()), rs.getLong(7));
  }

  public static void recordFailure(DUnitRun run, DUnitMethodInfo dumi,
      Throwable t, long tookMs) throws SQLException {
    PreparedStatement ps = Database
        .prepareStatement("INSERT INTO dunit_test_method_detail(method_id,status,error,run_id,tookMs) values (?,?,?,?,?);");
    ps.setInt(1, dumi.getId());
    ps.setString(2, "FAIL");
    ps.setString(3, getRootCause(t));
    ps.setInt(4, run.getId());
    ps.setLong(5, tookMs);
    ps.executeUpdate();
  }

  public static void recordPass(DUnitRun run, DUnitMethodInfo dumi, long tookMs)
      throws SQLException {
    PreparedStatement ps = Database
        .prepareStatement("INSERT INTO dunit_test_method_detail(method_id,status,error,run_id,tookMs) values (?,?,?,?,?);");
    ps.setInt(1, dumi.getId());
    ps.setString(2, "PASS");
    ps.setString(3, null);
    ps.setInt(4, run.getId());
    ps.setLong(5, tookMs);
    ps.executeUpdate();
  }

  private static String getRootCause(Throwable t) {
    if (t.getCause() == null) {
      StringWriter sw = new StringWriter();
      t.printStackTrace(new PrintWriter(sw));
      return sw.toString();
    }
    else {
      return getRootCause(t.getCause());
    }
  }
}
