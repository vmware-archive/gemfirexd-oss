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

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet implementation class SwarmServlet
 */
public class SwarmServlet extends HttpServlet {
  private static final long serialVersionUID = 1L;

  /**
   * @see HttpServlet#HttpServlet()
   */
  public SwarmServlet() {
    super();
    // TODO Auto-generated constructor stub
  }

  /**
   * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse
   *      response)
   */
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    // TODO Auto-generated method stub
    String pathInfo = request.getPathInfo();
    String path = request.getContextPath();
    String path3 = request.getPathTranslated();
    String path4 = request.getQueryString();
    System.out.println("pathInfo:" + pathInfo + " cpath:" + path + " tpath:"
        + path3 + " qs:" + path4);
    if (pathInfo == null) {
      showDUnitRuns(request, response);
    }
    else {
      if (pathInfo.startsWith("/")) {
        pathInfo = pathInfo.substring(1);
      }
      if (pathInfo.startsWith("run/")) {
        pathInfo = pathInfo.substring(4);
        int runId = Integer.parseInt(pathInfo);
        showDUnitRun(runId, request, response);
      }
      else if (pathInfo.startsWith("result/")) {
        pathInfo = pathInfo.substring(7);
        int methodId = Integer.parseInt(pathInfo);
        showDUnitMethodDetail(methodId, request, response);
      }
      else if (pathInfo.startsWith("method/")) {
        pathInfo = pathInfo.substring(7);
        int methodId = Integer.parseInt(pathInfo);
        showDUnitMethodInfo(methodId, request, response);
      }
      else if (pathInfo.startsWith("class/")) {
        pathInfo = pathInfo.substring(6);
        int methodId = Integer.parseInt(pathInfo);
        showDUnitClassInfo(methodId, request, response);
      }
      else if (pathInfo.startsWith("runs")) {
        showDUnitRuns(request, response);
      }
      else if (pathInfo.startsWith("classes")) {
        showDUnitClasses(request, response);
      } else if(pathInfo.startsWith("search")) {
        String query = request.getParameter("query");
        showSearchResults(query,request, response);
      } else {
        response.sendRedirect("/shh/runs");
      }
	  }
	}
	
	public static void showSearchResults(String query, HttpServletRequest request,HttpServletResponse response) throws ServletException,IOException {
	  try {
  	  List<DUnitTestMethodDetail> details = Swarm.queryFailures(query);
  	  request.setAttribute("query",query);
  	  request.setAttribute("failures",details);
	  } catch(SQLException se) {
	    se.printStackTrace();
	  }
	  request.getRequestDispatcher("/query_results.jsp").forward(request,response);
	}
	
	public static void showDUnitRun(int runId, HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
	  try {
	    DUnitRun run = Swarm.getDUnitRun(runId);
	    request.setAttribute("run",run);
	  } catch(SQLException se) {
      se.printStackTrace();
    }
    request.getRequestDispatcher("/dunit_run.jsp").forward(request, response);
  }

  public static void showDUnitMethodDetail(int methodId,
      HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    try {
      DUnitTestMethodDetail mutmd = Swarm.getDUnitTestMethodDetail(methodId);
      int runId = mutmd.getRunId();
      DUnitRun run = Swarm.getDUnitRun(runId);
      List<DUnitTestMethodDetail> classResults = Swarm.getDetailsForClassAndRun(mutmd.getMethodInfo().getClassId(),run.getId());
      Collections.sort(classResults);
      Collections.reverse(classResults);
      classResults.remove(mutmd);
      request.setAttribute("classResults",classResults);
      request.setAttribute("method",mutmd);
      request.setAttribute("run",run);
    } catch(SQLException se) {
      se.printStackTrace();
    }
    request.getRequestDispatcher("/dunit_method_result.jsp").forward(request,
        response);
  }

  public static void showDUnitMethodInfo(int methodId,
      HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    try {
      DUnitMethodInfo minfo = Swarm.getDUnitMethodInfo(methodId);
      DUnitClassInfo cinfo = minfo.getClassInfo();

      List<DUnitTestMethodDetail> passes = Swarm
          .getPassedDetailsForMethod(methodId);
      List<DUnitTestMethodDetail> fails = Swarm
          .getFailedDetailsForMethod(methodId);

      int passCount = passes.size();
      int failCount = fails.size();
      int totalCount = failCount + passCount;

      request.setAttribute("passCount", passCount);
      request.setAttribute("failCount", failCount);
      request.setAttribute("totalCount", totalCount);

      Collections.sort(passes);
      Collections.sort(fails);
      Collections.reverse(passes);
      Collections.reverse(fails);
      request.setAttribute("passes",passes);
      request.setAttribute("fails",fails);
      
      ArrayList<DUnitTestMethodDetail> both = new ArrayList<DUnitTestMethodDetail>(passes);
      both.addAll(fails);
      Collections.sort(both);
      Collections.reverse(both);
      request.setAttribute("both",both);
      request.setAttribute("minfo",minfo);
      request.setAttribute("cinfo",cinfo);
    } catch(SQLException se) {
      se.printStackTrace();
    }
    request.getRequestDispatcher("/dunit_method_info.jsp").forward(request,
        response);
  }

  public static void showDUnitClassInfo(int classId,
      HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    try {
      DUnitClassInfo cinfo = Swarm.getDUnitClassInfo(classId);
      List<DUnitMethodInfo> minfos = Swarm.getDUnitMethodInfosForClass(classId);
      System.out.println("!!!!!!!!!!!!!minfos:" + minfos.size());
      request.setAttribute("cinfo", cinfo);
      request.setAttribute("minfos", minfos);

      List<DUnitTestMethodDetail> passes = Swarm
          .getPassedDetailsForClass(classId);
      List<DUnitTestMethodDetail> fails = Swarm
          .getFailedDetailsForClass(classId);

      int passCount = passes.size();
      int failCount = fails.size();
      int totalCount = failCount + passCount;

      System.out.println("pc:" + passCount + " fc:" + failCount);
      request.setAttribute("passCount", passCount);
      request.setAttribute("failCount", failCount);
      request.setAttribute("totalCount", totalCount);

      Collections.sort(passes);
      Collections.sort(fails);
      Collections.reverse(passes);
      Collections.reverse(fails);

      request.setAttribute("passes", passes);
      request.setAttribute("fails", fails);

      ArrayList<DUnitTestMethodDetail> both = new ArrayList<DUnitTestMethodDetail>(
          passes);
      both.addAll(fails);

      Collections.sort(both);
      Collections.reverse(both);
      request.setAttribute("both", both);

    }
    catch (SQLException se) {
      se.printStackTrace();
    }
    request.getRequestDispatcher("/dunit_class_info.jsp").forward(request,
        response);
  }

  public static void showDashboard(HttpServletRequest request,
      HttpServletResponse response) throws ServletException, IOException {
    try {
      ResultSet rs = Database.executeQuery("SELECT user_name FROM \"USER\"",
          true);
      List<String> users = new ArrayList<String>();

      while (rs.next()) {
        String s = rs.getString(1);
        users.add(s);
      }
      System.out.println("users:" + users);
      request.setAttribute("users", users);
      request.setAttribute("base_envs", getBaseEnvs("gregp"));
      request.setAttribute("runtime_envs", getRuntimeEnvs("gregp"));
      List<DUnitClassInfo> ducis = Swarm.getAllDUnitClasses();
      request.setAttribute("ducis", ducis);

      DUnitRun latestRun = Swarm.getLatestDUnitRun("gregp");
      request.setAttribute("latestRun", latestRun);
      System.out.println("LatestRun:" + latestRun);

      FilterCriterion fc = buildFilterCriterion(request);
      request.setAttribute("os_name", request.getAttribute("os_name"));

      List<DUnitRun> druns = Swarm.getAllDUnitRuns(fc);
      request.setAttribute("druns", druns);
    }
    catch (SQLException se) {
      se.printStackTrace();
    }
    request.getRequestDispatcher("/swarm.jsp").forward(request, response);
  }

  public static void showDUnitClasses(HttpServletRequest request,
      HttpServletResponse response) throws ServletException, IOException {
    try {
      List<DUnitClassInfo> ducis = Swarm.getAllDUnitClasses();
      request.setAttribute("ducis", ducis);
      int duciCount = ducis.size();
      request.setAttribute("classCount", duciCount);
    }
    catch (Exception e) {
      e.printStackTrace();
    }
    request.getRequestDispatcher("/dunit_classes.jsp").forward(request,
        response);
  }

  public static void showDUnitRuns(HttpServletRequest request,
      HttpServletResponse response) throws ServletException, IOException {
    try {
      
      request.setAttribute("users", getUserList());
      request.setAttribute("branches", getBranchList());

      FilterCriterion fc = buildFilterCriterion(request);
      request.getSession().setAttribute("os_name", request.getParameter("os_name"));
      request.getSession().setAttribute("java_version", request.getParameter("java_version"));
      request.getSession().setAttribute("branch", request.getParameter("branch"));
      request.getSession().setAttribute("user", request.getParameter("user"));

      List<DUnitRun> druns = Swarm.getAllDUnitRuns(fc);
      Collections.sort(druns);
      Collections.reverse(druns);

      List<DUnitRun> last10 = druns;
      if (druns.size() > 10) {
        last10 = druns.subList(0, 10);
      }
      request.setAttribute("last10", last10);

      request.setAttribute("druns", druns);
    }
    catch (SQLException se) {
      se.printStackTrace();
    }
    request.getRequestDispatcher("/runs.jsp").forward(request, response);
  }

  /**
   * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse
   *      response)
   */
  protected void doPost(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    // TODO Auto-generated method stub
  }

  public static List<BaseEnv> getBaseEnvs(String user) throws SQLException {
    PreparedStatement ps = Database
        .prepareStatement("SELECT id,name,checkoutPath,outputPath from \"BASE_ENV\" WHERE user_name=?");
    ps.setString(1, user);
    ResultSet rs = ps.executeQuery();
    List<BaseEnv> base_envs = new ArrayList<BaseEnv>();
    while (rs.next()) {
      BaseEnv be = new BaseEnv(rs.getString(2), rs.getString(3), rs
          .getString(4), rs.getInt(1));
      base_envs.add(be);
    }
    return base_envs;
  }

  public static List<RuntimeEnv> getRuntimeEnvs(String user)
      throws SQLException {
    PreparedStatement ps = Database
        .prepareStatement("SELECT name,user_name,build_properties,targets,buildPath,resultPath from \"RUNTIME_ENV\" WHERE user_name=?");
    ps.setString(1, user);
    ResultSet rs = ps.executeQuery();
    List<RuntimeEnv> run_envs = new ArrayList<RuntimeEnv>();
    while (rs.next()) {
      RuntimeEnv re = new RuntimeEnv(rs.getString(1), rs.getString(2), rs
          .getString(3), rs.getString(4), rs.getString(5), rs.getString(6));
      run_envs.add(re);
    }
    return run_envs;
  }

  private static FilterCriterion buildFilterCriterion(HttpServletRequest request) {
    FilterCriterion fc = new FilterCriterion();
    fc.setUserName(request.getParameter("user"));
    fc.setOsName(request.getParameter("os_name"));
    fc.setJavaVersion(request.getParameter("java_version"));
    fc.setBranch(request.getParameter("branch"));
    return fc;
  }
  
  private static List<String> getUserList() {
    return getOneColumn("SELECT distinct user_name FROM dunit_run");
  }
  
  private static List<String> getBranchList() {
    return getOneColumn("select distinct branch from dunit_run");
  }

  private static List<String> getOneColumn(String sql) {
    ResultSet rs;
    List<String> list = null;
    try {
      rs = Database.executeQuery(sql, false);
      list = new ArrayList<String>();
      while (rs.next()) {
        String s = rs.getString(1);
        list.add(s);
      }
    }
    catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return list;
    
  }
}
