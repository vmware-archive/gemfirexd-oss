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

import hydra.Log;
import hydra.TestTask;

import java.io.PrintWriter;
import java.io.StringWriter;

import swarm.bb.SwarmBB;

import junit.framework.*;

import java.io.*;
import java.util.*;
import java.util.regex.*;

import org.apache.tools.ant.taskdefs.optional.junit.JUnitTest;
import org.apache.tools.ant.taskdefs.optional.junit.XMLJUnitResultFormatter;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.InvocationTargetException;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.*;

public class UnitTestObserverImpl implements UnitTestObserver {

  
  private static DUnitRun dunitRun = null;

  private static boolean dunitRecordResults = Boolean.getBoolean("dunitRecordResults");
  public UnitTestObserverImpl() {
    
  }
  
  public void incCurrentTestCount() {
    // TODO Auto-generated method stub
    SwarmBB.incCurrentTestCount();

  }

  public void testTypeDetected(TestType t) {
    // TODO Auto-generated method stub
    SwarmBB.setTestType(t);

  }

  public void totalTestCountDetected(int count) {
    // TODO Auto-generated method stub
    SwarmBB.setTotalTestCount(count);

  }
  
  public long getCurrentTestCount() {
    return SwarmBB.getCurrentTestCount();
  }
  
  public long getTotalTestCount() {
    return SwarmBB.getTotalTestCount();
  }
  
  
  private long startTime;
  private String testInProgress = null;
  String lastPassClass = null;
  String lastFailClass = null;
  String lastFailMethod = null;
  
  
  
  Throwable lastThrowable = null;
 
  public void startTest(Test test) {
    this.startTime = System.currentTimeMillis();
    String s = "zzzzzSTART " + test;
    Log.getLogWriter().info(s);
    testInProgress = test.toString();
  }

  public void endTest(Test test) {
    if (!dunitRecordResults) {
      Log.getLogWriter().info("Not recording results");
      return;
    }
    long delta = System.currentTimeMillis() - this.startTime;
    String s = "zzzzzEND " + test + " (took " + delta + "ms)";
    Log.getLogWriter().info(s);
    testInProgress = null;
    String className = getClassName(test);
    String methodName = getMethodName(test);
    
      
      if (className.equals(lastFailClass)) {
        if(methodName.equals(lastFailMethod)) {
          // Ok the test that just failed is ending
          recordFail(className,methodName,delta);
        } else {
          lastFailMethod = null;
          lastThrowable = null;
          // I think this means another method passed in a failing test
          recordPass(className,methodName,delta);
        }
        // Test failed, do nothing
        //recordFail = true;
        return;
      } else if (lastPassClass == null) {
        // The first test to pass
        lastPassClass = className;
        lastFailMethod = null;
        lastFailClass = null;
        recordPass(className,methodName,delta);
      } else if (!lastPassClass.equals(className)) {
        // Note that the previous test passed
        lastPassClass = className;
        lastFailMethod = null;
        lastFailClass = null;
        recordPass(className,methodName,delta);
      } else {
        recordPass(className,methodName,delta);
      }
  }

  public void recordPass(String className,String methodName,long tookMs) {
    try {
      DUnitRun du = getDUnitRun();
      DUnitClassInfo duci = Swarm.getOrCreateDUnitClassInfo(className);
      DUnitMethodInfo dumi = Swarm.getOrCreateDUnitMethodInfo(methodName,duci.getId());
      Swarm.recordPass(du,dumi,tookMs);
    } catch(SQLException se) {
      se.printStackTrace();
    }
  }
  
  public void recordFail(String className,String methodName,long tookMs) {
    try {
      DUnitRun du = getDUnitRun();
      DUnitClassInfo duci = Swarm.getOrCreateDUnitClassInfo(className);
      DUnitMethodInfo dumi = Swarm.getOrCreateDUnitMethodInfo(methodName,duci.getId());
      Swarm.recordFailure(du,dumi,lastThrowable,tookMs);
    } catch(SQLException se) {
      se.printStackTrace();
    }
  }
  
  public void addError(Test test, Throwable t) {
    StringBuffer sb = new StringBuffer();
    sb.append(test.toString());
    sb.append("\n");
    StringWriter sw = new StringWriter();
    t.printStackTrace(new PrintWriter(sw, true));
    sb.append(sw.toString());
    Log.getLogWriter().severe("zzzzzERROR IN " + test, t);
    //reportFailure(test, sb.toString());
    lastFailClass = getClassName(test);
    lastFailMethod = getMethodName(test);
    lastThrowable = t;
  }

  public void addFailure(Test test, AssertionFailedError t) {
    StringBuffer sb = new StringBuffer();
    sb.append(test.toString());
    sb.append("\n");
    StringWriter sw = new StringWriter();
    t.printStackTrace(new PrintWriter(sw, true));
    sb.append(sw.toString());
    Log.getLogWriter().severe("zzzzzFAILURE IN " + test, t);
    //reportFailure(test, sb.toString());
    lastFailClass = getClassName(test);
    lastFailMethod = getMethodName(test);
    lastThrowable = t;
  }

  
  
  private static Pattern methodpattern =
    Pattern.compile("(.*\\w+)\\(.*\\)");
  
  private static Pattern classpattern =
    Pattern.compile(".*\\w+\\((.*)\\)");
  //                   \\w+\\((.*)\\)
  
  /**
   * Returns the name of the given test's class
   */
  protected static String getClassName(Test test) {
    String className;
    String desc = test.toString();
    Matcher matcher = classpattern.matcher(desc);
    if (matcher.matches()) {
      className = matcher.group(1);
      
    } else {
      className = desc;
    }

    return className;
  }
  
  protected static String getMethodName(Test test) {
    String className;
    String desc = test.toString();
    Matcher matcher = methodpattern.matcher(desc);
    if (matcher.matches()) {
      className = matcher.group(1);
      
    } else {
      className = desc;
    }

    return className;
  }
  
  
  /**
   * Returns true if this VM has acquired the right to run this TestTask
   */
  private static DUnitRun getDUnitRun() {
    if(dunitRun!=null) {
      System.out.println("BBB dunitRun not null returning");
      return dunitRun;
    }
    
    try {
      SwarmBB.getBB().getSharedLock().lock();
      Integer runId = (Integer)SwarmBB.getBB().getSharedMap().get(SwarmBB.RUN_ID);
      System.out.println("BBB runID="+runId);
      if (runId!=null) {
        dunitRun = Swarm.getDUnitRun(runId);
        System.out.println("BBB lookedUp RUN:"+dunitRun);
      } else {
        dunitRun = Swarm.generateNewDUnitRun();
        System.out.println("BBB GENNED UP A RUN:"+dunitRun+" mapping to id:"+dunitRun.getId());
        SwarmBB.getBB().getSharedMap().put(SwarmBB.RUN_ID,dunitRun.getId());
      }
    } catch(SQLException e) {
      e.printStackTrace();
    } catch(IOException e) {
      e.printStackTrace();
    } finally {
      SwarmBB.getBB().getSharedLock().unlock();
    }
    return dunitRun;
  }
  
  
  
}
