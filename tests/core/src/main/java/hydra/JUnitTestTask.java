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
package hydra;

import com.gemstone.gemfire.*;
import com.gemstone.gemfire.internal.LocalLogWriter;
import com.gemstone.gemfire.internal.LogWriterImpl;
import com.gemstone.gemfire.internal.util.StopWatch;

import java.io.*;
import java.util.*;
import java.util.regex.*;

import org.apache.tools.ant.taskdefs.optional.junit.JUnitTest;
import org.apache.tools.ant.taskdefs.optional.junit.XMLJUnitResultFormatter;

import junit.framework.*;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.InvocationTargetException;

import swarm.Swarm;

/**
 * A <code>JUnitTestTask</code> is a <code>TestTask</code> that
 * executes a number of JUnit {@link TestCase}s.
 *
 * @see dunit.DistributedTestCase
 */
public class JUnitTestTask extends TestTask {

  /** The directory to write xml reports to */
  public static final String XML_REPORTS_DIR = "../xml-reports";
  
  /** The REAL System.err */
  private static final PrintStream ERR = 
    new PrintStream(new FileOutputStream(FileDescriptor.err));

  /** The package in which Ant JUnit classes reside */
  private static final String JUNIT_ANT =
    "org.apache.tools.ant.taskdefs.optional.junit.";

  private static String fromTestClass = System.getProperty("dunitFromTestClass");

  private static String uptoTestClass = System.getProperty("dunitUptoTestClass");
  
  /////////////////////  Instance Fields  /////////////////////

  /** Maps the names of classes to their methods that should be
   * invoked.  If there are no methods, then all of the test methods
   * should be run.*/
  private Map tests;

  /** The name of the most recent test class to be added. */
  private String lastClassName;

  /** The name of the test that is currently executing */
  protected String testInProgress;

  //////////////////////  Constructors  //////////////////////

  /**
   * Creates a new <code>JUnitTestTask</code>
   */
  public JUnitTestTask() {
    this.tests = new LinkedHashMap();
    this.setReceiver(this.getClass().getName());
    this.setSelector("foolHydra");
  }

  ////////////////////  Instance Methods  ////////////////////

  /**
   * Adds a test class to be tested
   *
   * @throws HydraConfigException
   *         If the class has already been added
   */
  void addTestClass(String className) {
    if (this.tests.containsKey(className)) {
      String s = "Duplicate class name: " + className;
      throw new HydraConfigException(s);
    }

    this.tests.put(className, new ArrayList());
    this.lastClassName = className;
  }

  /**
   * Adds a test method method  
   *
   * @throws HydraConfigException
   *         If the method has already been added
   */
  void addTestMethod(String methodName) {
    if (this.lastClassName == null) {
      String s = "Test method before test class: " + methodName;
      throw new HydraConfigException(s);
    }

    List methods = (List) this.tests.get(this.lastClassName);
    if (methods.contains(methodName)) {
      String s = "Duplicate method: " + methodName;
      throw new HydraConfigException(s);
    }

    methods.add(methodName);
  }

  /**
   * This method is the "selector" of this task.  It should never be
   * invoked. 
   */
  public static void foolHydra() {
    String s = "I pity the fool that messes with Hydra!";
    throw new IllegalStateException(s);
  }

  /** The name of the last test to pass */
  protected String lastPass;

  /** The name of the last test to fail */
  protected String lastFail;

  /**
   * Executes the JUnit tests using Ant's execution harness, or a
   * {@link TestSuite} if Ant is not available.
   */
  public TestTaskResult execute() {
//     if (isAntAvailable()) {
//       return executeUsingAnt();
//     }

    long beginTime = System.currentTimeMillis();

    final StringBuffer errorMessages = new StringBuffer();
    final StringBuffer failureMessages = new StringBuffer();

    final String logicalHostName = RemoteTestModule.getMyLogicalHost();
    Writer w;

    try {
      w = new FileWriter("dunit-progress-" + logicalHostName + ".txt", true);

    } catch (IOException ex) {
      w = new PrintWriter(System.out, true);
    }

    // log is the LogWriter for writing to dunit-progress.txt
    final LogWriter log =
      new LocalLogWriter(LogWriterImpl.ALL_LEVEL,
                         new PrintWriter(w, true));

    try {
      w = new FileWriter("dunit-passed-" + logicalHostName + ".txt", true);

    } catch (IOException ex) {
      w = new PrintWriter(System.out, true);
    }

    // passed is the LogWriter for writing to dunit-passed.txt
    final LogWriter passed =
      new LocalLogWriter(LogWriterImpl.ALL_LEVEL,
                         new PrintWriter(w, true));

    // create just one GemFireResultFormatter for entire task
    GemFireResultFormatter resultFormatter = new GemFireResultFormatter(
        log, passed, errorMessages, failureMessages);
    
    // wasSuccessful will remain true if all testcases were successful
    boolean wasSuccessful = true;
    Iterator iter = this.getTestClasses().iterator();
    while (iter.hasNext()) {
      String className = (String) iter.next();
      if (fromTestClass != null && fromTestClass.length() > 0) {
        if (!className.contains(fromTestClass)) {
          log.info("======== Skipping unit test: " + className);
          continue;
        }
        else {
          fromTestClass = null;
        }
      }
      
      if ( uptoTestClass != null && uptoTestClass.length() > 0) {
        if(uptoTestClass.equals("skip-now-on")) {
          log.info("======== Skipping unit test: " + className);
          continue;
        }
        if (className.contains(uptoTestClass)) {
          uptoTestClass = "skip-now-on";// this will match all rest of the dunit classes & thus skip.
        }
      }

      try {
        Class c = Class.forName(className);
        
        // define the xml output file for each testcase for CruiseControl
        String xmlFileName = "TEST-" + c.getName() + ".xml";
        File xmlResultDir = new File((String)null, XML_REPORTS_DIR);
        xmlResultDir.mkdir();
        File xmlFile = new File(xmlResultDir, xmlFileName);
        OutputStream xmlOut = System.out;
        try {
          xmlOut = new FileOutputStream(xmlFile);
        } catch (java.io.IOException e) {
          // leave xmlOut set to System.out
          log.error("Failed to create xml FileOutputStream", e);
        }
        
        // create a new XMLJUnitResultFormatter for each testcase class
        XMLJUnitResultFormatter xmlFormatter = new XMLJUnitResultFormatter();
        xmlFormatter.setOutput(xmlOut);
        
        // create a new result object for each testcase class
        TestResult result = new TestResult();
        // add the gemfire formatter to output dunit-progress.txt
        result.addListener(resultFormatter);
        // add the xml formatter to output xml for CruiseControl
        result.addListener(xmlFormatter);
        
        result.addListener(Swarm.getUnitTestObserver());
        
     
        //Check if this test has(or inherits) a caseSetUp or CaseTearDown method
        boolean requiresSpecialSetUpTearDownHook = hasCaseMethod(c); 

        // create TestSuite specific to testcase class
        TestSuite suite = new JUnitTestSuite();
        if ( requiresSpecialSetUpTearDownHook ) {
          //Use TestDecorator to enable caseSetUp() and caseTearDown()
          Test newTest = JUnitTestSetup.createJUnitTestSetup(c);
          suite.addTest(newTest);
        } else {
          suite.addTestSuite(c);
        }

        Enumeration en = suite.tests();
        int count = 0;
        while(en.hasMoreElements()) {
          en.nextElement();
          count++;
        }
        System.out.println("XXX: START SUITE "+c.getName()+" size:"+count);
        // create JUnitTest for testcase class... only used on xml callbacks
        JUnitTest test = new JUnitTest(c.getName());
        
        // invoke the xml callback startTestSuite
        xmlFormatter.startTestSuite(test);
        boolean timedOut = false;
        StopWatch timer = new StopWatch(true);
        try {
          // run the actual testcase class and return true only if timed out
          long s = System.nanoTime();
          timedOut = runTestSuite(suite, result, failureMessages, log);
          long e = System.nanoTime();
          long timeMs = (e-s)/1000000;
          System.out.println("XXX: DONE SUITE "+c.getName()+" size:"+count+" TOOK:"+timeMs);
        }
        finally {
          // set the runtime and counts for xml callbacks
          test.setRunTime(timer.elapsedTimeMillis());
          test.setCounts(
              result.runCount(), result.failureCount(), result.errorCount());
          wasSuccessful = wasSuccessful && 
              result.failureCount() == 0 && result.errorCount() == 0;
          
          // invoke the xml callback endTestSuite
          xmlFormatter.endTestSuite(test);
          if (timedOut) {
            // break out of loop because we timed out
            break;
          }
        }

      } catch (ClassNotFoundException ex) {
        errorMessages.append("Could not find class ");
        errorMessages.append(className);
        errorMessages.append("\n");
      }
    }

    long elapsedTime = System.currentTimeMillis() - beginTime;
    String messages = failureMessages + "\n" + errorMessages;
    com.gemstone.gemfire.internal.Assert.assertTrue(wasSuccessful || messages.length() > 0);

    if (lastPass != null) {
      passed.info("TEST " + lastPass + " PASSED");
    }

    return new JUnitTestTaskResult(messages, elapsedTime);
  }
  
  /**
   * Refactored from {@link #execute()}. <code>runTestSuite</code> is called
   * from loop inside <code>execute()</code> for each testcase class. Runs the
   * <code>TestSuite</code> with the argument <code>TestResult</code>. Each
   * <code>TestSuite</code> is a single testcase class.
   * 
   * @author Kirk Lund
   * @param suite the junit testcase class to run
   * @param result the junit results for the testcase to fill in
   * @param failureMessages the StringBuffer to append failure messages to 
   * @param log a LogWriter for writing to dunit-progress.txt
   * @return true if the testcase timed out; true will abort overall task
   */
  private boolean runTestSuite(final TestSuite suite, 
                               final TestResult result, 
                               final StringBuffer failureMessages,
                               final LogWriter log) {
    try {
      suite.run(result);
      return false;
    } catch (JUnitTestTimedOutException ex) {
      failureMessages.append('\n').append(ex.getMessage()).append('\n');
      lastPass = null;
      dumpStacks(3);
      File dir = new File("failures");
      dir.mkdir();

      try {
        File file = new File(dir, "HungDUnitTest.txt");
        file.createNewFile();
        FileWriter fw = new FileWriter(file, true /* append */);
        PrintWriter pw = new PrintWriter(fw, true);
        pw.println("DUnit test: " + ex.getMessage() + " HUNG, killing dunit without running the remaining tests");
        pw.flush();
        pw.close();
      } catch (IOException ignore) {
      }
      log.info("TIMEDOUT " + ex.getMessage());
      //logHangResult(null, ex.getMessage());
      DeadlockDetection.detectDeadlocks(null);
      nukeVms(); // this kills the test, including this VM
      return true;
    }
  }

  private static Pattern pattern =
    Pattern.compile("\\w+\\((.*)\\)");

  /**
   * Returns the name of the given test's class
   */
  protected static String getClassName(Test test) {
    String className;
    String desc = test.toString();
    Matcher matcher = pattern.matcher(desc);
    if (matcher.matches()) {
      className = matcher.group(1);
      
    } else {
      className = desc;
    }

    return className;
  }

  /**
   * Writes a failure message to a file in the "failures" directory
   */
  protected static void reportFailure(Test test, String message) {
    try {
      File dir = new File("failures");
      dir.mkdir();
      String className = getClassName(test);

      File testRes = new File(dir, className + ".txt");
      testRes.createNewFile();
      FileWriter fw = new FileWriter(testRes, true /* append */);
      PrintWriter pw = new PrintWriter(fw, true);
      pw.println(message);
      pw.flush();
      pw.close();

    } catch (IOException ex) {
      String s = "Couldn't log failure in " + test + ": " + message;
      Log.getLogWriter().severe(s, ex);
    }
  }
  private void dumpStacks(int numTimes) {
    dumpStacks();
    for (int i = 1; i < numTimes; i++) {
      try{ Thread.sleep(5000); } catch (InterruptedException ex) {}
      dumpStacks();
    }
  }
  private void dumpStacks() {
    if (HostHelper.isWindows()) {
      ProcessMgr.fgexec("cmd.exe /c dumprun.bat", 600);
    } else {
      ProcessMgr.fgexec("/bin/bash dumprun.sh", 600);
    }
  }
  private void nukeVms() {
    if (HostHelper.isWindows()) {
      ProcessMgr.fgexec("cmd.exe /c nukerun.bat", 600);
    } else {
      ProcessMgr.fgexec("/bin/bash nukerun.sh", 600);
    }
  }
  /**
   * In addition to doing all of the usual logging when Hydra detects
   * that a test has hung, also write a file with a special name in
   * the DUnit "failures" directory.
   *
   * @since 3.5
   */
  public void logHangResult( ClientRecord client, String msg ) {
    try {
      File dir = new File("failures");
      dir.mkdir();

      File file = new File(dir, "HungDUnitTest.txt");
      file.createNewFile();
      FileWriter fw = new FileWriter(file, true /* append */);
      PrintWriter pw = new PrintWriter(fw, true);
      pw.println("DUnit test " + testInProgress + " HUNG");
      pw.flush();
      pw.close();
    
    } catch (IOException ex) {
      String s = "Couldn't log hung DUnit test in " + testInProgress +
        ": " + testInProgress;
      Log.getLogWriter().severe(s, ex);
    }

    super.logHangResult(client, msg);
  }

  /**
   * Returns an immutable <code>Set</code> of the names of
   * <code>TestCase</code> classes to be run by this task.
   */
  public Set getTestClasses() {
    return Collections.unmodifiableSet(this.tests.keySet());
  }

//  /**
//   * Returns whether or not Ant classes are available
//   */
//  private boolean isAntAvailable() {
//    try {
//      String s = JUNIT_ANT + "JUnitTestRunner";
//      Class.forName(s);
//      return true;
//
//    } catch (ClassNotFoundException ex) {
//      return false;
//    }
//  }

//  /**
//   * Executes the unit tests using Ant's reporting tools
//   */
//  private JUnitTestTaskResult executeUsingAnt() {
//    long beginTime = System.currentTimeMillis();
//
//    StringBuffer messages = new StringBuffer();
//
//    Iterator classes = this.getTestClasses().iterator();
//    while (classes.hasNext()) {
//      String className = (String) classes.next();
//
//      JUnitTest t = new JUnitTest(className);
//      boolean haltOnError = false;
//      boolean haltOnFailure = false;
//      boolean filtertrace = true;
//      boolean showoutput = true;
//
//      JUnitTestRunner runner =
//        new JUnitTestRunner(t, haltOnError, filtertrace,
//                            haltOnFailure, showoutput);
//
//      ConfigHashtable tab = TestConfig.tab();
//      if ( tab.booleanAt( JUnitTestTaskPrms.useJUnitFormatter, true ) ) {
//        runner.addFormatter((JUnitResultFormatter) createFormatter(className));
//      }
//
//      runner.run();
//      if (runner.getRetCode() != 0) {
//        String s = "TEST " + className + " FAILED";
//        ERR.println(s);
//        messages.append(s);
//        messages.append('\n');
//      }
//    }
//
//    long elapsedTime = System.currentTimeMillis() - beginTime;
//    return new JUnitTestTaskResult(messages.toString(), elapsedTime);
//  }

//  /**
//   * Uses reflection to create a JUnit formatter for the given class
//   * name.  The return type of this method has to be Object because
//   * Hydra serializes this class.
//   */
//  private Object createFormatter(String testClassName) {
//    FormatterElement fe = new FormatterElement();
//    String formatterClassName = JUNIT_ANT +
//      "PlainJUnitResultFormatter"; 
//    fe.setClassname(formatterClassName);
//    fe.setUseFile(true);
//
//    try {
//      fe.setOutput(new FileOutputStream(testClassName + ".txt"));
//
//      Class c = FormatterElement.class;
//      Method m =
//        c.getDeclaredMethod("createFormatter", new Class[0]);
//      m.setAccessible(true);
//      return (JUnitResultFormatter) m.invoke(fe, new Object[0]);
//
//    } catch (Exception ex) {
//      String s = "Why couldn't we invoke createFormatter() for " +
//        testClassName + "?";
//      throw new InternalGemFireException(s, ex);
//    }
//  }

  ////////////////////////  Inner Classes  ////////////////////////

  /**
   * This inner class is a <code>TestTaskResult</code> that is
   * returned from invoking a bunch of JUnit <code>TestCase</code>s. 
   */
  static class JUnitTestTaskResult extends TestTaskResult {

    JUnitTestTaskResult(String messages, long elapsedTime) {
      super(elapsedTime);

      if (messages.trim().length() == 0) {
        this.errorStatus = false;
        this.result = "Successfully ran JUnit tests";

      } else {

        this.errorStatus = true;
        this.result = "Unsuccessfully ran JUnit tests";
        this.errorString = messages.toString();
      }
    }
  }

  /**
   * Refactored GemFireResultFormatter from anonymous inner class inside 
   * {@link #execute}. This formatter is a <code>TestListener</code> that 
   * writes out dunit-progress.txt and dunit-passed.txt.
   * 
   * @author Kirk Lund
   */
  class GemFireResultFormatter implements TestListener {
    
    private final LogWriter log;
    private final LogWriter passed;
    private final StringBuffer errorMessages;
    private final StringBuffer failureMessages;
    private long startTime;
    
    /**
     * Constructs the standard gemfire result formatter for this test task
     * 
     * @param log the LogWriter for writing to dunit-progress.txt
     * @param passed the LogWriter for writing to dunit-passed.txt
     * @param errorMessages the StringBuffer to append error messages to
     * @param failureMessages the StringBuffer to append failure messages to
     */
    GemFireResultFormatter(final LogWriter log,
                           final LogWriter passed,
                           final StringBuffer errorMessages,
                           final StringBuffer failureMessages) {
      this.log = log;
      this.passed = passed;
      this.errorMessages = errorMessages;
      this.failureMessages = failureMessages;
    }

    public void startTest(Test test) {
      this.startTime = System.currentTimeMillis();
      String s = "START " + test;
      Log.getLogWriter().info(s);
      log.info(s);
      testInProgress = test.toString();
    }

    public void endTest(Test test) {
      long delta = System.currentTimeMillis() - this.startTime;
      String s = "END " + test + " (took " + delta + "ms)";
      Log.getLogWriter().info(s);
      log.info(s);

      testInProgress = null;

      String className = getClassName(test);
      if (className.equals(lastFail)) {
        // Test failed, do nothing
        return;

      } else if (lastPass == null) {
        // The first test to pass
        lastPass = className;
        
      } else if (!lastPass.equals(className)) {
        // Note that the previous test passed
        passed.info("TEST " + lastPass + " PASSED");
        lastPass = className;
      }
    }

    public void addError(Test test, Throwable t) {
      StringBuffer sb = new StringBuffer();
      sb.append(test.toString());
      sb.append("\n");
      StringWriter sw = new StringWriter();
      t.printStackTrace(new PrintWriter(sw, true));
      sb.append(sw.toString());
      errorMessages.append(sb);

      log.severe("\nTEST " + test + " ERROR", t);
      Log.getLogWriter().severe("ERROR IN " + test, t);
      reportFailure(test, sb.toString());
      lastFail = getClassName(test);
    }

    public void addFailure(Test test, AssertionFailedError t) {
      StringBuffer sb = new StringBuffer();
      sb.append(test.toString());
      sb.append("\n");
      StringWriter sw = new StringWriter();
      t.printStackTrace(new PrintWriter(sw, true));
      sb.append(sw.toString());
      failureMessages.append(sb);

      log.severe("\nTEST " + test + " FAILURE", t);
      Log.getLogWriter().severe("FAILURE IN " + test, t);
      reportFailure(test, sb.toString());
      lastFail = getClassName(test);
    }
  }
  
  private boolean hasCaseMethod(final Class theClass) {
    boolean flag = false;
    Method[] methods = theClass.getMethods();
    for (int i= 0; i < methods.length; i++) {
      if ( methods[i].getName().equals("caseSetUp") 
         || methods[i].getName().equals("caseTearDown") ) {
        int mods = methods[i].getModifiers();
        Class returnType = methods[i].getReturnType();
        Class[] args = methods[i].getParameterTypes();
        if ( Modifier.isPublic(mods)
          && Modifier.isStatic(mods) 
          && args.length == 0 ) {
          flag = true;
        } else {
          Log.getLogWriter().severe("method \"" + methods[i].toString() 
            + "\", junit expects public static void  caseSetUp|caseTearDown()");
        }
      }
    }
    return flag;
  }
}
