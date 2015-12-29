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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Pattern;

public class JUnitOutParser {
  static final String BEGIN_ERR_PATTERN1 = "^There was 1 error:$";
  static final String BEGIN_ERR_PATTERN2 = "^There were [0-9]* errors:$";
  static final String BEGIN_FAIL_PATTERN1 = "^There was 1 failure:$";
  static final String BEGIN_FAIL_PATTERN2 = "^There were [0-9]* failures:$";
  static final String TEST_ERRFAIL_PATTERN = "^[0-9]*\\) .+?\\(.*\\).*"; // ##) testName(className)
  static final String TEST_ERRFAIL_DECORATOR_PATTERN = "^[0-9]*\\) \\S+Exception:.+"; // ##) testClasssome.Exception:
  static final String TEST_ERRFAIL_PATTERN2 = "^[0-9]*\\) .+"; // ##) testName(className)
  static final String SUMMARY_PATTERN = "^Tests run: [0-9]+,  Failures: [0-9]+,  Errors: [0-9]+";
  static final String OK_PATTERN = "^OK \\([0-9]+ tests\\)";
  static final String FAILURES_PATTERN = "^FAILURES.*";
  static final String TRACEBACK_AT_LINE_PATTERN = "\\sat .*";
  static final String TRACEBACK_CAUSED_BY_PATTERN = "^Caused by: .*";
  static final String SVN_CHANGENUM_TOKEN = "svnChangeNum is: ";
  static final String SVN_CHANGENUM_PATTERN = "^"+SVN_CHANGENUM_TOKEN+".*";
  static final String EOL = System.getProperty("line.separator");
  static final String UNIX_EOL = "\n";
  private File junitOutputF;

  public JUnitOutParser(String junitOutFilePath) throws IOException {
    junitOutputF = new File(junitOutFilePath);
  }

  private JUnitResult parseErrLine(String inpLine, boolean inFailureContext) {
    int beginTestcaseName = inpLine.indexOf(")") + 2;
    int endTestcaseName = inpLine.indexOf('(', beginTestcaseName);
    int endClassName = inpLine.indexOf(')', endTestcaseName);
    String testcaseName = inpLine.substring(beginTestcaseName, endTestcaseName);
    String testClassName = inpLine.substring(endTestcaseName + 1, endClassName);
    String failureSummary = inpLine.substring(endClassName + 1);

    String scopedTestCaseName;
    if (testClassName.lastIndexOf(".") > 0) {
      String classLast = testClassName.substring(testClassName.lastIndexOf(".") + 1);
      scopedTestCaseName = classLast + "." + testcaseName;
    } else {
      scopedTestCaseName = testcaseName;
    }
    JUnitResult juf = new JUnitResult(testcaseName, scopedTestCaseName, testClassName,
        failureSummary,new StringBuffer(), inFailureContext, grabNumber(inpLine),
        junitOutputF);
    return juf;
  }

  private JUnitResult parseSetupErrLine(String inpLine, boolean inFailureContext) {
    String setupLineTokens[] = inpLine.split(" ",3);
    String testClassName = "";
    if (setupLineTokens[1].indexOf(":") >= 0) {
      testClassName = setupLineTokens[1].substring(0, setupLineTokens[1].indexOf(":"));
    }
    String testcaseName = setupLineTokens[1];
    String failureSummary = setupLineTokens[2];
    JUnitResult juf = new JUnitResult(testcaseName, testcaseName, testClassName,
        failureSummary,new StringBuffer(), inFailureContext, grabNumber(inpLine),
        junitOutputF);
    return juf;
  }

  //JUnit output takes a different form when test setup fails
  // in the decorator code before it ever enters the test class:
  //  errorNumber) UnscopedClassNamejava.something.Exception: exception message
  //  Warning: hacky code coming...
  private JUnitResult parseDecoratorErrLine(String inpLine, boolean inFailureContext) {
    String testcaseName = "decoratorSetup";
    int beginClassName = inpLine.indexOf(')')+2;
    int endClassName = inpLine.indexOf("java.");  // IMPROVE_ME: *USUALLY* works
    String testClassName = inpLine.substring(beginClassName, endClassName);
    int endOfExceptionType = inpLine.indexOf("Exception:")+"Exception:".length();
    String failureSummary = inpLine.substring(endOfExceptionType + 1);
    JUnitResult juf = new JUnitResult(testcaseName, testClassName+"."+testcaseName, testClassName,
        failureSummary,new StringBuffer(), inFailureContext, grabNumber(inpLine),
        junitOutputF);
    return juf;
  }

  /*
   * returns the integer value of a string, stopping at the first invalid character
   */
  public int grabNumber(String s) {
    //s.getChars(srcBegin, srcEnd, dst, dstBegin)
    String sCut = s.replaceFirst("[^0-9].*", "");
    return Integer.parseInt(sCut);

  }
  public JUnitResultData getErrorsAndFailures() throws FileNotFoundException, IOException {
    JUnitResultData retData = new JUnitResultData();
    retData.setJunitResultfile(junitOutputF);
    ArrayList<JUnitResult> retList = new ArrayList<JUnitResult>();
    BufferedReader br = new BufferedReader(new FileReader(junitOutputF));
    // Read until we find the beginning of test failures or errors
    String inpLine = null;
    boolean haveErrorToken = false;
    boolean haveFailToken = false;
    String svnChangeNumber = null;
    JUnitResult activeJUnitFailure = null;
    //StringBuffer activeTraceback = null;
    while ((inpLine = br.readLine()) != null) {
      // Ignore lines until we get the error introduction, the failure introduction, or the summary line
      if (Pattern.matches(SVN_CHANGENUM_PATTERN, inpLine) ) {
        svnChangeNumber = inpLine.substring(SVN_CHANGENUM_TOKEN.length());
        retData.setSvnChangeNumber(svnChangeNumber);
        continue;
      }
      if (Pattern.matches(BEGIN_ERR_PATTERN1, inpLine) || Pattern.matches(BEGIN_ERR_PATTERN2, inpLine)) {
        haveErrorToken = true;
        haveFailToken = false;
        continue;
      }
      if (Pattern.matches(BEGIN_FAIL_PATTERN1, inpLine) || Pattern.matches(BEGIN_FAIL_PATTERN2, inpLine)) {
        haveFailToken = true;
        haveErrorToken = false;
        if (activeJUnitFailure != null) {
          // Store the traceback from the last test failure
          retList.add(activeJUnitFailure);
          activeJUnitFailure = null;
        }
        continue;
      }
      if (Pattern.matches(FAILURES_PATTERN, inpLine)) {
        continue;
      }
      if (Pattern.matches(OK_PATTERN, inpLine)) {
        String[] elements = inpLine.split("\\(");
        retData.setRunCount(grabNumber(elements[1]));
        retData.setFailCount(0);
        retData.setErrorCount(0);
        continue;
      }
      if (Pattern.matches(SUMMARY_PATTERN, inpLine)) {
        String[] elements = inpLine.split(": ");
        if (elements.length != 4) {
          System.err.println("LOGIC_ERROR: junit output line " + inpLine + " doesn't have 4 colon-spaces?");
        } else {
          retData.setRunCount(grabNumber(elements[1]));
          retData.setFailCount(grabNumber(elements[2]));
          retData.setErrorCount(grabNumber(elements[3]));
        }
        continue;
      }
      // Once we're found "There were # failures/errors", we can
      // expect details on the errors and failures.
      if (haveErrorToken || haveFailToken) {
        if (inpLine.trim().length() == 0) {
          // empty line terminates traceback (and junit failure definition)
          if (activeJUnitFailure != null) {
            // Store the traceback from the last test failure
            activeJUnitFailure.setFailed(true);
            retList.add(activeJUnitFailure);
          }
          activeJUnitFailure = null;
        } else if (Pattern.matches(TEST_ERRFAIL_PATTERN, inpLine)) {
          // This line introduces a test failure
          // Format is digits) testcaseName(className)errMessage
          if (activeJUnitFailure != null) {
            activeJUnitFailure.setFailed(true);
            retList.add(activeJUnitFailure);
          }
          activeJUnitFailure = parseErrLine(inpLine, haveFailToken);
        } else if (Pattern.matches(TEST_ERRFAIL_DECORATOR_PATTERN, inpLine)) {
          // This line introduces a test decorator failure
          // Format is digits) unscopedClassNamesome.Exception: errMessage
          if (activeJUnitFailure != null) {
            activeJUnitFailure.setFailed(true);
            retList.add(activeJUnitFailure);
          }
          activeJUnitFailure = parseDecoratorErrLine(inpLine, haveFailToken);
        } else if (Pattern.matches(TEST_ERRFAIL_PATTERN2, inpLine)) {
          // This line introduces a test setup failure
          // Format is digits) className errMessage
          if (activeJUnitFailure != null) {
            retList.add(activeJUnitFailure);
          }
          activeJUnitFailure = parseSetupErrLine(inpLine, haveFailToken);
          activeJUnitFailure.setFailed(true);
        } else {
          // This must be a line of traceback info
          activeJUnitFailure.getTraceback().append(inpLine.trim() + UNIX_EOL);
        }
      }

    }
    retData.setFailures(retList);
    return retData;
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    if (args.length != 1) {
      System.err.println("Please provide the name of the junit output file");
      System.exit(1);
    }
    try {
      JUnitOutParser parser = new JUnitOutParser(args[0]);
      JUnitResultData outData = parser.getErrorsAndFailures();
      ArrayList<JUnitResult> errfailList = outData.getFailures();
      for (JUnitResult errfail : errfailList) {
        System.out.println("" + errfail.getScopedTestcaseName() + System.getProperty("line.separator")
            + errfail.getTraceback());
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
    System.out.println("All done, exiting...");

  }

}
