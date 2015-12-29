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

package perffmwk;

import com.gemstone.gemfire.LogWriter;
import hydra.*;
import java.io.*;
import java.util.*;
import util.TestHelper;

/**
 * Generates performance comparisons for the specified test container
 * directories.
 * <p>
 * When only one test container directory is provided, the tests in that
 * directory are compared to each other in lexicographic order of the result
 * directory names.
 * <p>
 * When there is more than one test container directory, they are compared in
 * the order given in the command-line arguments, with duplicates removed.
 * Only tests with the same ID are compared with each other, where the test ID
 * consists of the hydra test configuration file name and properties.  The first
 * available run of a particular test is considered the baseline.  Each test
 * container directory must contain at most one test run with a given test ID.
 * <p>
 * Comparisons are made using values read from the statistic archives for each
 * test, as described in {@link perffmwk.PerfReporter}.  By default, the
 * statistics specification file used for a given test ID is the last run of the
 * test in the last container that includes it.  The <code>statSpecFile</code>
 * system property can be used to override with a different specification file.
 * <p>
 * Tests can be compared in "raw" or "ratio" <code>mode</code>.  Raw mode
 * simply reports statistics values.  Ratio mode indicates the percent change in
 * value, with a sign indicating whether the change was "good" (positive) or
 * "bad" (negative).  Use <code>ratioThreshold</code> to skip reporting values
 * under the threshold.
 * <p>
 * Set <code>omitFailedTests</code> true to omit reporting values for tests
 * that hung or failed.
 * <p>
 * Set <code>markFailedTests</code> true to mark tests that hung or failed with
 * "xxx". This overrides the setting for omitFailedTests to mark the failures
 * but does not report their statistics values.
 * <p>
 * Set <code>compareByKey</code> true to compare tests with the same value of
 * {@link PerfSorter#COMPARISON_KEY_PROP}.
 * <p>
 * Set <code>addTestKey</code> true to include a key of test descriptions in
 * the report.
 * <p>
 * Set <code>addConfigDiffs</code> true to report differences in hydra test
 * configuration files.
 * <p>
 * The comparison report is written by default to "perfcomparison.txt" in the
 * working directory.  The <code>compReportFile</code> system property can be
 * used to override with a different file.  Set <code>compReportFile</code> to
 * "none" to write the report to <code>stdout</code>.
 * <p>
 * Set <code>generateCSVFile</code> true to generate a CSV file suitable for
 * Excel.  When there is a single test container directory, a row is created
 * for each test.  A column is generated for each property found in a test
 * property file, and for each statistic value.  When there are multiple test
 * container directories, the rows are identical to the non-CSV report, and the
 * report banner and various keys are excluded.  The CSV file is written by
 * default to "perfcomparison.csv" in the working directory.  The <code>csvFile
 * </code> system property can be used to override with a different file.
 * <p>
 * Log messages for the tool are appended to "perfcomparer.log" in the working
 * directory.  Use <code>logLevel</code> to change the verbosity of the log.
 * <p>
 * Usage:
 * <blockquote><pre>
 *   java -Dgemfire.home=&lt;path_to_gemfire_product_tree&gt;
 *        -DJTESTS=&lt;path_to_test_classes&gt;
 *        [-DcompReportFile=&lt;report_filename(default:$pwd/perfcomparison.txt)&gt;]
 *        [-DlogLevel=&lt;level(default:info)&gt;]
 *        [-Dmode=&lt;comparison_mode(default:ratio)&gt;]
 *        [-Dratio.threshold=&lt;ratio_threshold(default:0.05)&gt;]
 *        [-DomitFailedTests=&lt;whether_to_omit_failed_tests(default:false)&gt;]
 *        [-DmarkFailedTests=&lt;whether_to_mark_failed_tests(default:false)&gt;]
 *        [-DcompareByKey=&lt;whether_to_compare_tests_by_key(default:false)&gt;]
 *        [-DaddTestKey=&lt;whether_to_include_test_key(default:false)&gt;]
 *        [-DaddConfigDiffs=&lt;whether_to_include_config_diffs(default:false)&gt;]
 *        [-DstatSpecFile=&lt;stat_spec_filename&gt;]
 *        [-DgenerateCSVFile=&lt;whether_to_generate_csv_file(default:false)&gt;]
 *        [-DcsvFile=&lt;csv_filename(default:$pwd/perfcomparison.csv)&gt;]
 *        perffmwk.PerfComparer
 *        &lt;two_or_more_comparable_test_directories&gt;
 * </pre></blockquote>
 * <p>
 * Example:
 * <blockquote><pre>
 *    java -classpath $JTESTS:$GEMFIRE/lib/gemfire.jar
 *         -Dgemfire.home=$GEMFIRE -DJTESTS=$JTESTS
 *         perffmwk.PerfComparer test-081303-* test-081403-*
 * </pre></blockquote>
 * Example:
 * <blockquote><pre>
 *    java -classpath $JTESTS:$GEMFIRE/lib/gemfire.jar
 *         -Dgemfire.home=$GEMFIRE -DJTESTS=$JTESTS
 *         perffmwk.PerfComparer testsOnBuildA testsOnBuildB
 * </pre></blockquote>
 */
public class PerfComparer extends Formatter
                                   implements ComparisonConstants {

  // Name of the output file for the comparison report.
  private static final String COMP_REPORT_FILE_PROP = "compReportFile";
  private static String CompReportFile;

  // Log level used by this tool.
  private static final String LOG_LEVEL_PROP = "logLevel";
  private static LogWriter log;

  // Mode in which stat values will be reported (RATIO_MODE or RAW_MODE).
  private static final String MODE_PROP = "mode";
  private static String Mode;
  private static final String RATIO_MODE = "ratio";
  private static final String RAW_MODE   = "raw";

  // Threshold over which ratio values will be reported.
  private static final String RATIO_THRESHOLD_PROP = "ratio.threshold";
  protected static double RatioThreshold;

  // Whether to report values for on tests that failed.
  private static final String OMIT_FAILED_TESTS_PROP = "omitFailedTests";
  private static boolean OmitFailedTests;

  // Whether to report values for on tests that failed.
  private static final String MARK_FAILED_TESTS_PROP = "markFailedTests";
  private static boolean MarkFailedTests;

  // Whether to compare tests by key.
  private static final String COMPARE_BY_KEY_PROP = "compareByKey";
  private static boolean CompareByKey;

  // Whether to include test key in the report.
  private static final String ADD_TEST_KEY_PROP = "addTestKey";
  private static boolean AddTestKey;

  // Whether to include test configuration difference in the report.
  private static final String ADD_CONFIG_DIFFS_PROP = "addConfigDiffs";
  private static boolean AddConfigDiffs;

  // Name of the statspec file to use instead of the ones in the test dirs.
  private static final String STAT_SPEC_FILE_PROP = "statSpecFile";
  private static String StatSpecFile;

  // Whether to generate a CSV file.
  private static final String GENERATE_CSV_FILE_PROP = "generateCSVFile";
  private static boolean GenerateCSVFile;

  // Name of the output file for the CSV data.
  private static final String CSV_FILE_PROP = "csvFile";
  private static String CSVFile;

  // The test container directories given as arguments, in the order given.
  private static List TestContainerDirs;

//------------------------------------------------------------------------------
// Configuration
//------------------------------------------------------------------------------

  /**
   * Reads the tool configuration from system properties and command-line args.
   */
  private static boolean readParameters(String[] args) {

    StringBuffer configuration = new StringBuffer();

    // compReportFile
    {
      String fileName =
        System.getProperty(COMP_REPORT_FILE_PROP, "perfcomparison.txt");
      if (fileName.equals("none")) {
        CompReportFile = null;
      } else {
        CompReportFile = FileUtil.absoluteFilenameFor(fileName);
      }
      configuration.append("\ncompReportFile = " + CompReportFile);
    }

    // logLevel
    String logLevel = System.getProperty(LOG_LEVEL_PROP, "info");
    configuration.append("\nlogLevel = " + logLevel);

    // open the log file, append if it already exists
    if (CompReportFile == null) {
      log = Log.createLogWriter("perfcomparer", logLevel);
    } else {
      log = Log.createLogWriter("perfcomparer", "perfcomparer", logLevel, true);
    }
    log.info("PerfComparer PID is " + ProcessMgr.getProcessId());

    // JTESTS (required)
    String jtests = System.getProperty("JTESTS");
    if (jtests == null) {
      usage("Missing JTESTS");
      return false;
    }
    configuration.append("\nJTESTS = " + jtests);

    // gemfire.home (required)
    String gemfire = System.getProperty("gemfire.home");
    if (gemfire == null) {
      usage("Missing gemfire.home");
      return false;
    }
    configuration.append("\ngemfire.home = " + gemfire);

    // mode
    Mode = System.getProperty(MODE_PROP, RATIO_MODE);
    if (!(Mode.equals(RATIO_MODE) || Mode.equals(RAW_MODE))) {
      usage("Illegal mode: " + Mode);
      return false;
    }
    configuration.append("\nmode = " + Mode);

    // ratio threshold
    try {
      String ratioThreshold = System.getProperty(RATIO_THRESHOLD_PROP, ".05");
      RatioThreshold = Double.parseDouble(ratioThreshold);
      if (RatioThreshold < 0) {
        usage("Illegal ratio.threshold: " + RatioThreshold);
        return false;
      }
    } catch(NumberFormatException e) {
      usage("Illegal ratio.threshold: " + RatioThreshold);
      return false;
    }
    configuration.append("\nratio.threshold = " + RatioThreshold);
    RatioThreshold += 1.0;

    // omit failed tests
    OmitFailedTests = Boolean.getBoolean(OMIT_FAILED_TESTS_PROP);
    MarkFailedTests = Boolean.getBoolean(MARK_FAILED_TESTS_PROP);
    if (MarkFailedTests) OmitFailedTests = false;
    configuration.append("\nomitFailedTests = " + OmitFailedTests);
    configuration.append("\nmarkFailedTests = " + MarkFailedTests);

    // compare by key
    CompareByKey = Boolean.getBoolean(COMPARE_BY_KEY_PROP);
    configuration.append("\ncompareByKey = " + CompareByKey);

    // include test key 
    AddTestKey = Boolean.getBoolean(ADD_TEST_KEY_PROP);
    configuration.append("\naddTestKey = " + AddTestKey);

    // skip configuration differences 
    AddConfigDiffs = Boolean.getBoolean(ADD_CONFIG_DIFFS_PROP);
    configuration.append("\naddConfigDiffs = " + AddConfigDiffs);

    // statSpecFile
    StatSpecFile = System.getProperty(STAT_SPEC_FILE_PROP);
    if (StatSpecFile != null) {
      StatSpecFile = FileUtil.absoluteFilenameFor(StatSpecFile);
      if (!FileUtil.exists(StatSpecFile)) {
        usage("File not found: " + StatSpecFile);
        return false;
      }
    }
    configuration.append("\nstatSpecFile = " + StatSpecFile);

    // generate csv file
    GenerateCSVFile = Boolean.getBoolean(GENERATE_CSV_FILE_PROP);
    configuration.append("\ngenerateCSVFile = " + GenerateCSVFile);

    // csvFile
    {
      String fileName =
        System.getProperty(CSV_FILE_PROP, "perfcomparison.csv");
      CSVFile = FileUtil.absoluteFilenameFor(fileName);
      configuration.append("\ncvsFile = " + CSVFile);
    }

    // test container dirs, in order given, without duplicates
    if (args.length == 0) {
      usage("No directories specified");
      return false;
    } else {
      TestContainerDirs = new ArrayList();
      for (int i = 0; i < args.length; i++) {
        String testContainerDir = FileUtil.absoluteFilenameFor(args[i]);
        if (!TestContainerDirs.contains(testContainerDir)) {
          TestContainerDirs.add(testContainerDir);
        }
      }
    }
    configuration.append("\ndirectories = " + TestContainerDirs);

    // log the tool configuration
    log.info(configuration.toString());

    return true;
  }

  /**
   * Prints usage information about this program.
   */
  private static void usage(String s) {
    StringBuffer buf = new StringBuffer();
    buf.append("\n** " + s);
    buf.append("\nUsage: java");
    buf.append(" -Dgemfire.home=<path_to_gemfire_product_tree>");
    buf.append(" -DJTESTS=<path_to_test_classes>");
    buf.append(" [optional_properties]");
    buf.append(" perffmwk.PerfComparer <list_of_directories>");
    buf.append("\nwhere optional properties include:");
    buf.append("\n-D" + COMP_REPORT_FILE_PROP
                      + "=<report_filename(default:$pwd/perfcomparison.txt)>");
    buf.append("\n-D" + LOG_LEVEL_PROP
                      + "=<log_level(default:info)>");
    buf.append("\n-D" + MODE_PROP
                      + "=<comparison_mode(default:ratio)>");
    buf.append("\n-D" + RATIO_THRESHOLD_PROP
                      + "=<ratio_threshold(default:0.05)>");
    buf.append("\n-D" + OMIT_FAILED_TESTS_PROP
                      + "=<whether_to_omit_failed_tests(default:false)>");
    buf.append("\n-D" + MARK_FAILED_TESTS_PROP
                      + "=<whether_to_mark_failed_tests(default:false)>");
    buf.append("\n-D" + COMPARE_BY_KEY_PROP
                      + "=<whether_to_compare_tests_by_key(default:false)>");
    buf.append("\n-D" + ADD_TEST_KEY_PROP
                      + "=<whether_to_include_test_key(default:false)>");
    buf.append("\n-D" + ADD_CONFIG_DIFFS_PROP
                      + "=<whether_to_include_config_diffs(default:false)>");
    buf.append("\n-D" + STAT_SPEC_FILE_PROP
                      + "=<stat_spec_filename>");
    buf.append("\n-D" + GENERATE_CSV_FILE_PROP
                      + "=<whether_to_generate_csv_file(default:false)>");
    buf.append("\n-D" + CSV_FILE_PROP
                      + "=<csv_filename(default:$pwd/perfcomparison.csv)>");
    System.out.println( buf.toString());
  }

//------------------------------------------------------------------------------
// Formatting and writing
//------------------------------------------------------------------------------

  /**
   * Writes the performance comparison report to the output file.
   */
  private static void writeReport(String report, String filename) {
    if (filename == null) {
      new PrintStream(new FileOutputStream(FileDescriptor.out)).println(report);
    } else {
      FileUtil.writeToFile(filename, report);
    }
  }

  /**
   * Generates a performance comparison report for the given tests, test
   * containers, test comparisons, and test configuration differences.
   */
  private static String formatValues(List testIds,
                                     List testDescriptions,
                                     List testContainers,
                                     List testComparisons,
                                     List testConfigDiffs) {
    log.info("Formatting summary...");
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter( sw, true );

    // print header info
    pw.println( DIVIDER );
    center("Comparison Report", pw);
    center(((new Date()).toString()), pw);

    // gather the columns together for all test comparisons in the list

    // initialize the columns
    List testIdColumn = new ArrayList();
    List statSpecColumn = new ArrayList();
    List opColumn = new ArrayList();
    List[] valColumns;
    if (testContainers.size() == 1) { // special case, one column per test
      TestContainer testContainer = (TestContainer)testContainers.get(0);
      valColumns = new ArrayList[testContainer.getTests().size()];
    } else { // one column per container
      valColumns = new ArrayList[testContainers.size()];
    }
    for (int i = 0; i < valColumns.length; i++) {
      valColumns[i] = new ArrayList();
    }

    for (int i = 0; i < testComparisons.size(); i++) {
      TestComparison testComparison = (TestComparison)testComparisons.get(i);
      String testId = (String)testIds.get(i);
      if (testContainers.size() != 1) { // omit in special case
        List valColumn = testComparison.getValColumns()[0];
        for (int j = 0; j < valColumn.size(); j++) {
          if (j == 0) {
            if (CompareByKey) {
              testIdColumn.add(testId);
            } else {
              testIdColumn.add(String.valueOf(i));
            }
          } else {
            testIdColumn.add("");
          }
        }
      }
      statSpecColumn.addAll(testComparison.getStatSpecColumn());
      opColumn.addAll(testComparison.getOpColumn());
      List[] tmp = testComparison.getValColumns();
      for (int j = 0; j < testComparison.getValColumns().length; j++) {
        valColumns[j].addAll(tmp[j]);
      }
    }

    // compute and add the averages
    if (testContainers.size() != 1) { // omit in special case
      if (Mode.equals(RATIO_MODE)) {
        testIdColumn.add("");
        statSpecColumn.add("");
        opColumn.add("AVG");
        for (int i = 0; i < valColumns.length; i++) {
          valColumns[i].add(new Double(average(valColumns[i], colnum(i))));
        }
      }
    }

    // add headers, then format and pad the data
    String numFormat = "0.00";
    List columns = new ArrayList();
    if (testContainers.size() != 1) { // omit in special case
      columns.add(padRight(addTitle("TEST", testIdColumn)));
    }
    columns.add(padRight(addTitle("STATSPEC", statSpecColumn)));
    columns.add(padRight(addTitle("OP", opColumn)));
    for (int i = 0; i < valColumns.length; i++) {
      columns.add(padLeft(addTitle(colnum(i), valColumns[i]), numFormat));
    }

    // print the rows
    pw.println(DIVIDER);
    for (int i = 0; i < ((List)columns.get(0)).size(); i++) {
      String row = "";
      for (int j = 0; j < columns.size(); j++) {
        String s = ((List)columns.get(j)).get(i) + " ";
        row += s;
      }
      pw.println(row);
    }
    if (MarkFailedTests) {
      pw.println("\nNOTE: Reported values include only passing tests but failures are marked.");
    } else if (OmitFailedTests) {
      pw.println("\nNOTE: Reported values include only passing tests.");
    } else {
      pw.println("\nNOTE: Reported values possibly include failed or hung tests.");
    }

    // print statistic value key
    pw.println(DIVIDER);
    center("Statistic Value Key", pw);
    pw.println("    " + MISSING_VAL + " = " + MISSING_VAL_DOC);
    pw.println(" " + NIL_VAL + " = " + NIL_VAL_DOC);
    pw.println(PLUS_INFINITY + " = " + PLUS_INFINITY_DOC);
    pw.println(MINUS_INFINITY + " = " + MINUS_INFINITY_DOC);
    pw.println(" " + ERR_VAL + " = " + ERR_VAL_DOC);
    if (MarkFailedTests) {
      pw.println(" " + FAIL_VAL + " = " + FAIL_VAL_DOC);
    }

    // append the test config diffs to the report
    if (testConfigDiffs != null) {
      String formattedDiffs = formatTestConfigDiffs(testConfigDiffs);
      pw.println(formattedDiffs);
    }

    // print native client build version key
    if (((TestContainer)testContainers.get(0)).isNativeClient()) {
      pw.println(DIVIDER);
      center("Native Client Build Key", pw);
      if (testContainers.size() == 1) { // special case
        TestContainer testContainer = (TestContainer)testContainers.get(0);
        List tests = testContainer.getTests();
        for (int i = 0; i < tests.size(); i++) {
          Test test = (Test)tests.get(i);
          if (test.getStatConfig().isNativeClient()) {
            pw.println(i + ": " + test.getNativeClientBuildVersion());
          } else {
            pw.println(i + ": none");
          }
        }
      } else {
        for (int i = 0; i < testContainers.size(); i++) {
          TestContainer testContainer = (TestContainer)testContainers.get(i);
          if (testContainer.isNativeClient()) {
            pw.println(i + ": " + testContainer.getNativeClientBuildVersion());
          } else {
            pw.println(i + ": none");
          }
        }
      }
    }

    // print native client source version key
    if (((TestContainer)testContainers.get(0)).isNativeClient()) {
      pw.println(DIVIDER);
      center("Native Client Source Key", pw);
      if (testContainers.size() == 1) { // special case
        TestContainer testContainer = (TestContainer)testContainers.get(0);
        List tests = testContainer.getTests();
        for (int i = 0; i < tests.size(); i++) {
          Test test = (Test)tests.get(i);
          if (test.getStatConfig().isNativeClient()) {
            pw.println(i + ": " + test.getNativeClientSourceVersion());
          } else {
            pw.println(i + ": none");
          }
        }
      } else {
        for (int i = 0; i < testContainers.size(); i++) {
          TestContainer testContainer = (TestContainer)testContainers.get(i);
          if (testContainer.isNativeClient()) {
            pw.println(i + ": " + testContainer.getNativeClientSourceVersion());
          } else {
            pw.println(i + ": none");
          }
        }
      }
    }

    // print build version key
    pw.println(DIVIDER);
    center("Java Build Key", pw);
    if (testContainers.size() == 1) { // special case
      TestContainer testContainer = (TestContainer)testContainers.get(0);
      List tests = testContainer.getTests();
      for (int i = 0; i < tests.size(); i++) {
        Test test = (Test)tests.get(i);
        pw.println(i + ": " + test.getBuildVersion());
      }
    } else {
      for (int i = 0; i < testContainers.size(); i++) {
        TestContainer testContainer = (TestContainer)testContainers.get(i);
        pw.println(i + ": " + testContainer.getBuildVersion());
      }
    }

    // print source version key
    pw.println(DIVIDER);
    center("Java Source Key", pw);
    if (testContainers.size() == 1) { // special case
      TestContainer testContainer = (TestContainer)testContainers.get(0);
      List tests = testContainer.getTests();
      for (int i = 0; i < tests.size(); i++) {
        Test test = (Test)tests.get(i);
        pw.println(i + ": " + test.getSourceVersion());
      }
    } else {
      for (int i = 0; i < testContainers.size(); i++) {
        TestContainer testContainer = (TestContainer)testContainers.get(i);
        pw.println(i + ": " + testContainer.getSourceVersion());
      }
    }

    // print JDK version key
    pw.println( DIVIDER );
    center( "JDK Key", pw );
    if (testContainers.size() == 1) { // special case
      TestContainer testContainer = (TestContainer)testContainers.get(0);
      List tests = testContainer.getTests();
      for (int i = 0; i < tests.size(); i++) {
        Test test = (Test)tests.get(i);
        pw.println(i + ": Build JDK " + test.getBuildJDK()
                     + " Runtime JDK " + test.getRuntimeJDK()
                     + " " + test.getJavaVMName());
      }
    } else {
      for (int i = 0; i < testContainers.size(); i++) {
        TestContainer testContainer = (TestContainer)testContainers.get(i);
        pw.println(i + ": Build JDK " + testContainer.getBuildJDK()
                     + " Runtime JDK " + testContainer.getRuntimeJDK()
                     + " " + testContainer.getJavaVMName());
      }
    }

    // print test container directory key
    pw.println( DIVIDER );
    center( "Directory Key", pw );
    if (testContainers.size() == 1) { // special case
      TestContainer testContainer = (TestContainer)testContainers.get(0);
      List tests = testContainer.getTests();
      for (int i = 0; i < tests.size(); i++) {
        Test test = (Test)tests.get(i);
        pw.println(i + ": " + test.getTestDir());
      }
    } else {
      for (int i = 0; i < testContainers.size(); i++) {
        TestContainer testContainer = (TestContainer)testContainers.get(i);
        pw.println(i + ": " + testContainer.getTestContainerDir());
      }
    }

    // print test id key
    if (AddTestKey) {
      pw.println(DIVIDER);
      center("Test Key", pw);
      for (int i = 0; i < testIds.size(); i++) {
        String testId = (String)testIds.get(i);
        String testDescription = (String)testDescriptions.get(i);
        if (CompareByKey) {
          String suffix = ":\n\"" + testDescription + "\"";
          pw.println("\nTEST " + testId + suffix);
        } else {
          String suffix = ":\n" + testId + "\n\"" + testDescription + "\"";
          pw.println("\nTEST " + i + suffix);
        }
      }
    }

    pw.flush();
    return sw.toString();
  }

  /**
   * Generates a CSV performance comparison report for the given tests, test
   * containers, and test comparisons.
   */
  private static String formatCSV(List testIds,
                                  List testDescriptions,
                                  List testContainers,
                                  List testComparisons) {
    if (TestContainerDirs.size() == 1) {
      return formatCSVSingle(testIds,
                             testDescriptions, testContainers, testComparisons);
    } else {
      return formatCSVMulti(testIds,
                            testDescriptions, testContainers, testComparisons);
    }
  }

  /**
   * Generates a CSV performance comparison report for the given tests, test
   * containers (multiple), and test comparisons.
   */
  private static String formatCSVMulti(List testIds,
                                       List testDescriptions,
                                       List testContainers,
                                       List testComparisons) {
    log.info("Formatting CSV...");
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter( sw, true );

    // gather the columns together for all test comparisons in the list

    // initialize the columns
    List testIdColumn = new ArrayList();
    List statSpecColumn = new ArrayList();
    List opColumn = new ArrayList();
    List[] valColumns;
    if (testContainers.size() == 1) { // special case, one column per test
      TestContainer testContainer = (TestContainer)testContainers.get(0);
      valColumns = new ArrayList[testContainer.getTests().size()];
    } else { // one column per container
      valColumns = new ArrayList[testContainers.size()];
    }
    for (int i = 0; i < valColumns.length; i++) {
      valColumns[i] = new ArrayList();
    }

    for (int i = 0; i < testComparisons.size(); i++) {
      TestComparison testComparison = (TestComparison)testComparisons.get(i);
      String testId = (String)testIds.get(i);
      if (testContainers.size() != 1) { // omit in special case
        List valColumn = testComparison.getValColumns()[0];
        for (int j = 0; j < valColumn.size(); j++) {
          if (j == 0) {
            if (CompareByKey) {
              testIdColumn.add(testId);
            } else {
              testIdColumn.add(String.valueOf(i));
            }
          } else {
            testIdColumn.add("");
          }
        }
      }
      statSpecColumn.addAll(testComparison.getStatSpecColumn());
      opColumn.addAll(testComparison.getOpColumn());
      List[] tmp = testComparison.getValColumns();
      for (int j = 0; j < testComparison.getValColumns().length; j++) {
        valColumns[j].addAll(tmp[j]);
      }
    }

    // add headers, then format and pad the data
    String numFormat = "0.00";
    List columns = new ArrayList();
    if (testContainers.size() != 1) { // omit in special case
      columns.add(padRight(addTitle("TEST", testIdColumn)));
    }
    columns.add(padRight(addTitle("STATSPEC", statSpecColumn)));
    columns.add(padRight(addTitle("OP", opColumn)));
    for (int i = 0; i < valColumns.length; i++) {
      columns.add(padLeft(addTitle(colnum(i), valColumns[i]), numFormat));
    }

    // print the rows
    for (int i = 0; i < ((List)columns.get(0)).size(); i++) {
      String row = "";
      for (int j = 0; j < columns.size(); j++) {
        if (j > 0) {
          row += ",";
        }
        String s = ((String)((List)columns.get(j)).get(i)).trim();
        row += s;
      }
      pw.println(row);
    }

    pw.flush();
    return sw.toString();
  }

  /**
   * Generates a CSV performance comparison report for the given tests, test
   * containers (single), and test comparisons.
   */
  private static String formatCSVSingle(List testIds,
                                        List testDescriptions,
                                        List testContainers,
                                        List testComparisons) {
    log.info("Formatting CSV...");

    // compute the column header row
    List headerRow = new ArrayList();
    headerRow.add("TestPath");
    headerRow.add("TestName");
    SortedSet testPropertyKeys = getTestPropertyKeys(testContainers);
    headerRow.addAll(resortHVT(testPropertyKeys));
    SortedSet testStatSpecNames = getTestStatSpecNames(testComparisons);
    headerRow.addAll(testStatSpecNames);
    if (AddTestKey) {
      headerRow.add("TestDescription");
    }

    // gather the rows together for all test comparisons in the list

    // initialize the rows
    TestContainer testContainer = (TestContainer)testContainers.get(0);
    List[] rows = new ArrayList[testContainer.getTests().size()];
    for (int i = 0; i < rows.length; i++) {
      rows[i] = new ArrayList();
    }

    // fill the rows
    for (int i = 0; i < rows.length; i++) {

      // look up the test properties
      String testId = (String)testIds.get(i);
      Test test = (Test)testContainer.getTests().get(i);
      if (!test.getTestId().equals(testId)) {
        String s = "Things aren't lining up.  Expected testId: " + testId
                 + " but got testId: " + test.getTestId();
        throw new HydraInternalException(s);
      }
      Map testProps = test.getTestProperties();

      // fill in the test path and name
      String testFullName = (String)testProps.get("testName");
      String testPath = FileUtil.pathFor(testFullName);
      rows[i].add(testPath);
      String testName = FileUtil.filenameFor(testFullName);
      rows[i].add(testName.substring(0, testName.indexOf(".conf")));

      // fill in the test properties
      for (Iterator it = testPropertyKeys.iterator(); it.hasNext();) {
        String testPropertyKey = (String)it.next();
        String testPropertyVal = (String)testProps.get(testPropertyKey);
        if (testPropertyVal == null) {
          rows[i].add("");
        } else {
          rows[i].add(testPropertyVal);
        }
      }

      // fill in the test values
      TestComparison testComparison = (TestComparison)testComparisons.get(0);
      List statSpecNames = testComparison.getStatSpecColumn();
      List statSpecVals = (List)testComparison.getValColumns()[i];
      for (Iterator it = testStatSpecNames.iterator(); it.hasNext();) {
        String testStatSpecName = (String)it.next();
        int index = statSpecNames.indexOf(testStatSpecName);
        if (index == -1) {
          rows[i].add("");
        } else {
          rows[i].add(statSpecVals.get(index));
        }
      }
      if (AddTestKey) {
        String testDescription = (String)testDescriptions.get(i);
        rows[i].add(testDescription.replace(',', ';'));
      }
    }

    // format the data
    String numFormat = "0.00";
    for (int i = 0; i < rows.length; i++) {
      rows[i] = new ArrayList(formatDecimal(rows[i], numFormat));
    }

    // print the rows
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter( sw, true );
    pw.println(rowToCSVString(headerRow));
    for (int i = 0; i < rows.length; i++) {
      pw.println(rowToCSVString(rows[i]));
    }
    pw.flush();
    return sw.toString();
  }

  /**
   * Resorts the test property keys to put things in HVT order.
   */
  private static List resortHVT(Set testPropertyKeys) {
    List keys = new ArrayList();
    for (Iterator i = testPropertyKeys.iterator(); i.hasNext();) {
      String key = (String)i.next();
      if (key.endsWith("ThreadsPerVM")) {
        if (i.hasNext()) {
          String nextKey = (String)i.next();
          if (nextKey.endsWith("VMsPerHost")) {
            keys.add(nextKey);
            keys.add(key);
          } else {
            keys.add(key);
            keys.add(nextKey);
          }
        } else {
          keys.add(key);
        }
      } else {
        keys.add(key);
      }
    }
    return keys;
  }

  /**
   * Generates a comma-separated CSV string from the given row.
   */
  private static String rowToCSVString(List row) {
    String csv = "";
    for (Iterator it = row.iterator(); it.hasNext();) {
      if (csv.length() == 0) {
        csv += it.next();
      } else {
        csv += "," + it.next();
      }
    }
    return csv;
  }

  /**
   * Returns the formatted string summarizing the test config differences.
   */
  private static String formatTestConfigDiffs(List testConfigDiffs) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw, true);
    pw.println(DIVIDER);
    center("Test Configuration Differences", pw);
    pw.flush();
    int headerSize = sw.toString().length();

    for (int i = 0; i < testConfigDiffs.size(); i++) {
      String diffStr = new String();
      List[] diffs = (List[])testConfigDiffs.get(i);
      for (int j = 0; j < diffs.length; j++) {
        List diff = diffs[j];
        if (diff != null && diff.size() > 0) {
          if (testConfigDiffs.size() == 1) { // special case
            diffStr += "Test ";
          } else {
            diffStr += "     Dir ";
          }
          diffStr += j + ": " + diff.get(0) + "\n";
          for (int k = 1; k < diff.size(); k++) {
            diffStr += "            " + diff.get(k) + "\n";
          }
        }
      }
      if ( diffStr.length() > 0) {
        if (testConfigDiffs.size() != 1) { // omit in special case
          pw.println("Test " + i + ":\n");
        }
        pw.println(diffStr);
      }
    }
    pw.flush();
    String result = sw.toString();
    return result.length() > headerSize ? result : "";
  }

  /**
   * Returns a new list that consists of a title string and the given list.
   */
  private static List addTitle(String title, List l) {
    List tl = new ArrayList();
    tl.add(title);
    tl.addAll(l);
    return tl;
  }

  /**
   * Returns the "display" column number for the given column offset.
   */
  private static String colnum(int i) {
    return "#" + i ;
  }

  /**
   * Returns the average of a list of values.
   */
  private static double average(List vals, String colnum) {
    double total = 0;
    int count = 0;
    boolean omissions = false;
    for (int i = 0; i < vals.size(); i++) {
      Object o = vals.get(i);
      if (o instanceof Double) {
        total += ((Double)o).doubleValue();
        ++count;
      } else {
        omissions = true;
      }
    }
    if (omissions) {
      log.fine("Omitting non-numerical values from average for column "
              + colnum);
    }
    return (count == 0) ? 0 : total/count;
  }

  /**
   * Returns a sorted set of all test property keys in the test containers.
   * Filters out unwanted keys.
   */
  private static SortedSet getTestPropertyKeys(List testContainers) {
    SortedSet allKeys = new TreeSet();
    for (Iterator i = testContainers.iterator(); i.hasNext();) {
      TestContainer testContainer = (TestContainer)i.next();
      Set keys = testContainer.getTestPropertyKeys();
      for (Iterator j = keys.iterator(); j.hasNext();) {
        String key = (String)j.next();
        if (
            !key.equals("testName")
            &&
            !key.equals("perffmwk.comparisonKey")
            &&
            !key.equals("onlyOnPlatforms")
            &&
            !key.equals("trimSeconds")
            &&
            !key.equals("workSeconds")
        ) {
          allKeys.add(key);
        }
      }
    }
    return allKeys;
  }

  /**
   * Returns a sorted set of all stat spec names in the test comparisons.
   */
  private static SortedSet getTestStatSpecNames(List testComparisons) {
    SortedSet allNames = new TreeSet();
    for (Iterator i = testComparisons.iterator(); i.hasNext();) {
      TestComparison testComparison = (TestComparison)i.next();
      List names = testComparison.getStatSpecColumn();
      for (Iterator j = names.iterator(); j.hasNext();) {
        String name = (String)j.next();
        allNames.add(name);
      }
    }
    return allNames;
  }

//------------------------------------------------------------------------------
// Comparison
//------------------------------------------------------------------------------

  /**
   * Returns comparison results for the given tests and test containers.
   */
  private static List compare(List testIds, List testContainers, boolean markFailedTests) {
    // compare each test in the suite
    List testComparisons = new ArrayList();
    if (testContainers.size() == 1) { // special case
      TestContainer testContainer = (TestContainer)testContainers.get(0);
      TestComparison testComparison = compare(testContainer, markFailedTests);
      testComparisons.add(testComparison);
    } else {
      for (Iterator i = testIds.iterator(); i.hasNext();) {
        String testId = (String)i.next();
        TestComparison testComparison = compare(testId, testContainers, markFailedTests);
        testComparisons.add(testComparison);
      }
    }
    return testComparisons;
  }

  /**
   * Returns comparison results for the given test containers. Used for
   * special case.
   */
  private static TestComparison compare(TestContainer testContainer, boolean markFailedTests) {
    // construct comparators for the test container
    log.info("Building value comparators for "
            + testContainer.getTestContainerDir() + "...");
    List comparators = buildValueComparators(testContainer);
    if (log.fineEnabled()) {
      log.fine("Built value comparators for "
              + testContainer.getTestContainerDir() + ": " + comparators);
    }

    // run the test comparison
    log.info("Comparing tests in " + testContainer.getTestContainerDir()
            + "...");
    TestComparison testComparison = compareValues(comparators, markFailedTests);
    if (log.fineEnabled()) {
      log.fine("Compared tests in " + testContainer.getTestContainerDir() + ": "
              + testComparison);
    }
    return testComparison;
  }

  /**
   * Returns comparison results for the given test and test containers.
   */
  private static TestComparison compare(String testId, List testContainers, boolean markFailedTests) {
    // construct comparators for each test container
    log.info("Building value comparators for " + testId + "...");
    List comparators = buildValueComparators(testId, testContainers);
    if (log.fineEnabled()) {
      log.fine("Built value comparators for " + testId + ": " + comparators);
    }

    // run the test comparison
    log.info("Comparing tests for " + testId + "...");
    TestComparison testComparison = compareValues(comparators, markFailedTests);
    if (testComparison.getStatSpecColumn().size() == 0) {
      log.warning("No statspecs found for " + testId);
    }
    if (log.fineEnabled()) {
      log.fine("Compared tests for " + testId + ": " + testComparison);
    }
    return testComparison;
  }

  /**
   * Returns a {@link TestComparison} with the results of comparing the values
   * of statistics in the given list of comparators.
   */
  private static TestComparison compareValues(List comparators, boolean markFailedTests) {
    if (Mode.equals(RAW_MODE)) {
      return new RawComparison(comparators, markFailedTests);
    } else if (Mode.equals(RATIO_MODE)) {
      return new RatioComparison(comparators, RatioThreshold, markFailedTests);
    } else {
      throw new HydraInternalException("Should not happen: " + Mode);
    }
  }

  /**
   * Builds the test containers representing the given test container dirs.
   */
  private static List buildTestContainers(List testContainerDirs,
                                          boolean omitFailedTests,
                                          boolean compareByKey) {
    log.info("Building test containers...");
    boolean enforceConsistency =
        (testContainerDirs.size() == 1) ? false // special case
                                        : true;
    List testContainers = new ArrayList();
    for (Iterator i = testContainerDirs.iterator(); i.hasNext();) {
      String testContainerDir = (String)i.next();
      TestContainer testContainer = new TestContainer(testContainerDir,
                                                      omitFailedTests,
                                                      compareByKey,
                                                      enforceConsistency);
      testContainers.add(testContainer);
    }
    if (log.fineEnabled()) {
      log.fine("Built test containers: " + testContainers);
    }
    return testContainers;
  }

  /**
   * Builds the test suite represented by the union of the test containers.
   * Each test in the suite has a unique id based on the hydra configuration
   * test name and properties.
   */
  private static List buildTestSuite(List testContainers) {
    log.info("Building test suite...");
    List testIds;
    if (testContainers.size() == 1) { // special case
      // create a list of all ids in the order given
      testIds = new ArrayList();
      TestContainer testContainer = (TestContainer)testContainers.get(0);
      for (Iterator i = testContainer.getTests().iterator(); i.hasNext();) {
        Test test = (Test)i.next();
        testIds.add(test.getTestId());
      }
    } else {
      // create a sorted set of unique ids
      SortedSet ids = new TreeSet();
      for (Iterator i = testContainers.iterator(); i.hasNext();) {
        TestContainer testContainer = (TestContainer)i.next();
        ids.addAll(testContainer.getTestIds());
      }
      testIds = new ArrayList(ids);
    }
    if (log.fineEnabled()) {
      log.fine("Built test suite: " + testIds);
    }
    return testIds;
  }

  /**
   * Builds the test descriptions for the test suite.
   */
  private static List buildTestDescriptions(List testIds, List testContainers) {
    log.info("Building test descriptions...");
    List testDescriptions = new ArrayList();
    if (testContainers.size() == 1) { // special case
      // create a list of all descriptions in the order given
      TestContainer testContainer = (TestContainer)testContainers.get(0);
      for (Iterator i = testContainer.getTests().iterator(); i.hasNext();) {
        Test test = (Test)i.next();
        testDescriptions.add(test.getTestDescription());
      }
    } else {
      // create a list containing the last description for each test id
      for (Iterator i = testIds.iterator(); i.hasNext();) {
        String testId = (String)i.next();
        String testDescription = getLastTestDescription(testId, testContainers);
        testDescriptions.add(testDescription);
      }
    }
    if (log.fineEnabled()) {
      log.fine("Built test descriptions: " + testDescriptions);
    }
    return testDescriptions;
  }

  /**
   * Returns the test description for the last test in the last container with
   * the given test id.
   */
  private static String getLastTestDescription(String testId,
                                               List testContainers) {
    for (int i = testContainers.size() - 1; i >= 0; i--) {
      TestContainer testContainer = (TestContainer)testContainers.get(i);
      List tests = testContainer.getTestsWithId(testId);
      for (int j = tests.size() - 1; j >= 0; j--) {
        Test test = (Test)tests.get(j);
        String testDescription = test.getTestDescription();
        if (testDescription != null) {
          return testDescription;
        }
      }
    }
    String s = "No test description found for test id: " + testId;
    throw new PerfComparisonException(s);
  }

  /**
   * Resets the statspecs to make them consistent for all tests in all
   * containers with a given test id, using the global override, if provided.
   */
  private static void resetStatSpecs(List testIds, List testContainers,
                                     String statSpecFile) {
    log.info("Resetting statspecs with global override " + statSpecFile);
    for (Iterator i = testIds.iterator(); i.hasNext();) {
      String testId = (String)i.next();
      resetStatSpecs(testId, testContainers, statSpecFile);
    }
  }

  /**
   * Resets the statspecs to make them consistent for all tests in all
   * containers with the given test id.  Uses a global override, if provided.
   */
  private static void resetStatSpecs(String testId, List testContainers,
                                     String statSpecFileOverride) {
    if (testContainers.size() == 1) { // special case
      TestContainer testContainer = (TestContainer)testContainers.get(0);
      // determine which statspec file to use
      String statSpecFile = statSpecFileOverride;
      if (statSpecFileOverride == null) {
        statSpecFile = getLastStatSpecFile(testContainer);
      } // @todo lises support option to use union of all statspecs

      // reset the statspec file for each test in the container
      List tests = testContainer.getTests();
      for (Iterator i = tests.iterator(); i.hasNext();) {
        Test test = (Test)i.next();
        test.resetStatSpecs(statSpecFile);
      }
    } else {
      // determine which statspec file to use for this test id
      String statSpecFile = statSpecFileOverride;
      if (statSpecFileOverride == null) {
        statSpecFile = getLastStatSpecFile(testId, testContainers);
      } // @todo lises support option to use union of all statspecs

      // reset the statspec file for each test with the given test id
      for (Iterator i = testContainers.iterator(); i.hasNext();) {
        TestContainer testContainer = (TestContainer)i.next();
        List tests = testContainer.getTestsWithId(testId);
        for (Iterator j = tests.iterator(); j.hasNext();) {
          Test test = (Test)j.next();
          test.resetStatSpecs(statSpecFile);
        }
      }
    }
  }

  /**
   * Returns the statspec file for the last test in the container.  Used for
   * special case.
   */
  private static String getLastStatSpecFile(TestContainer testContainer) {
    List tests = testContainer.getTests();
    for (int i = tests.size() - 1; i >= 0; i--) {
      Test test = (Test)tests.get(i);
      String statSpecFile = test.getStatSpecFile();
      if (statSpecFile != null) {
        return statSpecFile;
      }
    }
    String s = "No statspec file found for tests in container: "
             + testContainer.getTestContainerDir();
    throw new PerfComparisonException(s);
  }

  /**
   * Returns the statspec file for the last test in the last container with
   * the given test id.
   */
  private static String getLastStatSpecFile(String testId,
                                            List testContainers) {
    for (int i = testContainers.size() - 1; i >= 0; i--) {
      TestContainer testContainer = (TestContainer)testContainers.get(i);
      List tests = testContainer.getTestsWithId(testId);
      for (int j = tests.size() - 1; j >= 0; j--) {
        Test test = (Test)tests.get(j);
        String statSpecFile = test.getStatSpecFile();
        if (statSpecFile != null) {
          return statSpecFile;
        }
      }
    }
    String s = "No statspec file found for test id: " + testId;
    throw new PerfComparisonException(s);
  }

  /**
   * Returns a list of {@link ValueComparator}s for the given test container
   * with values for relevant statistics.  Used for special case.
   */
  private static List buildValueComparators(TestContainer testContainer) {
    List comparators = new ArrayList();
    List tests = testContainer.getTests();
    for (int i = 0; i < tests.size(); i++) {
      Test test = (Test)tests.get(i);
      ValueComparator comparator = new ValueComparator(i, test);
      comparators.add(comparator);
    }
    // make each instance read its archives
    for (Iterator i = comparators.iterator(); i.hasNext();) {
      ValueComparator comparator = (ValueComparator)i.next();
      if (comparator != null) {
        comparator.readArchives();
      }
    }
    return comparators;
  }

  /**
   * Returns a list of {@link ValueComparator}s for the given test and test
   * containers with values for relevant statistics.
   */
  private static List buildValueComparators(String testId,
                                            List testContainers) {
    List comparators = new ArrayList();
    for (int i = 0; i < testContainers.size(); i++) {
      TestContainer testContainer = (TestContainer)testContainers.get(i);
      List tests = testContainer.getTestsWithId(testId);
      if (tests.size() == 0) { // test is missing from this container
        comparators.add(null);
      } else if (tests.size() == 1) {
        Test test = (Test)tests.get(0);
        ValueComparator comparator = new ValueComparator(i, test);
        comparators.add(comparator);
      } else { // multiple runs of this test are present
        // @todo lises move this check into ValueComparator, for future
        //             averaging, make ValueComparator accept a list of tests
        String s = "Not implemented yet: multiple tests in "
                 + testContainer.getTestContainerDir() + " with id=" + testId;
        throw new UnsupportedOperationException(s);
      }
    }
    // make each instance read its archives
    for (Iterator i = comparators.iterator(); i.hasNext();) {
      ValueComparator comparator = (ValueComparator)i.next();
      if (comparator != null) {
        comparator.readArchives();
      }
    }
    return comparators;
  }

  /**
   * Returns a list of test configuration difference arrays for the given test
   * ids and containers.
   */
  private static List getTestConfigDiffs(List testIds, List testContainers) {
    List diffs = new ArrayList();
    if (testContainers.size() == 1) { // special case
      TestContainer testContainer = (TestContainer)testContainers.get(0);
      List[] testDiffs = getTestConfigDiffs(testContainer);
      diffs.add(testDiffs);
    } else {
      for (Iterator i = testIds.iterator(); i.hasNext();) {
        String testId = (String)i.next();
        List[] testDiffs = getTestConfigDiffs(testId, testContainers);
        diffs.add(testDiffs);
      }
    }
    return diffs;
  }

  /**
   * Returns a test configuration difference array for the given test container.
   * Used for special case.
   */
  private static List[] getTestConfigDiffs(TestContainer testContainer) {
    List testDirs = new ArrayList();
    List tests = testContainer.getTests();
    for (Iterator i = tests.iterator(); i.hasNext();) {
      Test test = (Test)i.next();
      testDirs.add(test.getTestDir());
    }
    return TestConfigComparison.getLatestConfDiffs(testDirs);
  }

  /**
   * Returns a test configuration difference array for the given test id and
   * the test containers.
   */
  private static List[] getTestConfigDiffs(String testId, List testContainers) {
    List testDirs = new ArrayList();
    for (Iterator i = testContainers.iterator(); i.hasNext();) {
      TestContainer testContainer = (TestContainer)i.next();
      testDirs.addAll(testContainer.getTestDirsWithId(testId));
    }
    return TestConfigComparison.getLatestConfDiffs(testDirs);
  }

//------------------------------------------------------------------------------
// Main
//------------------------------------------------------------------------------

  public static void main(String[] args) {
    try {
      if (readParameters(args)) {

        // build the test containers using the provided directories
        List testContainers = buildTestContainers(TestContainerDirs,
                                                  OmitFailedTests,
                                                  CompareByKey);

        // build the test suite
        List testIds = buildTestSuite(testContainers);

        // build the test descriptions
        List testDescriptions = buildTestDescriptions(testIds, testContainers);

        // make the statspecs consistent
        resetStatSpecs(testIds, testContainers, StatSpecFile);

        // do the comparison
        List testComparisons = compare(testIds, testContainers, MarkFailedTests);

        // generate the test configuration differences, if requested
        List testConfigDiffs = null;
        if (AddConfigDiffs) {
          testConfigDiffs = getTestConfigDiffs(testIds, testContainers);
        }

        // generate the report
        String report = formatValues(testIds, testDescriptions, testContainers,
                                     testComparisons, testConfigDiffs);
        // write the report
        writeReport(report, CompReportFile);

        if (GenerateCSVFile) {
          // generate the csv
          report = formatCSV(testIds, testDescriptions, testContainers,
                             testComparisons);
          // write the csv
          writeReport(report, CSVFile);
        }

        // graceful exit
        System.exit(0);

      } else {
        logError("runcomparison() returned false");
        System.exit(1);
      }

    } 
    catch (VirtualMachineError e) {
      // Don't try to handle this; let thread group catch it.
      throw e;
    }
    catch (Throwable t) {
      logError(TestHelper.getStackTrace(t));
      System.exit(1);
    }
  }

  /**
   * Logs an error message even if no log file has been opened yet.
   */
  private static void logError(String msg) {
    if (log == null)
      System.err.println(msg);
    else
      log.severe(msg);
  }
}
