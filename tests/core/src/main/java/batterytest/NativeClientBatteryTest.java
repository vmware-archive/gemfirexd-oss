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

package batterytest;

import com.gemstone.gemfire.LogWriter;
import hydra.*;
import java.io.*;
import java.util.*;
import perffmwk.PerfSorter;
import util.TestHelper;

/**
 * Sets up a batterytest by creating a test file containing a list of tests
 * to run, relative to $CTESTS if the file is otherwise not found.  For example:
 *   <blockquote><pre>
 *      tests.list:
 *          collections/collperf.xml numHosts=5
 *          locks/locktest.xml
 *          include $CTESTS/license/license.list
 *   </pre></blockquote>
 * <p>
 * A .list file can live anywhere, for example, in the same directory as the
 * tests it references.
 * <p>
 * Tests can be commented out using C or C++ style comments.  Multiple tests
 * can be listed on the same line as long as they are separated by whitespace.
 * A test name can only contain whitespace if it is quoted.
 * <p>
 * A test can depend on any number of "system properties" used in the
 * native client test test XML file via the ${} syntax.  Properties are used
 * to generate multiple XML test files based on a single input file.
 * They are intended for use only when using regular parameters would be
 * cumbersome, for example, when scaling the number of hosts used in a test.
 * <p>
 * Default values for system properties can optionally be specified in a .prop
 * file by the same name and in the same directory as the test, whether that is
 * relative to the current directory or to $CTESTS.  Batterytest will seek the
 * .prop file automatically.  There is no need to indicate to batterytest
 * whether such a file exists, and there is no way to force batterytest to look
 * in another location for this file.
 * <p>
 * Values for properties can also be specified after the test name in the .list
 * file.  Any properties specified in the .list file will override those in the
 * .prop file.  A property value cannot contain whitespace.  They can be given
 * on the same line as the test or on subsequent lines.  Each property must be
 * separated from the next with whitespace.
 * <p>
 * Usage:
 *   <blockquote><pre>
 *      java -DCTESTS=&lt;path_to_test_classes&gt;
 *           -DtestFileName=&lt;batterytest_input_file&gt;
 *           [-DresultDir=&lt;cwd_for_batterytest(default:$cwd)&gt;]
 *           [-DlogLevel=&lt;batterytest_log_level(default:info)&gt]
 *           batterytest.NativeClientBatteryTest
 *   </pre></blockquote>
 * <p>
 * Example:
 *   <blockquote><pre>
 *      java -classpath $CTESTS
 *           -DCTESTS=$CTESTS
 *           -DtestFileName=$CTESTS/smoketest/smoketest.list
 *           batterytest.NativeClientBatteryTest
 *   </pre></blockquote>
 *
 * <p>
 * A fully expanded list of tests with their configured properties is written
 * to "batterytest.bt".  The list of tests to run, using parmetrized names,
 * is written to "batterytest.list".  These files are overwritten every time
 * batterytest is invoked.  Log messages are written to "batterytest.log".
 * The verbosity of this log is controlled via <code>logLevel</code>.
 *
 *  @author Lise Storc
 *  @since 6.0
 */
public class NativeClientBatteryTest {

  public static void main( String[] args ) {
    try {
      runbattery(args);
    } 
    catch (VirtualMachineError e) {
      // Don't try to handle this; let thread group catch it.
      throw e;
    }
    catch( Throwable t ) {
      logError( TestHelper.getStackTrace( t ) );
    }
    System.exit(0);
  }

  public static void runbattery(String[] args) {

    // get the required settings
    String jtests = System.getProperty( "CTESTS" );
    String testFileName = System.getProperty( "testFileName" );
    if (jtests == null) {
      usage("Missing CTESTS");
      return;

    } else if (testFileName == null ) {
      usage("Missing testFileName");
      return;
    }

    // get the optional settings
    String resultDir = System.getProperty( "resultDir", System.getProperty( "user.dir" ) );
    String logLevel = System.getProperty( "logLevel", "info" );

    // open the batterytest log file, append if it already exists
    log = Log.createLogWriter( "batterytest", "batterytest", logLevel, true );
    log.info(ProcessMgr.processAndBuildInfoString());
    log.info( "Batterytest PID is " + ProcessMgr.getProcessId() );

    // log the batterytest configuration
    log.info( "\nCTESTS = " + jtests +
              "\ntestFileName = " + testFileName +
              "\nresultDir = " + resultDir +
              "\nlogLevel = " + logLevel
    );

    // get the list of tests from the batterytest input file
    log.info( "Parsing batterytest input file: " + testFileName + "..." );
    Vector tests = null;
    try {
      tests = ConfigParser.parseFile( testFileName );
      if ( log.fineEnabled() ) {
        for ( int i = 0; i < tests.size(); i++ ) {
          log.fine( "Test " + i + " = " + tests.get(i) );
        }
      }
    } catch( FileNotFoundException e ) {
      log.severe( "Batterytest input file not found: " + testFileName, e );
      return;
    }

    // log info about test run
    log.info( "Number of tests in " + testFileName + " is " + tests.size() + "..." );

    // generate the tests
    String sep = File.separator;

    for ( int j = 0; j < tests.size(); j++ ) {
      BatteryTestConfig btc = (BatteryTestConfig) tests.elementAt( j );
      String relativetest = btc.getName();

      String test = null;
      if ( FileUtil.exists( relativetest ) ) {
        test = FileUtil.absoluteFilenameFor( relativetest );
      } else {
        test = jtests + sep + relativetest;
      }
      // bring in default properties not overridden in the .bt file, if any
      int index = test.indexOf( ".xml" );
      if ( index != -1 ) { // native client test
        String propFileName = test.substring( 0, index ) + ".prop";
        btc.fillInWithDefaultsFrom( propFileName );
      } else {
        String s = "Unexpected file type: " + test;
        throw new HydraInternalException(s);
      }

      // add the test key to the properties
      btc.addProperty(PerfSorter.COMPARISON_KEY_PROP, relativetest);

      // process the test xml file using properties
      String testxml = null;
      try {
        testxml = NativeClientConfigParser.parseFile(test, btc.getProperties());
      } catch( FileNotFoundException e ) {
        String s = "Native client xml file not found: " + test;
        throw new HydraConfigException(s);
      }

      // create the test directory
      String relativetestdir = "";
      if (relativetest.indexOf("/") != -1) {
        relativetestdir = relativetest.substring(0,
                          relativetest.lastIndexOf("/"));
      }
      String testdir = resultDir + sep + relativetestdir;
      FileUtil.mkdir(testdir);

      // generate a parametrized test name
      String testname = FileUtil.filenameFor(test);
      testname = testname.substring(0, testname.lastIndexOf("."))
               + "_" + j;

      // write the test xml and system.properties to the test directory
      FileUtil.writeToFile(testdir + sep + testname + ".xml",
                           testxml);
      btc.writePropertiesToFile(testdir + sep + testname + ".prop");

      // write the test to batterytest.list
      FileUtil.appendToFile(resultDir + sep + "batterytest.list",
                            relativetestdir + sep + testname + ".xml\n");
    }
    reportStatus(tests.size());
    return;
  }
  private static void reportStatus(int total) {
    StringBuffer msg = new StringBuffer( 100 );
    msg.append( "\n" ).append( "==== STATUS REPORT ====" );
    msg.append( "  Total: " ).append( total );
    msg.append( "  ====" );
    log.info( msg.toString() );
  }

  private static void usage(String s) {
    PrintStream out = System.out;
    out.println("\n** " + s + "\n");
    out.println( "Usage: java ");
    out.println("  -DCTESTS=<path_to_test_classes> ");
    out.println("  -DtestFileName=<batterytest_input_file> ");
    out.println("  [-DlogLevel=<batterytest_log_level(default:info)>]");
    out.println("batterytest.NativeClientBatteryTest");
    out.println("");
  }

  private static void logError( String msg ) {
    if ( log == null )
      System.err.println( msg );
    else
      log.severe( msg );
  }
  private static LogWriter log;
}
