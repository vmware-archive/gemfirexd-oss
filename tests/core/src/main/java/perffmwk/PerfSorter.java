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

import batterytest.*;
import com.gemstone.gemfire.LogWriter;
import hydra.*;

import java.io.*;
import java.util.*;
import util.TestHelper;

/**
 *  Sorts a list of test directories into subdirectories based on their
 *  comparison keys, specified using the test configuration variable {@link
 *  #COMPARISON_KEY_PROP}.  Test directories with the same
 *  comparison key are placed into a subdirectory named for the key.  The
 *  subdirectory is created in the sorter's working directory.
 *  <p>
 *  Keys are obtained by default from the test properties file in the test
 *  directory, but can be overridden via an input batterytest file.  When a
 *  batterytest file is provided, only tests matching a batterytest entry are
 *  sorted.  Non-matching batterytest entries and test directories are ignored.
 *  By providing different batterytest files with different key assignments,
 *  the same set of tests can be sorted in different ways.
 *  <p>
 *  Failed tests can be optionally omitted.
 *  <p>
 *  Logging by the comparison sorter itself is written to "perfsorter.log".
 *  The verbosity of this log is controlled via the <code>logLevel</code>
 *  system property (see {@link com.gemstone.gemfire.LogWriter}).
 *  <p>
 *  This tool is useful for sorting tests that have the same name but different
 *  values of configuration variables, and that need to be compared using VSD,
 *  into convenient subdirectories that makes it easy to bring up VSD on the
 *  set of comparable directories.
 *  <p>
 *  Usage:
 *  <blockquote><pre>
 *    java -DJTESTS=&lt;path_to_test_classes&gt;
 *         [-DbatterytestFile=&lt;batterytest_filename&gt;]
 *         [-DomitFailedTests=&lt;whether_to_omit_failed_tests_from_sort(default:false)&gt;]
 *         [-DlogLevel=&lt;perf_comparer_log_level(default:info)&gt;]
 *         perffmwk.PerfSorter
 *         &lt;list_of_test_directories&gt;
 *  </pre></blockquote>
 *  <p>
 *  Example:
 *  <blockquote><pre>
 *     java -classpath $JTESTS:$GEMFIRE/lib/gemfire.jar
 *          -DJTESTS=$JTESTS
 *          -DbatterytestFile=compareByVendor.bt
 *          perffmwk.PerfSorter useCase17-*
 *  </pre></blockquote>
 */
public class PerfSorter {

  public static final String COMPARISON_KEY_PROP = "perffmwk.comparisonKey";

  private static final String OMIT_FAILED_TESTS_PROP = "omitFailedTests";
  private static final String BATTERYTEST_FILE_PROP  = "batterytestFile";
  private static final String LOG_LEVEL_PROP         = "logLevel";

  private static String    BatteryTestFile;
  private static boolean   OmitFailedTests;
  private static Vector    BatteryTests;
  private static Map       TestDirs;

  private static LogWriter TestLog;

  //////////////////////////////////////////////////////////////////////////////
  ////    MAIN                                                              ////
  //////////////////////////////////////////////////////////////////////////////

  public static void main( String[] args ) {
    try {
      if ( sort( args ) ) {
        System.exit(0);
      } else {
        logError( "sort() returned false" );
        System.exit(1);
      }
    } 
    catch (VirtualMachineError e) {
      // Don't try to handle this; let thread group catch it.
      throw e;
    }
    catch( Throwable t ) {
      logError( TestHelper.getStackTrace( t ) );
      System.exit(1);
    }
  }
  public static boolean sort( String[] args ) {
    if ( readParameters( args ) ) {
      moveDirs();
      return true;
    } else {
      return false;
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    WORK                                                              ////
  //////////////////////////////////////////////////////////////////////////////

  private static boolean readParameters( String[] args ) {

    // logLevel
    String logLevel = System.getProperty( LOG_LEVEL_PROP, "info" );

    // open the log file, appending if it already exists
    TestLog = Log.createLogWriter( "perfsorter", "perfsorter", logLevel, true );
    TestLog.info( "PerfSorter PID is " + ProcessMgr.getProcessId() );

    // JTESTS (required)
    String jtests = System.getProperty( "JTESTS" );
    if ( jtests == null ) {
      usage( "Missing JTESTS" );
      return false;
    }

    // BatteryTests
    BatteryTestFile = System.getProperty( BATTERYTEST_FILE_PROP );
    if ( BatteryTestFile != null ) {
      TestLog.info( "Parsing batterytest file: " + BatteryTestFile + "..." );
      try {
        BatteryTests = batterytest.ConfigParser.parseFile( BatteryTestFile );
        if ( BatteryTests.size() == 0 ) {
          TestLog.info( "No batterytests found in file: " + BatteryTestFile );
          BatteryTests = null;
        } else {
          for ( Iterator i = BatteryTests.iterator(); i.hasNext(); ) {
            BatteryTestConfig btc = (BatteryTestConfig) i.next();
            String testname = btc.getName();
            String defaults = jtests + "/"
                            + testname.substring( 0, testname.indexOf( ".conf" ) )
                            + ".prop";
            //TestLog.fine( "BEFORE: " + btc );
            btc.fillInWithDefaultsFrom( defaults );
            //TestLog.fine( "AFTER:  " + btc );
          }
          TestLog.info( "Using batterytests: " + printBatteryTests() );
        }
      } catch( Exception e ) {
        TestLog.severe( "Problem reading batterytest file: " + BatteryTestFile, e );
        return false;
      }
    }

    // OmitFailedTests
    OmitFailedTests = Boolean.getBoolean( OMIT_FAILED_TESTS_PROP );

    // TestDirs (required), mapped by the value of {@link #COMPARISON_KEY_PROP}.
    TestDirs = new TreeMap();
    for ( int i = 0; i < args.length; i++ ) {
      String dir = absoluteName( args[i] );
      TestLog.fine( "Considering test " + dir );
      if ( FileUtil.exists( dir ) ) {
        if ( TestFileUtil.isTestDirectory( dir ) ) {
          if ( passed( dir ) || ! OmitFailedTests ) {
            String testPropFile = getTestPropFile( dir );
            if ( FileUtil.exists( testPropFile ) ) {
              String key;
              if ( BatteryTests == null ) {
                key = getComparisonKey( testPropFile );
              } else {
                key = getBatteryTestKey( testPropFile );
              }
              if ( key == null ) {
                TestLog.warning( "...skipping test -- no comparison key: " + dir );
              } else {
                TestLog.fine( "Adding test " + key + " : " + dir );
                addDir( key, dir );
              }
            } else {
              TestLog.warning( "...skipping test -- prop file not found: " + testPropFile );
            }
          } else {
            TestLog.warning( "...skipping test -- failed: " + dir );
          }
        } else {
          TestLog.warning( "...skipping test -- not a test directory: " + dir );
        }
      } else {
        TestLog.warning( "...skipping test -- directory not found: " + dir );
      }
    }
    if ( TestDirs.size() < 1 ) {
      usage( "No test directories to sort: " + TestDirs.size() );
      return false;
    }

    // log the sorter configuration
    TestLog.info( "\nJTESTS = " + jtests +
                  "\n" + BATTERYTEST_FILE_PROP  + " = " + BatteryTestFile +
                  "\n" + OMIT_FAILED_TESTS_PROP + " = " + OmitFailedTests +
                  "\nlogLevel = "        + logLevel +
                  "\ntestDirs = " + TestDirs );
    return true;
  }
  private static void usage( String s ) {
    StringBuffer buf = new StringBuffer();
    buf.append("\n** " + s );
    buf.append( "\nUsage: java" );
    buf.append( " [optional_properties]" );
    buf.append( " perffmwk.PerfSorter <list_of_test_directories>" );
    buf.append( "\nwhere optional properties include:" );
    buf.append( "\n-D" + BATTERYTEST_FILE_PROP + "=<batterytest_filename>" );
    buf.append( "\n-D" + OMIT_FAILED_TESTS_PROP + "=<whether_to_omit_failed_tests" );
    System.out.println( buf.toString() );
  }
  private static String printBatteryTests() {
    StringBuffer buf = new StringBuffer();
    for ( Iterator i = BatteryTests.iterator(); i.hasNext(); ) {
      BatteryTestConfig btc = (BatteryTestConfig) i.next();
      buf.append( "\n" + btc.toString() );
    }
    return buf.toString();
  }
  // find the batterytest that matches the test props, if any, and return the key, if any
  private static String getBatteryTestKey( String testPropFile ) {
    SortedMap testmap = getTestProps( testPropFile );
    TestLog.fine( "Matching testmap: " + testmap );
    for ( Iterator i = BatteryTests.iterator(); i.hasNext(); ) {
      BatteryTestConfig btc = (BatteryTestConfig) i.next();
      SortedMap btmap = btc.getSortedProperties();
      btmap.put("testName", btc.getName());
      TestLog.fine( "Checking btmap: " + btmap );
      String key = (String)btmap.get(COMPARISON_KEY_PROP);
      if ( key != null ) {
        if ( matchesExceptForKey( testmap, btmap ) ) {
          return key;
        }
      } // else ignore this batterytest entry entirely
    }
    // ignore this test entirely
    return null;
  }
  private static boolean matchesExceptForKey( SortedMap map1, SortedMap map2 ) {
    for ( Iterator i = map1.keySet().iterator(); i.hasNext(); ) {
      String key = (String) i.next();
      if (!key.equals(COMPARISON_KEY_PROP)) {
        if ( map2.containsKey( key ) ) {
          String val1 = (String) map1.get( key );
          String val2 = (String) map2.get( key );
          if ( val1.equals( val2 ) ) {
            //continue;
          } else {
            TestLog.fine( "1: " + val1 + " not equal to " + val2 );
            return false;
          }
        } else {
          TestLog.fine( "1: map2 missing key " + key );
          return false;
        }
      }
    }
    for ( Iterator i = map2.keySet().iterator(); i.hasNext(); ) {
      String key = (String) i.next();
      if (!key.equals(COMPARISON_KEY_PROP)) {
        if ( map1.containsKey( key ) ) {
          String val1 = (String) map2.get( key );
          String val2 = (String) map1.get( key );
          if ( val1.equals( val2 ) ) {
            //continue;
          } else {
            TestLog.fine( "2: " + val1 + " not equal to " + val2 );
            return false;
          }
        } else {
          TestLog.fine( "2: map1 missing key " + key );
          return false;
        }
      }
    }
    return true;
  }
  private static void addDir( String key, String dir ) {
    Vector bucket = (Vector) TestDirs.get( key );
    if ( bucket == null ) {
      bucket = new Vector();
      TestDirs.put( key, bucket );
    }
    bucket.add( dir );
    TestLog.fine( "TestDirs: " + TestDirs );
  }
  private static void moveDirs() {
    String pwd = System.getProperty( "user.dir" );
    for ( Iterator i = TestDirs.keySet().iterator(); i.hasNext(); ) {
      String key = (String) i.next();
      Vector dirs = (Vector) TestDirs.get( key );
      for ( Iterator j = dirs.iterator(); j.hasNext(); ) {
        String dir = (String) j.next();
        String dst = pwd + "/" + key;
        if ( ! FileUtil.exists( dst ) ) {
          FileUtil.mkdir( dst );
        }
        if (HostHelper.isWindows()) {
          if(new File(dir).isDirectory()) {
            ProcessMgr.fgexec("xcopy.exe /Y /E /I /Q " + dir + " " + dst, 300); 
          } else {
            ProcessMgr.fgexec("cmd.exe /c copy " + dir + " " + dst, 300);
          }
        } else {
          ProcessMgr.fgexec("/bin/cp -R " + dir + " " + dst, 300);
        }
      }
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    MISC                                                              ////
  //////////////////////////////////////////////////////////////////////////////

  private static String getTestPropFile( String dir ) {
    return dir + "/" + TestFileUtil.getShortTestName( dir ) + ".prop";
  }
  private static SortedMap getTestProps( String fn ) {
    try {
      return FileUtil.getPropertiesAsMap( fn );
    } catch( Exception e ) {
      throw new HydraInternalException( "Should not happen" );
    }
  }
  private static String getComparisonKey( String fn ) {
    SortedMap map = getTestProps( fn );
    return (String)map.get(COMPARISON_KEY_PROP);
  }
  private static String absoluteName( String dir ) {
    return (new File(dir)).getAbsoluteFile().toString();
  }
  private static boolean passed( String dir ) {
    return ! FileUtil.exists( dir + "/errors.txt" );
  }
  private static void logError( String msg ) {
    if ( TestLog == null )
      System.err.println( msg );
    else
      TestLog.severe( msg );
  }
}
