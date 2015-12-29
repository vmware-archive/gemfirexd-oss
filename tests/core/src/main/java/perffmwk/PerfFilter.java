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
import hydra.FileUtil;
import hydra.Log;
import hydra.ProcessMgr;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import util.TestHelper;

public class PerfFilter implements ComparisonConstants {

  protected static final String DIVIDER = "================================================================================";

  private static String InputFile;

  // Name of the output file for the filtered report.
  private static final String FILTER_REPORT_FILE_PROP = "filterReportFile";
  private static String FilterReportFile;

  // Log level used by this tool.
  private static final String LOG_LEVEL_PROP = "logLevel";
  private static LogWriter log;

  private static final String BASE_COLS_PROP = "baseCols";
  protected static int BaseCols;

  private static final String COMP_COLS_PROP = "compCols";
  protected static int CompCols;

  // Threshold diff over which ratio values will be reported.
  private static final String RATIO_THRESHOLD_PROP = "ratio.threshold";
  protected static double RatioThreshold;

//------------------------------------------------------------------------------
// Configuration
//------------------------------------------------------------------------------

  /**
   * Reads the tool configuration from system properties and command-line args.
   */
  private static boolean readParameters(String[] args) {

    StringBuffer configuration = new StringBuffer();

    // inputFile
    if (args.length == 0) {
      usage("No input file specified.");
      return false;
    } else if (args.length > 1) {
      usage("Too many input files specified, provide only one.");
      return false;
    } else {
      InputFile = args[0];
    }
    configuration.append("\nInputFile = " + InputFile);

    // filterReportFile
    {
      String fileName =
        System.getProperty(FILTER_REPORT_FILE_PROP, InputFile + ".filter");
      if (fileName.equals("none")) {
        FilterReportFile = null;
      } else {
        FilterReportFile = FileUtil.absoluteFilenameFor(fileName);
      }
      configuration.append("\nfilterReportFile = " + FilterReportFile);
    }

    // baseCols
    BaseCols = Integer.getInteger(BASE_COLS_PROP, Integer.valueOf(1));
    configuration.append("\nbaseCols = " + BaseCols);

    // compCols
    CompCols = Integer.getInteger(COMP_COLS_PROP, Integer.valueOf(1));
    configuration.append("\ncompCols = " + CompCols);

    // logLevel
    String logLevel = System.getProperty(LOG_LEVEL_PROP, "info");
    configuration.append("\nlogLevel = " + logLevel);

    // open the log file, append if it already exists
    if (FilterReportFile == null) {
      log = Log.createLogWriter("perffilter", logLevel);
    } else {
      log = Log.createLogWriter("perffilter", "perffilter", logLevel, true);
    }
    log.info("PerfFilter PID is " + ProcessMgr.getProcessId());

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
    buf.append(" [optional_properties]");
    buf.append(" perffmwk.PerfFilter <file>");
    buf.append("\nwhere optional properties include:");
    buf.append("\n-D" + BASE_COLS_PROP
                      + "=<number_of_base_columns(default:1)>");
    buf.append("\n-D" + COMP_COLS_PROP
                      + "=<number_of_comparison_columnss(default:1)>");
    buf.append("\n-D" + FILTER_REPORT_FILE_PROP
                      + "=<filter_filename(default:<file>.filter)>");
    buf.append("\n-D" + LOG_LEVEL_PROP
                      + "=<log_level(default:info)>");
    buf.append("\n-D" + RATIO_THRESHOLD_PROP
                      + "=<ratio_threshold(default:0.05)>");
    System.out.println( buf.toString());
  }

//------------------------------------------------------------------------------
// Filtering
//------------------------------------------------------------------------------

  private static void filterFile(String infn, String outfn, int basecols, int compcols, double threshold)
  throws FileNotFoundException, IOException {
    log.info("Filtering ratio comparison file " + infn + " to " + outfn + "...");

    StringWriter headersw = new StringWriter();
    StringWriter resultsw = new StringWriter();
    StringWriter footersw = new StringWriter();
    PrintWriter header = new PrintWriter(headersw, true);
    PrintWriter result = new PrintWriter(resultsw, true);
    PrintWriter footer = new PrintWriter(footersw, true);
    boolean intoTests = false;
    boolean doneTests = false;
    String testName = null;
    List<String> lines = FileUtil.getTextAsList(infn);
    for (String line : lines) {
      //Log.getLogWriter().info("HEY line:" + line);
      if (doneTests) {
        footer.println(line);
      } else if (intoTests) {
        if (line.trim().startsWith("AVG ") || line.startsWith("NOTE: ") || line.startsWith(DIVIDER)) {
          doneTests = true;
          footer.println(line);
        } else { // compare the last compcols with the first basecols for threshold
          // create a list of comparable numerical values from the line
          String tokens[] = line.split(" +");
          List<Double> vals = new ArrayList();
          boolean hitbaseline = false;
          for (int i = 0; i < tokens.length; i++) {
            String token = tokens[i];
            if (hitbaseline) {
              if (token.equals(NIL_VAL)) {
                vals.add(Double.valueOf(1.00));
              } else {
                vals.add(Double.valueOf(token));
              }
            } else if (token.equals(NIL_VAL)) {
              vals.add(Double.valueOf(1.00));
              hitbaseline = true;
            }
          }
          if (vals.size() < basecols + compcols) {
            String s = "basecols(" + basecols + ") + compcols(" + compcols + ") < " + tokens.length + " in \"" + vals + "\"";
            throw new PerfComparisonException(s);
          }
          if (tokens.length - vals.size() == 3) {
            // this is the start of a test
            testName = tokens[0];
          }
          // find the worst basecol and best compcol
          Double worstbasecol = null;
          Double bestcompcol = null;
          for (int i = 0; i < basecols; i++) {
            Double val = vals.get(i);
            if (worstbasecol == null || val < worstbasecol) {
              worstbasecol = val;
            }
          }
          for (int i = vals.size() - compcols; i < vals.size(); i++) {
            Double val = vals.get(i);
            if (bestcompcol == null || val > bestcompcol) {
              bestcompcol = val;
            }
          }
          //double delta = Math.abs(bestcompcol - worstbasecol);
          double delta = Math.abs(Math.abs(bestcompcol) - Math.abs(worstbasecol));
          Log.getLogWriter().info("worstbasecol=" + worstbasecol + " bestcompcol=" + bestcompcol + " delta=" + delta + " threshold=" + threshold);
          if (bestcompcol < worstbasecol && delta > threshold) {
            //Log.getLogWriter().info("HEY tokens.length=" + tokens.length + " vals.size=" + vals.size());
            //if (tokens.length - vals.size() != 3) {
            //  Log.getLogWriter().info("HEY printing testName=" + testName);
            //  result.println(testName);
            //}
            result.println(line + " <=== down at least " + (int)(100*delta) + "%");
            Log.getLogWriter().info(line + " <=== down at least " + (int)delta + "%");
          } else {
            result.println(line);
            Log.getLogWriter().info(line);
          }
        }
      } else if (line.startsWith("TEST ")) {
        intoTests = true;
        header.println(line);
      } else {
        header.println(line);
      }
    }
    header.flush();
    result.flush();
    footer.flush();
    headersw.flush();
    resultsw.flush();
    footersw.flush();
    FileUtil.writeToFile(outfn, headersw.toString() + resultsw.toString() + footersw.toString());

    log.info("Filtered ratio comparison file " + infn + " to " + outfn);
  }

//------------------------------------------------------------------------------
// Main
//------------------------------------------------------------------------------

  public static void main(String[] args) {
    try {
      if (readParameters(args)) {
        filterFile(InputFile, FilterReportFile, BaseCols, CompCols, RatioThreshold);
        System.exit(0);
      } else {
        System.exit(1);
      }
    } catch (Throwable t) {
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
