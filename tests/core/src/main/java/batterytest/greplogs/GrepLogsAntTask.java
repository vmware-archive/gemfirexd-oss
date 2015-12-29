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
package batterytest.greplogs;

import org.apache.tools.ant.*;

/**
 * Ant task used to search for suspect strings within the test logs.
 * This is used for unit tests as well as regression tests.
 * @author kbanks
 */
public class GrepLogsAntTask extends Task {

  /** Required attribute: output is written to this file. */
  private String outFile = null;
  /** Required attribute: all log files in this directory are checked.*/
  private String sourceDir = null;
  /** Optional attribute: Defines the upperbound of errors to be tollerated */
  private int threshold = 0;
 

  /**
   * Search @{link sourceDir} for errors and report them in @{link outFile}
   * If the total number of errors found exceeds the @{link threshold} then
   * throw a @{link BuildException}.
   **/
  public void execute() throws BuildException {
    if( sourceDir == null) {
      throw new BuildException("sourceDir attribute is required.");
    }
     
    if( outFile == null) {
      throw new BuildException("outFile attribute is required.");
    }
     
    try { 
      String[] args = new String[]{ 
                                   "-out", outFile, 
                                   "-threshold", Integer.toString(threshold),
                                   sourceDir};
      GrepLogs grep = new GrepLogs(args);
      grep.process();
    }
    catch (GrepLogs.GrepLogsException e) {
      throw new BuildException("GrepLogs failed: " + e.getMessage(), e);
    }  
  }
  

  /** required ant boilerplate setter */ 
  public void setSourceDir(String dir) {
    sourceDir = dir;
  }
  /** required ant boilerplate setter */ 
  public void setOutFile(String out) {
    outFile = out;
  }
  /** required ant boilerplate setter */ 
  public void setThreshold(int t) {
    threshold = t;
  }
}
