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
package quickstart;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

/**
 * Compares test output to golden text file.
 * 
 * @author Kirk Lund
 * @since 4.1.1
 */
public abstract class GoldenComparator extends Assert {

  //private static final boolean ALLOW_EXTRA_WHITESPACE = true;
  
  private String[] expectedProblemLines;
  
  protected GoldenComparator(String[] expectedProblemLines) {
    this.expectedProblemLines = expectedProblemLines;
  }
  
  protected Reader readGoldenFile(String goldenFileName) throws IOException {
    InputStream goldenStream = ClassLoader.getSystemResourceAsStream(goldenFileName);
    assertNotNull("Golden file " + goldenFileName + " not found.", goldenStream);
    return new InputStreamReader(goldenStream);
  }
  
  public void assertOutputMatchesGoldenFile(String actualOutput, String goldenFileName) throws IOException {
    trace("GoldenComparator:assertOutputMatchesGoldenFile");
    BufferedReader goldenReader = new BufferedReader(readGoldenFile(goldenFileName));
    BufferedReader actualReader = new BufferedReader(new StringReader(actualOutput));
    
    List<String> goldenStrings = readLines(goldenReader);
    List<String> actualStrings = readLines(actualReader);

    scanForProblems(actualStrings);
    
    String actualLine = null;
    String goldenLine = null;
    
    int lineCount = 0;
    do {
      lineCount++;
      trace("GoldenComparator comparing line " + lineCount);

      actualLine = actualStrings.get(lineCount - 1);
      goldenLine = goldenStrings.get(lineCount - 1);
      
      //checkForProblem(lineCount, actualLine);
      if (actualLine == null && goldenLine != null) {
        fail("EOF reached in actual output but golden file, " + goldenFileName + ", continues at line " + lineCount + ": " + goldenLine + new OutputFormatter(actualStrings));
      
      } else if (actualLine != null && goldenLine == null) {
        fail("EOF reached in golden file, " + goldenFileName + ", but actual output continues at line " + lineCount + ": " + actualLine + new OutputFormatter(actualStrings));
      
      } else if (actualLine != null && goldenLine != null) {
        assertTrue("Actual output \"" + actualLine
            + "\" did not match expected pattern \"" + goldenLine
            + "\" at line " + lineCount + " in " + goldenFileName 
            + ": " + new OutputFormatter(actualStrings), 
            compareLines(actualLine, goldenLine));
      }
    } while (actualLine != null && goldenLine != null);
  }
  
  /**
   * Returns true if the line matches and is ok. Otherwise returns false.
   */
  protected abstract boolean compareLines(String actualLine, String goldenLine);
  
  private List<String> readLines(BufferedReader reader) throws IOException {
    List<String> listOfLines = new ArrayList<String>();
    String line = null;
    do {
      line = reader.readLine();
      listOfLines.add(line);
    } while(line != null);
    return listOfLines;
  }
  
  private void scanForProblems(List<String> lines) throws IOException {
    trace("GoldenComparator:scanForProblems");
    int lineCount = 0;
    for (String line : lines) {
      lineCount++;
      trace("GoldenComparator:scanForProblems scanning line " + lineCount);
      checkForProblem(lineCount, line);
    }
  }
  
  private void checkForProblem(int lineCount, String line) {
    if (line == null) {
      return;
    }
    checkLineFor(lineCount, line, "warning");
    checkLineFor(lineCount, line, "error");
    checkLineFor(lineCount, line, "severe");
  }
  
  private void checkLineFor(int lineCount, String line, String problem) {
    if (line != null && line.toLowerCase().contains(problem)) {
      if (this.expectedProblemLines != null && this.expectedProblemLines.length > 0) {
        for (int i = 0; i < this.expectedProblemLines.length; i++) {
          if (compareLines(line, this.expectedProblemLines[i])) {
            return;
          }
        }
      }
      // TODO: collect up entire stack trace if there is one (might span multiple lines)
      trace("About to fail because of " + line);
      fail("Actual output contains a problem (warning/error/severe) on line " + lineCount + ": " + line);
    }
  }
  
  protected void trace(String message) {
    QuickstartTestCase.trace(message);
  }
}
