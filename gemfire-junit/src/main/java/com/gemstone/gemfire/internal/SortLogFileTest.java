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
package com.gemstone.gemfire.internal;

import com.gemstone.gemfire.LogWriter;
import java.io.*;
import java.util.*;
import junit.framework.*;

/**
 * Tests the functionality of the {@link SortLogFile} program.
 *
 * @author David Whitlock
 *
 * @since 3.0
 */
public class SortLogFileTest extends TestCase {

  public SortLogFileTest(String name) {
    super(name);
  }

  /**
   * Generates a "log file" whose entry timestamps are in a random
   * order.  Then it sorts the log file and asserts that the entries
   * are sorted order.
   */
  public void testRandomLog() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter pw =
      new PrintWriter(new OutputStreamWriter(baos), true);
    LogWriter logger = new RandomLogWriter(pw);

    for (int i = 0; i < 100; i++) {
      logger.info(String.valueOf(i));
    }

    pw.flush();
    pw.close();
    
    byte[] bytes = baos.toByteArray();

//     System.out.println("RANDOM:");
//     System.out.println(new String(bytes));

    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);

    StringWriter sw = new StringWriter();
    pw = new PrintWriter(sw, true);
    SortLogFile.sortLogFile(bais, pw);

    String sorted = sw.toString();
//     System.out.println("SORTED");
//     System.out.println(sorted);

    BufferedReader br = new BufferedReader(new StringReader(sorted));
    LogFileParser parser = new LogFileParser(null, br);
    String prevTimestamp = null;
    while (parser.hasMoreEntries()) {
      LogFileParser.LogEntry entry = parser.getNextEntry();
      String timestamp = entry.getTimestamp();
      if (prevTimestamp != null) {
        assertTrue("Prev: " + prevTimestamp + ", current: " + timestamp,
                   prevTimestamp.compareTo(timestamp) <= 0);
      }
      prevTimestamp = entry.getTimestamp();
    }
  }

  ////////////////////  Inner Classes  ////////////////////

  /**
   * A <code>LogWriter</code> that generates random time stamps.
   */
  static class RandomLogWriter extends LocalLogWriter {

    /** Used to generate a random date */
    private Random random = new Random();

    /**
     * Creates a new <code>RandomLogWriter</code> that logs to the
     * given <code>PrintWriter</code>.
     */
    public RandomLogWriter(PrintWriter pw) {
      super(ALL_LEVEL, pw);
    }

    /**
     * Ignores <code>date</code> and returns the timestamp for a
     * random date.
     */
    public String formatDate(Date date) {
      long time = date.getTime() + (random.nextInt(100000) * 1000);
      date = new Date(time);
      return super.formatDate(date);
    }

  }

}
