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

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

/**
 * Reads the stdout and stderr from a running process and stores then for test 
 * validation. Also provides a mechanism to waitFor the process to terminate. 
 * Extracted from ProcessWrapper.
 * 
 * @author Kirk Lund
 */
public class ProcessOutputReader {

  private static final long PROCESS_TIMEOUT_MILLIS = 10 * 60 * 1000L; // 10 minutes

  private int exitCode;
  private String output;
  
  private final Process p;
  private final ProcessStreamReader stdout;
  private final ProcessStreamReader stderr;
  private final List<String> lines;
  
  public ProcessOutputReader(final Process p, final ProcessStreamReader stdout, final ProcessStreamReader stderr, final List<String> lines) {
    this.p = p;
    this.stdout = stdout;
    this.stderr = stderr;
    this.lines = lines;
  }
  
  public void waitFor() {
    stdout.start();
    stderr.start();

    long startMillis = System.currentTimeMillis();
    try {
      stderr.join(PROCESS_TIMEOUT_MILLIS);
    } catch (Exception ignore) {
    }

    long timeLeft = System.currentTimeMillis() + PROCESS_TIMEOUT_MILLIS - startMillis;
    try {
      stdout.join(timeLeft);
    } catch (Exception ignore) {
    }

    this.exitCode = 0;
    int retryCount = 9;
    while (retryCount > 0) {
      retryCount--;
      try {
        exitCode = p.exitValue();
        break;
      } catch (IllegalThreadStateException e) {
        // due to bugs in Process we may not be able to get
        // a process's exit value.
        // We can't use Process.waitFor() because it can hang forever
        if (retryCount == 0) {
          if (stderr.linecount > 0) {
            // The process wrote to stderr so manufacture
            // an error exist code
            synchronized (lines) {
              lines.add("Failed to get exit status and it wrote"
                  + " to stderr so setting exit status to 1.");
            }
            exitCode = 1;
          }
        } else {
          // We need to wait around to give a chance for
          // the child to be reaped.See bug 19682
          try {
            Thread.sleep(1000);
          } catch (InterruptedException ignore) {
          }
        }
      }
    }

    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);

    synchronized (lines) {
      for (String line : lines) {
        pw.println(line);
      }
    }

    pw.close();
    
    try {
      sw.close();
    } catch (IOException ignore) {
      // Nothing useful to do here.
    }

    StringBuffer buf = sw.getBuffer();
    if (buf != null && buf.length() > 0) {
      this.output = sw.toString();
    } else {
      this.output = "";
    }
  }

  public int getExitCode() {
    return exitCode;
  }

  public String getOutput() {
    return output;
  }
}
