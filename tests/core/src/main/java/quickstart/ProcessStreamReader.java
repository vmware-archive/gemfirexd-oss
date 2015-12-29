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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Queue;

/**
 * Reads the output from a stream and stores it for test validation. Extracted
 * from ProcessWrapper.
 * 
 * @author Kirk Lund
 */
public class ProcessStreamReader extends Thread {
  
  private final BufferedReader reader;
  private final Queue<String> lineBuffer;
  private final List<String> allLines;

  public int linecount = 0;

  public ProcessStreamReader(InputStream stream, Queue<String> lineBuffer, List<String> allLines) {
    this.reader = new BufferedReader(new InputStreamReader(stream));
    this.lineBuffer = lineBuffer;
    this.allLines = allLines;
  }

  @Override
  public void run() {
    try {
      String line;
      while ((line = reader.readLine()) != null) {
        linecount++;
        lineBuffer.offer(line);
        allLines.add(line);
      }

      // EOF
      reader.close();
    } catch (Exception e) {
      System.out.println("Failure while reading from child process: " + e.getMessage());
    }
  }
}
