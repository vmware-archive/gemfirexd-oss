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
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class GFXDBugs {
  private HashMap<String,GFXDBug> bugMap;  // keyed by bugNum

  public GFXDBugs() {
    bugMap = new HashMap<String,GFXDBug>();
  }

  private GFXDBug parseLineIntoBug(String csvLine) {
    GFXDBug gfxdBug = new GFXDBug();
    // IMPROVE_ME: dumb implementation that won't handle
    // commas inside quotes easily.  We cheat because our
    // summary column (which might contain commas) comes last.
    // Expected format:
    // ticket,status,severity,priority,owner,created,summary
    String line[] = csvLine.split(",",7);
    gfxdBug.setBugNum(line[0]);
    gfxdBug.setStatus(line[1]);
    gfxdBug.setSeverity(line[2]);
    gfxdBug.setPriority(line[3]);
    gfxdBug.setOwner(line[4]);
    gfxdBug.setCreationDate(line[5]);
    gfxdBug.setSummary(line[6]);
    return gfxdBug;
  }

  public void loadFromFile(File bugCsvFile) {
    BufferedReader readFile = null;
    try {
      readFile = new BufferedReader(new FileReader(bugCsvFile));

      String aLine = readFile.readLine();
      while (aLine!=null) {
        GFXDBug bug = parseLineIntoBug(aLine);
        bugMap.put(bug.getBugNum(), bug);
        //System.out.println("Remembering s2qa bug from "+bugCsvFile.getAbsolutePath()+" "+bug.getBugNum());
        aLine = readFile.readLine();
      }
      readFile.close();
    } catch (IOException ioe) {
      ioe.printStackTrace(System.out);
    }

  }
  public GFXDBug getBug(String bugNum) {
    return (GFXDBug) bugMap.get(bugNum);
  }
}
