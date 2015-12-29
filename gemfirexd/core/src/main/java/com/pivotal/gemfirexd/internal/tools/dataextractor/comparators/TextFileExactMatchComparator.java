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
package com.pivotal.gemfirexd.internal.tools.dataextractor.comparators;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Comparator;

import com.pivotal.gemfirexd.internal.tools.dataextractor.snapshot.GFXDSnapshot;
import com.pivotal.gemfirexd.internal.tools.dataextractor.snapshot.GFXDSnapshotExportStat;

class TextFileExactMatchComparator implements Comparator {
  public int compare(Object arg0, Object arg1) {
    try {
      return compare((GFXDSnapshotExportStat) arg0, (GFXDSnapshotExportStat) arg1);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public int compare(GFXDSnapshotExportStat stat1, GFXDSnapshotExportStat stat2) throws IOException {
    File file1 = new File(stat1.getFileName());
    File file2 = new File(stat2.getFileName());
    if (file1.getName().equals(file2.getName())) {
      if (file1.length() == file2.length()){
        BufferedReader reader = null;
        BufferedReader reader2 = null;
        try {
        reader = new BufferedReader(new FileReader(file1));
        reader2 = new BufferedReader(new FileReader(file2));
        
        String line = reader.readLine();
        String line2 = reader2.readLine();
        while (line != null) {
          if (!line.equals(line2)) {
            return -1;
          }
          line2 = reader2.readLine();
          line = reader.readLine();
        }
        return 0;
        }
        finally {
          if (reader != null) {
            reader.close();
          }
          if (reader2 != null) {
            reader2.close();
          }
        }
      }
    }
    return -1;
  }
}