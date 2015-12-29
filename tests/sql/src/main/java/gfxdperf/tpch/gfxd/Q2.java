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
package gfxdperf.tpch.gfxd;

import gfxdperf.tpch.AbstractQ2;
import hydra.Log;

import java.sql.Connection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * @author lynng
 *
 */
public class Q2 extends AbstractQ2 {

  public Q2(Connection conn, Random rng) {
    super();
    this.connection = conn;
    this.rng = rng;
    this.query = this.query + " fetch first 100 rows only";
  }
  
  /** This is called for queries that can hit known bugs; here we look for bug 51263.
   * @param qResults The results of the query executed.
   * @param answerLines The expected results of the query executed.
   * @param missingResults The rows that are missing from qResults.
   * @param extraResults The rows that are extra in qResults.
   * @return A String citing any known bug number.
   */
  @Override
  protected String getKnownBugNumber(List<String> qResults,
      List<String> answerLines, Set<String> missingResults,
      Set<String> extraResults) {
    // look for bug 51263; add the leading space stripped off in bug 51263
    Log.getLogWriter().info("Checking failed Q2 for bug 51263");
    // create a set of modified rows to add back in the leading space that was trimmed off as in 51263
    // this is very specialized for the answer to Q2
    // also, remove the last char of the column (which would be a space for Q2) to keep the column the proper size
    Set<String> modifiedRows = new HashSet<String>();
    for (String row: extraResults) {
      int index = row.lastIndexOf("|");
      if (index > 0) {
        StringBuilder sb = new StringBuilder();
        sb.append(row);
        sb.insert(index+1, " ");
        sb.deleteCharAt(sb.length()-1);
        
        String targetStr = "nluXJCuY1tu";
        index = sb.indexOf(targetStr);
        if (index > 0) {
          sb.insert(index, " ");
          sb.deleteCharAt(index + targetStr.length() + 1);
        }
        modifiedRows.add(sb.toString());
      }
    }
    if (modifiedRows.equals(missingResults)) {
      return "Bug 51263 detected; ";
    }
    return "";
  }
}
