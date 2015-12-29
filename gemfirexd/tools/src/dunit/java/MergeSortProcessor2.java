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
import java.util.Comparator;
import java.util.List;

import com.pivotal.gemfirexd.procedure.IncomingResultSet;
import com.pivotal.gemfirexd.procedure.ProcedureProcessorContext;
import com.pivotal.gemfirexd.procedure.ProcedureResultProcessor;

public class MergeSortProcessor2 implements ProcedureResultProcessor {

  private ProcedureProcessorContext context;

  /**
   * Initialize this processor.
   */
  public void init(ProcedureProcessorContext context) {
    this.context = context;
  }

  /**
   * Provide the out parameters for this procedure to the client as an Object[].
   * 
   * @throws InterruptedException
   *           if interrupted while waiting to receive data.
   */
  public Object[] getOutParameters() throws InterruptedException {
    throw new AssertionError("this procedure has no out parameters");
  }

  /**
   * Provide the next row for result set number resultSetNumber. The processor
   * should do whatever processing is required on the incoming data to provide
   * the next row.
   * 
   * Return the next row of the result set specified by resultSetNumber, or null
   * if there are no more rows in this result set.
   * 
   * @param resultSetNumber
   *          the 1-based result set number for the row being requested
   * @throws InterruptedException
   *           if interrupted while waiting to receive data.
   * 
   * 
   * @throws InterruptedException
   *           if interrupted while waiting to receive data.
   */
  public List<Object> getNextResultRow(int resultSetNumber)
      throws InterruptedException {
    // this procedure deals with only first result set
    assert resultSetNumber == 0: "unexpected resultSetNumber="
        + resultSetNumber;
    IncomingResultSet[] inSets = this.context.getIncomingResultSets(0);
    // getLogWriter().info("KN: inSets length: "+inSets.length);
    List<Object> lesserRow = null;
    Comparator<List<Object>> cmp = getComparator();

    IncomingResultSet setWithLeastRow = null;
    for (IncomingResultSet inSet : inSets) {
      List<Object> nextRow = inSet.waitPeekRow(); // blocks until row is
                                                  // available
      if (nextRow == IncomingResultSet.END_OF_RESULTS) {
        // getLogWriter().info("KN: nextRow: END_OF_RESULTS");
        // no more rows in this incoming results
        continue;
      }
      // getLogWriter().info("KN: lesserRow: "+lesserRow+" nextRow: "+nextRow);
      // find the least row so far
      if (lesserRow == null || cmp.compare(nextRow, lesserRow) <= 0) {
        lesserRow = nextRow;
        setWithLeastRow = inSet;
      }
    }

    if (setWithLeastRow != null) {
      // consume the lesserRow by removing lesserRow from the incoming result
      // set
      List<Object> takeRow = setWithLeastRow.takeRow();
      assert takeRow == lesserRow;
    }

    // if lesserRow is null, then there are no more rows in any incoming
    // results
    return lesserRow;
  }

  /**
   * Called by GemFireXD when this statement is closed.
   */
  public void close() {
    this.context = null;
  }

  /** Return an appropriate Comparator for sorting the rows */
  private Comparator<List<Object>> getComparator() {
    return new Comparator<List<Object>>() {
      public int compare(List<Object> a1, List<Object> a2) {
        if ((Integer)a1.get(1) < (Integer)a2.get(1)) {
          return -1;
        }
        else if ((Integer)a1.get(1) > (Integer)a2.get(1)) {
          return 1;
        }
        return 0;
      }
    };
  }
}
