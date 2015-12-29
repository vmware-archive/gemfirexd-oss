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
package com.pivotal.gemfirexd.dataawareprocedure.listAgg;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import com.gemstone.gnu.trove.THashMap;
import com.gemstone.gnu.trove.TObjectHashingStrategy;

import com.pivotal.gemfirexd.procedure.IncomingResultSet;
import com.pivotal.gemfirexd.procedure.ProcedureProcessorContext;
import com.pivotal.gemfirexd.procedure.ProcedureResultProcessor;

public final class LISTAGGPROCESSOR implements ProcedureResultProcessor {

  private ProcedureProcessorContext context;
  int counter = 0;
  long startTime = 0;
  THashMap aggResultsMap;
  ColumnDef[] metaRow;
  int numGroupByCols;
  int numCols;
  private Iterator<List<Object>> newRsItr;
  Logger logger = Logger.getLogger("com.pivotal.gemfirexd");

  private static final String DEFAULT_DELIM = ",";

  @Override
  public void close() {
    logger.config("Closing result set processor");
    this.context = null;
  }

  @Override
  public List<Object> getNextResultRow(int resultSetNumber)
      throws InterruptedException {
    return gatherFancyRow(resultSetNumber);
    /*
    logger.config("LISTAGGRESULT: Start of get Next Result Row. "
        + "ResultsetNumber = " + resultSetNumber);

    List<Object> lesserRow = gatherFancyRow(resultSetNumber);
    logger.config("LISTAGGRESULT: End of get Next Result Row.");

    return lesserRow;
    */
  }

  private List<Object> gatherFancyRow(int resultSetNumber)
      throws InterruptedException {
    assert resultSetNumber == 0: "unexpected resultSetNumber="
        + resultSetNumber;
    if (aggResultsMap == null) {
      // This map will be a list of rows against
      // a group by key
      /** this hashing strategy simply compares the group by columns of a row */
      final TObjectHashingStrategy hashingStrategy =
        new TObjectHashingStrategy() {

        private static final long serialVersionUID = 1L;

        @SuppressWarnings("unchecked")
        @Override
        public boolean equals(Object o1, Object o2) {
          ArrayList<Object> row1 = (ArrayList<Object>)o1;
          ArrayList<Object> row2 = (ArrayList<Object>)o2;
          for (int i = 0; i < numGroupByCols; i++) {
            Object col1 = row1.get(i);
            Object col2 = row2.get(i);
            if ((col1 == null && col2 != null) || !col1.equals(col2)) {
              return false;
            }
          }
          return true;
        }

        @Override
        public int computeHashCode(Object o) {
          int h = 0;
          @SuppressWarnings("unchecked")
          ArrayList<Object> row = (ArrayList<Object>)o;
          for (int i = 0; i < numGroupByCols; i++) {
            Object col = row.get(i);
            if (col != null) {
              h ^= col.hashCode();
            }
          }
          return h;
        }
      };
      aggResultsMap = new THashMap(hashingStrategy);
      // tm = new TreeMap<String,ResultRow>();
      startTime = System.currentTimeMillis();
      organizeResults(this.context.getIncomingResultSets(0));
      logger.config("LISTAGGPROC:Before Sort:"
          + (System.currentTimeMillis() - startTime));
      // TODO why is this sorting required
      // Collections.sort(aggResults);
      // resultsIter = aggResultsMap.values().iterator();
      @SuppressWarnings("unchecked")
      Iterator<List<Object>> iter = aggResultsMap.values().iterator();
      newRsItr = iter;
    }

    // now return all the rows
    if (newRsItr.hasNext()) {
      // convert aggregate columns to values separated by given delimiter
      List<Object> row = newRsItr.next();
      for (int i = this.numGroupByCols; i < this.numCols; i++) {
        @SuppressWarnings("unchecked")
        List<Object> agg = (List<Object>)row.get(i);
        row.set(i, convertToCSV(agg, this.metaRow[i].delimiter));
      }
      return row;
    }
    else {
      return null;
    }
  }

  private void organizeResults(IncomingResultSet[] inSets)
      throws InterruptedException {
    ArrayList<List<Object>> unprocessedRows = new ArrayList<List<Object>>();
    for (IncomingResultSet inSet : inSets) {
      List<Object> nextRow;

      while ((nextRow = inSet.takeRow()) != IncomingResultSet.END_OF_RESULTS) {
        // check for meta row first
        if (nextRow.get(0) instanceof ColumnDef) {
          // check if we have received a metaRow
          if (this.metaRow == null) {
            @SuppressWarnings({ "unchecked", "rawtypes" })
            ArrayList<ColumnDef> mrow = (ArrayList)nextRow;
            this.numGroupByCols = 0;
            for (ColumnDef col : mrow) {
              if (col.isGroupByCol()) {
                this.numGroupByCols++;
              }
              else if (col.delimiter == null) {
                // use a comma if delimiter is not set
                col.delimiter = DEFAULT_DELIM;
              }
            }
            this.numCols = mrow.size();
            this.metaRow = mrow.toArray(new ColumnDef[this.numCols]);
            // process the unprocessed rows
            if (unprocessedRows.size() > 0) {
              for (List<Object> row : unprocessedRows) {
                processRow(row);
              }
            }
            unprocessedRows = null;
          }
          continue;
        }
        else if (this.metaRow != null) {
          processRow(nextRow);
        }
        else {
          // keep accumulating in unprocessedRows
          unprocessedRows.add(nextRow);
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void processRow(List<Object> nextRow) {
    Object rowsForThisGroupBy = this.aggResultsMap.get(nextRow);
    if (rowsForThisGroupBy == null) {
      this.aggResultsMap.put(nextRow, nextRow);
    }
    else {
      // just append the aggregate column lists
      List<Object> row = (List<Object>)rowsForThisGroupBy;
      for (int i = this.numGroupByCols; i < this.numCols; i++) {
        List<Object> agg1 = (List<Object>)row.get(i);
        List<Object> agg2 = (List<Object>)nextRow.get(i);
        agg1.addAll(agg2);
      }
    }
  }

  static String convertToCSV(List<Object> agg, String delim) {
    StringBuilder sb = new StringBuilder();
    boolean firstCall = true;
    for (Object o : agg) {
      if (!firstCall) {
        sb.append(delim).append(o);
      }
      else {
        firstCall = false;
        sb.append(o);
      }
    }
    return sb.toString();
  }

  @Override
  public Object[] getOutParameters() throws InterruptedException {
    throw new AssertionError("this procedure has no out parameters");
  }

  @Override
  public void init(ProcedureProcessorContext context) {
    this.context = context;
    logger.config("LISTAGGRESULT: Using version:" + ListAggProcedure.version);
    logger.config("LISTAGGRESULT: Initializing the ListAggResultProcessor");
  }
}
