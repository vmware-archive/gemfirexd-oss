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
package sql.poc.useCase2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Logger;

import com.gemstone.gnu.trove.THashMap;
import com.gemstone.gnu.trove.TObjectHashingStrategy;
import com.gemstone.gnu.trove.TObjectProcedure;

import com.pivotal.gemfirexd.procedure.IncomingResultSet;
import com.pivotal.gemfirexd.procedure.ProcedureProcessorContext;
import com.pivotal.gemfirexd.procedure.ProcedureResultProcessor;

public final class LISTAGGPROCESSOR implements ProcedureResultProcessor {

  private ProcedureProcessorContext context;
  long startTime;
  THashMap aggResultsMap;
  ColumnDef[] metaRow;
  int numGroupByCols;
  int numCols;
  private Object[] newValues;
  private Object singleNewValue;
  private static final Object[] zeroResults = new Object[0];
  private int newIndex;
  static final Logger logger = Logger.getLogger("com.pivotal.gemfirexd");

  private static final String DEFAULT_DELIM = ",";

  static {
    logger.config("LISTAGGRESULT: Using version: " + ListAggProcedure.version);
    logger.config("LISTAGGRESULT: Initializing the ListAggResultProcessor");
  }

  @Override
  public void close() {
    //logger.config("Closing result set processor");
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
    if (this.singleNewValue == null && this.newValues == null) {
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
      this.aggResultsMap = new THashMap(hashingStrategy);
      // tm = new TreeMap<String,ResultRow>();
      startTime = System.currentTimeMillis();
      organizeResults(this.context.getIncomingResultSets(0));
      // TODO [sumedh] Why is this sorting required? Is there an ORDER BY
      // also assumed on the same columns as GROUP BY?
      // optimize for the common case of single row
      final int size = this.aggResultsMap.size();
      if (size == 1) {
        this.singleNewValue = this.aggResultsMap.getFirstKey();
      }
      else if (size == 0) {
        this.newValues = zeroResults;
      }
      else {
        // logger.config("LISTAGGPROC:Before Sort:"
        // + (System.currentTimeMillis() - startTime));
        final Comparator<Object> cmp = new Comparator<Object>() {

          @SuppressWarnings("unchecked")
          @Override
          public int compare(Object o1, Object o2) {
            ArrayList<Object> row1 = (ArrayList<Object>)o1;
            ArrayList<Object> row2 = (ArrayList<Object>)o2;
            for (int i = 0; i < numGroupByCols; i++) {
              Object col1 = row1.get(i);
              Object col2 = row2.get(i);
              int c;
              if (col1 == null && col2 != null) {
                return metaRow[i].sortOrder != ColumnDef.DESC ? -1 : 1;
              }
              else if ((c = ((Comparable<Object>)col1).compareTo(col2)) != 0) {
                return metaRow[i].sortOrder != ColumnDef.DESC ? c : -c;
              }
            }
            return 0;
          }
        };
        final Object[] values = new Object[this.aggResultsMap.size()];
        this.aggResultsMap.forEachKey(new TObjectProcedure() {
          private int index = 0;

          @Override
          public boolean execute(Object o) {
            values[this.index++] = o;
            return true;
          }
        });
        // TODO: PERF: if ProcedureSender.ORDERED_RESULTS is true then
        // do a merge instead of a full sort again
        Arrays.sort(values, cmp);
        this.newValues = values;
        this.aggResultsMap = null;
        // logger.config("LISTAGGPROC:After Sort:"
        // + (System.currentTimeMillis() - startTime));
      }
    }

    // now return all the rows
    if (this.singleNewValue != null) {
      if (this.newIndex == 0) {
        // convert aggregate columns to values separated by given delimiter
        @SuppressWarnings("unchecked")
        final List<Object> row = (List<Object>)this.singleNewValue;
        for (int i = this.numGroupByCols; i < this.numCols; i++) {
          @SuppressWarnings("unchecked")
          List<Object> agg = (List<Object>)row.get(i);
          row.set(i, convertToCSV(agg, this.metaRow[i].delimiter));
        }
        this.newIndex++;
        return row;
      }
      else {
        return null;
      }
    }
    else if (this.newIndex < this.newValues.length) {
      // convert aggregate columns to values separated by given delimiter
      @SuppressWarnings("unchecked")
      final List<Object> row = (List<Object>)this.newValues[this.newIndex];
      for (int i = this.numGroupByCols; i < this.numCols; i++) {
        @SuppressWarnings("unchecked")
        List<Object> agg = (List<Object>)row.get(i);
        row.set(i, convertToCSV(agg, this.metaRow[i].delimiter));
      }
      this.newIndex++;
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
    final int size = agg.size();
    if (size > 1) {
      // TODO [sumedh] Why is the sorting required?
      Object[] aggArray = agg.toArray();
      Arrays.sort(aggArray);
      StringBuilder sb = new StringBuilder();
      Object o = aggArray[0];
      sb.append(o != null ? o.toString() : "NULL");
      for (int i = 1; i < size; i++) {
        o = aggArray[i];
        sb.append(delim).append(o != null ? o.toString() : "NULL");
      }
      return sb.toString();
    }
    else if (size == 1) {
      Object o = agg.get(0);
      return o != null ? o.toString() : "NULL";
    }
    else {
      return "";
    }
  }

  @Override
  public Object[] getOutParameters() throws InterruptedException {
    throw new AssertionError("this procedure has no out parameters");
  }

  @Override
  public void init(ProcedureProcessorContext context) {
    this.context = context;
  }
}
