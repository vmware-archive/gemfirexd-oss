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
package sql.datagen;

import hydra.Log;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * 
 * @author Rahul Diyewar
 */

public class TableMetaData {
  private String tableName;
  private String csvFileName;
  private int totalRows;
  private int currentRowID;
  private List<String> PKList;
  private List<FKContraint> FKList;
  private List<String> uniqueList;
  private List<ColumnMetaData> columns;

  private Map<Integer, List<Map<String, Object>>> fkCompositeMapping;

  public TableMetaData(String tableName) {
    this.tableName = tableName;
    PKList = new ArrayList<String>();
    FKList = new ArrayList<FKContraint>();
    uniqueList = new ArrayList<String>();
    columns = new ArrayList<ColumnMetaData>();
    currentRowID = 0;
    fkCompositeMapping = new HashMap<Integer, List<Map<String, Object>>>();
  }

  public String getTableName() {
    return tableName;
  }

  public int getTotalRows() {
    return totalRows;
  }

  public void setTotalRows(int totalRows) {
    this.totalRows = totalRows;
  }

  public void setCsvFileName(String csvFileName) {
    this.csvFileName = csvFileName;
  }

  public String getCsvFileName() {
    return csvFileName;
  }

  public List<String> getPKList() {
    return PKList;
  }

  public void addtoPKList(String pKColumn) {
    PKList.add(pKColumn);
  }

  public List<FKContraint> getFKList() {
    return FKList;
  }

  public void addToFKList(FKContraint fKcolumn) {
    FKList.add(fKcolumn);
  }

  public ColumnMetaData getColumnMetaForFKConstraint(FKContraint fk) {
    for (ColumnMetaData col : columns) {
      if (fk.getColumnName().equals(col.getColumnName())) {
        return col;
      }
    }
    return null;
  }

  public ColumnMetaData getColumnMeta(String columnName) {
    for (ColumnMetaData col : columns) {
      if (columnName.equals(col.getColumnName())) {
        return col;
      }
    }
    return null;
  }

  public boolean isCompositeFKColumn(ColumnMetaData column) {
    boolean flag = false;
    String parentTable = ((FKMappedColumn) column.getMappedColumn())
        .getFkParentTable();
    for (FKContraint fk : FKList) {
      if (!column.getColumnName().equals(fk.getColumnName())
          && parentTable.contains(fk.getParentTable())) {
        flag = true;
      }
    }
    return flag;
  }

  public List<String> getUniqueList() {
    return uniqueList;
  }

  public void addToUniqueList(String uniqueCol) {
    uniqueList.add(uniqueCol);
  }

  public List<ColumnMetaData> getColumns() {
    return columns;
  }

  public void addColumns(ColumnMetaData column) {
    this.columns.add(column);
  }

  public int getCurrentRowID() {
    return currentRowID;
  }

  public void increamentCurrentRowID() {
    this.currentRowID++;
  }

  public void addToFKCompositeMap(Map<String, Object> rowData, int tid) {
    if (PKList.size() > 1) {
      List<Map<String, Object>> fkValuesTidMapList = fkCompositeMapping
          .get(tid);
      if (fkValuesTidMapList == null) {
        fkValuesTidMapList = new LinkedList<Map<String, Object>>();
        fkCompositeMapping.put(tid, fkValuesTidMapList);
      }

      boolean isfkParent = true;
      Map rowMap = new HashMap<String, Object>();
      for (String pkCol : PKList) {
        if (!getColumnMeta(pkCol).isFKParent()) {
          isfkParent = false;
        }
        rowMap.put(pkCol, rowData.get(pkCol));
      }
      if (isfkParent) {
        fkValuesTidMapList.add(rowMap);
      }
    }
  }

  public Object getValueFromCokmpositeFK(ColumnMetaData column,
      Map<String, Object> valueMap, int tid) {
    Object value = null;
    if (PKList.size() > 1) {
      List<Map<String, Object>> fkValuesTidMapList = fkCompositeMapping
          .get(tid);
      String pkcol = null;
      for (String pk : PKList) {
        if (valueMap.get(pk) != null) {
          pkcol = pk;
        }
      }
      if (pkcol != null) {
        Object pkval = valueMap.get(pkcol);
        for (Map<String, Object> rowMap : fkValuesTidMapList) {
          if (rowMap.get(pkcol) != null && rowMap.get(pkcol).equals(pkval)) {
            value = rowMap.get(column.getColumnName());
          }
        }
      } else {
        Log.getLogWriter().warning(
            "No pk column in value list for. column="
                + column.getColumnName() + " valueMap=" + valueMap);
      }

    } else {
      throw new RuntimeException("Not Expected to be here."
          + this.getTableName() + " do not have composite PK");
    }
    return value;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(this.getClass().getName()).append("[")
        .append("name=" + tableName).append(",csvFileName=" + csvFileName)
        .append(",totalRows=" + totalRows).append(",PKList=" + PKList)
        .append(",FKList=" + FKList).append(",uniqueList=" + uniqueList)
        .append(",columns=" + columns).append("]");
    return sb.toString();
  }
}
