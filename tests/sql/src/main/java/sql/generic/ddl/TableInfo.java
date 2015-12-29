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
package sql.generic.ddl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import sql.generic.GenericBBHelper;

public class TableInfo implements java.io.Serializable {
  private static final long serialVersionUID = 1L;

  String tableName;

  SchemaInfo schema;

  Map<String, ColumnInfo> columns;

  List<ColumnInfo> columnList;

  boolean hasConstraints = false;

  PKConstraint primaryKey;

  List<FKConstraint> foreignKeys;

  List<UniqueConstraint> uniqueKeys;

  List<CheckConstraint> checkConstraints;

  String derbyDDL;

  String gfxdDDL;

  int numPrForRecovery;

  TableInfo colocatedParent = null;

  String partitioningClause = "";

  List<ColumnInfo> partitioningColumns;

  String persistClause = "";

  String evictionClause = "";

  String serverGroups = "";
  
  boolean isOffHeap;

  boolean enableConcurrencyCheck = false;

  String hdfsClause = "";

  List<String> asyncEventListnerList = new ArrayList<String>();
  List<String> gatewaySenderList = new ArrayList<String>();

  public TableInfo(String schemaName, String tableName) {
    this.tableName = tableName;
    this.schema = GenericBBHelper.getSchemaInfo(schemaName);
    this.foreignKeys = new ArrayList<FKConstraint>();
    this.uniqueKeys = new ArrayList<UniqueConstraint>();
    this.checkConstraints = new ArrayList<CheckConstraint>();
  }

  public TableInfo(String fullyQualifiedtableName) {
    this(fullyQualifiedtableName.split("\\.")[0], fullyQualifiedtableName
        .split("\\.")[1]);
  }

  public int getNumPrForRecovery() {
    return numPrForRecovery;
  }

  public void setNumPrForRecovery(int numPrForRecovery) {
    this.numPrForRecovery = numPrForRecovery;
  }

  public String getPartitioningClause() {
    return partitioningClause;
  }

  public void setPartitioningClause(String partitioningClause) {
    this.partitioningClause = partitioningClause;
  }

  public List<ColumnInfo> getPartitioningColumns() {
    return partitioningColumns;
  }

  public void setPartitioningColumns(List<ColumnInfo> partitioningColumns) {
    this.partitioningColumns = partitioningColumns;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public SchemaInfo getSchemaInfo() {
    return this.schema;
  }

  public String getSchemaName() {
    return schema.getSchemaName();
  }

  public void setSchemaInfo(SchemaInfo schemaInfo) {
    this.schema = schemaInfo;
  }

  public Map<String, ColumnInfo> getColumns() {
    return columns;
  }

  public void setColumns(Map<String, ColumnInfo> columns) {
    this.columns = columns;
  }

  public ColumnInfo getColumn(String name) {

    if (name.contains(tableName)) {
      name = name.split("\\.")[2];
    }
    return columns.get(name);
  }

  public boolean isHasConstraints() {
    return hasConstraints;
  }

  public void setHasConstraints(boolean hasConstraints) {
    this.hasConstraints = hasConstraints;
  }

  public String getFullyQualifiedTableName() {
    return schema.getSchemaName() + "." + tableName;
  }

  public PKConstraint getPrimaryKey() {
    return primaryKey;
  }

  public void setPrimaryKey(PKConstraint primaryKey) {
    this.primaryKey = primaryKey;
  }

  public List<FKConstraint> getForeignKeys() {
    return Collections.unmodifiableList(foreignKeys);
  }

  public void setForeignKeys(List<FKConstraint> foreignKeys, ConstraintInfoHolder constraintInfoHolder) {
    //List<String> tableinfoList = new ArrayList<String>(constraintInfoHolder.getTablesWithFKConstraint());
    
    List<FKConstraint>  oldFK = new ArrayList<FKConstraint>(this.foreignKeys);
    List <FKConstraint> newFK = new ArrayList<FKConstraint>(foreignKeys);
    
    HashMap<String, List<FKConstraint>> droppedFKMap = new HashMap<String, List<FKConstraint>>(constraintInfoHolder.getDroppedFkList());
    List<FKConstraint> droppedFKs = new ArrayList<FKConstraint>();
    
    if (!droppedFKMap.isEmpty()) {
      droppedFKs = droppedFKMap.get(this.getFullyQualifiedTableName());
      if (droppedFKs == null)
        droppedFKs = new ArrayList<FKConstraint>();
    }
    //drop FK
    if (oldFK.size() > newFK.size()) {
      oldFK.removeAll(newFK);
      droppedFKs.addAll(oldFK);
      droppedFKMap.put(this.getFullyQualifiedTableName(), droppedFKs);
    }
    else { // add FK
      newFK.removeAll(oldFK);
      droppedFKs.removeAll(newFK);
      if (droppedFKs.isEmpty())
        droppedFKMap.remove(this); 
      else 
        droppedFKMap.put(this.getFullyQualifiedTableName(), droppedFKs);
    }
    constraintInfoHolder.setDroppedFkList(droppedFKMap);
    this.foreignKeys = foreignKeys;
  }

  public List<UniqueConstraint> getUniqueKeys() {
    return Collections.unmodifiableList(uniqueKeys);
  }

  public void setUniqueKeys(List<UniqueConstraint> uniqueKeys) {
    this.uniqueKeys = uniqueKeys;
  }

  public List<ColumnInfo> getColumnList() {
    return columnList;
  }

  public String getHdfsClause() {
    return hdfsClause;
  }

  public void setHdfsClause(String hdfsClause) {
    this.hdfsClause = hdfsClause;
  }

  public void setColumnList(List<ColumnInfo> columnList) {
    // generate the Map as well
    this.columnList = columnList;
    columns = new HashMap<String, ColumnInfo>();
    for (ColumnInfo column : columnList) {
      columns.put(column.getColumnName(), column);
    }
  }

  public String getServerGroups() {
    return serverGroups;
  }

  public void setServerGroups(String serverGroups) {
    this.serverGroups = serverGroups;
  }

  public TableInfo getColocatedParent() {
    return colocatedParent;
  }

  public void setColocatedParent(TableInfo colocatedParent) {
    this.colocatedParent = colocatedParent;
  }

  public boolean isOffHeap() {
    return isOffHeap;
  }

  public void setOffHeap(boolean isOffHeap) {
    this.isOffHeap = isOffHeap;
  }

  public String getPersistClause() {
    return persistClause;
  }

  public void setPersistClause(String persistClause) {
    this.persistClause = persistClause;
  }

  public List<String> getAsyncEventListnerList() {
    return asyncEventListnerList;
  }

  public void addAsyncEventListnerToList(String asyncEventListnerName) {
    if (!this.asyncEventListnerList.contains(asyncEventListnerName)) {
      this.asyncEventListnerList.add(asyncEventListnerName);
    }
  }

  public List<String> getGatewaySenderList() {
    return gatewaySenderList;
  }

  public void addGatewaySenderToList(String gatewaySenderName) {
    if (!this.gatewaySenderList.contains(gatewaySenderName)) {
      this.gatewaySenderList.add(gatewaySenderName);
    }
  }
  
  public boolean isEnableConcurrencyCheck() {
    return enableConcurrencyCheck;
  }

  public void setEnableConcurrencyCheck(boolean enableConcurrencyCheck) {
    this.enableConcurrencyCheck = enableConcurrencyCheck;
  }

  public String getEvictionClause() {
    return evictionClause;
  }

  public void setEvictionClause(String evictionClause) {
    this.evictionClause = evictionClause;
  }

  public String toString() {
    StringBuilder finalInfo = new StringBuilder();
    String columnInfo = tableName + " "
        + getLoggingForColumnList(columnList, false);
    String partitionClause = "\n Partitioning Clause :: " + getPartitioningClause();
    // get PK information
    StringBuilder pkInfo = new StringBuilder();
    if (primaryKey != null) {
      pkInfo = new StringBuilder("\n Primary Key: ")
          .append(primaryKey.getConstraintName()).append(" ")
          .append(getLoggingForColumnList(primaryKey.getColumns(), false));
    }
    // get FK information
    StringBuilder fkInfo = new StringBuilder("\n Foreign Keys: ");
    ;
    for (FKConstraint foreignKey : foreignKeys)
      fkInfo.append(foreignKey.getConstraintName()).append(" ")
          .append(getLoggingForColumnList(foreignKey.getColumns(), false))
          .append("\n");

    // get UK information
    StringBuilder ukInfo = new StringBuilder("\n Unique Keys: ");
    for (UniqueConstraint uniqueKey : uniqueKeys)
      ukInfo.append(uniqueKey.getConstraintName()).append(" ")
          .append(getLoggingForColumnList(uniqueKey.getColumns(), false))
          .append("\n");

    // get Check Information
    StringBuilder checkInfo = new StringBuilder("\n Check Constraints: ");
    
    for (CheckConstraint check : checkConstraints)
      checkInfo.append(check.getConstraintName()).append(" ")
          .append(check.definition)
          .append(getLoggingForColumnList(check.getColumns(), false))
          .append(" ").append("\n");

    return finalInfo.append(columnInfo).append(partitionClause).append(pkInfo).append(fkInfo)
        .append(ukInfo).append(checkInfo).toString();
  }

  public String getLoggingForColumnList(List<ColumnInfo> columns,
      boolean valueNeeded) {
    String columnInfo = " \n ColumnInformation [";
    // get columnInformation
    for (ColumnInfo column : columns) {
      columnInfo += column.getColumnName() + ":" + column.getColumnType()
          + " isNull:" + column.isNull() + ",";

      if (valueNeeded) {
        columnInfo += " ValueList: ";

        if (column.getValueList() != null) {
          for (Object Obj : column.getValueList()) {
            columnInfo += " " + Obj + " ";
          }
        }
        else
          columnInfo += column.getValueList();
      }
    }
    columnInfo += "]";
    return columnInfo;
  }

  public SchemaInfo getSchema() {
    return schema;
  }

  public void setSchema(SchemaInfo schema) {
    this.schema = schema;
  }

  public List<CheckConstraint> getCheckConstraints() {
    return Collections.unmodifiableList(checkConstraints);
  }

  public void setCheckconstraints(List<CheckConstraint> checkConstraints) {
    this.checkConstraints = checkConstraints;
  }

  public String getDerbyDDL() {
    return derbyDDL;
  }

  public void setDerbyDDL(String derbyDDL) {
    this.derbyDDL = derbyDDL;
  }

  public static long getSerialVersionUID() {
    return serialVersionUID;
  }

  public String getGfxdDDL() {
    return new CreateTableDDL(this).getDDL();
  }

  public boolean checkUniqIsPartOfPk(UniqueConstraint constraint) {
    List<ColumnInfo> uniqColumns = constraint.getColumns();
    List<ColumnInfo> pkColumns = primaryKey.getColumns();
    boolean partOfPk = true;
    for (ColumnInfo column : pkColumns) {
      if (!uniqColumns.contains(column))
        partOfPk = false;
    }
    return partOfPk;
  }

  @Override
  public boolean equals(Object obj) {
    // TODO Auto-generated method stub    
    if ( obj instanceof TableInfo  && obj != null )  {
       if (  ((TableInfo) obj ).getFullyQualifiedTableName().equals( this.getFullyQualifiedTableName() )) 
         return true;
       else 
         return false;
    }
    else 
      return false;
  }

  @Override
  public int hashCode() {
    // TODO Auto-generated method stub
    return this.getFullyQualifiedTableName().hashCode();
     
  }
}
