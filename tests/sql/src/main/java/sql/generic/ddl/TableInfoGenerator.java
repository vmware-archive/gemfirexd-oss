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

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;

import sql.generic.GenericBBHelper;
import sql.generic.SqlUtilityHelper;

/**
 * TableInfoGenerator
 * 
 * @author Namrata Thanvi
 */

public class TableInfoGenerator {
  Executor executor;

  String fullyQualifiedTableName;

  ResultSet rs;

  ArrayList<GenericTableInfoObjectFetcher> tableInfoObjectFetcherList = new ArrayList<GenericTableInfoObjectFetcher>();

  TableInfo tableInfo;

  ConstraintInfoHolder constraintInfo;

  public TableInfoGenerator(String fullyQualifiedTableName, Executor executor,
      ConstraintInfoHolder constraintInfo) {
    this.executor = executor;
    this.fullyQualifiedTableName = fullyQualifiedTableName.toUpperCase();
    this.tableInfo = new TableInfo(fullyQualifiedTableName);
    this.constraintInfo = constraintInfo;
    register();
  }

  public TableInfoGenerator(TableInfo tableInfo, Executor executor,
      ConstraintInfoHolder constraintInfo) {
    this.executor = executor;
    this.tableInfo = tableInfo;
    this.constraintInfo = constraintInfo;
    register();
  }

  public TableInfo buildTableInfo() {

    for (GenericTableInfoObjectFetcher fetcher : tableInfoObjectFetcherList) {
      fetcher.fetch();
    }
    return tableInfo;

  }

  public TableInfo updateTableInfo() {
    for (GenericTableInfoObjectFetcher fetcher : tableInfoObjectFetcherList) {
      fetcher.fetch();
    }
    return tableInfo;
  }

  public void saveTableInfoToBB() {
    GenericBBHelper.putTableInfo(tableInfo);
    String[] tables = SqlUtilityHelper.getTableNames();
    ArrayList<String> tableList = new ArrayList<String>(
        constraintInfo.getTablesList());
   if(Arrays.asList(tables).contains(tableInfo.getFullyQualifiedTableName())
        && !tableList.contains(tableInfo.getFullyQualifiedTableName())) {
      tableList.add(tableInfo.getFullyQualifiedTableName());
      constraintInfo.setTablesList(tableList);
    }
    GenericBBHelper.putConstraintInfoHolder(constraintInfo);
  }

  private void register() {
    tableInfoObjectFetcherList.add(new ColumnInfoObjectFetcher(tableInfo,
        executor, constraintInfo));
    tableInfoObjectFetcherList.add(new PKInfoObjectFetcher(tableInfo, executor,
        constraintInfo));
    tableInfoObjectFetcherList.add(new FKInfoObjectFetcher(tableInfo, executor,
        constraintInfo));
    tableInfoObjectFetcherList.add(new UniqueKeyInfoObjectFetcher(tableInfo,
        executor, constraintInfo));
    tableInfoObjectFetcherList.add(new CheckInfoObjectFetcher(tableInfo,
        executor, constraintInfo));
  }
}
