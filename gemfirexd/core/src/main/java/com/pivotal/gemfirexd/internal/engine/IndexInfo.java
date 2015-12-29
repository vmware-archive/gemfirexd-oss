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

package com.pivotal.gemfirexd.internal.engine;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.pivotal.gemfirexd.internal.engine.access.MemConglomerate;
import com.pivotal.gemfirexd.internal.engine.access.index.GfxdIndexManager;
import com.pivotal.gemfirexd.internal.engine.access.index.GlobalHashIndex;
import com.pivotal.gemfirexd.internal.engine.access.index.Hash1Index;
import com.pivotal.gemfirexd.internal.engine.access.index.MemIndex;
import com.pivotal.gemfirexd.internal.engine.access.index.SortedMap2Index;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.engine.store.GemFireStore;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultColumnDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.ConglomerateDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.ReferencedKeyConstraintDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.TableDescriptor;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedResultSetMetaData;
import com.pivotal.gemfirexd.internal.impl.jdbc.Util;

/**
 * 
 * @author kneeraj
 */
public final class IndexInfo extends GfxdVTITemplate implements
    GfxdVTITemplateNoAllNodesRoute {

  private ArrayList<Object[]> results;

  private Iterator<Object[]> iterator;

  private Object[] currentRow;

  @Override
  protected Object getObjectForColumn(int columnNumber) throws SQLException {
    return this.currentRow[columnNumber - 1];
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return metadata;
  }

  @Override
  public boolean next() throws SQLException {
    if (this.results == null) {
      this.results = new ArrayList<Object[]>();

      GemFireStore store = Misc.getMemStore();
      for (GemFireContainer baseContainer : store.getAllContainers()) {
        if (baseContainer.isApplicationTable()) {
          String schemaName = baseContainer.getSchemaName();
          String tableName = baseContainer.getTableName();
          TableDescriptor td = baseContainer.getTableDescriptor();
          String[] tableColumns = td.getColumnNamesArray();

          // first add row for primary key
          try {
            ReferencedKeyConstraintDescriptor pkDesc = td.getPrimaryKey();
            if (pkDesc != null) {
              Object[] pkRow = new Object[7];
              pkRow[0] = schemaName;
              pkRow[1] = tableName;
              pkRow[2] = pkDesc.getIndexConglomerateDescriptor(
                  pkDesc.getDataDictionary()).getConglomerateName();
              String colNamesWithAscOrDesc = getColNamesWithAscOrDesc(pkDesc
                  .getColumnDescriptors().getColumnNames(), null);
              pkRow[3] = colNamesWithAscOrDesc;
              pkRow[4] = "UNIQUE";
              pkRow[5] = Boolean.TRUE;
              pkRow[6] = "PRIMARY KEY";
              this.results.add(pkRow);
            }
          } catch (StandardException se) {
            throw Util.generateCsSQLException(se);
          }

          GfxdIndexManager gfxdim = baseContainer.getIndexManager();
          if (gfxdim == null) {
            continue;
          }
          List<GemFireContainer> indexContainers = gfxdim
              .getIndexContainers();
          if (indexContainers != null) {
            List<ConglomerateDescriptor> indexDesc = gfxdim
                .getIndexConglomerateDescriptors();
            if (indexDesc != null) {
              int idx = 0;
              for (ConglomerateDescriptor cd : indexDesc) {
                assert cd.isIndex():
                  "GfxdIndexManager should have index conglomerate descriptors";
                GemFireContainer idxContainer = indexContainers.get(idx);
                assert idxContainer != null: "index container cannot be null";

                String indexName = cd.getConglomerateName();
                String[] columnNames = null;

                int[] baseColumnPositions = cd.getIndexDescriptor()
                    .baseColumnPositions();
                if (baseColumnPositions != null) {
                  columnNames = new String[baseColumnPositions.length];
                  for (int i = 0; i < baseColumnPositions.length; i++) {
                    columnNames[i] = tableColumns[baseColumnPositions[i] - 1];
                  }
                }
                boolean[] ascendingArray = cd.getIndexDescriptor()
                    .isAscending();

                Object[] indexInfoRow = new Object[7];
                indexInfoRow[0] = schemaName;
                indexInfoRow[1] = tableName;
                indexInfoRow[2] = indexName;
                String colNamesWithAscOrDesc = getColNamesWithAscOrDesc(
                    columnNames, ascendingArray);
                indexInfoRow[3] = colNamesWithAscOrDesc;
                indexInfoRow[4] = cd.getIndexDescriptor().isUnique() ? "UNIQUE"
                    : "NOT_UNIQUE";
                MemConglomerate memconglom = idxContainer.getConglomerate();
                assert memconglom instanceof MemIndex:
                  "conglom has to be of type MemIndex";

                final MemIndex index = (MemIndex)memconglom;
                indexInfoRow[5] = Boolean.valueOf(index.caseSensitive());
                indexInfoRow[6] = getIndexType(index);

                this.results.add(indexInfoRow);
                idx++;
              }
            }
          }
        }
      }
      this.iterator = this.results.iterator();
    }
    if (this.iterator != null) {
      if (this.iterator.hasNext()) {
        this.currentRow = this.iterator.next();
        return true;
      }
    }
    return false;
  }

  private String getIndexType(MemIndex midx) {
    if (midx instanceof SortedMap2Index) {
      return "LOCAL:SORTED";
    }

    if (midx instanceof Hash1Index) {
      return "LOCAL:HASH";
    }

    assert midx instanceof GlobalHashIndex: "unknown index of type "
        + midx.getClass() + ": " + midx;
    return "GLOBAL:HASH";
  }

  private String getColNamesWithAscOrDesc(String[] columnNames,
      boolean[] ascArray) {
    if (columnNames == null) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    int len = columnNames.length;
    for (int i = 0; i < len; i++) {
      // Show asc/desc as leading plus/minus sign
      // i.e. "+COL1+COL2-COL3+COL4" like other RDBMS systems.
      sb.append(ascArray == null || ascArray[i] ? '+' : '-');
      sb.append(columnNames[i]);
    }
    return sb.toString();
  }

  public static final String SCHEMA_NAME = "SCHEMANAME";

  public static final String TABLE_NAME = "TABLENAME";

  public static final String INDEX_NAME = "INDEXNAME";

  public static final String COLUMNS = "COLUMNS_AND_ORDER";

  public static final String IS_UNIQUE = "UNIQUE";

  public static final String CASE_SENSITIVE = "CASESENSITIVE";

  public static final String INDEX_TYPE = "INDEXTYPE";

  private static final ResultColumnDescriptor[] columnInfo = {
      EmbedResultSetMetaData.getResultColumnDescriptor(SCHEMA_NAME,
          Types.VARCHAR, false, 128),
      EmbedResultSetMetaData.getResultColumnDescriptor(TABLE_NAME,
          Types.VARCHAR, false, 128),
      EmbedResultSetMetaData.getResultColumnDescriptor(INDEX_NAME,
          Types.VARCHAR, false, 128),
      EmbedResultSetMetaData.getResultColumnDescriptor(COLUMNS, Types.VARCHAR,
          false, 256),
      EmbedResultSetMetaData.getResultColumnDescriptor(IS_UNIQUE,
          Types.VARCHAR, false, 64),
      EmbedResultSetMetaData.getResultColumnDescriptor(CASE_SENSITIVE,
          Types.BOOLEAN, false),
      EmbedResultSetMetaData.getResultColumnDescriptor(INDEX_TYPE,
          Types.VARCHAR, false, 128),

  };

  private static final ResultSetMetaData metadata = new EmbedResultSetMetaData(
      columnInfo);
  
  //to enable this class to be included in gemfirexd.jar
  public static void dummy() {
  }
}
