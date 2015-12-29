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
/**
 * 
 */
package com.pivotal.gemfirexd.internal.engine.distributed.metadata;

import java.util.List;
import java.util.Set;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.cache.AbstractRegion;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.engine.store.RowFormatter;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultDescription;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.ColumnDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.ColumnDescriptorList;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.TableDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.impl.sql.compile.ResultColumn;
import com.pivotal.gemfirexd.internal.impl.sql.compile.ResultColumnList;

/**
 * This class may be removed once the system has stabilized & all the functions
 * present in QueryInfo have been given meaningful definition
 * 
 * @author Asif
 * 
 */
abstract class AbstractQueryInfo implements QueryInfo {  
  
  public void computeNodes(Set<Object> routingKeys, Activation activation,
      boolean forSingleHopPreparePhase) throws StandardException {
    throw new UnsupportedOperationException(
        "Override or implement the method appropriately");

  }

  public Object getPrimaryKey() throws StandardException {
    throw new UnsupportedOperationException("Override the method appropriately");
  }
  
  public Object getLocalIndexKey() throws StandardException {
    throw new UnsupportedOperationException("Override the method appropriately");
  }
  
  @Override
  public Object getIndexKey() throws StandardException {
    throw new UnsupportedOperationException("Override the method appropriately");
  }

  public boolean isPrimaryKeyBased() throws StandardException {
    throw new UnsupportedOperationException("Override the method appropriately");
  }

  public AbstractRegion getRegion() {
    throw new UnsupportedOperationException("Override the method appropriately");
  }

  public boolean isSelect() {
    throw new UnsupportedOperationException("Override the method appropriately");
  }

  /**
   * For select queries having DSID() in the where clause we will like to
   * distribute those to all nodes with primaries (no "optimizeForWrite") since
   * it is likely for management and not prune to "preferred" read nodes.
   */
  public boolean optimizeForWrite() {
    throw new UnsupportedOperationException("Override the method appropriately");
  }

  public boolean withSecondaries() {
    throw new UnsupportedOperationException("Override the method appropriately");
  }
  
  public boolean isUpdate() {
    throw new UnsupportedOperationException("Override the method appropriately");
  }

  public boolean isDelete() {
    throw new UnsupportedOperationException("Override the method appropriately");
  } 

  public boolean isDeleteWithReferencedKeys() {
    throw new UnsupportedOperationException("Override the method appropriately");
  }

  public String getTableName() {
    throw new UnsupportedOperationException("Override the method appropriately");
  }

  public String getFullTableName() {
    throw new UnsupportedOperationException("Override the method appropriately");
  }

  public String getSchemaName() {
    throw new UnsupportedOperationException("Override the method appropriately");
  }

  public ResultDescription getResultDescription() {
    throw new UnsupportedOperationException("Override the method appropriately");
  }

  public boolean isDynamic() {
    return false;
  }

  public int getParameterCount() {
    throw new UnsupportedOperationException("Override the method appropriately");
  }
  @Override
  public void throwExceptionForInvalidParameterizedData(int validateResult) throws StandardException {
    throw new UnsupportedOperationException("Override the method appropriately");
  }
 
  /*
  public ColumnOrdering[] getColumnOrdering() {
     return null;
  }*/
  
  public boolean isQuery(int flags) {
    throw new UnsupportedOperationException("Override the method appropriately");
  }

  public boolean isQuery(int ... flags) {
    throw new UnsupportedOperationException("Override the method appropriately");
  }
  
  public boolean isSelectForUpdateQuery() {
    return false;
  }
  
  public boolean needKeysForSelectForUpdate() {
    return false;
  }
  
  public boolean isInsert() {
    throw new UnsupportedOperationException("Override the method appropriately");
  }
  
  public boolean createGFEActivation() throws StandardException {
    return false;
  }
  
  public boolean isOuterJoin() {
    throw new UnsupportedOperationException("Override the method appropriately");
  }
  
  public List<Region> getOuterJoinRegions() {
    throw new UnsupportedOperationException("Override the method appropriately");
  }
  
  public void setOuterJoinSpecialCase() {
    throw new UnsupportedOperationException("Override the method appropriately");
  }
  
  public void setInsertAsSubSelect(boolean b, String targetTableName) {
    throw new UnsupportedOperationException("Override the method appropriately");
  }
  
  public boolean isInsertAsSubSelect() {
    return false;
  }
  
  public String getTargetTableName() {
    return null;
  }
  
  public boolean isOuterJoinSpecialCase() {
    throw new UnsupportedOperationException("Override the method appropriately");
  }

  @Override
  public boolean isTableVTI() {
    return false;
  }

  @Override
  public boolean routeQueryToAllNodes() {
    return false;
  }

  @Override
  public boolean isDML() {
    return false;
  }
  
  public void setExplainStatement(ExecRow explainRow) {
    throw new UnsupportedOperationException("Override the method appropriately");
  }
  
  public ExecRow getExplainStatement() {
    throw new UnsupportedOperationException("Override the method appropriately");
  }
  
  public boolean hasUnionNode() {
    return false;
  }

  public boolean hasIntersectOrExceptNode() {
    return false;
  }
  
  /*
   * Is Insert Sub select
   */
  public boolean hasSubSelect() {
    return false;
  }

  @SuppressWarnings("unchecked")
  protected static RowFormatter getRowFormatterFromRCL(
      ResultColumnList expandedRCL, LanguageConnectionContext lcc)
      throws StandardException {
    int columnCount = expandedRCL.size();
    if (columnCount > 0) {
      GemFireContainer container = null;
      TableDescriptor td = null;
      ColumnDescriptorList cdl = new ColumnDescriptorList();
      for (int i = 0; i < columnCount; i++) {
        ResultColumn rc = (ResultColumn)expandedRCL.elementAt(i);
        ColumnDescriptor cd = rc.getTableColumnDescriptor();
        if (cd == null) {
          // try searching by column name
          if (container == null) {
            String tableName = rc.findSourceTableName();
            if (tableName != null) {
              String schemaName = rc.findSourceSchemaName();
              Region<?, ?> r = Misc.getRegionByPath(
                  Misc.getRegionPath(schemaName, tableName, lcc), false);
              if (r != null) {
                container = (GemFireContainer)r.getUserAttribute();
              }
            }
            if (container == null) {
              break;
            }
          }
          if (td == null) {
            td = container.getTableDescriptor();
          }
          if (td != null) {
            String columnName = rc.findSourceColumnName();
            if (columnName != null) {
              cd = td.getColumnDescriptor(columnName);
            }
          }
        }
        else if (container == null) {
          td = cd.getTableDescriptor();
          if (td != null) {
            Region<?, ?> r = Misc.getRegionByPath(td, lcc, false);
            if (r != null) {
              container = (GemFireContainer)r.getUserAttribute();
            }
          }
        }
        if (cd != null && container != null) {
          cdl.add(cd);
        }
        else {
          container = null;
          break;
        }
      }
      if (container != null) {
        // in case column descriptors cover the whole row, then treat it
        // as a full table RowFormatter so that first byte will be read to
        // get the correct schema (this does not distinguish between
        // "select *" vs "select c1, c2, ..." where latter has all columns
        // but that is a very uncommon use-case)
        return new RowFormatter(cdl, container.getSchemaName(),
            container.getTableName(), container.getCurrentSchemaVersion(),
            container, td != null && cdl.size() == td.getNumberOfColumns());
      }
    }
    return null;
  }
}
