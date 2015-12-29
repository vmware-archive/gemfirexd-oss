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
package com.pivotal.gemfirexd.internal.engine.distributed.metadata;


import com.gemstone.gemfire.internal.cache.AbstractRegion;

import com.pivotal.gemfirexd.internal.engine.ddl.resolver.GfxdPartitionResolver;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.TableDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.DataTypeDescriptor;

/**
 * 
 * @author Asif
 *
 */
public class PrunedExpressionQueryInfo extends AbstractQueryInfo implements AbstractColumnQueryInfo
{
  //private final QueryInfo[] dependentColumns;
  final private AbstractRegion region;
  private final DataTypeDescriptor dtdType;
  private final GfxdPartitionResolver rslvr;
  private final String tableName;
  private final String schemaName;
  private final String canonicalizedString;
  private final TableDescriptor td ;
  public PrunedExpressionQueryInfo(DataTypeDescriptor dtd, AbstractRegion rgn, GfxdPartitionResolver rslvr, String schemaName, String tableName, String canonicalizedStr, TableDescriptor td) throws StandardException {    
    this.region = rgn;   
    this.dtdType = dtd;
    this.rslvr = rslvr;
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.canonicalizedString = canonicalizedStr;
    this.td = td;
    
  } 
  

  public DataTypeDescriptor getType() {
    return this.dtdType;
  }
  
  @Override
  public AbstractRegion getRegion() {
    return this.region;
  }
  
  public boolean isExpression() {
    return true;
  }


  public GfxdPartitionResolver getResolverIfSingleColumnPartition()
  {
    return this.rslvr;
  }

  
  public String getActualColumnName()
  {
    return this.canonicalizedString;
  }


  public int getActualColumnPosition()
  {
   return QueryInfoConstants.EXPRESSION_NODE_COL_POS;
  }

  @Override
  public String getTableName() {
    return this.tableName;
  }

  @Override
  public String getFullTableName() {
    return this.schemaName != null ? (this.schemaName + '.' + this.tableName)
        : this.tableName;
  }

  @Override
  public String getSchemaName() {
    return this.schemaName;
  }

  public TableDescriptor getTableDescriptor() {
    return this.td;
  }

}
