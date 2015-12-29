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
/*
 * Created on Apr 18, 2005
 *
 * 
 */
package com.gemstone.gemfire.cache.query.internal.index;

import java.util.List;

import com.gemstone.gemfire.cache.query.IndexType;

/**
 * @author asifs
 * 
 * This class contains the information needed to create an index It will
 * contain the callback data between <index></index> invocation
 */
public class IndexCreationData {

  private String name = null;
  private IndexType indexType = null;
  private String fromClause = null;
  private String expression = null;
  private String importStr = null;
  private PartitionedIndex partitionedIndex = null;
  
  public IndexCreationData(String name) {
    this.name = name;
  }

  public void setIndexType(IndexType indexType) {
    this.indexType = indexType;
  }

  public IndexType getIndexType() {
    return this.indexType;
  }

  public void setIndexData(IndexType type, String fromClause, String expression,
      String importStr) {
    this.indexType = type;
    this.fromClause = fromClause;
    this.expression = expression;
    this.importStr = importStr;
  }

  public void setPartitionedIndex(PartitionedIndex index) {
    this.partitionedIndex = index;
  }

  public PartitionedIndex getPartitionedIndex() {
    return this.partitionedIndex;
  }

  public String getIndexFromClause() {
    return this.fromClause;
  }

  public String getIndexExpression() {
    return this.expression;
  }

  public String getIndexImportString() {
    return this.importStr;
  }

  public String getIndexName() {
    return this.name;
  }

}
