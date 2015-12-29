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
package com.gemstone.gemfire.internal.cache.xmlcache;

/**
 * @author asifs
 * 
 * This class contains the information needed to create an index It will
 * contain the callback data between <index></index> invocation
 */
class IndexCreationData {

  private String name = null;
  private String indexType = null;
  private String fromClause = null;
  private String expression = null;
  private String importStr = null;

  IndexCreationData(String name) {
    this.name = name;
  }

  void setIndexType(String indexType) {
    this.indexType = indexType;
  }

  String getIndexType() {
    return this.indexType;
  }

  void setFunctionalIndexData(String fromClause, String expression,
      String importStr) {
    this.fromClause = fromClause;
    this.expression = expression;
    this.importStr = importStr;
  }

  void setPrimaryKeyIndexData(String field) {
    this.expression = field;
  }

  String getIndexFromClause() {
    return this.fromClause;
  }

  String getIndexExpression() {
    return this.expression;
  }

  String getIndexImportString() {
    return this.importStr;
  }

  String getIndexName() {
    return this.name;
  }
}
