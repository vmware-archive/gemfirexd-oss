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
package com.pivotal.gemfirexd.hadoop.mapred;

import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedResultSet;

/**
 * A container of table row impacted by a create, update or delete operation. An
 * instance of this class will provide user with the operation's metadata and
 * row after the DML operation. This includes operation type, operation
 * timestamp, key, column metadata and new values of the columns.
 * 
 * @author ashvina
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value="NM_SAME_SIMPLE_NAME_AS_SUPERCLASS")
public class Row extends
    com.pivotal.gemfirexd.hadoop.mapreduce.Row {

  public Row() {
    super();
  }

  public Row(EmbedResultSet resultSet) {
    super(resultSet);
  }
}
