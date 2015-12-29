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

package com.pivotal.gemfirexd.thrift.common;

import com.pivotal.gemfirexd.thrift.PrepareResult;
import com.pivotal.gemfirexd.thrift.StatementAttrs;

/**
 * Describes a holder for thrift's <code>PrepareResult</code> typically a
 * client-side prepared statement.
 * 
 * @author swale
 * @since gfxd 1.1
 */
public interface PrepareResultHolder {

  /** the SQL string that was prepared */
  public String getSQL();

  /** the statement ID assigned by the current server */
  public int getStatementId();

  /** update the {@link PrepareResult} e.g. after a failover */
  public void updatePrepareResult(PrepareResult pr);

  /** return the statement attributes used when preparing the statement */
  public StatementAttrs getAttributes();
}
