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

package com.pivotal.gemfirexd.internal.engine.sql.conn;

/**
 * This interface encapsulates the behaviour required to express a change in
 * connection state that needs to be distributed to other nodes.
 * <p>
 * In particular it provides for batching many such changes together, expressing
 * the required batch size and the maximum time to wait for the batch to
 * complete.
 * 
 * @author swale
 */
public interface ConnectionState {

  /**
   * Aggregate changes of another {@link ConnectionState} of compatible type
   * into this one.
   */
  public boolean accumulate(ConnectionState other);

  /**
   * The total number of changes for this connection so far.
   */
  public int numChanges();

  /**
   * The minimum batch size of changes ({@link #numChanges()}) required before
   * distributing to other nodes.
   */
  public int minBatchSize();

  /**
   * The maximum time to wait for {@link #minBatchSize()} to fill.
   */
  public long waitMillis();

  /**
   * Distribute the changes accumulated so far to other nodes as required.
   */
  public void distribute();
}
