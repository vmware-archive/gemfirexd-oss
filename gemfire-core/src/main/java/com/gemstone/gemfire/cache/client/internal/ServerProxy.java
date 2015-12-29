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
package com.gemstone.gemfire.cache.client.internal;

import java.util.List;

import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.i18n.LogWriterI18n;

/**
 * Used to send operations from a client to a server.
 * @author darrel
 * @since 5.7
 */
public class ServerProxy {
  protected final InternalPool pool;
  /**
   * Creates a server proxy for the given pool.
   * @param pool the pool that this proxy will use to communicate with servers
   */
  public ServerProxy(InternalPool pool) {
    this.pool = pool;
  }
  /**
   * Returns the pool the proxy is using.
   */
  public InternalPool getPool() {
    return this.pool;
  }
  public LogWriterI18n getLogger() {
    return getPool().getLoggerI18n();
  }
  /**
   * Release use of this pool
   */
  public void detach() {
    this.pool.detach();
  }
  /**
   * Send a list of gateway events to a server to execute
   * @param events list of gateway events
   * @param batchId the ID of this batch
   * @param earlyAck true if ack should be returned early
   */
  public void dispatchBatch(Connection con, List events, int batchId, boolean earlyAck)
  {
    GatewayBatchOp.executeOn(con, this.pool, events, batchId, earlyAck);
  }

  public void dispatchBatch_NewWAN(Connection con, List events, int batchId, boolean removeFromQueueOnException)
  {
    GatewaySenderBatchOp.executeOn(con, this.pool, events, batchId, removeFromQueueOnException);
  }
  
  public Object receiveAckFromReceiver(Connection con)
  {
    return GatewaySenderBatchOp.executeOn(con, this.pool);
  }

  /**
   * Ping the specified server to see if it is still alive
   * @param server the server to do the execution on
   */
  public void ping(ServerLocation server) {
    PingOp.execute(this.pool, server);
  }
  /**
   * Does a query on a server
   * @param queryPredicate A query language boolean query predicate
   * @return  A <code>SelectResults</code> containing the values
   *            that match the <code>queryPredicate</code>.
   */
  public SelectResults query(String queryPredicate, Object[] queryParams)
  {
    return QueryOp.execute(this.pool, queryPredicate, queryParams);
  }
  
}
