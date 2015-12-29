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
package com.pivotal.gemfirexd.internal.shared.common;

import java.sql.SQLException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.pivotal.gemfirexd.internal.shared.common.error.ExceptionSeverity;
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState;

/**
 * 
 * @author kneeraj
 * 
 */
public final class BoundedLinkedQueue extends LinkedBlockingQueue {

  private static final long serialVersionUID = -1140833386102748319L;

  AtomicInteger createdCnt;

  final int maxQueueSize;

  private final QueueObjectCreator objectCreator;

  final long MAX_POLL_MILLIS = 1000L;

  final int MAX_POLL_TRIES = 360;

  public BoundedLinkedQueue(int capacity, QueueObjectCreator c) {
    super(capacity);
    createdCnt = new AtomicInteger(0);
    maxQueueSize = capacity;
    this.objectCreator = c;
  }

  public void returnBack(Object o) throws InterruptedException {
    this.put(o);
  }

  public Object createOrGetExisting() throws SQLException, InterruptedException {
    int tries = 1;
    for (;;) {
      Object ret = this.poll();
      if (ret != null) {
//        SanityManager.DEBUG_PRINT(SanityManager.TRACE_SINGLE_HOP,
//            "KN: BoundedLinkedQueue::createOrGetExisting returning conn = " + ret + ", created = " + createdCnt);
        return ret;
      }

      int currCnt = createdCnt.intValue();
      if (currCnt >= maxQueueSize) {
        ret = this.poll(MAX_POLL_MILLIS, TimeUnit.MILLISECONDS);
        if (ret != null) {
//          SanityManager.DEBUG_PRINT(SanityManager.TRACE_SINGLE_HOP,
//              "KN: BoundedLinkedQueue2::createOrGetExisting returning conn = " + ret + ", created = " + createdCnt);
          return ret;
        }
        else {
          if (tries++ >= MAX_POLL_TRIES) {
            //throw new SQLException("40XL1" /* SQLState.LOCK_TIMEOUT */);
            throw new SQLException(
                "A connection could not be obtained within the time ("
                    + (MAX_POLL_MILLIS * MAX_POLL_TRIES) + "ms) requested",
                SQLState.LOCK_TIMEOUT, ExceptionSeverity.SESSION_SEVERITY);
          }
          continue;
        }
      }
      else if (this.createdCnt.compareAndSet(currCnt, currCnt + 1)) {
        // if there was an exception in creating the connection itself, then
        // decrement the count and then throw back the exception
        Object conn = null;
        try {
          conn = this.objectCreator.create();
//          SanityManager.DEBUG_PRINT(SanityManager.TRACE_SINGLE_HOP,
//              "KN: BoundedLinkedQueue::createOrGetExisting creating and returning conn = " + ret + ", created = " + createdCnt);
          return conn;
        } finally {
          if (conn == null) {
            this.createdCnt.decrementAndGet();
          }
        }
      }
    }
  }

  public void removeConnection(Object conn) throws InterruptedException {
    this.createdCnt.decrementAndGet();
    remove(conn);
  }
}
