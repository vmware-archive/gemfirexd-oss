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

package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsFactory;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.StatisticsTypeFactory;
import com.gemstone.gemfire.internal.StatisticsTypeFactoryImpl;
import com.gemstone.gemfire.distributed.internal.DistributionStats;

/**
 * GemFire statistics about a {@link com.gemstone.gemfire.cache.util.GatewayHub}.
 * These statistics are related to events sent from a <code>GatewayHub</code>
 * to its <code>Gateway</code>s.
 *
 * @author Barry Oglesby
 *
 * @since 4.2.1
 */
public class GatewayHubStats {

  /** The <code>StatisticsType</code> of the statistics */
  private static final StatisticsType _type;

  ////////////////////  Statistic "Id" Fields  ////////////////////

  /** Name of the events received statistic */
  private static final String EVENTS_RECEIVED = "eventsReceived";
  /** Name of the event queue time statistic */
  private static final String EVENTS_QUEUED = "eventsQueued";
  /** Name of the events received statistic */
  private static final String EVENT_QUEUE_TIME = "eventQueueTime";
  /** Name of the event queue size statistic */
  private static final String EVENT_QUEUE_SIZE = "eventQueueSize";
  /** Name of the events distributed statistic */
  private static final String EVENTS_PROCESSED = "eventsProcessed";
  /** Name of the number of gateways statistic */
  private static final String NUMBER_OF_GATEWAYS = "numberOfGateways";

  /** Id of the events received statistic */
  private static final int _eventsReceivedId;
  /** Id of the events queued statistic */
  private static final int _eventsQueuedId;
  /** Id of the event queue time statistic */
  private static final int _eventQueueTimeId;
  /** Id of the event queue size statistic */
  private static final int _eventQueueSizeId;
  /** Id of the events processed statistic */
  private static final int _eventsProcessedId;
  /** Id of the number of gateways statistic */
  private static final int _numberOfGatewaysId;

  public static final String typeName = "GatewayHubStatistics";

  /**
   * Static initializer to create and initialize the <code>StatisticsType</code>
   */
  static {
    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();

    _type = f.createType(typeName, typeName,
       new StatisticDescriptor[] {
            f.createIntCounter
            (NUMBER_OF_GATEWAYS,
             "Number of gateways known to this hub.",
             "operations"),

            f.createIntCounter
            (EVENTS_RECEIVED,
             "Number of events received by this hub.",
             "operations"),

            f.createIntCounter
             (EVENTS_QUEUED,
              "Number of events added to the event queue by this hub.",
              "operations"),

            f.createLongCounter
             (EVENT_QUEUE_TIME,
              "Total time spent queueing events.",
              "nanoseconds"),

            f.createIntGauge
             (EVENT_QUEUE_SIZE,
              "Size of the event queue.",
              "operations"),

            f.createIntCounter
             (EVENTS_PROCESSED,
              "Number of events removed from the event queue and processed by this hub.",
              "operations"),
       });

    // Initialize id fields
    _eventsReceivedId = _type.nameToId(EVENTS_RECEIVED);
    _eventsQueuedId = _type.nameToId(EVENTS_QUEUED);
    _eventQueueTimeId = _type.nameToId(EVENT_QUEUE_TIME);
    _eventQueueSizeId = _type.nameToId(EVENT_QUEUE_SIZE);
    _eventsProcessedId = _type.nameToId(EVENTS_PROCESSED);
    _numberOfGatewaysId = _type.nameToId(NUMBER_OF_GATEWAYS);
  }

  //////////////////////  Instance Fields  //////////////////////

  /** The <code>Statistics</code> instance to which most behavior is delegated */
  private final Statistics _stats;

  ///////////////////////  Constructors  ///////////////////////

  /**
   * Constructor.
   *
   * @param f The <code>StatisticsFactory</code> which creates the
   * <code>Statistics</code> instance
   * @param gatewayHubId The id of the <code>GatewayHub</code> used to
   * generate the name of the <code>Statistics</code>
   */
  public GatewayHubStats(StatisticsFactory f, String gatewayHubId) {
    this._stats = f.createAtomicStatistics(_type, "gatewayHubStats-"+gatewayHubId);
  }

  /////////////////////  Instance Methods  /////////////////////

  /**
   * Closes the <code>GatewayHubStats</code>.
   */
  public void close() {
    this._stats.close();
  }

  /**
   * Returns whether this <code>GatewayHubStats</code> is closed.
   * @return whether this <code>GatewayHubStats</code> is closed
   */
  public boolean isClosed() {
    return this._stats.isClosed();
  }

  /**
   * Returns the current value of the "numberOfGateways" stat.
   * @return the current value of the "numberOfGateways" stat
   */
  public int getNumberOfGateways() {
    return this._stats.getInt(_numberOfGatewaysId);
  }

  /**
   * Returns the current value of the "eventsReceived" stat.
   * @return the current value of the "eventsReceived" stat
   */
  public int getEventsReceived() {
    return this._stats.getInt(_eventsReceivedId);
  }

  /**
   * Returns the current value of the "eventsQueued" stat.
   * @return the current value of the "eventsQueued" stat
   */
  public int getEventsQueued() {
    return this._stats.getInt(_eventsQueuedId);
  }

  /**
   * Returns the current value of the "eventQueueSize" stat.
   * @return the current value of the "eventQueueSize" stat
   */
  public int getEventQueueSize() {
    return this._stats.getInt(_eventQueueSizeId);
  }

  /**
   * Returns the current value of the eventsProcessed" stat.
   * @return the current value of the eventsProcessed" stat
   */
  public int getEventsProcessed() {
    return this._stats.getInt(_eventsProcessedId);
  }

  /**
   * Sets the "eventQueueSize" stat.
   * @param size The size of the queue
   */
  public void setQueueSize(int size)
  {
    this._stats.setInt(_eventQueueSizeId, size);
  }

  /**
   * Increments the number of gateways by 1.
   */
  public void incNumberOfGateways() {
    this._stats.incInt(_numberOfGatewaysId, 1);
  }
  /**
   * Increments the number of gateways by value.
   */
  public void incNumberOfGateways(int value) {
    this._stats.incInt(_numberOfGatewaysId, value);
  }
  
  /**
   * Increments the number of events received by 1.
   */
  public void incEventsReceived() {
    this._stats.incInt(_eventsReceivedId, 1);
  }
  
  /**
   * Increments the number of events processed by 1.
   */
  public void incEventsProcessed() {
    this._stats.incInt(_eventsProcessedId, 1);
  }
  
  /**
   * Returns the current time (ns).
   * @return the current time (ns)
   */
  public long startTime()
  {
    return DistributionStats.getStatTime();
  }

  /**
   * Increments the "eventsQueued" and "eventQueueTime" stats.
   * @param start The start of the put (which is decremented from the current
   * time to determine the queue processing time).
   */
  public void endPut(long start)
  {
    long ts = DistributionStats.getStatTime();

    // Increment number of event queued
    this._stats.incInt(_eventsQueuedId, 1);

    // Increment event queue time
    long elapsed = ts-start;
    this._stats.incLong(_eventQueueTimeId, elapsed);
  }
}
