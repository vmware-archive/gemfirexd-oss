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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsFactory;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.StatisticsTypeFactory;
import com.gemstone.gemfire.distributed.internal.DistributionStats;
import com.gemstone.gemfire.internal.StatisticsTypeFactoryImpl;

/**
 * GemFire statistics about a {@link com.gemstone.gemfire.cache.util.Gateway}.
 * These statistics are related to events sent from one <code>Gateway</code>
 * to another.
 *
 * @author Barry Oglesby
 *
 * @since 4.2
 */
public class GatewayStats {

 public static final String typeName = "GatewayStatistics";


  /** The <code>StatisticsType</code> of the statistics */
  private static final StatisticsType _type;

  ////////////////////  Statistic "Id" Fields  ////////////////////

  /** Name of the events queued statistic */
  private static final String EVENTS_QUEUED = "eventsQueued";
  /** Name of the events not queued because conflated statistic */
  private static final String EVENTS_NOT_QUEUED_CONFLATED = "eventsNotQueuedConflated";
  /** Name of the event queue time statistic */
  private static final String EVENT_QUEUE_TIME = "eventQueueTime";
  /** Name of the event queue size statistic */
  private static final String EVENT_QUEUE_SIZE = "eventQueueSize";
  /** Name of the events distributed statistic */
  private static final String EVENTS_DISTRIBUTED = "eventsDistributed";
  /** Name of the events exceeding alert threshold statistic */
  private static final String EVENTS_EXCEEDING_ALERT_THRESHOLD = "eventsExceedingAlertThreshold";
  /** Name of the batch distribution time statistic */
  private static final String BATCH_DISTRIBUTION_TIME = "batchDistributionTime";
  /** Name of the batches distributed statistic */
  private static final String BATCHES_DISTRIBUTED = "batchesDistributed";
  /** Name of the batches redistributed statistic */
  private static final String BATCHES_REDISTRIBUTED = "batchesRedistributed";
  /** Name of the unprocessed events added by primary statistic */
  private static final String UNPROCESSED_TOKENS_ADDED_BY_PRIMARY = "unprocessedTokensAddedByPrimary";
  /** Name of the unprocessed events added by secondary statistic */
  private static final String UNPROCESSED_EVENTS_ADDED_BY_SECONDARY = "unprocessedEventsAddedBySecondary";
  /** Name of the unprocessed events removed by primary statistic */
  private static final String UNPROCESSED_EVENTS_REMOVED_BY_PRIMARY = "unprocessedEventsRemovedByPrimary";
  /** Name of the unprocessed events removed by secondary statistic */
  private static final String UNPROCESSED_TOKENS_REMOVED_BY_SECONDARY = "unprocessedTokensRemovedBySecondary";
  private static final String UNPROCESSED_EVENTS_REMOVED_BY_TIMEOUT = "unprocessedEventsRemovedByTimeout";
  private static final String UNPROCESSED_TOKENS_REMOVED_BY_TIMEOUT = "unprocessedTokensRemovedByTimeout";
  /** Name of the unprocessed events map size statistic */
  private static final String UNPROCESSED_EVENT_MAP_SIZE = "unprocessedEventMapSize";
  private static final String UNPROCESSED_TOKEN_MAP_SIZE = "unprocessedTokenMapSize";
  
  private static final String CONFLATION_INDEXES_MAP_SIZE = "conflationIndexesSize";
  
  private static final String SENT_BYTES = "sentBytes";
  
  
  /** Id of the events queued statistic */
  private static final int _eventsQueuedId;
  /** Id of the events not queued because conflated statistic */
  private static final int _eventsNotQueuedConflatedId;
  /** Id of the event queue time statistic */
  private static final int _eventQueueTimeId;
  /** Id of the event queue size statistic */
  private static final int _eventQueueSizeId;
  /** Id of the events distributed statistic */
  private static final int _eventsDistributedId;
  /** Id of the events exceeding alert threshold statistic */
  private static final int _eventsExceedingAlertThresholdId;
  /** Id of the batch distribution time statistic */
  private static final int _batchDistributionTimeId;
  /** Id of the batches distributed statistic */
  private static final int _batchesDistributedId;
  /** Id of the batches redistributed statistic */
  private static final int _batchesRedistributedId;
  /** Id of the unprocessed events added by primary statistic */
  private static final int _unprocessedTokensAddedByPrimaryId;
  /** Id of the unprocessed events added by secondary statistic */
  private static final int _unprocessedEventsAddedBySecondaryId;
  /** Id of the unprocessed events removed by primary statistic */
  private static final int _unprocessedEventsRemovedByPrimaryId;
  /** Id of the unprocessed events removed by secondary statistic */
  private static final int _unprocessedTokensRemovedBySecondaryId;
  private static final int _unprocessedEventsRemovedByTimeoutId;
  private static final int _unprocessedTokensRemovedByTimeoutId;
  /** Id of the unprocessed events map size statistic */
  private static final int _unprocessedEventMapSizeId;
  private static final int _unprocessedTokenMapSizeId;
  /** Id of the conflation indexes size statistic */
  private static final int _conflationIndexesMapSizeId;
  
  private static final int _sentBytesId;

  /**
   * Static initializer to create and initialize the <code>StatisticsType</code>
   */
  static {

    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();

    _type = f.createType(typeName, typeName,
       new StatisticDescriptor[] {
        f.createIntCounter
        (EVENTS_QUEUED,
         "Number of events added to the event queue.",
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
         (EVENTS_NOT_QUEUED_CONFLATED,
          "Number of events received but not added to the event queue because the queue already contains an event with the event's key.",
          "operations"),
        f.createIntCounter
         (EVENTS_DISTRIBUTED,
          "Number of events removed from the event queue and sent.",
          "operations"),
        f.createIntCounter
         (EVENTS_EXCEEDING_ALERT_THRESHOLD,
          "Number of events exceeding the alert threshold.",
          "operations"),
        f.createLongCounter
         (BATCH_DISTRIBUTION_TIME,
          "Total time spent distributing batches of events to other gateways.",
          "nanoseconds"),
        f.createIntCounter
         (BATCHES_DISTRIBUTED,
          "Number of batches of events removed from the event queue and sent.",
          "operations"),
        f.createIntCounter
         (BATCHES_REDISTRIBUTED,
          "Number of batches of events removed from the event queue and resent.",
          "operations"),
        f.createIntCounter
         (UNPROCESSED_TOKENS_ADDED_BY_PRIMARY,
          "Number of tokens added to the secondary's unprocessed token map by the primary (though a listener).",
          "tokens"),
        f.createIntCounter
         (UNPROCESSED_EVENTS_ADDED_BY_SECONDARY,
          "Number of events added to the secondary's unprocessed event map by the secondary.",
          "events"),
        f.createIntCounter
         (UNPROCESSED_EVENTS_REMOVED_BY_PRIMARY,
          "Number of events removed from the secondary's unprocessed event map by the primary (though a listener).",
          "events"),
        f.createIntCounter
         (UNPROCESSED_TOKENS_REMOVED_BY_SECONDARY,
          "Number of tokens removed from the secondary's unprocessed token map by the secondary.",
          "tokens"),
        f.createIntCounter
         (UNPROCESSED_EVENTS_REMOVED_BY_TIMEOUT,
          "Number of events removed from the secondary's unprocessed event map by a timeout.",
          "events"),
        f.createIntCounter
         (UNPROCESSED_TOKENS_REMOVED_BY_TIMEOUT,
          "Number of tokens removed from the secondary's unprocessed token map by a timeout.",
          "tokens"),
        f.createIntGauge
         (UNPROCESSED_EVENT_MAP_SIZE,
          "Current number of entries in the secondary's unprocessed event map.",
          "events"),
        f.createIntGauge
         (UNPROCESSED_TOKEN_MAP_SIZE,
          "Current number of entries in the secondary's unprocessed token map.",
          "tokens"),
        f.createIntGauge
         (CONFLATION_INDEXES_MAP_SIZE,
          "Current number of entries in the conflation indexes map.",
          "events"),
        f.createLongCounter
          (SENT_BYTES,
           "Total number of bytes sent to the remote site.",
           "bytes"),
    });

    // Initialize id fields
    _eventsQueuedId = _type.nameToId(EVENTS_QUEUED);
    _eventsNotQueuedConflatedId = _type.nameToId(EVENTS_NOT_QUEUED_CONFLATED);
    _eventQueueTimeId = _type.nameToId(EVENT_QUEUE_TIME);
    _eventQueueSizeId = _type.nameToId(EVENT_QUEUE_SIZE);
    _eventsDistributedId = _type.nameToId(EVENTS_DISTRIBUTED);
    _eventsExceedingAlertThresholdId = _type.nameToId(EVENTS_EXCEEDING_ALERT_THRESHOLD);
    _batchDistributionTimeId = _type.nameToId(BATCH_DISTRIBUTION_TIME);
    _batchesDistributedId = _type.nameToId(BATCHES_DISTRIBUTED);
    _batchesRedistributedId = _type.nameToId(BATCHES_REDISTRIBUTED);
    _unprocessedTokensAddedByPrimaryId = _type.nameToId(UNPROCESSED_TOKENS_ADDED_BY_PRIMARY);
    _unprocessedEventsAddedBySecondaryId = _type.nameToId(UNPROCESSED_EVENTS_ADDED_BY_SECONDARY);
    _unprocessedEventsRemovedByPrimaryId = _type.nameToId(UNPROCESSED_EVENTS_REMOVED_BY_PRIMARY);
    _unprocessedTokensRemovedBySecondaryId = _type.nameToId(UNPROCESSED_TOKENS_REMOVED_BY_SECONDARY);
    _unprocessedEventsRemovedByTimeoutId = _type.nameToId(UNPROCESSED_EVENTS_REMOVED_BY_TIMEOUT);
    _unprocessedTokensRemovedByTimeoutId = _type.nameToId(UNPROCESSED_TOKENS_REMOVED_BY_TIMEOUT);
    _unprocessedEventMapSizeId = _type.nameToId(UNPROCESSED_EVENT_MAP_SIZE);
    _unprocessedTokenMapSizeId = _type.nameToId(UNPROCESSED_TOKEN_MAP_SIZE); 
    _conflationIndexesMapSizeId = _type.nameToId(CONFLATION_INDEXES_MAP_SIZE); 
    _sentBytesId = _type.nameToId(SENT_BYTES);
  }

  //////////////////////  Instance Fields  //////////////////////

  /** The <code>Statistics</code> instance to which most behavior is delegated */
  private final Statistics _stats;

  /**
   * The <code><GatewayStats</code> used to provide a roll-up view for parallel
   * <code>Gateway</code>s
   */
  private final GatewayStats _rollupStatistics;
  
  ///////////////////////  Constructors  ///////////////////////

  /**
   * Constructor.
   *
   * @param f The <code>StatisticsFactory</code> which creates the
   * <code>Statistics</code> instance
   * @param gatewayHubId The id of the <code>GatewayHub</code> used to
   * generate the name of the <code>Statistics</code>
   * @param gatewayId The id of the <code>Gateway</code> used to
   * generate the name of the <code>Statistics</code>
   */
  public GatewayStats(StatisticsFactory f, String gatewayHubId, String gatewayId, GatewayStats rollupStatistics) {
    this._stats = f.createAtomicStatistics(_type, "gatewayStats-"+gatewayHubId+"-"+gatewayId);
    this._rollupStatistics = rollupStatistics;
  }

  /////////////////////  Instance Methods  /////////////////////

  /**
   * Closes the <code>GatewayStats</code>.
   */
  public void close() {
    this._stats.close();
  }

  /**
   * Returns whether this <code>GatewayStats</code> is closed.
   * @return whether this <code>GatewayStats</code> is closed
   */
  public boolean isClosed() {
    return this._stats.isClosed();
  }
  
  /**
   * Returns the current value of the "eventsQueued" stat.
   * @return the current value of the "eventsQueued" stat
   */
  public int getEventsQueued() {
    return this._stats.getInt(_eventsQueuedId);
  }

  /**
   * Returns the current value of the "eventsNotQueuedConflated" stat.
   * @return the current value of the "eventsNotQueuedConflated" stat
   */
  public int getEventsNotQueuedConflated() {
    return this._stats.getInt(_eventsNotQueuedConflatedId);
  }

  /**
   * Returns the current value of the "eventQueueSize" stat.
   * @return the current value of the "eventQueueSize" stat
   */
  public int getEventQueueSize() {
    return this._stats.getInt(_eventQueueSizeId);
  }

  /**
   * Returns the current value of the "eventsDistributed" stat.
   * @return the current value of the "eventsDistributed" stat
   */
  public int getEventsDistributed() {
    return this._stats.getInt(_eventsDistributedId);
  }

  /**
   * Returns the current value of the "eventsExceedingAlertThreshold" stat.
   * @return the current value of the "eventsExceedingAlertThreshold" stat
   */
  public int getEventsExceedingAlertThreshold() {
    return this._stats.getInt(_eventsExceedingAlertThresholdId);
  }

  /**
   * Increments the value of the "eventsExceedingAlertThreshold" stat by 1.
   */
  public void incEventsExceedingAlertThreshold() {
    this._stats.incInt(_eventsExceedingAlertThresholdId, 1);
    if (this._rollupStatistics != null) {
      this._rollupStatistics.incEventsExceedingAlertThreshold();
    }
  }

  /**
   * Returns the current value of the "batchDistributionTime" stat.
   * @return the current value of the "batchDistributionTime" stat
   */
  public long getBatchDistributionTime() {
    return this._stats.getLong(_batchDistributionTimeId);
  }

  /**
   * Returns the current value of the batchesDistributed" stat.
   * @return the current value of the batchesDistributed" stat
   */
  public int getBatchesDistributed() {
    return this._stats.getInt(_batchesDistributedId);
  }

  /**
   * Returns the current value of the batchesRedistributed" stat.
   * @return the current value of the batchesRedistributed" stat
   */
  public int getBatchesRedistributed() {
    return this._stats.getInt(_batchesRedistributedId);
  }

  /**
   * Increments the value of the "batchesRedistributed" stat by 1.
   */
  public void incBatchesRedistributed() {
    this._stats.incInt(_batchesRedistributedId, 1);
    if (this._rollupStatistics != null) {
      this._rollupStatistics.incBatchesRedistributed();
    }
  }

  /**
   * Sets the "eventQueueSize" stat.
   * @param size The size of the queue
   */
  public void setQueueSize(int size)
  {
    this._stats.setInt(_eventQueueSizeId, size);
    if (this._rollupStatistics != null) {
      this._rollupStatistics.setQueueSize(this._stats.getTextId(), size);
    }
  }
  
  private Map<String,Integer> allQueueSizes = new ConcurrentHashMap<String, Integer>();
  
  private void setQueueSize(String gatewayStatsName, int size) {
    //System.out.println(Thread.currentThread().getName() + ": GatewayStats: Queue size for " + gatewayStatsName + " is " + size);
    //System.out.println(this.allQueueSizes);
    this.allQueueSizes.put(gatewayStatsName, size);
    int allQueuesSize = 0;
    for (Integer queueSize : this.allQueueSizes.values()) {
      allQueuesSize += queueSize;
    }
    setQueueSize(allQueuesSize);
  }

  /**
   * Increments the "eventsNotQueuedConflated" stat.
   */
  public void incEventsNotQueuedConflated()
  {
    this._stats.incInt(_eventsNotQueuedConflatedId, 1);
    if (this._rollupStatistics != null) {
      this._rollupStatistics.incEventsNotQueuedConflated();
    }
  }

  /**
   * Returns the current value of the "unprocessedTokensAddedByPrimary" stat.
   * @return the current value of the "unprocessedTokensAddedByPrimary" stat
   */
  public int getUnprocessedTokensAddedByPrimary() {
    return this._stats.getInt(_unprocessedTokensAddedByPrimaryId);
  }

  /**
   * Returns the current value of the "unprocessedEventsAddedBySecondary" stat.
   * @return the current value of the "unprocessedEventsAddedBySecondary" stat
   */
  public int getUnprocessedEventsAddedBySecondary() {
    return this._stats.getInt(_unprocessedEventsAddedBySecondaryId);
  }

  /**
   * Returns the current value of the "unprocessedEventsRemovedByPrimary" stat.
   * @return the current value of the "unprocessedEventsRemovedByPrimary" stat
   */
  public int getUnprocessedEventsRemovedByPrimary() {
    return this._stats.getInt(_unprocessedEventsRemovedByPrimaryId);
  }

  /**
   * Returns the current value of the "unprocessedTokensRemovedBySecondary" stat.
   * @return the current value of the "unprocessedTokensRemovedBySecondary" stat
   */
  public int getUnprocessedTokensRemovedBySecondary() {
    return this._stats.getInt(_unprocessedTokensRemovedBySecondaryId);
  }

  /**
   * Returns the current value of the "unprocessedEventMapSize" stat.
   * @return the current value of the "unprocessedEventMapSize" stat
   */
  public int getUnprocessedEventMapSize() {
    return this._stats.getInt(_unprocessedEventMapSizeId);
  }
  public int getUnprocessedTokenMapSize() {
    return this._stats.getInt(_unprocessedTokenMapSizeId);
  }

  /**
   * Increments the value of the "unprocessedTokensAddedByPrimary" stat by 1.
   */
  public void incUnprocessedTokensAddedByPrimary() {
    this._stats.incInt(_unprocessedTokensAddedByPrimaryId, 1);
    incUnprocessedTokenMapSize();
    if (this._rollupStatistics != null) {
      this._rollupStatistics.incUnprocessedTokensAddedByPrimary();
    }
  }

  /**
   * Increments the value of the "unprocessedEventsAddedBySecondary" stat by 1.
   */
  public void incUnprocessedEventsAddedBySecondary() {
    this._stats.incInt(_unprocessedEventsAddedBySecondaryId, 1);
    incUnprocessedEventMapSize();
    if (this._rollupStatistics != null) {
      this._rollupStatistics.incUnprocessedEventsAddedBySecondary();
    }
  }

  /**
   * Increments the value of the "unprocessedEventsRemovedByPrimary" stat by 1.
   */
  public void incUnprocessedEventsRemovedByPrimary() {
    this._stats.incInt(_unprocessedEventsRemovedByPrimaryId, 1);
    decUnprocessedEventMapSize();
    if (this._rollupStatistics != null) {
      this._rollupStatistics.incUnprocessedEventsRemovedByPrimary();
    }
  }

  /**
   * Increments the value of the "unprocessedTokensRemovedBySecondary" stat by 1.
   */
  public void incUnprocessedTokensRemovedBySecondary() {
    this._stats.incInt(_unprocessedTokensRemovedBySecondaryId, 1);
    decUnprocessedTokenMapSize();
    if (this._rollupStatistics != null) {
      this._rollupStatistics.incUnprocessedTokensRemovedBySecondary();
    }
  }
  public void incUnprocessedEventsRemovedByTimeout(int count) {
    this._stats.incInt(_unprocessedEventsRemovedByTimeoutId, count);
    decUnprocessedEventMapSize(count);
    if (this._rollupStatistics != null) {
      this._rollupStatistics.incUnprocessedEventsRemovedByTimeout(count);
    }
  }
  public void incUnprocessedTokensRemovedByTimeout(int count) {
    this._stats.incInt(_unprocessedTokensRemovedByTimeoutId, count);
    decUnprocessedTokenMapSize(count);
    if (this._rollupStatistics != null) {
      this._rollupStatistics.incUnprocessedTokensRemovedByTimeout(count);
    }
  }

  /**
   * Sets the "unprocessedEventMapSize" stat.
   */
  public void clearUnprocessedMaps() {
    this._stats.setInt(_unprocessedEventMapSizeId, 0);
    this._stats.setInt(_unprocessedTokenMapSizeId, 0);
    if (this._rollupStatistics != null) {
      this._rollupStatistics.clearUnprocessedMaps();
    }
  }
  private void incUnprocessedEventMapSize() {
    this._stats.incInt(_unprocessedEventMapSizeId, 1);
    if (this._rollupStatistics != null) {
      this._rollupStatistics.incUnprocessedEventMapSize();
    }
  }
  private void decUnprocessedEventMapSize() {
    this._stats.incInt(_unprocessedEventMapSizeId, -1);
    if (this._rollupStatistics != null) {
      this._rollupStatistics.decUnprocessedEventMapSize();
    }
  }
  private void decUnprocessedEventMapSize(int decCount) {
    this._stats.incInt(_unprocessedEventMapSizeId, -decCount);
    if (this._rollupStatistics != null) {
      this._rollupStatistics.decUnprocessedEventMapSize(decCount);
    }
  }
  private void incUnprocessedTokenMapSize() {
    this._stats.incInt(_unprocessedTokenMapSizeId, 1);
    if (this._rollupStatistics != null) {
      this._rollupStatistics.incUnprocessedTokenMapSize();
    }
  }
  private void decUnprocessedTokenMapSize() {
    this._stats.incInt(_unprocessedTokenMapSizeId, -1);
    if (this._rollupStatistics != null) {
      this._rollupStatistics.decUnprocessedTokenMapSize();
    }
  }
  private void decUnprocessedTokenMapSize(int decCount) {
    this._stats.incInt(_unprocessedTokenMapSizeId, -decCount);
    if (this._rollupStatistics != null) {
      this._rollupStatistics.decUnprocessedTokenMapSize(decCount);
    }
  }
   
  /**
   * Increments the value of the "conflationIndexesMapSize" stat by 1
   */
  public void incConflationIndexesMapSize() {
    this._stats.incInt(_conflationIndexesMapSizeId, 1);
    if (this._rollupStatistics != null) {
      this._rollupStatistics.incConflationIndexesMapSize();
    }
  }
   
  /**
   * Decrements the value of the "conflationIndexesMapSize" stat by 1
   */
  public void decConflationIndexesMapSize() {
    this._stats.incInt(_conflationIndexesMapSizeId, -1);
    if (this._rollupStatistics != null) {
      this._rollupStatistics.decConflationIndexesMapSize();
    }
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
   * Increments the "eventsDistributed" and "batchDistributionTime" stats.
   * @param start The start of the batch (which is decremented from the current
   * time to determine the batch processing time).
   * @param numberOfEvents The number of events to add to the events
   * distributed stat
   */
  public void endBatch(long start, int numberOfEvents)
  {
    long end = DistributionStats.getStatTime();
    endBatch(start, end, numberOfEvents);
    
    if (this._rollupStatistics != null) {
      this._rollupStatistics.endBatch(start, end, numberOfEvents);
    }
  }
  
  private void endBatch(long start, long end, int numberOfEvents)
  {
    // Increment number of batches distributed
    this._stats.incInt(_batchesDistributedId, 1);

    // Increment number of events distributed
    this._stats.incInt(_eventsDistributedId, numberOfEvents);

    // Increment batch distribution time
    long elapsed = end-start;
    this._stats.incLong(_batchDistributionTimeId, elapsed);
  }

  /**
   * Increments the "eventsQueued" and "eventQueueTime" stats.
   * @param start The start of the put (which is decremented from the current
   * time to determine the queue processing time).
   */
  public void endPut(long start)
  {
    long end = DistributionStats.getStatTime();
    endPut(start, end);
    
    if (this._rollupStatistics != null) {
      this._rollupStatistics.endPut(start, end);
    }
  }
  
  private void endPut(long start, long end)
  {
    // Increment number of event queued
    this._stats.incInt(_eventsQueuedId, 1);

    // Increment event queue time
    long elapsed = end-start;
    this._stats.incLong(_eventQueueTimeId, elapsed);
  }
  
  public static int getEventQueueSizeId() {
    return _eventQueueSizeId;
  }

  public void incSentBytes(long v) {
    this._stats.incLong(_sentBytesId, v);
    if (this._rollupStatistics != null) {
      this._rollupStatistics.incSentBytes(v);
    }
  }
}
