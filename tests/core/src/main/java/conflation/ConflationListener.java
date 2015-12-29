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

package conflation;

import cacheperf.CachePerfStats;
import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.*;
import hydra.*;
import java.util.*;
import objects.*;
import util.TestHelper;

/**
 *  A cache listener used in conflation tests.
 */

public class ConflationListener implements CacheListener {

  boolean validateMonotonic;
  boolean validateStrict;
  CachePerfStats statistics;
  LogWriter log;
  Map timestamps;
  Map values;

  //----------------------------------------------------------------------------
  // Constructors
  //----------------------------------------------------------------------------

  public ConflationListener() {
    this.validateMonotonic = ConflationPrms.validateMonotonic();
    this.validateStrict = ConflationPrms.validateStrict();
    if (this.validateMonotonic || this.validateStrict) {
      this.timestamps = new HashMap();
      this.values = new HashMap();
    }
    this.statistics = CachePerfStats.getInstance();
    this.log = Log.getLogWriter();
  }

  //----------------------------------------------------------------------------
  // CacheListener API
  //----------------------------------------------------------------------------

  public void afterCreate(EntryEvent event) {
    // read the object
    Object key = event.getKey();
    Object value = event.getNewValue();

    // update statistics
    synchronized(ConflationListener.class) {
      this.statistics.incCreateEvents(1);
    }

    // log for debugging
    if (this.log.fineEnabled()) {
      this.log.fine("Got create event on key " + key + "=" + value);
    }

    // validate monotonic or strict as instructed
    if (this.validateMonotonic || this.validateStrict) {

      Integer oldIndex = (Integer)this.values.get(key);
      Integer newIndex = new Integer(ObjectHelper.getIndex(value));
      this.values.put(key, newIndex);

      Long oldTimestamp = (Long)this.timestamps.get(key);
      Long newTimestamp = new Long(ObjectHelper.getTimestamp(value));
      this.timestamps.put(key, newTimestamp);

      // for destroy tests, we will still catch missing destroy events most of
      // the time if the destroy percentage is not too large, since the
      // follow-on create will contain a lower value than the current one
      if (oldIndex != null && newIndex.intValue() < oldIndex.intValue() &&
          newTimestamp.longValue() <= oldTimestamp.longValue()) {
        String s = "Received create on key " + key + " with timestamp " + newTimestamp + " and index " + newIndex + " after timestamp " + oldTimestamp + " and index " + oldIndex;
        ConflationBlackboard.getInstance().getSharedMap()
                            .put(TestHelper.EVENT_ERROR_KEY, s);
        throw new HydraRuntimeException(s);
      }
    }
  }
  public void afterUpdate(EntryEvent event) {
    // read the object
    Object key = event.getKey();
    Object value = event.getNewValue();

    // update statistics
    long elapsed = System.currentTimeMillis()
                 - ((TimestampedObject)value).getTimestamp();
    synchronized(ConflationListener.class) {
      this.statistics.incUpdateLatency(elapsed);
    }

    // log for debugging
    if (this.log.fineEnabled()) {
      this.log.fine("Got update event on key " + key + "=" + value);
    }

    // validate monotonic as instructed
    if (this.validateMonotonic || this.validateStrict) {

      Integer oldIndex = (Integer)this.values.get(key);
      Integer newIndex = new Integer(ObjectHelper.getIndex(value));
      this.values.put(key, newIndex);

      Long oldTimestamp = (Long)this.timestamps.get(key);
      Long newTimestamp = new Long(ObjectHelper.getTimestamp(value));
      this.timestamps.put(key, newTimestamp);

      if (oldIndex != null) {
        if (newTimestamp.longValue() <= oldTimestamp.longValue() &&
            newIndex.intValue() < oldIndex.intValue()) {
          String s = "Received obsolete update on key " + key + " with timestamp " + newTimestamp + " and index " + newIndex + " after timestamp " + oldTimestamp + " and index " + oldIndex;
          ConflationBlackboard.getInstance().getSharedMap()
                              .put(TestHelper.EVENT_ERROR_KEY, s);
          throw new HydraRuntimeException(s);

        } else if (newTimestamp.longValue() == oldTimestamp.longValue() &&
                   newIndex.intValue() == oldIndex.intValue()) {
          String s = "Received duplicate update on key " + key + " with timestamp " + newTimestamp + " and index " + newIndex + " after timestamp " + oldTimestamp + " and index " + oldIndex;
          ConflationBlackboard.getInstance().getSharedMap()
                              .put(TestHelper.EVENT_ERROR_KEY, s);
          throw new HydraRuntimeException(s);
        } else if (this.validateStrict &&
                   newIndex.intValue() != oldIndex.intValue() + 1) {
          String s = "Missing update on key " + key + " with timestamp " + newTimestamp + " and index " + newIndex + " after timestamp " + oldTimestamp + " and index " + oldIndex;
          ConflationBlackboard.getInstance().getSharedMap()
                              .put(TestHelper.EVENT_ERROR_KEY, s);
          throw new HydraRuntimeException(s);
        }
      }
    }

    // sleep as instructed
    int sleepMs = ConflationPrms.getListenerSleepMs();
    if (sleepMs > 0) {
      MasterController.sleepForMs(sleepMs);
    }
  }
  public void afterInvalidate(EntryEvent event) {
  }
  public void afterDestroy(EntryEvent event) {
    // read the object
    Object key = event.getKey();
    Object value = event.getNewValue();

    // update statistics
    synchronized(ConflationListener.class) {
      this.statistics.incDestroyEvents(1);
    }

    // log for debugging
    if (this.log.fineEnabled()) {
      this.log.fine("Got destroy event on key " + key + "=" + value);
    }

    // validate monotonic as instructed
    if (this.validateMonotonic || this.validateStrict) {

      Integer oldIndex = (Integer)this.values.get(key);
      Integer newIndex = null;
      this.values.put(key, newIndex);

      Long oldTimestamp = (Long)this.timestamps.get(key);
      Long newTimestamp = null;
      this.timestamps.put(key, newTimestamp);

      if (oldIndex == null) {
        String s = "Received destroy on key " + key
		 + " when index is already null";
        ConflationBlackboard.getInstance().getSharedMap()
                            .put(TestHelper.EVENT_ERROR_KEY, s);
        throw new HydraRuntimeException(s);
      }
    }
  }
  public void afterRegionClear(RegionEvent event) {
  }
  public void afterRegionCreate(RegionEvent event) {
  }
  public void afterRegionLive(RegionEvent event) {
  }
  public void afterRegionInvalidate(RegionEvent event) {
  }
  public void afterRegionDestroy(RegionEvent event) {
  }
  public void close() {
  }
}
