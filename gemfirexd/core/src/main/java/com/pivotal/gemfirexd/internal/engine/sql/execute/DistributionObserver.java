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
package com.pivotal.gemfirexd.internal.engine.sql.execute;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverAdapter;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.impl.sql.execute.xplain.XPLAINUtil;

/**
 * Observer that gets attached transiently via
 * {@link AbstractGemFireDistributionActivation} to record distribution related
 * data.
 * <p>
 * We cannot keep statistics in ThreadLocal for a longer duration because a
 * query node resultSet can be consumed by different threads than thread opened
 * it. So, it will accumulate & kept in either {@link AbstractGemFireResultSet}
 * or {@link AbstractGemFireDistributionActivation}
 * <p>
 * Any data captured in this class should have minimal overhead as it is
 * conditionally used just for timing/aggregation purpose only.
 * 
 * @author soubhikc
 * 
 */
public final class DistributionObserver extends GemFireXDQueryObserverAdapter {

  private static final long serialVersionUID = -3504542001869619050L;

  private static final DistributionObserver _theObserver;
  
  private static final AtomicInteger refCount = new AtomicInteger(0);
  
  private volatile boolean deleteMode = false;
  
  static {
    _theObserver = new DistributionObserver();
  }

  private DistributionObserver() {
  }

  public final static void setObserver() {
    if (refCount.incrementAndGet() > 0) {
      if (GemFireXDQueryObserverHolder.getInstance() == null || _theObserver.deleteMode) {
        synchronized (_theObserver) {
          if (GemFireXDQueryObserverHolder.getInstance() == null) {
            GemFireXDQueryObserverHolder.putInstanceIfAbsent(_theObserver);
          }
        }
      }
    }
  }
  
  public final static void unsetObserver() {
    final int existingCount = refCount.decrementAndGet();
    if ( existingCount < 1) {
      if (GemFireXDQueryObserverHolder.getInstance() != null) {
        synchronized (_theObserver) {
          if (GemFireXDQueryObserverHolder.getInstance() != null) {
            if (refCount.get() == existingCount) {
              try {
                _theObserver.deleteMode = true;
                GemFireXDQueryObserverHolder.removeObserver(DistributionObserver.class);
              }
              finally {
                _theObserver.deleteMode = false;
              }
            }
          }
        }
      }
    }
  }
  
  public static void clearStatics() {
    refCount.set(0);
  }
  
  private static Object[] popData() {
    final Object[] ret = dataPoints.get();
    dataPoints.remove();
    return ret;
  }

  @Override
  public void reset() {
  }

  @SuppressWarnings("unused")
  private static void clean() {
    final Object[] stat = dataPoints.get();

    final GlobalIndexStat gbs = (GlobalIndexStat)stat[StatObjects.GLOBAL_INDEX
        .ordinal()];
    gbs.release();
  }

  // implementation variables.

  /*
   * this will capture either non-singleton object or just the statistics data.
   * e.g. resolver, global index will capture per function artifacts for query plan.
   * 
   * TODO:mm: further fine tune to allocate object just for query plan & not other types.
   */
  private static ThreadLocal<Object[]> dataPoints = new ThreadLocal<Object[]>() {
    @Override
    public Object[] initialValue() {
      final Object[] val = new Object[StatObjects.values().length];
      val[StatObjects.GLOBAL_INDEX.ordinal()] = new GlobalIndexStat();
      return val;
    }
  };

  public static enum StatObjects {
    GLOBAL_INDEX,
  }

  public static class GlobalIndexStat {

    String indexName;

    int[] baseColPos;

    short numOpens;

    long esitmatedRowCount;

    long esitmatedCost;

    long seekTime;

    Serializable lookupKey;

    Object result;

    void release() {
      indexName = null;
      baseColPos = null;
      numOpens = 0;
      esitmatedRowCount = 0;
      esitmatedCost = 0;
      seekTime = 0;
      lookupKey = null;
      result = null;
    }
  }

  public static class ResultHolderStat {

    void release() {
    }
  }
  

  private boolean statisticsTimingOn = false;

  @Override
  public void beforeGemFireActivationCreate(
      final AbstractGemFireActivation ac) {
    statisticsTimingOn = ac.getLanguageConnectionContext()
        .getStatisticsTiming();

    if (statisticsTimingOn) {
      ac.constructorTime = XPLAINUtil.nanoTime();
    }
  }

  @Override
  public void afterGemFireActivationCreate(
      final AbstractGemFireActivation ac) {
    if (statisticsTimingOn) {
      ac.constructorTime = XPLAINUtil.nanoTime() - ac.constructorTime;
    }
  }

  @Override
  public void beforeGlobalIndexLookup(
      final LanguageConnectionContext lcc,
      final PartitionedRegion indexRegion,
      final Serializable indexKey) {

    final Object[] stat = dataPoints.get();
    final GlobalIndexStat gbs = (GlobalIndexStat)stat[StatObjects.GLOBAL_INDEX
        .ordinal()];
    final Object o = indexRegion.getUserAttribute();
    assert o instanceof GemFireContainer;
    final GemFireContainer c = ((GemFireContainer)o);
    gbs.indexName = c.toString();
    gbs.baseColPos = c.getBaseColumnPositions();
    gbs.esitmatedRowCount = c.getNumRows();
    gbs.esitmatedCost = c.getRowSize();
    gbs.lookupKey = indexKey;

    if (statisticsTimingOn) {
      gbs.seekTime = XPLAINUtil.nanoTime();
    }
  }

  @Override
  public void afterGlobalIndexLookup(
      final LanguageConnectionContext lcc,
      final PartitionedRegion indexRegion,
      final Serializable indexKey,
      final Object result) {

    final Object[] stat = dataPoints.get();
    final GlobalIndexStat gbs = (GlobalIndexStat)stat[StatObjects.GLOBAL_INDEX
        .ordinal()];
    gbs.result = result;
    gbs.numOpens++;

    if (statisticsTimingOn) {
      gbs.seekTime = XPLAINUtil.nanoTime() - gbs.seekTime;
    }
  }

  @Override
  public void beforeGemFireResultSetExecuteOnActivation(
      final AbstractGemFireActivation activation) {
  }

  @Override
  public void afterGemFireResultSetExecuteOnActivation(
      final AbstractGemFireActivation activation) {

    if (activation instanceof AbstractGemFireDistributionActivation) {
      ((AbstractGemFireDistributionActivation)activation).observerStatistics = popData();
    }
  }

}
