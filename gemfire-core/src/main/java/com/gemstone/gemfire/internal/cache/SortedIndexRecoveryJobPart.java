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

import java.util.Arrays;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.GemFireIOException;
import com.gemstone.gemfire.cache.DiskAccessException;
import com.gemstone.gemfire.cache.query.IndexMaintenanceException;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * Submit and execute a set of index insert jobs for concurrent index population
 * during recovery, index creation or otherwise. This will batch up jobs and
 * submit to the function executor pool.
 * 
 * @author kneeraj, swale
 * @since gfxd 1.0
 */
final class SortedIndexRecoveryJobPart extends BatchJobControl {

  private final DiskStoreImpl dsi;
  private final SortedIndexContainer indexContainer;
  private final SortedIndexRecoveryJob controller;

  public SortedIndexRecoveryJobPart(GemFireCacheImpl cache, DiskStoreImpl ds,
      CancelCriterion cc, SortedIndexContainer indexContainer,
      SortedIndexRecoveryJob controller) {
    // use the function executor thread pool
    super(cache.getDistributionManager().getFunctionExcecutor(), cache
        .getLoggerI18n(), cc, controller.maxQueuedJobs);
    this.dsi = ds;
    this.indexContainer = indexContainer;
    this.controller = controller;
  }

  public void addJob(SortedIndexKey indexKey, RegionEntry entry) {
    getBatchJob((IndexBulkJob)null).addJob(indexKey, entry);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected BatchJob newJob(BatchJob job) {
    return new IndexBulkJob();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void addJobResult(BatchJob job) {
    IndexBulkJob indexJob = (IndexBulkJob)job;
    this.controller.addMergeJob(null, indexJob.indexKeys, indexJob.submitted);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected RuntimeException getFailure() {
    RuntimeException re = super.getFailure();
    if (re != null) {
      return re;
    }
    re = this.controller.getFailure();
    if (re != null) {
      return re;
    }
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected boolean doLog() {
    return DiskStoreImpl.INDEX_LOAD_DEBUG || logger.fineEnabled();
  }

  final class IndexBulkJob extends BatchJob {

    private final SortedIndexKey[] indexKeys;

    private int submitted;

    static final int BATCH_SIZE = 32768;

    public IndexBulkJob() {
      this.indexKeys = new SortedIndexKey[BATCH_SIZE];
      this.submitted = 0;
    }

    @Override
    public boolean isFull() {
      return submitted >= BATCH_SIZE;
    }

    @Override
    public int numSubmitted() {
      return this.submitted;
    }

    public void addJob(SortedIndexKey indexKey, RegionEntry entry) {
      if (DiskStoreImpl.INDEX_LOAD_DEBUG_FINER) {
        logger.info(LocalizedStrings.DEBUG, String.format(
            "IndexBulkJob#addJob index=%s, indexKey=%s, regionEntry=%s",
            indexContainer, indexKey, entry));
      }
      this.indexKeys[submitted] = indexKey;
      indexKey.setTransientValue(entry);
      submitted++;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void executeJob() throws Exception {
      Arrays.sort(this.indexKeys, 0, this.submitted,
          indexContainer.getComparator());
    }

    @Override
    protected String getCurrentStateString(Throwable t) {
      if (t instanceof IndexMaintenanceException
          || t.getClass().getName().contains("GemFireXDRuntimeException")) {
        // in this case the exception will reflect most of the details
        return "inserting into index " + indexContainer;
      }
      int currentIndex = this.submitted - 1;
      final SortedIndexKey[] indexKeys = this.indexKeys;
      if (indexKeys.length <= currentIndex) {
        currentIndex = indexKeys.length - 1;
      }
      SortedIndexKey currentKey = this.indexKeys[currentIndex];
      return "inserting index key=" + currentKey + ", value="
          + currentKey.getTransientValue() + ", into index " + indexContainer;
    }

    @Override
    protected RuntimeException getFailureException(String reason, Throwable t) {
      final DiskStoreImpl dsi = SortedIndexRecoveryJobPart.this.dsi;
      return dsi != null ? new DiskAccessException(reason, t, dsi)
          : new GemFireIOException(reason, t);
    }
  }
}
