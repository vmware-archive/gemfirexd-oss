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
package com.gemstone.gemfire.cache.hdfs.internal.hoplog;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

/**
 * Function responsible for forcing a compaction on all members
 * of the system
 *
 * @author sbawaska
 */
@SuppressWarnings("serial")
public class HDFSForceCompactionFunction implements Function {

  public static final int FORCE_COMPACTION_MAX_RETRIES = Integer.getInteger("gemfireXD.maxCompactionRetries", 3);

  public static final int BUCKET_ID_FOR_LAST_RESULT = -1;

  public static final String ID = "HDFSForceCompactionFunction";

  @Override
  public void execute(FunctionContext context) {
    RegionFunctionContext rfc = (RegionFunctionContext) context;
    try {
      if (context.isPossibleDuplicate()) {
        // do not re-execute the function, another function
        // targeting the failed buckets will be invoked
        context.getResultSender().lastResult(new CompactionStatus(BUCKET_ID_FOR_LAST_RESULT, false));
        return;
      }
      PartitionedRegion pr = (PartitionedRegion) rfc.getDataSet();
      HDFSForceCompactionArgs args = (HDFSForceCompactionArgs) rfc.getArguments();
      Set<Integer> buckets = new HashSet<Integer>(args.getBuckets()); // copying avoids race when the function coordinator
                                                                      // also runs the function locally
      buckets.retainAll(pr.getDataStore().getAllLocalPrimaryBucketIds());
  
      List<Future<CompactionStatus>> futures =  pr.forceLocalHDFSCompaction(buckets, args.isMajor(), 0);
      int waitFor = args.getMaxWaitTime();
      for (Future<CompactionStatus> future : futures) {
        long start = System.currentTimeMillis();
        CompactionStatus status = null;
        try {
          // TODO use a CompletionService instead
          if (!args.isSynchronous() && waitFor <= 0) {
            break;
          }
          status = args.isSynchronous() ? future.get() : future.get(waitFor, TimeUnit.MILLISECONDS);
          buckets.remove(status.getBucketId());
          if (pr.getLogWriterI18n().fineEnabled()) {
            pr.getLogWriterI18n().fine("HDFS: ForceCompaction sending result:"+status);
          }
          context.getResultSender().sendResult(status);
          long elapsedTime = System.currentTimeMillis() - start;
          waitFor -= elapsedTime;
        } catch (InterruptedException e) {
          // send a list of failed buckets after waiting for all buckets
        } catch (ExecutionException e) {
          // send a list of failed buckets after waiting for all buckets
        } catch (TimeoutException e) {
          // do not wait for other buckets to complete
          break;
        }
      }
      // for asynchronous invocation, the status is true for buckets that we did not wait for
      boolean status = args.isSynchronous() ? false : true;
      for (Integer bucketId : buckets) {
        if (pr.getLogWriterI18n().fineEnabled()) {
          pr.getLogWriterI18n().fine("HDFS: ForceCompaction sending result for bucket:"+bucketId);
        }
        context.getResultSender().sendResult(new CompactionStatus(bucketId, status));
      }
      if (pr.getLogWriterI18n().fineEnabled()) {
        pr.getLogWriterI18n().fine("HDFS: ForceCompaction sending last result");
      }
      context.getResultSender().lastResult(new CompactionStatus(BUCKET_ID_FOR_LAST_RESULT, true));
    } catch (Exception e) {
      // always send the error to avoid a hang
      context.getResultSender().sendException(e);
    }
  }

  @Override
  public String getId() {
    return ID;
  }

  @Override
  public boolean hasResult() {
    return true;
  }

  @Override
  public boolean optimizeForWrite() {
    // run compaction on primary members
    return true;
  }

  @Override
  public boolean isHA() {
    // so that we can target re-execution on failed buckets
    return true;
  }
}
