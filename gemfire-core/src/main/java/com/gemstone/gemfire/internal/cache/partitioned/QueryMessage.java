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
package com.gemstone.gemfire.internal.cache.partitioned;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.GemFireRethrowable;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.query.QueryException;
import com.gemstone.gemfire.cache.query.QueryExecutionLowMemoryException;
import com.gemstone.gemfire.cache.query.internal.DefaultQuery;
import com.gemstone.gemfire.cache.query.internal.QueryMonitor;
import com.gemstone.gemfire.cache.query.internal.QueryObserver;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.streaming.StreamingOperation.StreamingReplyMessage;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.HeapDataOutputStream;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.PRQueryProcessor;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.Token;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

public final class QueryMessage extends StreamingPartitionOperation.StreamingPartitionMessage
  {
  private volatile String queryString;
  private volatile boolean cqQuery;
  private volatile Object[] parameters;
  private volatile List buckets;
  private volatile boolean isPdxSerialized;
  private volatile boolean traceOn;

//  private transient PRQueryResultCollector resultCollector = new PRQueryResultCollector();
  private transient Collection<Collection> resultCollector = new ArrayList<Collection>();
  private transient int tokenCount = 0; // counts how many end of stream tokens received
  private transient Iterator currentResultIterator;
  private transient Iterator<Collection> currentSelectResultIterator;

  /**
   * Empty constructor to satisfy {@link DataSerializer} requirements
   */
  public QueryMessage() {}

  public QueryMessage(InternalDistributedMember recipient,  int regionId, ReplyProcessor21 processor,
      DefaultQuery query, Object[] parameters, final List buckets) {
    super(recipient, regionId, processor);
    this.queryString = query.getQueryString();
    this.buckets = buckets;
    this.parameters = parameters;
    this.cqQuery = query.isCqQuery();
    this.traceOn = query.isTraced() || DefaultQuery.QUERY_VERBOSE;
  }


  /**  Provide results to send back to requestor.
    *  terminate by returning END_OF_STREAM token object
    */
  @Override
  protected Object getNextReplyObject(PartitionedRegion pr)
  throws CacheException, ForceReattemptException, InterruptedException {
    if (QueryMonitor.isLowMemory()) {
      String reason = LocalizedStrings.QueryMonitor_LOW_MEMORY_CANCELED_QUERY.toLocalizedString(QueryMonitor.getMemoryUsedDuringLowMemory());
      throw new QueryExecutionLowMemoryException(reason);
    }
    if (Thread.interrupted()) throw new InterruptedException();
    LogWriterI18n l = pr.getCache().getLoggerI18n();
    while ((this.currentResultIterator == null || !this.currentResultIterator.hasNext())) {
      if (this.currentSelectResultIterator.hasNext()) {
        Collection results = this.currentSelectResultIterator.next();
        if (l.fineEnabled()) {
          l.fine("Query result size: " + results.size());
        }
        this.currentResultIterator = results.iterator();
      } else {
        //Assert.assertTrue(this.resultCollector.isEmpty());
        return Token.END_OF_STREAM;
      }
    }
    return this.currentResultIterator.next();
  }

  @Override
  protected boolean operateOnPartitionedRegion(DistributionManager dm, PartitionedRegion r, long startTime)
  throws CacheException, QueryException, ForceReattemptException, InterruptedException {
    if (Thread.interrupted()) throw new InterruptedException();
    LogWriterI18n l = r.getCache().getLoggerI18n();
    if (DistributionManager.VERBOSE) {
      l.fine("QueryMessage operateOnPartitionedRegion: " + r.getFullPath() + " buckets " +buckets);
    }

    r.waitOnInitialization();

    //PartitionedRegionDataStore ds = r.getDataStore();

    //if (ds != null) {
    if (QueryMonitor.isLowMemory()) {
      String reason = LocalizedStrings.QueryMonitor_LOW_MEMORY_CANCELED_QUERY.toLocalizedString(QueryMonitor.getMemoryUsedDuringLowMemory());
      //throw query exception to piggyback on existing error handling as qp.executeQuery also throws the same error for low memory
      throw new QueryExecutionLowMemoryException(reason);
    }
    
      DefaultQuery query = new DefaultQuery(this.queryString, r.getCache());
      // Remote query, use the PDX types in serialized form.
      DefaultQuery.setPdxReadSerialized(r.getCache(), true);
      // In case of "select *" queries we can keep the results in serialized
      // form and send
      query.setRemoteQuery(true);
      QueryObserver indexObserver = query.startTrace();

      try {
        query.setIsCqQuery(this.cqQuery);
        //ds.queryLocalNode(query, this.parameters, this.buckets, this.resultCollector);
        PRQueryProcessor qp = new PRQueryProcessor(r, query, parameters, buckets);
        if (l.fineEnabled()) {
          l.fine("Started executing query from remote node: " + query.getQueryString());
        }
        qp.executeQuery(this.resultCollector);
        this.currentSelectResultIterator = this.resultCollector.iterator();
        //resultSize = this.resultCollector.size() - this.buckets.size(); //Minus END_OF_BUCKET elements.
        super.operateOnPartitionedRegion(dm, r, startTime);
      } finally {
        DefaultQuery.setPdxReadSerialized(r.getCache(), false);
        query.setRemoteQuery(false);
        query.endTrace(indexObserver, startTime, this.resultCollector);
      }
    //}
    //else {
    //  l.warning(LocalizedStrings.QueryMessage_QUERYMESSAGE_DATA_STORE_NOT_CONFIGURED_FOR_THIS_MEMBER);
    //}

    // Unless there was an exception thrown, this message handles sending the response
    return false;
  }

  @Override
  protected void appendFields(StringBuilder buff) {
    super.appendFields(buff);
    buff.append("; query=").append(this.queryString)
    .append("; bucketids=").append(this.buckets);
  }

  public int getDSFID() {
    return PR_QUERY_MESSAGE;
  }

  /** send a reply message.  This is in a method so that subclasses can override the reply message type
   *  @see PutMessage#sendReply
   */
  @Override
  protected void sendReply(InternalDistributedMember member, int procId, DM dm, ReplyException ex, PartitionedRegion pr, long startTime) {
    // if there was an exception, then throw out any data
    if (ex != null) {
      this.outStream = null;
      this.replyMsgNum = 0;
      this.replyLastMsg = true;
    }
    if (this.replyLastMsg) {
      if (pr != null && startTime > 0) {
        pr.getPrStats().endPartitionMessagesProcessing(startTime);
      }
    }
    StreamingReplyMessage.send(member, procId, ex, dm, this.outStream,
        this.numObjectsInChunk, this.replyMsgNum,
        this.replyLastMsg, this.isPdxSerialized);
  }

  @Override
  public void fromData(DataInput in)
      throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.queryString = DataSerializer.readString(in);
    this.buckets = DataSerializer.readArrayList(in);
    this.parameters = DataSerializer.readObjectArray(in);
    this.cqQuery = DataSerializer.readBoolean(in);
    this.isPdxSerialized = DataSerializer.readBoolean(in);
    this.traceOn = DataSerializer.readBoolean(in);
  }

  @Override
  public void toData(DataOutput out)
      throws IOException {
    super.toData(out);
    DataSerializer.writeString(this.queryString, out);
    DataSerializer.writeArrayList((ArrayList)this.buckets, out);
    DataSerializer.writeObjectArray(this.parameters, out);
    DataSerializer.writeBoolean(this.cqQuery, out);
    DataSerializer.writeBoolean(true, out);
    DataSerializer.writeBoolean(this.traceOn, out);
  }
  
}
