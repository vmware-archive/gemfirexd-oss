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
/**
 * 
 */
package com.gemstone.gemfire.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.HighPriorityDistributionMessage;
import com.gemstone.gemfire.distributed.internal.MessageWithReply;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * @author bruce
 *
 */
public class FindVersionTagOperation {
  
  public static VersionTag findVersionTag(LocalRegion r, EventID eventId, boolean isPutAll) {
    DM dm = r.getDistributionManager();
    Set recipients;
    if (r instanceof DistributedRegion) {
      recipients = ((DistributedRegion)r).getDistributionAdvisor().adviseCacheOp();
    } else {
      recipients = ((PartitionedRegion)r).getRegionAdvisor().adviseDataStore();
    }
    ResultReplyProcessor processor = new ResultReplyProcessor(dm, recipients);
    FindVersionTagMessage msg = new FindVersionTagMessage(recipients, processor.getProcessorId(), r.getFullPath(), eventId, isPutAll);
    dm.putOutgoing(msg);
    try {
      processor.waitForReplies();
    } catch (InterruptedException e) {
      dm.getCancelCriterion().checkCancelInProgress(e);
      Thread.currentThread().interrupt();
      return null;
    }
    return processor.getVersionTag();
  }
  
  public static class ResultReplyProcessor extends ReplyProcessor21 {

    VersionTag versionTag;
    
    public ResultReplyProcessor(DM dm, Collection initMembers) {
      super(dm, initMembers);
    }
    
    @Override
    public void process(DistributionMessage msg) {
      if (msg instanceof VersionTagReply) {
        VersionTagReply reply = (VersionTagReply) msg;
        if (reply.versionTag != null) {
          this.versionTag = reply.versionTag;
          this.versionTag.replaceNullIDs(reply.getSender());
        }
      }
      super.process(msg);
    }

    public VersionTag getVersionTag() {
      return versionTag;
    }
    
    @Override
    public boolean stillWaiting() {
      return this.versionTag == null && super.stillWaiting();
    }

  }

  /**
   * FindVersionTagOperation searches other members for version information for a replayed
   * operation.  If we don't have version information the op may be applied by
   * this cache as a new event.  When the event is then propagated to other servers
   * that have already seen the event it will be ignored, causing an inconsistency.
   * @author bruce
   */
  public static class FindVersionTagMessage extends HighPriorityDistributionMessage 
     implements MessageWithReply {
    
    int processorId;
    String regionName;
    EventID eventId;
    private boolean isPutAll;
    
    protected FindVersionTagMessage(Collection recipients, int processorId, String regionName, EventID eventId, boolean isPutAll) {
      super();
      setRecipients(recipients);
      this.processorId = processorId;
      this.regionName = regionName;
      this.eventId = eventId;
      this.isPutAll = isPutAll;
    }

    /** for deserialization */
    public FindVersionTagMessage() {
    }

    /* (non-Javadoc)
     * @see com.gemstone.gemfire.distributed.internal.DistributionMessage#process(com.gemstone.gemfire.distributed.internal.DistributionManager)
     */
    @Override
    protected void process(DistributionManager dm) {
      VersionTag result = null;
      try {
        LocalRegion r = findRegion();
        if (r == null) {
          if (dm.getLoggerI18n().fineEnabled()) {
            dm.getLoggerI18n().fine("Region not found, so ignoring version tag request: " + this);
          }
          return;
        }
        if(isPutAll) {
          result = r.findVersionTagForClientPutAll(eventId);
          
        } else {
          result = r.findVersionTagForClientEvent(eventId);
        }
        if (dm.getLoggerI18n().fineEnabled()) {
          dm.getLoggerI18n().fine("Found version tag " + result);
        }
 
      }
      catch (RuntimeException e) {
        dm.getLoggerI18n().warning(LocalizedStrings.DEBUG, "Exception thrown while searching for a version tag", e);
      }
      finally {
        VersionTagReply reply = new VersionTagReply(result);
        reply.setProcessorId(this.processorId);
        reply.setRecipient(getSender());
        try {
          dm.putOutgoing(reply);
        } catch (CancelException e) {
          // can't send a reply, so ignore the exception
        }
      }
    }

    private LocalRegion findRegion() {
      GemFireCacheImpl cache = null;
      try {
        cache = GemFireCacheImpl.getInstance();
        if (cache != null) {
          return cache.getRegionByPathForProcessing(regionName);
        }
      } catch (CancelException e) {
        // nothing to do
      }
      return null;
    }

    public int getDSFID() {
      return FIND_VERSION_TAG;
    }
    
    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      out.writeInt(this.processorId);
      out.writeUTF(this.regionName);
      InternalDataSerializer.invokeToData(this.eventId, out);
      out.writeBoolean(this.isPutAll);
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      super.fromData(in);
      this.processorId = in.readInt();
      this.regionName = in.readUTF();
      this.eventId = new EventID();
      InternalDataSerializer.invokeFromData(this.eventId, in);
      this.isPutAll = in.readBoolean();
    }
    
    @Override
    public String toString() {
      return this.getShortClassName() + "(processorId=" + this.processorId
      + ";region=" + this.regionName
      + ";eventId=" + this.eventId
      + ";isPutAll=" + this.isPutAll
      + ")";
    }
  }

  public static class VersionTagReply extends ReplyMessage {
    VersionTag versionTag;
    
    VersionTagReply(VersionTag result) {
      this.versionTag = result;
    }

    /** for deserialization */
    public VersionTagReply() {
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      DataSerializer.writeObject(this.versionTag, out);
    }

    @Override
    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException {
      super.fromData(in);
      this.versionTag = (VersionTag)DataSerializer.readObject(in);
    }

    @Override
    public int getDSFID() {
      return VERSION_TAG_REPLY;
    }
  }

}
