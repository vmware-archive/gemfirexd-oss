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
package com.pivotal.gemfirexd.tools.planexporter;

import java.io.CharArrayWriter;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.GemFireCheckedException;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.GfxdSerializable;
import com.pivotal.gemfirexd.internal.engine.distributed.GfxdMessage;
import com.pivotal.gemfirexd.internal.engine.distributed.GfxdReplyMessage;
import com.pivotal.gemfirexd.internal.engine.distributed.GfxdReplyMessageProcessor;
import com.pivotal.gemfirexd.internal.engine.distributed.GfxdResponseCode;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnection;
import com.pivotal.gemfirexd.internal.impl.sql.execute.xplain.XPLAINUtil.XMLForms;

/**
 * This doesn't inherits from MemberExecutorMessage as we don't want retry of
 * these messages during member down instead just mentioning member down will do
 * fine.
 * 
 * These messages are registered during GfxdDataSerializable service boot up.To
 * avoid build dependencies we use reflection instead.
 * 
 * @author soubhikc
 * 
 */
public class ExecutionPlanMessage extends GfxdMessage {

  private String schema;

  private String stmtUUID;
  
  private XMLForms xmlForm;

  private String embedXslFileName;
  
  private final ExecutionPlanMessageCollector replyProcessor;

  private final List<char[]> plans = Collections
      .synchronizedList(new ArrayList<char[]>());

  /** used only for de-serialization on remote node */
  public ExecutionPlanMessage() {
    replyProcessor = null;
  }

  public ExecutionPlanMessage(
      final String schema,
      final String stmtUUID,
      final XMLForms xmlForm,
      final String embedXslFileName,
      final InternalDistributedSystem dm, final Set<DistributedMember> members) {
    this.schema = schema;
    this.stmtUUID = stmtUUID;
    this.xmlForm = xmlForm;
    this.embedXslFileName = embedXslFileName;
    this.replyProcessor = new ExecutionPlanMessageCollector(dm, members);
    setProcessorId(replyProcessor.getProcessorId());
    setRecipients(members);
  }

  @Override
  public GfxdReplyMessageProcessor getReplyProcessor() {
    return this.replyProcessor;
  }

  public List<char[]> getResults() {
    return plans;
  }

  public final void addResult(final char[] c) {
    plans.add(c);
  }

  @Override
  public byte getGfxdID() {
    return GfxdSerializable.EXECUTION_PLAN_MSG;
  }

  @Override
  public void toData(final DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeString(this.schema, out);
    DataSerializer.writeString(this.stmtUUID, out);
    DataSerializer.writeInteger(xmlForm.ordinal(), out);
    DataSerializer.writeString(this.embedXslFileName, out);
  }

  @Override
  public void fromData(DataInput in)
      throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.schema = DataSerializer.readString(in);
    this.stmtUUID = DataSerializer.readString(in);
    this.xmlForm = XMLForms.values()[DataSerializer.readInteger(in)];
    this.embedXslFileName = DataSerializer.readString(in);
  }

  @Override
  protected void processMessage(final DistributionManager dm)
      throws GemFireCheckedException {
    EmbedConnection conn = null;
    boolean contextSet = false;
    final CharArrayWriter cwriter = new CharArrayWriter();
    try {
      conn = GemFireXDUtils.getTSSConnection(true, true, true);
      conn.getTR().setupContextStack();
      contextSet = true;
      final AccessDistributedSystem ds = new AccessDistributedSystem(
          conn,
          schema,
          stmtUUID, null);

      final ExecutionPlanReplyMessage replyMsg ;
      new CreateXML(ds, true,
          xmlForm,
          embedXslFileName).processPlan(cwriter, false);
      replyMsg = new ExecutionPlanReplyMessage(cwriter);


      replyMsg.setRecipient(this.sender);
      replyMsg.setProcessorId(getProcessorId());
      if (SanityManager.DEBUG) {
        if (GemFireXDUtils.TracePlanGeneration) {
          SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_PLAN_GENERATION,
              "sending plan to " + Arrays.toString(replyMsg.getRecipients())
                  + " as " + cwriter.toString());
        }
      }

      dm.putOutgoing(replyMsg);

    } catch (final StandardException ex) {
      throw new ReplyException(
          "ExecutionPlanMessage: Unexpected StandardException on member "
              + dm.getDistributionManagerId(), ex);
    } catch (final SQLException ex) {
      throw new ReplyException(
          "ExecutionPlanMessage: Unexpected SQLException on member "
              + dm.getDistributionManagerId(), ex);
    } catch (final IOException ioe) {
      throw new ReplyException(
          "ExecutionPlanMessage: Unexpected IOException on member "
              + dm.getDistributionManagerId(), ioe);
    } finally {
      if (contextSet) {
        conn.getTR().restoreContextStack();
      }
      cwriter.close();
    }
  }

  @Override
  protected boolean waitForNodeInitialization() {
    return true;
  }

  @Override
  protected void sendReply(final ReplyException ex, final DistributionManager dm) {
    if (ex == null)
      return;
    if (GemFireXDUtils.TracePlanGeneration) {
      SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_PLAN_GENERATION,
          " Raising exception from " + dm + " processorId=" + this.processorId
              + " recipient " + getSender() + " for " + stmtUUID
              + " exception = " + ex, ex);
    }
    ReplyMessage.send(getSender(), this.processorId, ex, dm, this);
  }

  public class ExecutionPlanMessageCollector extends GfxdReplyMessageProcessor {

    public ExecutionPlanMessageCollector(final InternalDistributedSystem ds,
        final Set<DistributedMember> initMembers) {
      super(ds.getDistributionManager(), initMembers, true);
    }

    @Override
    public final void process(final DistributionMessage msg) {
      try {
        if (msg instanceof ExecutionPlanReplyMessage) {
          final char[] p = ((ExecutionPlanReplyMessage)msg).plan;

          if (p == null || p.length == 0) {
            if (GemFireXDUtils.TracePlanGeneration) {
              SanityManager.DEBUG_PRINT(
                  GfxdConstants.TRACE_PLAN_GENERATION,
                  " NO result from " + msg.getSender() + " processorId="
                      + msg.getProcessorId() + " received for " + stmtUUID);
            }
            return;
          }

          if (GemFireXDUtils.TracePlanGeneration) {
            SanityManager.DEBUG_PRINT(
                GfxdConstants.TRACE_PLAN_GENERATION,
                " adding remote result from " + msg.getSender()
                    + " processorId=" + msg.getProcessorId() + " for "
                    + stmtUUID + " with " + String.valueOf(p));
          }
          addResult(p);
        }
      } finally {
        super.process(msg);
      }
    }

    @Override
    protected Set<DistributedMember> virtualReset() {
      return null;
    }
  }

  /**
   * Plan reply message from the remote nodes as a string and can be directly
   * loaded as an XML DOM object.
   * 
   * @author soubhikc
   * 
   */
  public static class ExecutionPlanReplyMessage extends GfxdReplyMessage {

    private char[] plan;

    public ExecutionPlanReplyMessage() {
    }

    protected ExecutionPlanReplyMessage(final CharArrayWriter writer) {
      this.plan = writer.toCharArray();
    }

    @Override
    public GfxdResponseCode getResponseCode() {
      return null;
    }

    @Override
    public byte getGfxdID() {
      return GfxdSerializable.EXECUTION_PLAN_REPLY_MSG;
    }

    @Override
    public void fromData(DataInput in)
            throws IOException, ClassNotFoundException {
      super.fromData(in);
      plan = DataSerializer.readCharArray(in);
    }

    @Override
    public void toData(final DataOutput out) throws IOException {
      super.toData(out);
      DataSerializer.writeCharArray(plan, out);
    }
  }
}
