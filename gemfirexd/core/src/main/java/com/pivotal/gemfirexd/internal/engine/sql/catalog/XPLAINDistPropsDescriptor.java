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

package com.pivotal.gemfirexd.internal.engine.sql.catalog;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;

import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.pivotal.gemfirexd.internal.catalog.UUID;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.diag.SortedCSVProcedures;
import com.pivotal.gemfirexd.internal.engine.distributed.message.GfxdFunctionMessage;
import com.pivotal.gemfirexd.internal.engine.distributed.message.GfxdFunctionMessage.GfxdFunctionReplyMessage;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.store.ServerGroupUtils;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.ParameterValueSet;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.SystemColumn;
import com.pivotal.gemfirexd.internal.iapi.types.TypeId;
import com.pivotal.gemfirexd.internal.impl.sql.catalog.SystemColumnImpl;
import com.pivotal.gemfirexd.internal.impl.sql.catalog.XPLAINTableDescriptor;
import com.pivotal.gemfirexd.internal.impl.sql.execute.xplain.XPLAINUtil;
import com.pivotal.gemfirexd.internal.shared.common.SharedUtils;

/**
 * Tuple describing distribution statistics into XPLAIN_DIST_PROPS table.
 * 
 * @author soubhikc
 * 
 */
public final class XPLAINDistPropsDescriptor extends XPLAINTableDescriptor {

  private UUID dist_rs_id;
  
  private UUID stmt_rs_id;

  private UUID parent_rs_id = null;

  private XPLAINUtil.DIRECTION dist_direction; // towards data node or from data

  // node.

  private short dist_level; // sub-query re-distribution

  private String dist_object_type; // distribution/serialization

  private String dist_object_name; // message class etc.

  private String originator; // querying sender member id

  // query node
  // -----------
  private String routing_objects; // toString of sample routing objects

  private int no_routing_objects; // count of routing objects

  private String pruned_member_list; // members remoting is pruned to

  private int no_pruned_member_list; // total number of members query is routed.

  private String pruned_server_groups; // members mapped to server groups

  private String prep_stmt_aware_members; // MemberMappedArguments for nodes.

  private String message_flags; // message flags.

  private String parameter_valueset;

  private short query_retry_count; // query re-tried due to node failures.

  private short member_mapping_retry_count; // routing object to bucket mapping

  // timing
  private long routing_object_compute_time; // computeNodes

  private Timestamp begin_scatter_time;

  private long member_mapping_time;

  private long[] replySingleResultStatistics;
  
  /**
   * computed field, sum of process_time of request message
   * + root_send_time + member_mapping_time.
   * 
   * root_msg.process_time - root_msg.scatter_time will give
   * extra work time by executeFunction(...) including retries.
   */
  private long scatter_time;
  
  private InternalDistributedMember target_member;

  private Timestamp construct_time;

  private long ser_deser_time;

  /**
   * This attribute will almost always be in conjunction with ser_deser_time
   * attribute. Its used for multiple information viz.
   * 
   * <ol>
   * <li>
   * For <code>distribution message</code>, requesting node records its sending
   * time whereas receiving node records message honoring time.<br>
   * if parent_dist_rs_id is null, then it captures total processing time for all the
   * messages</li>
   * <li>
   * For <code>distribution message reply</code>, sending node records replying
   * preparation time whereas receiving node captures reply collection time i.e.
   * RC.addResult</li>
   * <li>
   * For <code>ResultHolder</code>, replying node captures local resultset
   * iteration time whereas receiving node captures row iteration time</li>
   * </ol>
   */
  private long process_time;

  /**
   * if the query results were throttled on data node before sending back due to
   * critical up on query node or data node, then this records the total sleep
   * time for throttling
   */
  private long throttle_time;

  // data node
  private long local_dataset_size; // FnExn localDataSet.

  // timing
  private Timestamp begin_receive_time; // Executor msg de-ser time

  public List<XPLAINDistPropsDescriptor> memberSentMappedDesc = Collections
      .emptyList();
  
  public List<XPLAINDistPropsDescriptor> memberReplyMappedDesc = Collections
      .emptyList();

  public boolean locallyExecuted; 
  
  /** not to be used other than for registration on tableDescriptor */
  public XPLAINDistPropsDescriptor() {
  }

  public XPLAINDistPropsDescriptor(
      final UUID dist_rs_id,
      final UUID stmt_rs_id,
      final XPLAINUtil.DIRECTION dist_direction,
      final short dist_level,
      final String dist_object_type,
      final String originator,
      final Set<Object> routingKeys,
      final long routing_object_compute_time,
      final Set<DistributedMember> prepStmtAwareMembers,
      final ParameterValueSet parameter_valueset) {
    this.dist_rs_id = dist_rs_id;
    this.stmt_rs_id = stmt_rs_id;
    this.parent_rs_id = null;
    this.dist_direction = dist_direction;
    this.dist_level = dist_level;
    this.dist_object_type = dist_object_type;
    this.originator = originator;

    // query node
    // -----------
    if (routingKeys != null) {
      this.no_routing_objects = routingKeys.size();
      this.routing_objects = no_routing_objects > 0 ? SharedUtils
          .toCSV(routingKeys) : null;
    }

    this.routing_object_compute_time = routing_object_compute_time;

    this.prep_stmt_aware_members = SharedUtils.toCSV(prepStmtAwareMembers);
    this.parameter_valueset = parameter_valueset.toString();
  }

  public XPLAINDistPropsDescriptor(
      final int insert_order,
      final UUID dist_rs_id,
      final UUID stmt_rs_id,
      final UUID parent_rs_id,
      final XPLAINUtil.DIRECTION dist_direction,
      final short dist_level,
      final String dist_object_type,
      final String dist_object_name,
      final String originator,
      // query node
      final Set<Object> routingKeys,
      final InternalDistributedMember[] pruned_members,
      final Set<String> prepStmtAwareMembers,
      final String messageflags,
      final ParameterValueSet parameter_valueset,
      final short query_retry_count,
      final short member_mapping_retry_count,
      final long routing_object_compute_time,
      final Timestamp begin_scatter_time,
      final long member_mapping_time,
      final long distribution_time,
      final String msg_target_member,
      final Timestamp msg_construct_time,
      final long msg_ser_deser_time,
      final long msg_process_time,
      final int local_dataset_size,
      final Timestamp begin_receive_time) {

    this.dist_rs_id = dist_rs_id;
    this.stmt_rs_id = stmt_rs_id;
    this.parent_rs_id = parent_rs_id;
    this.dist_direction = dist_direction;
    this.dist_level = dist_level;
    this.dist_object_type = dist_object_type;
    this.dist_object_name = dist_object_name;
    this.originator = originator;

    // query node
    // -----------
    this.routing_objects = routingKeys != null && routingKeys.size() > 0 ? SharedUtils
        .toCSV(routingKeys)
        : null;
    this.no_routing_objects = routingKeys.size();
    this.routing_object_compute_time = routing_object_compute_time;
    this.pruned_member_list = GemFireXDUtils.toCSV(pruned_members);
    this.no_pruned_member_list = pruned_members.length;
    this.pruned_server_groups = getParticipatingServerGroups(pruned_members);
    this.prep_stmt_aware_members = SharedUtils.toCSV(prepStmtAwareMembers);
    this.parameter_valueset = parameter_valueset.toString();
    this.query_retry_count = query_retry_count;
  }

  public XPLAINDistPropsDescriptor(
      final UUID dist_rs_id,
      UUID stmt_rs_id,
      final UUID parent_rs_id,
      final XPLAINUtil.DIRECTION dist_direction,
      final short dist_level,
      final String dist_object_type,
      final String originator) {

    this.dist_rs_id = dist_rs_id;
    this.stmt_rs_id = stmt_rs_id;
    this.parent_rs_id = parent_rs_id;
    this.dist_direction = dist_direction;
    this.dist_level = dist_level;
    this.dist_object_type = dist_object_type;
    this.originator = originator;
  }

  private String getParticipatingServerGroups(
      final InternalDistributedMember[] members) {

    String s = null;

    for (final DistributedMember m : members) {
      final SortedSet<String> g = ServerGroupUtils.getServerGroups(m);
      if (s == null) {
        s = SharedUtils.toCSV(g);
        continue;
      }
      s = SortedCSVProcedures.groupsIntersection(s, SharedUtils.toCSV(g));
    }

    return s;
  }

  // ensure order of declaration & member data population is same.
  public void setStatementParameters(
      final PreparedStatement ps) throws SQLException {
    int pi = 0;
    ps.setString(++pi, dist_rs_id.toString());
    ps.setString(++pi, stmt_rs_id.toString());
    ps.setString(++pi, parent_rs_id != null ? parent_rs_id.toString() : null);
    ps.setString(++pi, dist_direction.name());
    ps.setShort(++pi, dist_level);
    ps.setString(++pi, dist_object_type);
    ps.setString(++pi, dist_object_name);
    ps.setString(++pi, originator);

    // query node
    // -----------
    if (routing_objects != null) {
      ps.setString(++pi, routing_objects);
      ps.setInt(++pi, no_routing_objects);
    }
    else {
      ps.setNull(++pi, Types.VARCHAR);
      ps.setNull(++pi, Types.INTEGER);
    }

    if (pruned_member_list != null) {
      ps.setString(++pi, pruned_member_list);
      ps.setInt(++pi, no_pruned_member_list);
    }
    else {
      ps.setNull(++pi, Types.VARCHAR);
      ps.setNull(++pi, Types.INTEGER);
    }
    ps.setString(++pi, pruned_server_groups);
    ps.setString(++pi, prep_stmt_aware_members);
    ps.setString(++pi, message_flags);
    ps.setString(++pi, parameter_valueset);
    ps.setShort(++pi, query_retry_count);
    ps.setShort(++pi, member_mapping_retry_count);

    // timing
    ps.setLong(++pi, routing_object_compute_time);
    ps.setTimestamp(++pi, begin_scatter_time);
    ps.setLong(++pi, member_mapping_time);
    ps.setLong(++pi, scatter_time);

    ps.setString(++pi, target_member != null ? target_member.toString() : null);

    ps.setTimestamp(++pi, construct_time);
    ps.setLong(++pi, ser_deser_time);
    ps.setLong(++pi, process_time);
    ps.setLong(++pi, throttle_time);

    // data node
    // -----------
    ps.setLong(++pi, local_dataset_size);

    // timing
    ps.setTimestamp(++pi, begin_receive_time);
  }

  @Override
  protected void addConstraints(
      final StringBuilder sb) {
  }

  @Override
  protected SystemColumn[] buildColumnList() {

    return new SystemColumn[] {
        SystemColumnImpl.getUUIDColumn("DIST_RS_ID", false),
        SystemColumnImpl.getUUIDColumn("STMT_RS_ID", false),
        SystemColumnImpl.getUUIDColumn("PARENT_DIST_RS_ID", true),
        SystemColumnImpl.getColumn("DIST_DIRECTION", Types.CHAR, false, 7),
        SystemColumnImpl.getColumn("DIST_LEVEL", Types.SMALLINT, false),
        SystemColumnImpl
            .getColumn("DIST_OBJECT_TYPE", Types.VARCHAR, false, 32),
        SystemColumnImpl.getColumn(
            "DIST_OBJECT_NAME",
            Types.VARCHAR,
            false,
            128),
        SystemColumnImpl.getColumn("ORIGINATOR", Types.VARCHAR, false, 128),

        // query node
        // -----------
        SystemColumnImpl.getColumn(
            "ROUTING_OBJECTS",
            Types.VARCHAR,
            true,
            TypeId.VARCHAR_MAXWIDTH),
        SystemColumnImpl.getColumn("NO_ROUTING_OBJECTS", Types.INTEGER, true),
        SystemColumnImpl.getColumn(
            "PRUNED_MEMBERS_LIST",
            Types.VARCHAR,
            true,
            TypeId.VARCHAR_MAXWIDTH),
        SystemColumnImpl.getColumn("NO_PRUNED_MEMBERS", Types.INTEGER, true),
        SystemColumnImpl.getColumn(
            "PRUNED_SERVER_GROUPS_LIST",
            Types.VARCHAR,
            true,
            TypeId.VARCHAR_MAXWIDTH),
        SystemColumnImpl.getColumn(
            "PREP_STMT_AWARE_MEMBERS",
            Types.VARCHAR,
            true,
            TypeId.VARCHAR_MAXWIDTH),
        SystemColumnImpl.getColumn(
            "MESSAGE_FLAGS",
            Types.VARCHAR,
            true,
            TypeId.VARCHAR_MAXWIDTH),

        SystemColumnImpl.getColumn(
            "PARAMETER_VALUES",
            Types.VARCHAR,
            true,
            TypeId.VARCHAR_MAXWIDTH),

        SystemColumnImpl.getColumn("QUERY_RETRY_COUNT", Types.SMALLINT, true),
        SystemColumnImpl.getColumn(
            "MEMBER_MAPPING_RETRY_COUNT",
            Types.SMALLINT,
            true),

        // timing
        SystemColumnImpl.getColumn(
            "ROUTING_OBJECT_COMPUTE_TIME",
            Types.BIGINT,
            true),
        SystemColumnImpl
            .getColumn("BEGIN_SCATTER_TIME", Types.TIMESTAMP, false),
        SystemColumnImpl.getColumn("MEMBER_MAPPING_TIME", Types.BIGINT, true),
        SystemColumnImpl.getColumn("SCATTER_TIME", Types.BIGINT, false), // member_mapping_time + individual message process_time + root messages sending time.

        // captured on individual message
        SystemColumnImpl.getColumn("TARGET_MEMBER", Types.VARCHAR, true,
            TypeId.VARCHAR_MAXWIDTH),

        SystemColumnImpl.getColumn("CONSTRUCT_TIME", Types.TIMESTAMP, true),

        SystemColumnImpl.getColumn("SER_DESER_TIME", Types.BIGINT, true),

        SystemColumnImpl.getColumn("PROCESS_TIME", Types.BIGINT, true),

        SystemColumnImpl.getColumn("THROTTLE_TIME", Types.BIGINT, true),

        // data node
        // -----------
        SystemColumnImpl.getColumn("LOCAL_DATASET_SIZE", Types.INTEGER, true),

        // timing
        SystemColumnImpl
            .getColumn("BEGIN_RECEIVE_TIME", Types.TIMESTAMP, false), };
  }

  @Override
  public final String getCatalogName() {
    return TABLENAME_STRING;
  }

  static final String TABLENAME_STRING = "SYSXPLAIN_DIST_PROPS";

  @Override
  public String toString() {
    return "DistProps@" + System.identityHashCode(this) + " STMT_ID="
        + stmt_rs_id + " DIST_ID=" + dist_rs_id + " DIST_LEVEL=" + dist_level
        + " OBJECT_TYPE=" + dist_object_type + " PROCESS_TIME=" + process_time
        + " SER_DESER_TIME=" + ser_deser_time + " THROTTLE_TIME="
        + throttle_time;
  }
  
  // -------------------------------------
  // getters/setters
  // -------------------------------------

  public final void setDistRSID(
      final UUID dist_rs_id) {
    this.dist_rs_id = dist_rs_id;
  }

  public final UUID getRSID() {
    return this.dist_rs_id;
  }

  public final String getDistObjectType() {
    return this.dist_object_type;
  }

  public final void setDistObjectName(
      final String dist_object_name) {
    this.dist_object_name = dist_object_name;
  }

  public final String getDistObjectName() {
    return this.dist_object_name;
  }

  public final void setPrunedMembers(
      final InternalDistributedMember[] pruned_members) {
    this.pruned_member_list = GemFireXDUtils.toCSV(pruned_members);
    this.no_pruned_member_list = pruned_members.length;
    this.pruned_server_groups = getParticipatingServerGroups(pruned_members);
  }

  public final void setMessageFlags(
      final String message_flags) {
    this.message_flags = message_flags;
  }

  public final String getMessageFlags() {
    return message_flags;
  }

  public final void setQueryRetryCount(
      final short query_retry_count) {
    this.query_retry_count = query_retry_count;
  }

  public final void setBeginScatterTime(
      final Timestamp begin_scatter_time) {
    this.begin_scatter_time = begin_scatter_time;
  }

  public final Timestamp getBeginScatterTime() {
    return begin_scatter_time;
  }

  private XPLAINDistPropsDescriptor cloneMe() {

    final XPLAINDistPropsDescriptor r = new XPLAINDistPropsDescriptor();

    r.dist_rs_id = null; // will be filled by *PlanCollector.
    r.stmt_rs_id = this.stmt_rs_id;
    r.parent_rs_id = this.dist_rs_id;
    r.dist_direction = this.dist_direction;
    r.dist_level = this.dist_level;
    r.originator = this.originator;

    return r;
  }

  // never hold the messages in array locally here ...
  public final <T> void processMemberSentMessages(
      final ArrayList<GfxdFunctionMessage<T>> membersMsgsSent,
      GfxdFunctionMessage<T> rootMsg) {

    // will be null at the receiving node
    if (membersMsgsSent == null) {
      return;
    }

    if (memberSentMappedDesc == Collections.EMPTY_LIST) {
      memberSentMappedDesc = new ArrayList<XPLAINDistPropsDescriptor>();
    }
    
    long scatter_time = 0;
    for (GfxdFunctionMessage<?> m : membersMsgsSent) {
      XPLAINDistPropsDescriptor msg = cloneMe();

      msg.dist_object_type = XPLAINUtil.OP_QUERY_SEND;
      msg.dist_object_name = m.getClass().getSimpleName();
      msg.locallyExecuted = m.isLocallyExecuted();
      msg.originator = (m.getSender() != null ? m.getSender().toString()
          : "SELF");
      msg.target_member = msg.getRecipients(m, false)[0];
      msg.construct_time = m.getConstructTime();
      msg.ser_deser_time = m.getSerializeDeSerializeTime();
      // processTime on the root message encompasses everything (member map
      // time, each message posting time etc.)
      msg.process_time = m == rootMsg ? m.getRootMessageSendTime() : m
          .getProcessTime();
      msg.throttle_time = 0;
      scatter_time += msg.process_time;

      // in case the clock doesn't ticks
      if(msg.construct_time != null && msg.process_time == 0 && msg.ser_deser_time == 0 && msg.throttle_time == 0) {
        msg.process_time = 1;
      }

      memberSentMappedDesc.add(msg);
    }
    
    this.scatter_time = scatter_time + member_mapping_time;
  }

  public final void processMemberReplyMessages(
      // never hold the reply messages list locally here.
      final List<GfxdFunctionReplyMessage> replyMsgs) {
    
    if(replyMsgs == null) {
      return;
    }

    Timestamp begin_receive_time = null;

    if (memberReplyMappedDesc == Collections.EMPTY_LIST) {
      memberReplyMappedDesc = new ArrayList<XPLAINDistPropsDescriptor>();
    }

    final GemFireCacheImpl c = Misc.getGemFireCacheNoThrow();
    for (GfxdFunctionReplyMessage r : replyMsgs) {

      XPLAINDistPropsDescriptor msg = cloneMe();

      // find the first reply.
      Timestamp ct = r.getConstructTime();
      if (begin_receive_time == null || begin_receive_time.before(ct)) {
        begin_receive_time = ct;
      }

      // record individual message details.
      msg.flipdirection();
      if (msg.dist_direction == XPLAINUtil.DIRECTION.IN) {
        msg.dist_object_type = XPLAINUtil.OP_RESULT_RECEIVE;
        msg.originator = r.getSender().toString();
        msg.target_member = c.getDistributedSystem().getDistributedMember();
      }
      else {
        msg.dist_object_type = XPLAINUtil.OP_RESULT_SEND;
        msg.originator = c.getDistributedSystem().getDistributedMember()
            .toString();
        msg.target_member = msg.getRecipients(r, false)[0];
      }

      msg.dist_object_name = r.getClass().getSimpleName();
      msg.construct_time = r.getConstructTime();
      msg.ser_deser_time = r.getSerializeDeSerializeTime();
      msg.process_time = r.getProcessTime();
      msg.throttle_time = 0;
      msg.replySingleResult = r.getSingleResult();
      msg.replySingleResultStatistics = r.getSingleResultStatistics();

      // in case the clock doesn't ticks
      if(msg.construct_time != null && msg.process_time == 0 && msg.ser_deser_time == 0 && msg.throttle_time == 0) {
        msg.process_time = 1;
      }
      memberReplyMappedDesc.add(msg);
    }

    setBeginReceiveTime(begin_receive_time);
  }

  /** flip the direction for reply messages. */
  private final void flipdirection() {

    switch (this.dist_direction) {
      case IN:
        this.dist_direction = XPLAINUtil.DIRECTION.OUT;
        break;
      case OUT:
        this.dist_direction = XPLAINUtil.DIRECTION.IN;
        break;
      default:
        SanityManager.THROWASSERT("distribution direction not handled");
    }
  }

  public final void setSerDeSerTime(
      long ser_deser_time) {
    this.ser_deser_time = ser_deser_time;
  }

  public final void setThrottleTime(long throttle_time) {
    this.throttle_time = throttle_time;
  }

  public final void setLocalDatasetSize(
      final long local_dataset_size) {
    this.local_dataset_size = local_dataset_size;
  }

  public final long getLocalDatasetSize() {
    return local_dataset_size;
  }

  public final void setBeginReceiveTime(
      final Timestamp begin_receive_time) {
    this.begin_receive_time = begin_receive_time;
  }

  public final Timestamp getBeginReceiveTime() {
    return begin_receive_time;
  }

  public final void setProcessTime(
      final Long process_time) {
    this.process_time = process_time;
  }

  public final long getProcessTime() {
    return process_time;
  }
  
  public final long getSerDeSerTime() {
    return ser_deser_time;
  }
  
  public final long getThrottleTime() {
    return throttle_time;
  }

  public final void setMemberMappingRetryCount(
      final short mapping_retry_count) {
    this.member_mapping_retry_count = mapping_retry_count;
  }

  public final void setMemberMappingTime(
      final long member_mapping_time) {
    this.member_mapping_time = member_mapping_time;
  }
  
  public final String getOriginator() {
    return originator;
  }
  
  public final String getTargetMember() {
    return target_member.toString();
  }
  
  public String getPrunedMemberList() {
    return pruned_member_list;
  }
  
  public long getExecuteTime() {
    return this.ser_deser_time + this.process_time + this.throttle_time;
  }
  
  public long[] getReplySingleResultStatistics() {
    return replySingleResultStatistics;
  }
  
  // working variables.
  public Object replySingleResult;

  @Override
  protected void createIndex(StringBuilder idx, String schemaName) {
    idx.append("create index ").append(schemaName).append(
        ".idx_dist_dist_rs_id on ").append(TABLENAME_STRING).append(
        "(dist_rs_id)");
  }

  /**
   * This method handles special case of self distribution as opposed to m.getRecipients().
   * 
   * Locally executed sent message have isLocallyExecuted true in root msg descriptor.
   * 
   * Local reply message have recipient list 1 with 'null' as target member.
   * 
   * @param m
   * @param getAll 
   * @return
   */
  public InternalDistributedMember[] getRecipients(DistributionMessage m, final boolean getAll) {

    final InternalDistributedMember[] recipients;
    
    if(getAll) {
      // this is because, root functionMsg object captures only one member now
      // instead of list of recipients.
      final int numMembers = memberSentMappedDesc.size();
      recipients = new InternalDistributedMember[numMembers];
      for(int i = numMembers - 1; i >= 0; i--) {
        recipients[i] = memberSentMappedDesc.get(i).target_member;
      }
    }
    // individual messages is targeted only to one member
    else {
      final InternalDistributedMember[] mlist = m.getRecipients();
      // local reply message don't have recipient appropriately set.
      if ((m instanceof GfxdFunctionReplyMessage)
          && ((mlist != null && mlist.length > 0 && mlist[0] == null) || mlist.length == 0)) {
        recipients = new InternalDistributedMember[] { Misc.getGemFireCache().getMyId() };
      }
      else if(mlist != null && mlist.length == 1
          && mlist[0] != DistributionMessage.ALL_RECIPIENTS) {
        recipients = mlist;
      }
      else {
        if (SanityManager.ASSERT) {
          if (!locallyExecuted) {
            SanityManager.THROWASSERT("Only one member is expected as target "
                + "per message. ALL_RECIPIENTS will already get enumerated "
                + "at this stage to individual specific member. msg=" + m
                + " mlist=" + Arrays.toString(mlist));
          }
        }
        recipients = new InternalDistributedMember[] { Misc.getGemFireCache().getMyId() };
      }
    }
    return recipients;
  }


}
