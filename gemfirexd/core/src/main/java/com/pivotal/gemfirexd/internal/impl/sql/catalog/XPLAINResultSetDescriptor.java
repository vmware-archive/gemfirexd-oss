/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.catalog.XPLAINResultSetDescriptor

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

/*
 * Changes for GemFireXD distributed data platform (some marked by "GemStone changes")
 *
 * Portions Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
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

package com.pivotal.gemfirexd.internal.impl.sql.catalog;

import java.io.IOException;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.pivotal.gemfirexd.internal.catalog.UUID;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.sql.catalog.XPLAINDistPropsDescriptor;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.SystemColumn;
import com.pivotal.gemfirexd.internal.impl.sql.execute.PlanUtils;
import com.pivotal.gemfirexd.internal.impl.sql.execute.xplain.XPLAINUtil;
import com.pivotal.gemfirexd.tools.planexporter.StatisticsCollectionObserver;

import java.sql.Types;

public final class XPLAINResultSetDescriptor extends XPLAINTableDescriptor implements Comparable<XPLAINResultSetDescriptor> {
  
  public static final double oneMillisNanos = XPLAINUtil.oneMillisNanos;

  // GemStone changes BEGIN
  public final String rs_name;
  
  public final int num_children;
  
  private int insert_order; // the result set row insertion order
  
  private double totalExecutionTimeNanos;
  // GemStone changes END
  
  private UUID rs_id; // the result set UUID identifier

  private String op_identifier; // the operator code identifier

  private String op_details; // the operator details, operator-specific

  // information

  private Integer no_opens; // the number of open calls of this resultset

  private Integer no_index_updates; // the number of index updates, executed by

  // this dml write operation

  private String lock_granularity; // the lock granularity, either (T)able or

  // (R)ow locking

  private String lock_mode; // the lock mode, either instant share, share or

  // instant exclusive, exclusive

  private UUID parent_rs_id; // the parent UUID of this resultset, null if root

  // (top) resultset

  private Double est_row_count; // the estimated row count, forwarded by the

  // optimizer

  private Double est_cost; // the estimated costs, forwarded by the optimizer

  private Integer affected_rows; // the affected rows, specific for

  // insert/update/delete stmts

  private String deferred_rows; // the deferred rows, specific for

  // insert/update/delete stmts

  private Integer input_rows; // the number of input rows

  private Integer seen_rows; // the seen rows from this operator

  private Integer seen_rows_right; // the seen right rows from this operator,

  // only filled by a join operator, seen_rows
  // has then the rows from the outer(left)
  // partner of the join

  private Integer filtered_rows; // the filtered rows

  private Integer returned_rows; // the returned rows

  private Integer empty_right_rows; // the number of empty right rows

  private String index_key_optimization; // does this node use index key

  // optimization

  private transient XPLAINScanPropsDescriptor scan; // the UUID of the scan info properties of this node,

  // if this node is a scan node, otherwise null

  private transient XPLAINSortPropsDescriptor sort; // the UUID of the sort info properties of this node.

  // if this node is a groupby or sort node, otherwise
  // null

  private UUID stmt_id; // the UUID of the statement, which this resultset

  // belongs to

  private transient XPLAINResultSetTimingsDescriptor timing; // the UUID of the resultset timing information, if

  // statistics timing was on, otherwise null
  
  //GemStone changes BEGIN
  private transient XPLAINDistPropsDescriptor dist;
  private int rank; 
  //GemStone changes END

  protected XPLAINResultSetDescriptor() {
    rs_name = "protected-constructor";
    num_children = -1;
  }

  public XPLAINResultSetDescriptor(
  // GemStone changes BEGIN
      String rs_name,
      int num_children,
      int insert_order,
      // GemStone changes END
      UUID rs_id,
      String op_identifier,
      String op_details,
      Integer no_opens,
      Integer no_index_updates,
      String lock_mode,
      String lock_granularity,
      UUID parent_rs_id,
      Double est_row_count,
      Double est_cost,
      Integer affected_rows,
      String deferred_rows,
      Integer input_rows,
      Integer seen_rows,
      Integer seen_rows_right,
      Integer filtered_rows,
      Integer returned_rows,
      Integer empty_right_rows,
      String index_key_optimization,
      XPLAINScanPropsDescriptor scan,
      XPLAINSortPropsDescriptor sort,
      UUID stmt_id,
      XPLAINResultSetTimingsDescriptor timing,
      XPLAINDistPropsDescriptor distdesc) {

    // GemStone changes BEGIN
    this.rs_name = rs_name; 
    this.num_children = num_children;
    this.insert_order = insert_order;
    // GemStone changes END
    this.rs_id = rs_id;
    this.op_identifier = op_identifier;
    this.op_details = op_details;
    this.no_opens = no_opens;
    this.no_index_updates = no_index_updates;
    this.lock_granularity = lock_granularity;
    this.lock_mode = lock_mode;
    this.parent_rs_id = parent_rs_id;
    this.est_row_count = est_row_count;
    this.est_cost = est_cost;
    this.affected_rows = affected_rows;
    this.deferred_rows = deferred_rows;
    this.input_rows = input_rows;
    this.seen_rows = seen_rows;
    this.seen_rows_right = seen_rows_right;
    this.filtered_rows = filtered_rows;
    this.returned_rows = returned_rows;
    this.empty_right_rows = empty_right_rows;
    this.index_key_optimization = index_key_optimization;
    this.scan = scan;
    this.sort = sort;
    this.stmt_id = stmt_id;
    this.timing = timing;
    this.dist = distdesc;
  }

  public static final void setStatementParameters(Connection conn, PreparedStatement ps,
      UUID stmt_id, 
      StringBuilder xmlFragment) throws SQLException {
    ps.setString(1, (stmt_id != null ? stmt_id.toString() : null));
    final Clob c = conn.createClob();
    try {
      c.setCharacterStream(1).write(xmlFragment.toString());
    } catch (IOException e) {
      if (GemFireXDUtils.TracePlanAssertion) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_PLAN_ASSERTION,
            "couldn't set clob stream.", e);
      }
      else if (GemFireXDUtils.TracePlanGeneration) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_PLAN_GENERATION,
            "couldn't set clob stream.");
      }
    }
    ps.setClob(2, c);
  }
  
  public void setStatementParameters(
      PreparedStatement ps) throws SQLException {
    
    ps.setString(1, rs_id.toString());
    ps.setString(2, op_identifier);
    ps.setString(3, op_details);
    if (no_opens != null)
      ps.setInt(4, no_opens.intValue());
    else
      ps.setNull(4, Types.INTEGER);
    if (no_index_updates != null)
      ps.setInt(5, no_index_updates.intValue());
    else
      ps.setNull(5, Types.INTEGER);
    ps.setString(6, lock_mode);
    ps.setString(7, lock_granularity);
    ps.setString(8, (parent_rs_id != null ? parent_rs_id.toString() : null));
    if (est_row_count != null)
      ps.setDouble(9, est_row_count.doubleValue());
    else
      ps.setNull(9, Types.DOUBLE);
    if (est_cost != null)
      ps.setDouble(10, est_cost.doubleValue());
    else
      ps.setNull(10, Types.DOUBLE);
    if (affected_rows != null)
      ps.setInt(11, affected_rows.intValue());
    else
      ps.setNull(11, Types.INTEGER);
    ps.setString(12, deferred_rows);
    if (input_rows != null)
      ps.setInt(13, input_rows.intValue());
    else
      ps.setNull(13, Types.INTEGER);
    if (seen_rows != null)
      ps.setInt(14, seen_rows.intValue());
    else
      ps.setNull(14, Types.INTEGER);
    if (seen_rows_right != null)
      ps.setInt(15, seen_rows_right.intValue());
    else
      ps.setNull(15, Types.INTEGER);
    if (filtered_rows != null)
      ps.setInt(16, filtered_rows.intValue());
    else
      ps.setNull(16, Types.INTEGER);
    if (returned_rows != null)
      ps.setInt(17, returned_rows.intValue());
    else
      ps.setNull(17, Types.INTEGER);
    if (empty_right_rows != null)
      ps.setInt(18, empty_right_rows.intValue());
    else
      ps.setNull(18, Types.INTEGER);
    ps.setString(19, index_key_optimization);
    ps.setString(20, (scan != null ? scan.getRSID().toString() : null));
    ps.setString(21, (sort != null ? sort.getRSID().toString() : null));
    ps.setString(22, (stmt_id != null ? stmt_id.toString() : null));
    ps.setString(23, "-");
    // GemStone changes BEGIN
    ps.setInt(24, insert_order);
    ps.setString(25, (dist != null ? dist.getRSID().toString() : null));
    ps.setInt(26, rank);
    // GemStone changes END
  }

  public String getCatalogName() {
    return TABLENAME_STRING;
  }

  static final String TABLENAME_STRING = "SYSXPLAIN_RESULTSETS";

  private static final String[][] indexColumnNames = { { "RS_ID" } };

  /**
   * Builds a list of columns suitable for creating this Catalog.
   * 
   * @return array of SystemColumn suitable for making this catalog.
   */
  public SystemColumn[] buildColumnList() {

    return new SystemColumn[] {
        SystemColumnImpl.getUUIDColumn("STMT_ID", false),
        SystemColumnImpl.getColumn(
            "PLAN_XML_FRAGMENT",
            Types.CLOB,
            false),
            /*
        SystemColumnImpl.getUUIDColumn("RS_ID", false),
        SystemColumnImpl.getColumn(
            "OP_IDENTIFIER",
            Types.VARCHAR,
            false,
            TypeId.VARCHAR_MAXWIDTH),
        SystemColumnImpl.getColumn(
            "OP_DETAILS",
            Types.VARCHAR,
            true,
            TypeId.VARCHAR_MAXWIDTH),
        SystemColumnImpl.getColumn("NO_OPENS", Types.INTEGER, true),
        SystemColumnImpl.getColumn("NO_INDEX_UPDATES", Types.INTEGER, true),
        SystemColumnImpl.getColumn("LOCK_MODE", Types.CHAR, true, 2),
        SystemColumnImpl.getColumn("LOCK_GRANULARITY", Types.CHAR, true, 1),
        SystemColumnImpl.getUUIDColumn("PARENT_RS_ID", true),
        SystemColumnImpl.getColumn("EST_ROW_COUNT", Types.DOUBLE, true),
        SystemColumnImpl.getColumn("EST_COST", Types.DOUBLE, true),
        SystemColumnImpl.getColumn("AFFECTED_ROWS", Types.INTEGER, true),
        SystemColumnImpl.getColumn("DEFERRED_ROWS", Types.CHAR, true, 1),
        SystemColumnImpl.getColumn("INPUT_ROWS", Types.INTEGER, true),
        SystemColumnImpl.getColumn("SEEN_ROWS", Types.INTEGER, true),
        SystemColumnImpl.getColumn("SEEN_ROWS_RIGHT", Types.INTEGER, true),
        SystemColumnImpl.getColumn("FILTERED_ROWS", Types.INTEGER, true),
        SystemColumnImpl.getColumn("RETURNED_ROWS", Types.INTEGER, true),
        SystemColumnImpl.getColumn("EMPTY_RIGHT_ROWS", Types.INTEGER, true),
        SystemColumnImpl.getColumn("INDEX_KEY_OPT", Types.CHAR, true, 1),
        SystemColumnImpl.getUUIDColumn("SCAN_RS_ID", true),
        SystemColumnImpl.getUUIDColumn("SORT_RS_ID", true),
        SystemColumnImpl.getUUIDColumn("STMT_ID", false),
        SystemColumnImpl.getUUIDColumn("TIMING_ID", true),
        // GemStone changes BEGIN
        SystemColumnImpl.getColumn("INSERT_ORDER", Types.INTEGER, false),
        SystemColumnImpl.getUUIDColumn("DIST_RS_ID", true),
        SystemColumnImpl.getColumn("RANK", Types.INTEGER, true),
        // GemStone changes END*/
        
    };
  }

  @Override
  protected void addConstraints(
      StringBuilder sb) {
    // sb.append(" ,constraint insert_order_uniq unique(INSERT_ORDER, STMT_ID) ");
  }
  
  public String toString() {
    return "ResultSet@" + System.identityHashCode(this) + " rank=" + rank
        + " OP_IDENTIFIER=" + op_identifier + " RS_ID=" + rs_id + " STMT_ID="
        + stmt_id + (timing != null ? " TIMING=" + timing.getTimingID() : "")
        + (dist != null ? " DIST_ID=" + dist.getRSID() : "")
        + (scan != null ? " SCAN_ID=" + scan.getRSID() : "")
        + (sort != null ? " SORT_ID=" + sort.getRSID() : "") + " OP_DETAILS="
        + op_details;
  }

  // GemStone changes BEGIN
  public void setInputRows(
      int rows) {
    if (rows != -1)
      this.input_rows = rows;
  }

  public void setIndexKeyOptimization(
      String yesNo) {
    index_key_optimization = yesNo;
  }

  public void setRowsSeenRight(
      int rows) {
    if (rows != -1)
      seen_rows_right = rows;
  }
  
  public void setRowsSeen(
      int rows) {
    if (rows != -1)
      seen_rows = rows;
  }

  public void setEmptyRightRowsReturned(
      int rows) {
    if (rows != -1)
      empty_right_rows = rows;
  }
  
  public void setAffectedRows(
      int rows) {
    affected_rows = rows;
  }

  public void setDeferredRows(
      boolean deferred) {
    deferred_rows = XPLAINUtil.getYesNoCharFromBoolean(deferred);
  }

  public void setIndexesUpdated(int numIndex) {
    no_index_updates = numIndex;
  }
  
  public long getExecuteTime() {
    final long executeTime;
    if (timing != null) {
      executeTime = timing.getExecuteTime();
    } else if (dist != null) {
      executeTime = dist.getExecuteTime();
    }
    else {
      executeTime = 0;
    }
    return executeTime;
  }
  
  public UUID getRSID() {
    return rs_id;
  }

  @Override
  protected void createIndex(StringBuilder idx, String schemaName) {
    /*
    idx.append("create index ").append(schemaName).append(
        ".idx_rs_dist_rs_id on ").append(TABLENAME_STRING).append(
        "(dist_rs_id)");
    */
  }

  @Override
  public boolean equals(Object obj) {
    return compareTo((XPLAINResultSetDescriptor)obj) == 0;
  }

  @Override
  public int hashCode() {
    final long executeTime = getExecuteTime();
    return (int)(executeTime ^ (executeTime >>> 32));
  }

  public int compareTo(XPLAINResultSetDescriptor other) {
    
    assert other != null;
    
    long _this = getExecuteTime();
    long _other = other.getExecuteTime();
    
    // sort descending... 
    return (_this > _other ? -1 : _this < _other ? 1 : 0);  
  }

  public void setRank(int i) {
    this.rank = i;
  }
  
  public void setTotalExecuteTimeNanos(double executeTime) {
    this.totalExecutionTimeNanos = (executeTime == 0 ? 1 : executeTime);
  }
  
  public StringBuilder getXMLAttributes(final StringBuilder sb, final StatisticsCollectionObserver observer) {

    PlanUtils.xmlAttribute(sb, "rs_name", rs_name);
    PlanUtils.xmlAttribute(sb, "no_children", num_children);
    PlanUtils.xmlAttribute(sb, "name", op_identifier);
    PlanUtils.xmlAttribute(sb, "rank", rank);
    
    final long executeTime = getExecuteTime();
    PlanUtils.xmlAttribute(sb, "execute_time", executeTime/oneMillisNanos, "ms");
    
    if (timing != null) {
      if (rank <= 5) {
        PlanUtils.xmlAttribute(sb, "construct_time", timing.constructor_time/oneMillisNanos, "ms");
        PlanUtils.xmlAttribute(sb, "open_time", timing.open_time/oneMillisNanos, "ms");
        PlanUtils.xmlAttribute(sb, "next_time", timing.next_time/oneMillisNanos, "ms");
        PlanUtils.xmlAttribute(sb, "close_time", timing.close_time/oneMillisNanos, "ms");
      }
      if (timing.avg_next_time_per_row >= 0)
        PlanUtils.xmlAttribute(sb, "avg_next_time_per_row",
            timing.avg_next_time_per_row / oneMillisNanos, "ms");

      if (timing.projection_time >= 0)
        PlanUtils.xmlAttribute(sb, "projection_time", timing.projection_time
            / oneMillisNanos, "ms");

      if (timing.restriction_time >= 0)
        PlanUtils.xmlAttribute(sb, "restriction_time", timing.restriction_time
            / oneMillisNanos, "ms");

      if (timing.temp_cong_create_time >= 0)
        PlanUtils.xmlAttribute(sb, "temp_cong_create_time",
            timing.temp_cong_create_time / oneMillisNanos, "ms");

      if (timing.temp_cong_fetch_time >= 0)
        PlanUtils.xmlAttribute(sb, "temp_cong_fetch_time",
            timing.temp_cong_fetch_time / oneMillisNanos, "ms");

      if (observer != null) {
        observer.processedResultSetTimingDescriptor(timing);
      }
    }
    
    double delta = ((double)executeTime / totalExecutionTimeNanos);
    double percent =  delta * 100d;
    PlanUtils.xmlAttribute(sb, "percent_exec_time", PlanUtils.format.format(percent));

    if (no_opens != null)
      PlanUtils.xmlAttribute(sb, "no_opens", no_opens);
    
    if (input_rows != null)
      PlanUtils.xmlAttribute(sb, "input_rows", input_rows);
    
    if (returned_rows != null)
      PlanUtils.xmlAttribute(sb, "returned_rows", returned_rows);
    
    if (scan != null) {
      PlanUtils.xmlAttribute(sb, "scanned_object", scan.scan_object_name);
      PlanUtils.xmlAttribute(sb, "scan_type", scan.scan_type);
      
      if (scan.no_visited_pages != null)
        PlanUtils.xmlAttribute(sb, "visited_pages", scan.no_visited_pages);
      
      if (scan.no_visited_rows != null)
        PlanUtils.xmlAttribute(sb, "visited_rows", scan.no_visited_rows);
      
      PlanUtils.xmlAttribute(sb, "scan_qualifiers", scan.scan_qualifiers);
      PlanUtils.xmlAttribute(sb, "next_qualifiers", scan.next_qualifiers);
      if(observer != null) {
        observer.processedScanPropsDescriptor(scan);
      }
    }
    
    if (sort != null) {
      PlanUtils.xmlAttribute(sb, "sort_type", sort.sort_type);
      
      if (sort.no_input_rows != null)
        PlanUtils.xmlAttribute(sb, "sorter_input", sort.no_input_rows);
      
      if (sort.no_output_rows != null)
        PlanUtils.xmlAttribute(sb, "sorter_output", sort.no_output_rows);
      if(observer != null) {
        observer.processedSortPropsDescriptor(sort);
      }
    }
    
    PlanUtils.xmlAttribute(sb, "node_details", op_details);

    final String member_node;
    if (dist != null) {
      if (rank <= 5) {
        PlanUtils.xmlAttribute(sb, "ser_deser_time", dist.getSerDeSerTime()/oneMillisNanos, "ms");
        
        PlanUtils.xmlAttribute(sb, "process_time", dist.getProcessTime()/oneMillisNanos, "ms");
        
        PlanUtils.xmlAttribute(sb, "throttle_time", dist.getThrottleTime()/oneMillisNanos, "ms");
      }
      
      final String objectType = dist.getDistObjectType();
      if (XPLAINUtil.OP_QUERY_SEND.equals(objectType) || XPLAINUtil.OP_RESULT_SEND.equals(objectType)) {
        member_node = dist.getTargetMember();
      }
      else if (XPLAINUtil.OP_QUERY_SCATTER.equals(objectType)) {
        member_node = dist.getPrunedMemberList();
      }
      else {
        member_node = dist.getOriginator();
      }
      
      PlanUtils.xmlAttribute(sb, "member_node", member_node);
      
      if(observer != null) {
        observer.processedDistPropsDescriptor(dist);
      }
    }
    
    return sb;
  }

}
