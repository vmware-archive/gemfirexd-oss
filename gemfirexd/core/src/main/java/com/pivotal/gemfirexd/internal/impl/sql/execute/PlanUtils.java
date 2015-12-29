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
package com.pivotal.gemfirexd.internal.impl.sql.execute;

import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.util.ArrayDeque;
import java.util.Properties;

import com.gemstone.gemfire.internal.NanoTimer;
import com.gemstone.gemfire.internal.shared.NativeCalls;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.procedure.cohort.OutgoingResultSetImpl;
import com.pivotal.gemfirexd.internal.engine.sql.execute.AbstractGemFireResultSet;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.i18n.MessageService;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultSet;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;
import com.pivotal.gemfirexd.internal.impl.sql.catalog.XPLAINSortPropsDescriptor;

public final class PlanUtils {
  
  public static final String quotes = "\"";
  public static final String equals = "=";
  public static final String space = " ";
  public static final DecimalFormat format = new DecimalFormat("###.##");
  
  public final static class Context {
    final static ArrayDeque<Context> contextStack = new ArrayDeque<Context>();

    private boolean isNested;
    private final StringBuilder lineage;
    public int depth;
    public final Context parent;
    public long totalExecTime;
    
    public Context() {
      this.depth = 0;
      this.lineage = new StringBuilder();
      this.isNested = false;
      this.parent = null;
      this.totalExecTime = 0;
    }
    
    public Context(final Context parent) {
      this.depth = parent.depth + 1;
      this.lineage = new StringBuilder().append(parent.lineage).append("/");
      this.isNested = false;
      this.parent = parent;
      this.totalExecTime = parent.totalExecTime;
    }
    
    public Context pushContext() {
      contextStack.addLast(new Context(this));
      return contextStack.getLast();
    }
    
    public Context popContext() {
      return contextStack.removeLast();
    }

    public boolean setNested() {
      // if already nested, return unsuccess
      // for this method.
      if (this.isNested) {
        return false;
      }
      
      this.isNested = true;
      return true;
    }
    
    public Context setRoot() {
      this.isNested = false;
      return this;
    }

    public StringBuilder getLineage() {
      return this.lineage;
    }

    public void addLineage(String tagName) {
      this.lineage.append(tagName);
    }
    
  }

  /** isolation level codes */
  public static final String ISOLATION_READ_UNCOMMITED = "RU"; // 0

  public static final String ISOLATION_READ_COMMIT = "RC"; // 1

  public static final String ISOLATION_REPEAT_READ = "RR"; // 2

  public static final String ISOLATION_SERIALIZABLE = "SE"; // 3

  /** lock modes */
  public static final String LOCK_MODE_EXCLUSIVE = "EX";

  public static final String LOCK_MODE_INSTANTENOUS_EXCLUSIVE = "IX";

  public static final String LOCK_MODE_SHARE = "SH";

  public static final String LOCK_MODE_INSTANTENOUS_SHARE = "IS";

  /** lock granularity */
  public static final String LOCK_GRANULARITY_TABLE = "T";

  public static final String LOCK_GRANULARITY_ROW = "R";

  /** the rs operator codes */
  // scan operations
  // ---------------
  /** Scan on base table or covering index scan. data node only operation. */
  public static final String OP_TABLESCAN = "TABLESCAN";

  public static final String OP_MULTITABLESCAN = "MULTITABLESCAN";
  
  /** Index Scan which does a {@link #OP_ROWIDSCAN} to pick the base table row. data node only operation. */
  public static final String OP_INDEXSCAN = "INDEXSCAN";

  /** Hash Table scan once {@link #OP_HASHTABLE} is built. data node only operation. */
  public static final String OP_HASHSCAN = "HASHSCAN";

  /** Executing a DISTINCT sort. data node only operation. */
  public static final String OP_DISTINCTSCAN = "DISTINCTSCAN";

  /** Last Key optimised operation for determining MIN/MAX. data node only operation. */
  public static final String OP_LASTINDEXKEYSCAN = "LASTINDEXKEYSCAN";

  /** Hash Table creation. data node only operation. */
  public static final String OP_HASHTABLE = "HASHTABLE";

  /** Base Table row fetch from a index key. data node only operation. */
  public static final String OP_ROWIDSCAN = "ROWIDSCAN";

  /** Index scan created out of a CONSTRAINT in table definition. data node only operation. */
  public static final String OP_CONSTRAINTSCAN = "CONSTRAINTSCAN";

  // GemStone changes BEGIN
  /** Global Hash Index scan done for reference checks etc. data node only operation. */
  public static final String OP_GLOBALINDEXSCAN = "GLOBALINDEXSCAN";

  // GemStone changes END

  // join operations
  // ---------------
  /** NestedLoop Join operation. data node only operation. */
  public static final String OP_JOIN_NL = "NLJOIN";

  /** Hash Join operation. data node only operation. */
  public static final String OP_JOIN_HASH = "HASHJOIN";
  
  /** Non Colocated Join operation. data node only operation. */
  public static final String OP_JOIN_NCJ = "NCJOIN";

  /** Left or Right NestedLoop Outer Join operation. data node only operation. */
  public static final String OP_JOIN_NL_LO = "LOUTERNLJOIN";

  /** Left or Right Hash Outer Join operation. data node only operation. */
  public static final String OP_JOIN_HASH_LO = "LOUTERHASHJOIN";

  /** Merge Join operation. data node only operation. */
  public static final String OP_JOIN_MERGE = "MERGEJOIN";
  
  /** UNION/UNION ALL/UNION DISTINCT operation of two queries. data node only operation. */
  public static final String OP_UNION = "UNION";

  /** SET operation indicating one of {@link #OP_UNION}, {@link #OP_SET_INTERSECT} or {@link #OP_SET_EXCEPT}.
   * data node only operation. */
  public static final String OP_SET = "SET";

  // set operation details
  /** INTERSECT/INTERSECT ALL/INTERSECT DISTINCT operation of two queries. data node only operation. */
  public static final String OP_SET_INTERSECT = "INTERSECT";

  /** EXCEPT/EXCEPT ALL/EXCEPT DISTINCT operation of two queries. data node only operation. */
  public static final String OP_SET_EXCEPT = "EXCEPT";

  public static final String OP_SET_INTERSECT_ALL = "INTERSECTALL";

  /** EXCEPT/EXCEPT ALL/EXCEPT DISTINCT operation of two queries. data node only operation. */
  public static final String OP_SET_EXCEPT_ALL = "EXCEPTALL";
  
  // dml write operations
  // --------------------
  // basic operations
  /** INSERT dml operation */
  public static final String OP_INSERT = "INSERT";

  /** UPDATE dml operation */
  public static final String OP_UPDATE = "UPDATE";

  /** DELETE dml operation */
  public static final String OP_DELETE = "DELETE";

  // specialized op_details
  public static final String OP_CASCADE = "CASCADE";

  public static final String OP_VTI = "VTI";

  public static final String OP_BULK = "BULK";

  /** implicit or explicit DISTINCT clause */
  public static final String OP_DISTINCT = "DISTINCT";

  // other operations
  // ----------------
  public static final String OP_NORMALIZE = "NORMALIZE";

  public static final String OP_ANY = "ANY";

  /** Scroll Insensitive operation. data node only operation. */
  public static final String OP_SCROLL = "SCROLL-INSENSITIVE";

  public static final String OP_MATERIALIZE = "MATERIALIZE";

  public static final String OP_ONCE = "ONCE";

  public static final String OP_VTI_RS = "VTI";

  public static final String OP_ROW = "ROW";

  /** Projecting out few columns from the below source. data node only operation. */
  public static final String OP_PROJECT = "PROJECTION";

  /** Predicate filtering. data node only operation. */
  public static final String OP_FILTER = "FILTER";

  /** One of the aggregate operation among SUM / AVG / MIN / MAX. data node only operation. */
  public static final String OP_AGGREGATE = "AGGREGATION";

  /** Predicate filtering while projecting out columns at {@link #OP_PROJECT}. data node only operation. */
  public static final String OP_PROJ_RESTRICT = "PROJECT-FILTER";

  public static final String OP_AUTO_GEN_KEYS = "AUTO-GEN-KEYS";
  
  // sort operations
  // ----------------
  /** Sorting of rows while processing ORDER BY, DISTINCT, GROUP BY. data node only operation.  */
  public static final String OP_SORT = "SORT";

  /** Grouping of columns. data node only operation. */
  public static final String OP_GROUP = "GROUPBY";

  public static final String OP_CURRENT_OF = "CURRENT-OF";

  /** Fetch n Rows processing. data node only operation. */
  public static final String OP_ROW_COUNT = "ROW-COUNT";

  public static final String OP_WINDOW = "WINDOW";

  public static final String SCAN_PROPS = "SCAN-PROPERTIES";
  
  /** GemFire region entries scan. data node only operation. */
  public static final String SCAN_HEAP = "HEAP";

  public static final String SCAN_BTREE = "BTREE";

  /** Scan properties for sorting. data node only operation. */
  public static final String SCAN_SORT = "SORT";

  public static final String SCAN_BITSET_ALL = "ALL";

  public static final String DETAILS = "node_details";
  
  public static final String TG_SCAN_PROPS = "ScanProperties";
  
  public static final String TG_OPTIMIZER_OVERRIDE = "optimizer_overrides";
  
  /** the different statement type constants */
  public static final String SELECT_STMT_TYPE = "S";

  public static final String SELECT_APPROXIMATE_STMT_TYPE = "SA";

  public static final String INSERT_STMT_TYPE = "I";

  public static final String UPDATE_STMT_TYPE = "U";

  public static final String DELETE_STMT_TYPE = "D";

  public static final String CALL_STMT_TYPE = "C";

  public static final String DDL_STMT_TYPE = "DDL";

  /** the explain type constants */
  public static final String XPLAIN_ONLY = "O";

  public static final String XPLAIN_FULL = "F";

  /** sort info properties */
  public static final String SORT_EXTERNAL = "EX";

  /** in-memory sort happened without disk overflow */
  public static final String SORT_INTERNAL = "IN";

  /** yes no codes */
  public static final String YES_CODE = "Y";

  public static final String NO_CODE = "N";

  // GemStone changes BEGIN

  // distribute operations
  // --------------------
  // basic operations

  /*ATOMIC region operations */
  /** GemFire Region.get() query node only operation. */
  public static final String OP_GET = "REGION-GET";

  /** GemFire Region.put() query node only operation */
  public static final String OP_PUT = "REGION-PUT";

  /** GemFire Region.getAll() query node only operation */
  public static final String OP_GETTALL = "REGION-GETALL";
  
  /** GetAllLocalIndexExecutorMessage */
  public static final String OP_GETTALL_IDX = "INDEX-GETALL";

  /** GemFire Region.putAll() query node only operation */
  public static final String OP_PUTALL = "REGION-PUTALL";

  /** GemFire Region.get() query node only operation. */
  public static final String OP_SUBQUERY = "SUBQUERY";
  
  /** NCJ: NCJ Pull operation thats alike subquery. */
  public static final String OP_NCJPULL = "NCJ-PULL";
  
  /* Function messaging timing */
  /** Total messaging time (to & from) remote members. Only for query node. */
  public static final String OP_QUERY_SCATTER = "QUERY-SCATTER";

  /** Query message send time to a particular remote member. Only for query node. */
  public static final String OP_QUERY_SEND = "QUERY-SEND";
  
  /** Query message receive (including wait on scheduling) and de-serialize time on remote member. Only for data node. */
  public static final String OP_QUERY_RECEIVE = "QUERY-RECEIVE";

  /** Result serialization and send time to the originator. Only for data node. */
  public static final String OP_RESULT_SEND = "RESULT-SEND";

  /** Response de-serialization and receive time from a particular remote member. Only for query node. */
  public static final String OP_RESULT_RECEIVE = "RESULT-RECEIVE";
  
  /** Result data buffer deserialization and iteration time on query node and result serialization on data node. */
  public static final String OP_RESULT_HOLDER = "RESULT-HOLDER" ;
  
  /** Sequential consumption of multiple {@link #OP_RESULT_HOLDER} received from individual remote members on query node. */
  public static final String OP_SEQUENTIAL_ITERATOR = "SEQUENTIAL-ITERATION";
  
  /** RoundRobin consumption of multiple {@link #OP_RESULT_HOLDER} mainly during sorting performing n-way merge on query node.*/
  public static final String OP_ROUNDROBIN_ITERATOR = "ROUNDROBIN-ITERATION";
  
  /** Re-sorting of resulting rows on the querying node for total ordering on query node.*/
  public static final String OP_ORDERED_ITERATOR = "ORDERED-ITERATION";
  
  /** Re-grouping of rows received from individual remote members on query node.*/
  public static final String OP_GROUPED_ITERATOR = "GROUPED-ITERATION";
  
  /** Re-evaluation of outer joins after receiving rows from individual remote members on query node.*/
  public static final String OP_OUTERJOIN_ITERATOR = "OUTER-JOIN-ITERATION";
  
  /** Final top 'n' rows returned mentioned in FETCH n ROWS only clause on query node.*/
  public static final String OP_ROWCOUNT_ITERATOR = "ROW-COUNT-ITERATION";
  
  /** User held result set closing time and end of distribution time on query node */
  public static final String OP_DISTRIBUTION_END = "DISTRIBUTION-END" ;

  // distribution direction
  // --------------------
  public enum DIRECTION {
    IN,
    OUT
  }

  // GemStone changes END

  // ---------------------------------------------
  // utility functions
  // ---------------------------------------------

  public static String getYesNoCharFromBoolean(
      boolean test) {
    if (test) {
      return YES_CODE;
    }
    else {
      return NO_CODE;
    }
  }

  public static String getHashKeyColumnNumberString(
      int[] hashKeyColumns) {
    if (hashKeyColumns == null)
      return null;
    // original derby encoding
    String hashKeyColumnString;
    if (hashKeyColumns.length == 1) {
      hashKeyColumnString = MessageService
          .getTextMessage(SQLState.RTS_HASH_KEY)
          + " " + hashKeyColumns[0];
      return hashKeyColumnString;
    }
    else {
      hashKeyColumnString = MessageService
          .getTextMessage(SQLState.RTS_HASH_KEYS)
          + " (" + hashKeyColumns[0];
      final StringBuilder sb = new StringBuilder(hashKeyColumnString);
      for (int index = 1; index < hashKeyColumns.length; index++) {
        sb.append(",").append(hashKeyColumns[index]);
      }
      sb.append(")");
      return sb.toString();
    }
  }

  /** util function, to resolve the lock mode, and return a lock mode code */
  public static String getLockModeCode(
      String lockString) {
    lockString = lockString.toUpperCase();
    if (lockString.startsWith("EXCLUSIVE")) {
      return LOCK_MODE_EXCLUSIVE;
    }
    else if (lockString.startsWith("SHARE")) {
      return LOCK_MODE_SHARE;
    }
    else if (lockString.startsWith("INSTANTANEOUS")) {
      int start = "INSTANTANEOUS".length();
      int length = lockString.length();
      String sub = lockString.substring(start + 1, length);
      if (sub.startsWith("EXCLUSIVE")) {
        return LOCK_MODE_INSTANTENOUS_EXCLUSIVE;
      }
      else if (sub.startsWith("SHARE")) {
        return LOCK_MODE_INSTANTENOUS_SHARE;
      }
      else
        return null;
    }
    else
      return null;
  }

  /**
   * util function, to resolve the isolation level and return a isolation level
   * code
   */
  public static String getIsolationLevelCode(
      int isolationLevel) {
    switch (isolationLevel) {
      case TransactionController.ISOLATION_SERIALIZABLE:
        return ISOLATION_SERIALIZABLE; // 3

      case TransactionController.ISOLATION_REPEATABLE_READ:
        return ISOLATION_REPEAT_READ; // 2

      case TransactionController.ISOLATION_READ_COMMITTED_NOHOLDLOCK:
        // fall through
      case TransactionController.ISOLATION_READ_COMMITTED:
        return ISOLATION_READ_COMMIT; // 1

      case TransactionController.ISOLATION_READ_UNCOMMITTED:
        return ISOLATION_READ_UNCOMMITED; // 0
      default:
        return null;
    }
  }

  /**
   * util function, to resolve the lock granularity and return a lock
   * granularity code
   */
  public static String getLockGranularityCode(
      String lockString) {
    lockString = lockString.toUpperCase();
    if (lockString.endsWith("TABLE")) {
      return LOCK_GRANULARITY_TABLE;
    }
    else {
      return LOCK_GRANULARITY_ROW;
    }
  }

  /**
   * This method helps to figure out the statement type and returns an
   * appropriate return code, characterizing the stmt type.
   */
  public static String getStatementType(
      String SQLText) {
    String type = "";
    String text = SQLText.toUpperCase().trim();
    if (text.startsWith("CALL")) {
      type = CALL_STMT_TYPE;
    }
    else if (text.startsWith("SELECT")) {
      if (text.indexOf("~") > -1) {
        type = SELECT_APPROXIMATE_STMT_TYPE;
      }
      else {
        type = SELECT_STMT_TYPE;
      }
    }
    else if (text.startsWith("DELETE")) {
      type = DELETE_STMT_TYPE;
    }
    else if (text.startsWith("INSERT")) {
      type = INSERT_STMT_TYPE;
    }
    else if (text.startsWith("UPDATE")) {
      type = UPDATE_STMT_TYPE;
    }
    else if (text.startsWith("CREATE") || text.startsWith("ALTER")
        || text.startsWith("DROP")) {
      type = DDL_STMT_TYPE;
    }
    return type;
  }

  /**
   * helper method which extracts the right (non-internationalzed) scan
   * properties of the scan info properties
   */
  public static StringBuilder extractScanProps(
      final StringBuilder builder,
      final Properties scanProps) {

    // Heap Scan Info Properties
    // extract scan type with the help of the international message service
    String scan_type = "";
    String scan_type_property = scanProps.getProperty(MessageService
        .getTextMessage(SQLState.STORE_RTS_SCAN_TYPE));
    if (scan_type_property != null) {
      if (scan_type_property.equalsIgnoreCase(MessageService
          .getTextMessage(SQLState.STORE_RTS_HEAP))) {
        scan_type = SCAN_HEAP;
      }
      else if (scan_type_property.equalsIgnoreCase(MessageService
          .getTextMessage(SQLState.STORE_RTS_SORT))) {
        scan_type = SCAN_SORT;
      }
      else if (scan_type_property.equalsIgnoreCase(MessageService
          .getTextMessage(SQLState.STORE_RTS_BTREE))) {
        scan_type = SCAN_BTREE;
      }
    }
    else {
      scan_type = null;
    }
    xmlAttribute(builder, "scan_type", scan_type);

    // extract the number of visited pages
    String vp_property = scanProps.getProperty(MessageService
        .getTextMessage(SQLState.STORE_RTS_NUM_PAGES_VISITED));
    if (vp_property != null) {
      xmlAttribute(builder, "visited_pages", Integer.valueOf(vp_property));
    }

    // extract the number of visited rows
    String vr_property = scanProps.getProperty(MessageService
        .getTextMessage(SQLState.STORE_RTS_NUM_ROWS_VISITED));
    if (vr_property != null) {
      xmlAttribute(builder, "visited_rows", Integer.valueOf(vr_property));
    }

    // extract the number of qualified rows
    String qr_property = scanProps.getProperty(MessageService
        .getTextMessage(SQLState.STORE_RTS_NUM_ROWS_QUALIFIED));
    if (qr_property != null) {
      xmlAttribute(builder, "qualified_rows", Integer.valueOf(qr_property));
    }

    // extract the number of fetched columns
    String fc_property = scanProps.getProperty(MessageService
        .getTextMessage(SQLState.STORE_RTS_NUM_COLUMNS_FETCHED));
    if (fc_property != null) {
      xmlAttribute(builder, "fetched_cols", Integer.valueOf(fc_property));
    }

    // extract the number of deleted visited rows
    String dvr_property = scanProps.getProperty(MessageService
        .getTextMessage(SQLState.STORE_RTS_NUM_DELETED_ROWS_VISITED));
    if (dvr_property != null) {
      xmlAttribute(builder, "visited_deleted_rows", Integer.valueOf(dvr_property));
    }

    // extract the btree height
    String bth_property = scanProps.getProperty(MessageService
        .getTextMessage(SQLState.STORE_RTS_TREE_HEIGHT));
    if (bth_property != null) {
      xmlAttribute(builder, "btree_height", Integer.valueOf(bth_property));
    }

    // extract the fetched bit set
    String bs_property = scanProps.getProperty(MessageService
        .getTextMessage(SQLState.STORE_RTS_COLUMNS_FETCHED_BIT_SET));
    if (bs_property != null) {
      if (bs_property.equalsIgnoreCase(MessageService
          .getTextMessage(SQLState.STORE_RTS_ALL))) {
        xmlAttribute(builder, "bitset_fetched_cols", SCAN_BITSET_ALL);
      }
      else {
        xmlAttribute(builder, "bitset_fetched_cols", bs_property);
      }
    }

    // return the filled descriptor
    return builder;
  }

  /**
   * helper method which extracts the right (non-internationalzed) sort
   * properties of the sort info properties object
   * 
   * @param descriptor
   *          the descriptor to fill with properties
   * @param sortProps
   *          the provided sort props
   * @return the filled descriptor
   */
  public static XPLAINSortPropsDescriptor extractSortProps(
      XPLAINSortPropsDescriptor descriptor,
      Properties sortProps) {

    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TracePlanGeneration) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_PLAN_GENERATION,
            "Extracting Sort properties from " + sortProps);
      }
    }
    
    String sort_type = null;
    String sort_type_property = sortProps.getProperty(MessageService
        .getTextMessage(SQLState.STORE_RTS_SORT_TYPE));
    if (sort_type_property != null) {
      if (sort_type_property.equalsIgnoreCase(MessageService
          .getTextMessage(SQLState.STORE_RTS_EXTERNAL))) {
        sort_type = SORT_EXTERNAL;
      }
      else {
        sort_type = SORT_INTERNAL;
      }
    }
    descriptor.setSort_type(sort_type);

    String ir_property = sortProps.getProperty(MessageService
        .getTextMessage(SQLState.STORE_RTS_NUM_ROWS_INPUT));
    if (ir_property != null) {
      descriptor.setNo_input_rows(Integer.valueOf(ir_property));
    }

    String or_property = sortProps.getProperty(MessageService
        .getTextMessage(SQLState.STORE_RTS_NUM_ROWS_OUTPUT));
    if (or_property != null) {
      descriptor.setNo_output_rows(Integer.valueOf(or_property));
    }

    if (sort_type == SORT_EXTERNAL) {
      String nomr_property = sortProps.getProperty(MessageService
          .getTextMessage(SQLState.STORE_RTS_NUM_MERGE_RUNS));

      if (nomr_property != null) {
        descriptor.setNo_merge_runs(Integer.valueOf(nomr_property));
      }

      String nomrd_property = sortProps.getProperty(MessageService
          .getTextMessage(SQLState.STORE_RTS_MERGE_RUNS_SIZE));

      if (nomrd_property != null) {
        descriptor.setMerge_run_details(nomrd_property);
      }

    }

    return descriptor;
  }

  /**
   * Compute average, avoiding divide-by-zero problems.
   * 
   * @param dividend
   *          the long value for the dividend (the whole next time)
   * @param divisor
   *          the long value for the divisor (the sum of all rows seen)
   * @return the quotient or null
   */
  public static long getAVGNextTime(
      long dividend,
      long divisor) {
    if (divisor == 0)
      return 0;
    if (dividend == 0)
      return 0;
    return dividend / divisor;
  }

  public static final long nanoTime() {
    return NanoTimer.nanoTime();
  }

  public static final Timestamp currentTimeStamp() {
    return new Timestamp(System.currentTimeMillis());
  }

  public static final long currentTimeMillis() {
    return System.currentTimeMillis();
  }

  public static final long recordTiming(final long startTime) {
    //start timer
    if (startTime == -1) {
      return nanoTime();
    }
    // no op 
    else if (startTime == -2) {
      return 0;
    }
    //end timer
    else {
      return nanoTime() - startTime;
    }
  }

  public final static class ChildNodeTimeCollector extends
      AbstractPolymorphicStatisticsCollector {

    private ResultSet rootrs = null;

    private long totalTime = 0;

    public ChildNodeTimeCollector(final ResultSetStatisticsVisitor nextCollector) {
      super();
    }

    public void visitVirtual(
        final NoRowsResultSetImpl rs) {
      // ignore self.
      if (rs == rootrs) {
        return;
      }

      totalTime += ( rs.endExecutionTime - rs.beginExecutionTime);
    }

    public void visitVirtual(
        final BasicNoPutResultSetImpl rs) {

      // ignore self.
      if (rs == rootrs) {
        return;
      }

      totalTime += rs.constructorTime + rs.openTime  + rs.nextTime + rs.closeTime;
    }

    @Override
    public void visitVirtual(
        final AbstractGemFireResultSet rs) {
      // ignore self.
      if (rs == rootrs) {
        return;
      }
      
      totalTime += rs.openTime + rs.nextTime + rs.closeTime;      
    }

    @Override
    public void visitVirtual(
        final OutgoingResultSetImpl rs) {
      // TODO Auto-generated method stub

    }

    @Override
    public void visitVirtual(
        final TemporaryRowHolderResultSet rs) {
      // TODO Auto-generated method stub

    }

    public void clear() {
      totalTime = 0;
      rootrs = null;
    }

    public void setRootRs(
        final ResultSet rs) {
      this.rootrs = rs;
    }

    public long getNodeTime() {
      return totalTime;
    }
  }
  
  
  public static final long getTimeSpent(final long construct, final long open, final long next, final long close, final int attribute) {
    SanityManager.ASSERT(construct >= 0 , Long.toString(construct));
    SanityManager.ASSERT(open >= 0, Long.toString(open));
    SanityManager.ASSERT(next >= 0, Long.toString(next));
    SanityManager.ASSERT(close >= 0 , Long.toString(close));
    
    switch(attribute) {
      case ResultSet.ALL:
        final long v = (construct + open + next + close);
        SanityManager.ASSERT(v >= 0, Long.toString(v));
        return v;
      case ResultSet.CONSTRUCT_TIME:
        return construct;
      case ResultSet.OPEN_TIME:
        return open;
      case ResultSet.NEXT_TIME:
        return next;
      case ResultSet.CLOSE_TIME:
        return close;
      default:
        return construct + open + next + close;
    }
    
  }
  
  public static final StringBuilder addSpaces(StringBuilder builder, int num) {
    do {
      builder.append(space);
    }
    while (--num > 0);
    return builder;
  }
  
  public static final StringBuilder xmlBeginTag(final StringBuilder builder, final Context context, final Object tag) {
    final String tagName = tag.getClass().getSimpleName();
    return xmlBeginTag(builder, context, tagName);
  }

  public static final StringBuilder xmlBeginTag(final StringBuilder builder, final Context context, final String tagName) {
    context.addLineage(tagName);
    return addSpaces(builder,context.depth*2).append("<").append(tagName);
  }
  
  public static final StringBuilder xmlTermTag(final StringBuilder builder,
      final Context context, final String name) {
    return xmlTermTag(builder, context, name, false);
  }
  
  public static final StringBuilder xmlTermTag(final StringBuilder builder,
      final Context context, final String name, final boolean isLeaf) {
    xmlAttribute(builder, "name", name);
    xmlAttribute(builder, "lineage", context.getLineage());
    xmlAttribute(builder, "depth", context.depth);
    return builder.append(isLeaf ? "/>" : ">").append(SanityManager.lineSeparator);
  }
  
  public static final StringBuilder xmlCloseTag(final StringBuilder builder,
      final Context context, final Object tag) {
    final String tagName = tag.getClass().getSimpleName();
    return xmlCloseTag(builder, context, tagName);
  }

  public static final StringBuilder xmlCloseTag(final StringBuilder builder,
      final Context context, final String tagName) {
    final Context current = context.popContext();
    return addSpaces(builder, current.depth * 2).append("</").append(tagName)
        .append(">").append(SanityManager.lineSeparator);
  }
  
  public static final StringBuilder xmlAddTag(final StringBuilder builder, Context context, final String name) {
    addSpaces(builder,context.depth*2).append("<").append(name);
    return xmlTermTag(builder, context, name, true);
  }
  
  
  public static final StringBuilder escapeForXML(String text) {
    if (text == null) {
      return new StringBuilder("NULL");
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < text.length(); i++) {
      char ch = text.charAt(i);
        switch (ch) {
        case '&':
          sb.append("&amp;");
          break;
        case '<':
          sb.append("&lt;");
          break;
        case '>':
          sb.append("&gt;");
          break;
        case '\'':
          sb.append("&apos;");
          break;
        case '"':
          sb.append("&quot;");
          break;
        default:
          sb.append(ch);
        }
    }
    
    return sb;
  }
  
  public static final StringBuilder xmlAttribute(final StringBuilder builder, final String attName, final StringBuilder value) {
    return builder.append(space).append(attName).append(equals).append(quotes).append(value).append(quotes);
  }
  
  public static final StringBuilder xmlAttribute(final StringBuilder builder, final String attName, final String value, final String comment) {
    return builder.append(space).append(attName).append(equals).append(quotes).append(escapeForXML(value)).append(comment != null ? comment : "").append(quotes);
  }
  
  public static final StringBuilder xmlAttribute(final StringBuilder builder, final String attName, final double value, final String comment) {
    return builder.append(space).append(attName).append(equals).append(quotes).append(value).append(comment != null ? comment : "").append(quotes);
  }
  
  public static final StringBuilder xmlAttribute(final StringBuilder builder, final String attName, final long value) {
    return builder.append(space).append(attName).append(equals).append(quotes).append(value).append(quotes);
  }
  
  public static final StringBuilder xmlAttribute(final StringBuilder builder, final String attName, final String value) {
    return builder.append(space).append(attName).append(equals).append(quotes).append(escapeForXML(value)).append(quotes);
  }
  
  public static final StringBuilder xmlAttribute(final StringBuilder builder, final String attName, final boolean value) {
    return builder.append(space).append(attName).append(equals).append(quotes).append(value).append(quotes);
  }

  public static final StringBuilder xmlAttribute(final StringBuilder builder, final String attName, final Timestamp value) {
    return builder.append(space).append(attName).append(equals).append(quotes).append(escapeForXML(value.toString())).append(quotes);
  }
  
}
