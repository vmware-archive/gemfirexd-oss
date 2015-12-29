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
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.impl.jdbc.Util;
import com.pivotal.gemfirexd.internal.impl.services.uuid.BasicUUID;
import com.pivotal.gemfirexd.internal.impl.sql.execute.xplain.XPLAINUtil;
import com.pivotal.gemfirexd.internal.impl.sql.execute.xplain.XPLAINUtil.XMLForms;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;


/**
 * This outputs a shorter version of the query plan with optimized 
 * brittle set of queries.
 *  
 * @author soubhikc
 * 
 */
public final class CreateResultSet extends AbstractCreatePlan {

  private final long[] maxMinAvgSum = new long[4];

  public CreateResultSet(AccessDistributedSystem access, boolean isRemote) {
    super(access, isRemote, XMLForms.none, null);
  }

  @SuppressFBWarnings(value="RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
  public List<String> getPlan() throws SQLException {

    final List<String> rows = new ArrayList<String>();
    
    try {
      
      final ExecutionPlanMessage msg = sendMessage();
      
      String queryNodePlanText = null;
      Element maxTimeNodePlanX = null, minTimeNodePlanX = null;
      for (char[] c : msg.getResults()) {
        if (SanityManager.DEBUG) {
          if (GemFireXDUtils.TracePlanGeneration) {
            SanityManager.DEBUG_PRINT(
                GfxdConstants.TRACE_PLAN_GENERATION,
                "received plan "
                    + new String(c)
                    + " from "
                    + (msg.getSender() == null ? " local member " : msg
                        .getSender()));
          }
        }
        
        if (GemFireXDUtils.TracePlanGeneration) {
            SanityManager.DEBUG_PRINT(
                GfxdConstants.TRACE_PLAN_GENERATION,
                " processing with "
                    + (c != null ? " successful XML transformation "
                        : " ALERT!!!!, unsucessful XML transformation "));
        }
        if (c != null && c.length > 5 && c[0] == '<' && c[1] == '?'
            && c[2] == 'x' && c[3] == 'm' && c[4] == 'l') {
          Element e = CreateXML.transformToXML(c);
          long currentPlanTime = getTimeTaken(e);
          if (currentPlanTime > getTimeTaken(maxTimeNodePlanX)) {
            maxTimeNodePlanX = e;
          }
          if (minTimeNodePlanX == null
              || currentPlanTime < getTimeTaken(minTimeNodePlanX)) {
            minTimeNodePlanX = e;
          }
        }
        else if(queryNodePlanText == null) {
          queryNodePlanText = c != null ? String.valueOf(c) : "NULL";
        }
      }

      rows.add(queryNodePlanText);

      final List<Element> el = new ArrayList<Element> ();
      
      if (maxTimeNodePlanX != null) {
        el.add(maxTimeNodePlanX);
        rows.add("Slowest Member Plan:\n"
            + String.valueOf(Misc.serializeXMLAsCharArr(el, "vanilla_text.xsl")));
      }

      el.clear();

      if (minTimeNodePlanX != null) {
        el.add(minTimeNodePlanX);
        rows.add("Fastest Member Plan:\n"
            + String.valueOf(Misc.serializeXMLAsCharArr(el, "vanilla_text.xsl")));
      }

    } catch (SQLException ex) {
      throw ex;
    } catch (Throwable t) {
      throw Util.javaException(t);
    }

    return rows;
  }

  private long getTimeTaken(Element e) {
    if(e == null) {
      return 0;
    }
    NodeList nl = e.getChildNodes();
    java.sql.Timestamp begin = null, end = null;
    for(int i = 0; i < nl.getLength(); i++) {
      Node n = nl.item(i);
      if (n == null) {
        continue;
      }
      else if(n.getNodeName().equalsIgnoreCase("begin_exe_time")) {
        begin = java.sql.Timestamp.valueOf(n.getTextContent());
      }
      else if (n.getNodeName().equalsIgnoreCase("end_exe_time")) {
        end = java.sql.Timestamp.valueOf(n.getTextContent());
      }
      else {
        continue;
      }
    }
    
    assert (end != null && begin != null);
    
    return (end.getTime() - begin.getTime());
  }

  @Override
  void processPlan(CharArrayWriter out, boolean isLocalPlanExtracted)
      throws SQLException, IOException {

    // query node plan
    if(!ds.isRemote() && !ds.isDerbyActivation()) {
      createBaseInfo(out);
      createScatterInfo(out);
      createBaseIterationInfo(out);
      createOtherIterationInfo(out);
    }
    else {
      
      /*if( !createBaseResponseInfo(out) ) {
        // no possibility of local execution as well.
        return;
      }
      
      createMsgReceiveAndResultSendInfo(out);*/
      
    }
    
    // to avoid recursion further.
    if (!isLocalPlanExtracted) {
      BasicUUID locallyExecutedId = new BasicUUID(ds.getQueryID());
      // for derby activation, without this flag, local plan will be captured.
      if (!ds.isDerbyActivation()) {
        locallyExecutedId.setLocallyExecuted(1);
      }
      if (SanityManager.DEBUG) {
        if (GemFireXDUtils.TracePlanGeneration) {
          SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_PLAN_GENERATION,
              "Now determining locally Executed Plan if any for "
                  + ds.getQueryID() + " with local stmtUUID="
                  + locallyExecutedId.toString());
        }
      }

      CharArrayWriter tmpXML = new CharArrayWriter();
      new CreateXML(ds.getClone(locallyExecutedId.toString()), true, XMLForms.none, null)
          .processPlan(tmpXML, true);
      if (SanityManager.DEBUG) {
        if (GemFireXDUtils.TracePlanGeneration) {
          SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_PLAN_GENERATION,
              "CreateResultSet: Got Local Plan as : " + tmpXML.size()
                  + " data " + tmpXML.toString());
        }
      }
      if(tmpXML.size() > 0) {
        Element e = CreateXML.transformToXML(tmpXML.toCharArray());
        final List<Element> el = new ArrayList<Element> ();
        el.add(e);
        out.write(String.valueOf(Misc.serializeXMLAsCharArr(el, "vanilla_text.xsl")));
      }
      tmpXML.close();
      
      /*new CreateResultSet(ds.getClone(locallyExecutedId.toString()), true)
          .processPlan(out, true);*/
    }
  }

  /**
   * Query Node base information. Begin distribution and result merging.
   * 
   * @param out
   * @throws SQLException
   * @throws IOException
   */
  private void createBaseInfo(CharArrayWriter out) throws SQLException, IOException {
    PreparedStatement ps;
    ResultSet results;
    StringBuilder introLine = new StringBuilder();

    final String introQuery = "SELECT ORIGIN_MEMBER_ID, BEGIN_EXE_TIME, END_EXE_TIME from "
        + GfxdConstants.PLAN_SCHEMA
        + ".SYSXPLAIN_STATEMENTS a join "
        + GfxdConstants.PLAN_SCHEMA
        + ".SYSXPLAIN_STATEMENT_TIMINGS b on a.TIMING_ID = b.TIMING_ID where a.STMT_ID = ? ";
    ps = ds.conn.prepareStatementByPassQueryInfo(-1, introQuery, false, false, false, null, 0, 0);
    ps.setString(1, ds.getQueryID());
    results = ps.executeQuery();
    if (results.next()) {
      introLine.append("ORIGINATOR ").append(results.getString(1)).append(
          " BEGIN TIME ").append(results.getTimestamp(2)).append(" END TIME ")
          .append(results.getTimestamp(3)).append("\n");
    }
    assert !results.next();

    // introLine.append(Misc.getGemFireCache().getMyId().toString());
    out.write(introLine.toString());
  }

  /**
   * Data Node base information. Receive query and return individual member results.
   *  
   * @param out
   * @return
   * @throws SQLException
   * @throws IOException
   */
  private boolean createBaseResponseInfo(CharArrayWriter out) throws SQLException, IOException {
    PreparedStatement ps;
    ResultSet results;
    StringBuilder introLine = new StringBuilder();
    
    final String introQuery = "SELECT CURRENT_MEMBER_ID, BEGIN_EXE_TIME, END_EXE_TIME from "
        + GfxdConstants.PLAN_SCHEMA
        + ".SYSXPLAIN_STATEMENTS a join "
        + GfxdConstants.PLAN_SCHEMA
        + ".SYSXPLAIN_STATEMENT_TIMINGS b on a.TIMING_ID = b.TIMING_ID where a.STMT_ID = ? ";
    ps = ds.conn.prepareStatementByPassQueryInfo(-1, introQuery, false, false,
        false, null, 0, 0);
    ps.setString(1, ds.getQueryID());
    results = ps.executeQuery();
    if (results.next()) {
      introLine.append("LOCAL MEMBER ").append(results.getString(1)).append(
          " BEGIN TIME ").append(results.getTimestamp(2)).append(" END TIME ")
          .append(results.getTimestamp(3));
      assert !results.next();
      out.write(introLine.toString());
      return true;
    }

    // nothing here.
    return false;
  }

  /**
   * Query Node scattering information.
   * 
   * @param out
   * @throws SQLException
   * @throws IOException
   */
  private void createScatterInfo(CharArrayWriter out) throws SQLException, IOException {
    PreparedStatement ps;
    ResultSet results;
    StringBuilder scatterLine = new StringBuilder();

    // DISTRIBUTION ( n members ) 99999 microseconds
    {
      final String queryScatter = "SELECT 'DISTRIBUTION to ' || TRIM(CHAR(NO_PRUNED_MEMBERS)) || ' members took ' || TRIM(CHAR(SCATTER_TIME/1000)) || ' microseconds '  from "
          + GfxdConstants.PLAN_SCHEMA
          + ".SYSXPLAIN_DIST_PROPS "
          + " where STMT_RS_ID = ? and DIST_OBJECT_TYPE = '"
          + XPLAINUtil.OP_QUERY_SCATTER + "' ";

      ps = ds.conn.prepareStatementByPassQueryInfo(-1, queryScatter, false,
          false, false, null, 0, 0);
      ps.setString(1, ds.getQueryID());
      results = ps.executeQuery();
      if (results.next()) {
        scatterLine.append(results.getString(1));
      }
      assert !results.next();

      results.close();
      ps.close();
    }

    getCompressedDistDetails(XPLAINUtil.OP_QUERY_SEND, maxMinAvgSum);
    scatterLine.append(" ( message sending min/max/avg time ").append(
        maxMinAvgSum[0]).append("/").append(maxMinAvgSum[1]).append("/")
        .append(maxMinAvgSum[2]).append(" microseconds");

    getCompressedDistDetails(XPLAINUtil.OP_RESULT_RECEIVE, maxMinAvgSum);
    scatterLine.append(" and receiving min/max/avg time ").append(
        maxMinAvgSum[0]).append("/").append(maxMinAvgSum[1]).append("/")
        .append(maxMinAvgSum[2]).append(" microseconds ) ").append("\n");

    out.write(scatterLine.toString());
  }

  /**
   * Data Node messaging information.
   * 
   * @param out
   * @throws SQLException 
   * @throws IOException
   */
  private void createMsgReceiveAndResultSendInfo(CharArrayWriter out) throws SQLException, IOException {
    PreparedStatement ps;
    ResultSet results;
    StringBuilder gatherLine = new StringBuilder();
    
    getCompressedDistDetails(XPLAINUtil.OP_QUERY_RECEIVE, maxMinAvgSum);
    
    getCompressedDistDetails(XPLAINUtil.OP_RESULT_SEND, maxMinAvgSum);
    
    
    {
      final String queryScatter = "SELECT 'DISTRIBUTION to ' || TRIM(CHAR(NO_PRUNED_MEMBERS)) || ' members took ' || TRIM(CHAR(SCATTER_TIME/1000)) || ' microseconds '  from "
          + GfxdConstants.PLAN_SCHEMA
          + ".SYSXPLAIN_DIST_PROPS "
          + " where STMT_RS_ID = ? and DIST_OBJECT_TYPE = '"
          + XPLAINUtil.OP_QUERY_SCATTER + "' ";

      ps = ds.conn.prepareStatementByPassQueryInfo(-1, queryScatter, false,
          false, false, null, 0, 0);
      ps.setString(1, ds.getQueryID());
      results = ps.executeQuery();
      if (results.next()) {
        gatherLine.append(results.getString(1));
      }
      assert !results.next();

      results.close();
      ps.close();
    }
    
    
    out.write(gatherLine.toString());
  }
  
  /**
   * Query Node result retrieval and iteration information.
   * 
   * @param out
   * @throws SQLException
   * @throws IOException
   */
  private void createBaseIterationInfo(CharArrayWriter out)
      throws SQLException, IOException {
    PreparedStatement ps;
    ResultSet results;
    final StringBuilder resultIterationLine = new StringBuilder();

    getCompressedDistDetails(XPLAINUtil.OP_RESULT_HOLDER, maxMinAvgSum);
    ps = ds.conn.prepareStatementByPassQueryInfo(-1,
        "SELECT b.OP_IDENTIFIER, b.RETURNED_ROWS, a.EXECUTE_TIME " + " from ("
            + GfxdConstants.PLAN_SCHEMA
            + ".SYSXPLAIN_RESULTSET_TIMINGS a JOIN "
            + GfxdConstants.PLAN_SCHEMA
            + ".SYSXPLAIN_RESULTSETS b on a.timing_id = b.timing_id) "
            + " where b.STMT_ID = ? and b.OP_IDENTIFIER IN ('"
            + XPLAINUtil.OP_SEQUENTIAL_ITERATOR + "','"
            + XPLAINUtil.OP_ROUNDROBIN_ITERATOR + "','"
            + XPLAINUtil.OP_GET + "','"
            + XPLAINUtil.OP_PUT + "','"
            + XPLAINUtil.OP_GETTALL + "','"
            + XPLAINUtil.OP_LI_GETTALL + "','"
            + XPLAINUtil.OP_PUTALL + "')", false, false,
            false, null, 0, 0);
    ps.setString(1, ds.getQueryID());
    results = ps.executeQuery();
    if (results.next()) {
      resultIterationLine.append(results.getString(1)).append(" of ").append(
          results.getInt(2)).append(" rows took ").append(
          (results.getLong(3) / 1000)).append(" microseconds").append("\n");
    }

    if (SanityManager.ASSERT) {
      if (results.next()) {
        StringBuilder errStr = new StringBuilder();
        do {
          errStr.append("op_identifier ").append(results.getString(1)).append(
              " stmt_id ").append(ds.getQueryID()).append("\n");
        } while (results.next());
        SanityManager.THROWASSERT("ROWS found \n" + errStr.toString()
            + "\n current iteration line " + resultIterationLine.toString());
      }
    }

    out.write(resultIterationLine.toString());
  }

  /**
   * Query Node result aggregation/reduction post distribution.
   * 
   * @param out
   * @throws SQLException
   * @throws IOException
   */
  private void createOtherIterationInfo(CharArrayWriter out)
      throws SQLException, IOException {
    PreparedStatement ps;
    ResultSet results;
    final StringBuilder resultIterationLine = new StringBuilder();

    ps = ds.conn
        .prepareStatementByPassQueryInfo(
            -1,
            " SELECT b.OP_IDENTIFIER, b.RETURNED_ROWS, a.EXECUTE_TIME, c.NO_INPUT_ROWS "
                + " from ("
                + GfxdConstants.PLAN_SCHEMA
                + ".SYSXPLAIN_RESULTSET_TIMINGS a JOIN "
                + GfxdConstants.PLAN_SCHEMA
                + ".SYSXPLAIN_RESULTSETS b on a.timing_id = b.timing_id LEFT OUTER JOIN "
                + GfxdConstants.PLAN_SCHEMA
                + ".SYSXPLAIN_SORT_PROPS c on b.SORT_RS_ID = c.SORT_RS_ID) "
                + " where b.STMT_ID = ? and b.OP_IDENTIFIER IN ('"
                + XPLAINUtil.OP_ORDERED_ITERATOR + "','"
                + XPLAINUtil.OP_GROUPED_ITERATOR + "','"
                + XPLAINUtil.OP_OUTERJOIN_ITERATOR + "','"
                + XPLAINUtil.OP_ROWCOUNT_ITERATOR
                + "' ) order by INSERT_ORDER ", false, false, false, null, 0, 0);
    ps.setString(1, ds.getQueryID());
    results = ps.executeQuery();
    while (results.next()) {
      final String opIdentifier = results.getString(1);
      resultIterationLine.append(opIdentifier).append(" of ").append(
          results.getInt(2)).append(" rows with ");

      final int numInputRows = results.getInt(4);
      if (numInputRows != 0) {
        resultIterationLine.append(numInputRows).append(" input rows took ");
      }
      resultIterationLine.append((results.getLong(3) / 1000)).append(
          " microseconds \n");
    }
    assert !results.next();

    out.write(resultIterationLine.toString());
  }

  private void getCompressedDistDetails(String opDetails, long[] output)
      throws SQLException {
    final String messagingTime = "SER_DESER_TIME + PROCESS_TIME + THROTTLE_TIME";

    PreparedStatement ps;
    ResultSet results;

    // final String messagingQuery = " || 'MIN/MAX/AVG ' || TRIM(CHAR(MIN("
    // + messagingTime + ")/1000)) || '/' || TRIM(CHAR(MAX(" + messagingTime
    // + ")/1000)) || '/' || TRIM(CHAR(AVG(" + messagingTime
    // + ")/1000)) from " + GfxdConstants.PLAN_SCHEMA
    // + ".SYSXPLAIN_DIST_PROPS "
    // + " where STMT_RS_ID = ? and DIST_OBJECT_TYPE = '";
    // final String sendQuery = "SELECT ' SEND TIME '" + messagingQuery
    // + XPLAINUtil.OP_QUERY_SEND + "' ";

    // SEND TIME MIN/MAX/AVG 9999/9999/9999
    final String messagingQuery = " MIN(" + messagingTime + "), MAX("
        + messagingTime + "), AVG(" + messagingTime + "), SUM ( "
        + messagingTime + ") from " + GfxdConstants.PLAN_SCHEMA
        + ".SYSXPLAIN_DIST_PROPS "
        + " where STMT_RS_ID = ? and DIST_OBJECT_TYPE = '";

    final String sendQuery = "SELECT " + messagingQuery + opDetails + "' ";

    ps = ds.conn.prepareStatementByPassQueryInfo(-1, sendQuery, false, false,
        false, null, 0, 0);
    ps.setString(1, ds.getQueryID());
    results = ps.executeQuery();
    if (results.next()) {
      output[0] = results.getLong(1) / 1000;
      output[1] = results.getLong(2) / 1000;
      output[2] = results.getLong(3) / 1000;
      output[3] = results.getLong(4) / 1000;
    }
    assert !results.next();
  }
}
