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

package com.pivotal.gemfirexd.internal.engine.procedure.cohort;

import com.gemstone.gemfire.cache.execute.ResultSender;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.cache.execute.InternalResultSender;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.procedure.ProcedureChunkMessage;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.JDBC30Translation;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.ParameterValueSet;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnection;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedResultSet;
import com.pivotal.gemfirexd.procedure.OutgoingResultSet;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/***
 * This class implements the function of transmitting the result sets (or out parameters) 
 * from remote data nodes to the coordinate node
 * The implementation concerns:
 *   1. Multiple or single thread  
 * @author yjing
 *
 */
public final class ProcedureSender {

  protected InternalResultSender sender;

  //The sender for OutgoingResultSets.
  //private OutgoingResultSetSender outgoingResultSetSender;

  //this latch is used to coordinate the procedure running thread and OutgoingResultSet
  //threads.
  //when its value reaches zero, it means that all OutgoingResultSet threads have sent back
  //the rows; then the procedure thread sends back a procedure termination message to 
  // coordinate node.
  // As  a cohort node could only generate a subset of dynamic result sets, the procedure 
  // termination message is used by the coordinate node to terminate those empty result
  // sets.
  //
  //private CountDownLatch latch;

  private final AtomicInteger seqno;

  private ProcedureExecutionContextImpl procedureExecutionContext;

  private final EmbedConnection parentConn;

  /***
   * 
   * @param sender: the underlying result sender of the function service.
   */
     
  public ProcedureSender(ResultSender<?> sender, EmbedConnection conn) {
    this.sender = (InternalResultSender)sender;
    this.sender
        .enableOrderedResultStreaming(GemFireXDUtils.PROCEDURE_ORDER_RESULTS);

    //this.resultSetSenders=new Vector<ResultSetSender>();    
    this.seqno = new AtomicInteger(0);
    this.parentConn = conn;
  }

  public void initialize() {
//    OutgoingResultSetSender outgoingSender=new OutgoingResultSetSender(this);
//    outgoingSender.Initialize();
//    this.outgoingResultSetSender=outgoingSender;
    
  }
  
  /***
   * 
   * @param OutgoingResultSet
   */
  public void addOutgoingResultSet(OutgoingResultSet outgoingResultSet) {
         assert outgoingResultSet!=null : "Expect a non-null dynamicResultSet!";
         OutgoingResultSetImpl rs=(OutgoingResultSetImpl) outgoingResultSet;
         rs.setResultSetSender(this);   
         //this.outgoingResultSetSender.addOutgoingResultSet(rs);            
  }
  
  /****
   * add the member id to the message so that the coordinate node knows which node
   * sends back this message.
   * @param message
   */
  void send(ProcedureChunkMessage message) {
    final int seqNo = this.seqno.getAndIncrement();
    message.setPrevSeqNumber(seqNo);
    message.setSeqNumber(seqNo + 1);
    if (GemFireXDUtils.TraceQuery) {
      SanityManager.DEBUG_PRINT("DataAwareDistribution", "Sending " + message);
    }
    this.sender.sendResult(message);
  }

  void sendLast(ProcedureChunkMessage message) {
    final int seqNo = this.seqno.getAndIncrement();
    message.setPrevSeqNumber(seqNo);
    message.setSeqNumber(seqNo + 1);
    if (GemFireXDUtils.TraceQuery) {
      SanityManager.DEBUG_PRINT("DataAwareDistribution",
          "Sending " + message.toString());
    }
    this.sender.lastResult(message);
  }

  /***
   * 
   */
  public void close() {
    //this.outgoingResultSetSender.close(); 
  }

  /***
   * Send back the out parameters if the procedure does have the out parameters.
   * 
   * @param pvs
   * @throws StandardException
   */
  public void sendOutParameters(ParameterValueSet pvs)
      throws StandardException {
    ArrayList<Object> outParameters = new ArrayList<Object>();
    int numParameters = pvs.getParameterCount();
    for (int i = 1; i <= numParameters; i++) {
      int mode = pvs.getParameterMode(i);
      if (mode == JDBC30Translation.PARAMETER_MODE_IN_OUT
          || mode == JDBC30Translation.PARAMETER_MODE_OUT) {
        DataValueDescriptor dvd = pvs.getParameterForGet(i - 1);
        assert dvd != null: "the dvd should not be null!";
        outParameters.add(dvd.getObject());
      }
    }
    ArrayList<List<Object>> chunks = new ArrayList<List<Object>>();
    if (outParameters.size() > 0) {
      chunks.add(outParameters);
      ProcedureChunkMessage message = new ProcedureChunkMessage(
          ProcedureChunkMessage.OUT_PARAMETER, 0, chunks);
      send(message);
    }
  }

  /****
   * Send back the java.sql.ResultSet, which is generated by executing the SQL
   * statements insider a procedure.
   * 
   * @param rs
   * @throws SQLException
   */
  public void sendResultSet(EmbedResultSet rs) throws SQLException {
    ArrayList<List<Object>> chunk = getRowSet(rs);

    ProcedureChunkMessage message = new ProcedureChunkMessage(
        ProcedureChunkMessage.RESULTSET, rs.getResultsetIndex(), chunk);
    message.setLast();
    send(message);
  }

  /***
   * divide the java.sql.ResultSet into chunks based on the threshold, which
   * specifies how many number of rows in a chunk. Note. current implementation
   * only send all the rows back.
   * 
   * @param rs
   * @return
   * @throws SQLException
   */
  private ArrayList<List<Object>> getRowSet(EmbedResultSet rs)
      throws SQLException {
    int colCount = rs.getMetaData().getColumnCount();
    ArrayList<List<Object>> rows = new ArrayList<List<Object>>();
    while (rs.next()) {
      List<Object> row = new ArrayList<Object>();
      for (int colIndex = 1; colIndex <= colCount; ++colIndex) {
        row.add(colIndex - 1, rs.getObject(colIndex));
      }
      rows.add(row);
    }
    return rows;
  }

  /***
   * this function is called when a procedure call is complete. It sends a procedure end
   * request to each outgoing result sender and wait them finishing their work.
   */
  public void endProcedureCall() {
    if (GemFireXDUtils.TraceQuery) {
      SanityManager.DEBUG_PRINT("DataAwareDistribution", "End procedure call");
    }
//    try {
//      if (this.outgoingResultSetSender != null) {
//        this.outgoingResultSetSender.endProcedureCall();
//      }
//    } catch (InterruptedException e) {
//      // todo how to process this exception?
//      throw new AssertionError("the interrupted exception!");
//    }
    // till now, every outgoing result set has finished their work.
    sendLast(new ProcedureChunkMessage(ProcedureChunkMessage.PROCEDURE_END));
    closeNestedConnections();
  }

  public EmbedConnection setProcedureExecutionContext(ProcedureExecutionContextImpl pecImpl) {
    this.procedureExecutionContext = pecImpl;
    return this.parentConn;
  }
  
  private void closeNestedConnections() {
    try {
      if (this.procedureExecutionContext == null) {
        return;
      }
      ArrayList<Connection> nestedConns = this.procedureExecutionContext
          .getNestedConnectionList();
      if (nestedConns != null) {
        for (Connection conn : nestedConns) {
          conn.close();
        }
      }
    } catch (SQLException e) {
      LogWriterI18n logger = Misc.getI18NLogWriter();
      if (logger.warningEnabled()) {
        logger.warning(LocalizedStrings.DEBUG, e);
      }
    }
  }
}
