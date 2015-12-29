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

package com.pivotal.gemfirexd.internal.engine.procedure.coordinate;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.gemstone.gemfire.GemFireException;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.execute.InternalResultCollector;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.procedure.ProcedureChunkMessage;
import com.pivotal.gemfirexd.internal.engine.procedure.cohort.OutgoingResultSetImpl;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultDescription;
import com.pivotal.gemfirexd.procedure.IncomingResultSet;

public final class ProcedureResultCollector implements
    InternalResultCollector<Object, Object> {

  //Integer ResultSet number, String member id.
  private ConcurrentHashMap<Integer, ConcurrentHashMap<String,
      IncomingResultSetImpl>> incomingResultSets;

  private ProxyResultDescription[] proxyResultDescriptions;

  private ConcurrentHashMap<String, IncomingResultSetImpl> outParameters;

  //private ScrollInsensitiveResultSet[] scrollResultSets;

  // this variable is used to set those empty result sets, which no data nodes
  // send back results for them.
  private final AtomicInteger finishedNodeCount = new AtomicInteger(0);

  // String is a node id and Integer is the preSeqNumber of the
  // procedureChunkMessage.
  private ConcurrentHashMap<String, ConcurrentHashMap<Integer,
      ProcedureChunkMessage>> disorderMessages;

  // This variable records the seq number of the current processed chunk
  // message. At the same time, it works as a lock to make sure only one thread
  // can process message at one time.
  private ConcurrentHashMap<String, SequenceNumber> seqNumbers;

  // This variable is used to prevent this result collector is not ready but the
  // procedure message is coming.
  private CountDownLatch prepareLatch;

  private ResultCollector<?, ?> rc;
  private ReplyProcessor21 proc;

  private boolean reexecute;

  private final int numResultSets;

  private final String sqlText;

  public void setRC(ResultCollector<?, ?> resCol) {
    this.rc = resCol;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setProcessor(ReplyProcessor21 processor) {
    this.proc = processor;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ReplyProcessor21 getProcessor() {
    return this.proc;
  };

  public void getResultFromTheInternalRCForBlocking() throws StandardException {
    if (this.rc != null) {
      try {
        this.rc.getResult();
      } catch (GemFireException gfeex) {
        throw Misc.processGemFireException(gfeex, gfeex, "execution of "
            + this.sqlText, true);
      }
    }
  }

  ProcedureResultCollector(int numResults, String sqlText) {
    this.numResultSets = numResults;
    this.sqlText = sqlText;
    initMembers(numResultSets);
  }

  private void initMembers(int numResultSets) {
    this.incomingResultSets = new ConcurrentHashMap<Integer,
        ConcurrentHashMap<String, IncomingResultSetImpl>>();
    for (int i = 0; i < numResultSets; ++i) {
      this.incomingResultSets.put(Integer.valueOf(i),
          new ConcurrentHashMap<String, IncomingResultSetImpl>());
    }
    //this.proxyResultDescriptions = null;
    this.outParameters = new ConcurrentHashMap<String, IncomingResultSetImpl>();
    this.prepareLatch = new CountDownLatch(1);
  }

  /***
   * 
   */
  // !!ezoerner: added the sender argument as result of merge, but not yet
  // used in implementation
  // [sumedh] now using the sender argument instead of serializing as part
  // ProcedureChunkMessage
  public void addResult(DistributedMember sender,
      Object resultOfSingleExecution) {
    try {
      this.prepareLatch.await();
    }
    catch (InterruptedException ex) {
      throw new AssertionError(" the thread has been interrupted!");
    }

    assert (resultOfSingleExecution instanceof ProcedureChunkMessage):
      "resultOfSingleExecution is: " + resultOfSingleExecution;
    ProcedureChunkMessage message = (ProcedureChunkMessage)resultOfSingleExecution;

    String member = sender.getId();
    SequenceNumber sn = this.seqNumbers.get(member);
    // The null sn only happens when the received message comes from a node,
    // which is not in the list of obtained members.
    if (sn == null) {
      return;
    }
    synchronized (sn) {
      //disorder message
      if (message.getPrevSeqNumber() != sn.getSeqNo()) {
        ConcurrentHashMap<Integer, ProcedureChunkMessage> memberDisorderMessages
            = this.disorderMessages.get(member);
        if (memberDisorderMessages == null) {
          memberDisorderMessages =
            new ConcurrentHashMap<Integer, ProcedureChunkMessage>();
          this.disorderMessages.put(member, memberDisorderMessages);
        }
        if (GemFireXDUtils.TraceProcedureExecution) {
          SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_PROCEDURE_EXEC,
              "Recieving disordered " + message.toString());
        }
        memberDisorderMessages.put(message.getPrevSeqNumber(), message);
        return;
      }

      //now ordered message
      sn.setSeqNo(message.getSeqNumber());
      processMessage(message, member);

      //process the disordered messages
      ConcurrentHashMap<Integer, ProcedureChunkMessage> memberDisorderMessages
          = this.disorderMessages.get(member);
      if (memberDisorderMessages == null) {
        return;
      }
      while (true) {
        //not null
        ProcedureChunkMessage disorderMessage = memberDisorderMessages
            .remove(Integer.valueOf(sn.seqNo));
        if (disorderMessage == null) {
          break;
        }
        if (GemFireXDUtils.TraceProcedureExecution) {
          SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_PROCEDURE_EXEC,
              "Processing disordered " + disorderMessage.toString());
        }
        sn.setSeqNo(disorderMessage.getSeqNumber());
        processMessage(disorderMessage, member);
      }
    }
  }

  private void processMessage(ProcedureChunkMessage message, String memberId) {

    byte messageType = message.getType();
    if (GemFireXDUtils.TraceProcedureExecution) {
      SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_PROCEDURE_EXEC,
          "Recieving " + message.toString());
    }
    switch (messageType) {
      case ProcedureChunkMessage.RESULTSET:
        processResultSetMessage(message, memberId);
        break;
      case ProcedureChunkMessage.META_DATA:
        processMetaDataMessage(message, memberId);
        break;
      case ProcedureChunkMessage.OUT_PARAMETER:
        processOutParameterMessage(message, memberId);
        break;
      case ProcedureChunkMessage.PROCEDURE_END:
        processProcedureEndMessage(message, memberId);
        break;

    }
  }

  /***
   * 
   * @param message
   */
  private void processResultSetMessage(ProcedureChunkMessage message,
      String memberId) {
    int resultSetNumber = message.getResultSetNumber();
    ConcurrentHashMap<String, IncomingResultSetImpl> resultSetGroup =
      this.incomingResultSets.get(Integer.valueOf(resultSetNumber));
    IncomingResultSetImpl irs = resultSetGroup.get(memberId);
    assert irs != null: "the thead conflict exists!";
    //maintain the result set for this specific member and avoid the conflict
    //between this thread and the initialization thread.
    //if(irs==null) {
    //   resultSetGroup.putIfAbsent(memberId, new IncomingResultSetImpl());
    //   irs=resultSetGroup.get(memberId);
    //}    
    ArrayList<List<Object>> rows = message.getChunks();
    int size = rows.size();
    //the chunk is empty
    if (size < 1) {
      return;
    }
    //process the result description
    assert (resultSetNumber > -1 && resultSetNumber < proxyResultDescriptions
        .length): "the result set number is out of bound!";
    ProxyResultDescription proxyDescription =
      this.proxyResultDescriptions[resultSetNumber];
    //trade off between the network cost of transmitting the result description and
    //processing the row chunk messages.
    //Note: @todo if the final client needs the specific JDBC SQL types instead of a general
    //Java object type, it becomes necessary to transmit back the result description information;
    //then, the following code needs to be moved to the function of processing meta data message.
    //But it still needs to consider multi-threads update the same ResultDescription.

    if (!proxyDescription.isSet()) {
      List<Object> firstRow = rows.get(0);
      //this.scrollResultSets[resultSetNumber].setSourceRowWidth(firstRow.size());
      proxyDescription.setResultDescription(OutgoingResultSetImpl
          .generateResultDescriptionOnRow(firstRow));
    }
    for (int rowIndex = 0; rowIndex < size; ++rowIndex) {
      irs.addRow(rows.get(rowIndex));
    }
    if (message.isLast()) {
      irs.addRow(IncomingResultSet.END_OF_RESULTS);
    }
  }

  /***
   * 
   * @param message
   */
  private void processOutParameterMessage(ProcedureChunkMessage message,
      String memberId) {
    IncomingResultSetImpl irs = this.outParameters.get(memberId);
    if (irs == null) {
      this.outParameters.putIfAbsent(memberId, new IncomingResultSetImpl());
      irs = this.outParameters.get(memberId);
    }
    ArrayList<List<Object>> rows = message.getChunks();
    //when the procedure does not contain the out parameters.
    int size = (rows == null ? 0 : rows.size());
    for (int rowIndex = 0; rowIndex < size; ++rowIndex) {
      irs.addRow(rows.get(rowIndex));
    }
    irs.addRow(IncomingResultSet.END_OF_RESULTS);
  }

  /***
   * Assuming each data node only send back one Procedure end message.
   * @param message
   */
  private void processProcedureEndMessage(ProcedureChunkMessage message,
      String memberId) {
    for (Integer key : this.incomingResultSets.keySet()) {
      ConcurrentHashMap<String, IncomingResultSetImpl> groupResultSets =
        this.incomingResultSets.get(key);
      IncomingResultSetImpl resultSet = groupResultSets.get(memberId);
      resultSet.addRow(IncomingResultSet.END_OF_RESULTS);
    }
    if (this.finishedNodeCount.decrementAndGet() > 0) {
      return;
    }
    //set the result description for those not really existed result set.
    //no dynamic result set
    if (this.proxyResultDescriptions == null) {
      return;
    }
    for (ProxyResultDescription prd : this.proxyResultDescriptions) {
      if (!prd.isSet()) {
        ResultDescription rd = OutgoingResultSetImpl
            .generateResultDescriptionOnRow(new ArrayList<Object>());
        prd.setResultDescription(rd);
      }
    }
  }

  /***
   * 
   */
  public void endResults() {

  }
  
  public void clearResults() {
    initMembers(this.numResultSets);
    this.reexecute = true;
  }

  public boolean getIfReExecute() {
    return this.reexecute;
  }
  
  public Object getResult() throws FunctionException {
    return null;
  }

  public Object getResult(long timeout, TimeUnit unit)
      throws FunctionException, InterruptedException {
    return null;
  }

  /***
   * 
   * @param resultSetNumber
   * @return
   */
  public IncomingResultSet[] getIncomingResultSets(int resultSetNumber) {
    IncomingResultSet[] retValue = null;
    Integer key = Integer.valueOf(resultSetNumber);
    if (this.incomingResultSets.containsKey(key)) {
      ConcurrentHashMap<String, IncomingResultSetImpl> sameNumberResultSets =
        this.incomingResultSets.get(key);
      retValue = new IncomingResultSet[sameNumberResultSets.size()];
      retValue = sameNumberResultSets.values().toArray(retValue);
    }
    return retValue;
  }

  /****
   * 
   * @return
   */
  public IncomingResultSet[] getOutParameters() {
    int numNodes = this.outParameters.size();

    if (numNodes == 0) {
      return null;
    }

    IncomingResultSet[] retValue = new IncomingResultSet[numNodes];
    retValue = this.outParameters.values().toArray(retValue);
    return retValue;

  }

  void setProxyResultDescritptions(ProxyResultDescription[] prds) {
    this.proxyResultDescriptions = prds;
  }

  void processMetaDataMessage(ProcedureChunkMessage message, String memberId) {
    // TODO: need processing of meta-data message here
  }

  public void initializeResultSets(
      Collection<InternalDistributedMember> members) {
    if (this.proxyResultDescriptions != null
        && (members == null || members.size() == 0)) {
      for (ProxyResultDescription rsd : this.proxyResultDescriptions) {
        assert rsd != null;
        rsd.setReady();
      }
      return;
    }
    //it is possible that this initialization does not finish but the results
    //and out parameters is coming. To avoid this conflict, the initialization thread
    //and the incoming result set processing thread both maintain the incomingResultSets.
    //The latter first checks if the IncomingResultSet for a specific member exists. If not, 
    //it initializes a new IncomingResultSet and puts it into the incoming result sets using
    //the putIfAbsent().

    //see processResultSetMessage and processOutParameterMessage methods.

    //set incoming result sets for each member and each dynamic result set
    for (Integer key : this.incomingResultSets.keySet()) {
      ConcurrentHashMap<String, IncomingResultSetImpl> resultSets =
        this.incomingResultSets.get(key);
      for (InternalDistributedMember m : members) {
        resultSets.putIfAbsent(m.getId(), new IncomingResultSetImpl());
      }
    }

    this.disorderMessages = new ConcurrentHashMap<String,
        ConcurrentHashMap<Integer, ProcedureChunkMessage>>();
    this.seqNumbers = new ConcurrentHashMap<String, SequenceNumber>();
    //set parameters
    String member;
    for (InternalDistributedMember m : members) {
      member = m.getId();
      this.outParameters.putIfAbsent(member, new IncomingResultSetImpl());
      this.seqNumbers.put(member, new SequenceNumber());
    }
    this.finishedNodeCount.set(members.size());
    this.prepareLatch.countDown();
  }

  /*
  public void setScrollResultSets(ScrollInsensitiveResultSet[] resultSets) {
    this.scrollResultSets = resultSets;
  }
  */

  static class SequenceNumber {
    int seqNo;

    SequenceNumber() {
      this.seqNo = 0;
    }

    public int getSeqNo() {
      return this.seqNo;
    }

    public void setSeqNo(int no) {
      this.seqNo = no;
    }
  }
}
