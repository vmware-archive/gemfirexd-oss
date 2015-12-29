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

import java.sql.SQLWarning;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import com.pivotal.gemfirexd.internal.catalog.UUID;
import com.pivotal.gemfirexd.internal.engine.access.GemFireTransaction;
import com.pivotal.gemfirexd.internal.engine.procedure.ObjectArrayRow;
import com.pivotal.gemfirexd.internal.engine.procedure.ProcedureChunkMessage;
import com.pivotal.gemfirexd.internal.engine.procedure.coordinate.ProxyResultDescription;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultColumnDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultDescription;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultSet;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.NoPutResultSet;
import com.pivotal.gemfirexd.internal.iapi.types.DataTypeDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.TypeId;
import com.pivotal.gemfirexd.internal.impl.sql.GenericColumnDescriptor;
import com.pivotal.gemfirexd.internal.impl.sql.GenericResultDescription;
import com.pivotal.gemfirexd.internal.impl.sql.execute.ResultSetStatisticsVisitor;
import com.pivotal.gemfirexd.procedure.OutgoingResultSet;

 public class OutgoingResultSetImpl implements OutgoingResultSet, ResultSet {
  private ArrayList<List<Object>> rowsList;
  private ProcedureSender sender;
  private final int resultSetNumber;
  private ResultDescription resultDescription;
  //this result description is a proxy to satisfy the requirement of the initialize the EmbedResultSet;
  private final ProxyResultDescription proxyResultDescription;
  static final TypeId javaObjectType;

  private final Activation activation;
  //this variable temporarily stores the ColumnDescriptor because the ResultDescription does
  //not provide a method to add columns one by one.
  Vector<ResultColumnDescriptor> columnDescriptors;

  //the number of columns
  private int numColumn;     

  //these variables are used when the out going result set is generated in a procedure
  //through derby procedure call.
  
  private int position;
  private boolean beforeFirst;
  private boolean afterLast;
  private Object[] rows;  
  private int numRows;
  private int batchSize;

  static {
    try {
      javaObjectType = TypeId.getUserDefinedTypeId("java.lang.Object", true);
    } catch (StandardException se) {
      throw new ExceptionInInitializerError(se);
    }
  }

  public OutgoingResultSetImpl(Activation activation, 
                               int resultSetNumber, 
                               ResultDescription prd) {
    this.activation=activation;
    this.proxyResultDescription=(ProxyResultDescription)prd;
    this.sender = null;
    this.rowsList=new ArrayList<List<Object>>();
    this.resultSetNumber=resultSetNumber;
    this.resultDescription=null;
    this.columnDescriptors=new Vector<ResultColumnDescriptor>();
    this.numColumn=0;
    this.position=-1;    
    this.numRows=0;
    this.beforeFirst=true;
    this.afterLast=false;
    this.batchSize = DEFAULT_BATCH_SIZE;
  }

  public int getResultSetNumber() {
     return this.resultSetNumber;
  }
  public void addColumn(String name) {
    if(this.resultDescription!=null) {
      throw new AssertionError("Meta data cannot be modified any more!");
    }        
    DataTypeDescriptor dtd=new DataTypeDescriptor(javaObjectType, true);
    ResultColumnDescriptor rcd=new GenericColumnDescriptor(name,dtd);
    this.columnDescriptors.add(rcd);
           
  }

  private void prepareResultSetDescription() {
    if (this.resultDescription == null) {
      if (this.columnDescriptors.size() == 0) {
        throw new IllegalStateException("Outgoing result set does not have any columns definition added");
      }
      else {
        int num = this.columnDescriptors.size();
        this.numColumn = num;
        ResultColumnDescriptor[] temp = new ResultColumnDescriptor[this.numColumn];
        temp=this.columnDescriptors.toArray(temp);
        this.resultDescription = new GenericResultDescription(temp,
            "data aware procedure");
      }
      this.proxyResultDescription.setResultDescription(this.resultDescription);
    }
  }
  
  /**
   * add a row by the procedure. At present, we only check the number of columns is the
   * same as the number of the result column descriptors if they  are constructed before.
   * Otherwise, generating the result column descriptors based on the input row length; 
   * each column has a default name CX (X is a column position);
   * 
   * Note: we do not check every column in the input row has the specified type in their
   * column descriptor.
   */

  public void addRow(List<Object> row) {
    int length = row.size();
    if (length < 1) {
      return;
    }

    if (this.resultDescription == null) {
      if (this.columnDescriptors.size() == 0) {
        this.numColumn=length;
        this.resultDescription = generateResultDescriptionOnRow(row);
      }
      else {
        int num = this.columnDescriptors.size();
        // add a row at the first time, check if two are equal
        if (num != length) {
          throw new AssertionError("The number of column is "
              + this.columnDescriptors.size() + " But the input row size is "
              + length);
        }
        this.numColumn = num;
        ResultColumnDescriptor[] temp = new ResultColumnDescriptor[this.numColumn];
        temp=this.columnDescriptors.toArray(temp);
        this.resultDescription = new GenericResultDescription(temp,
            "data aware procedure");
      }
      this.proxyResultDescription.setResultDescription(this.resultDescription);
    }
    else {

      if (this.numColumn != length) {
        throw new AssertionError("The number of column is "
            + this.columnDescriptors.size() + " But the input row size is "
            + length);
      }

    }
    this.rowsList.add(row);
    if (this.rowsList.size() >= this.batchSize) {
      if (this.sender != null) {
        sendOutgoingResultSet();
        this.rowsList.clear();
      }
    }
  }

  private void sendOutgoingResultSet() {
      ProcedureChunkMessage message = new ProcedureChunkMessage(
          ProcedureChunkMessage.RESULTSET, getResultSetNumber(), this.rowsList);
      sender.send(message);
  }
  
  public static ResultDescription generateResultDescriptionOnRow(List<Object> row) {
    
      int length=row.size();
      ResultColumnDescriptor[] columnDescriptors=new ResultColumnDescriptor[length];
      for(int i=1; i<=length; ++i) {
            columnDescriptors[i-1]=new GenericColumnDescriptor("C"+i, 
                         new DataTypeDescriptor(javaObjectType, true));
      } 
      ResultDescription rd=new GenericResultDescription(columnDescriptors, "data aware procedure");
      return rd;      
  }

  public void endResults() {
    prepareResultSetDescription();
    // it is a derby procedure call
    if (this.sender == null) {
      this.rows = this.rowsList.toArray();
      this.rowsList = null;
      if (this.rows == null || this.rows.length == 0) {
        this.numRows = 0;
      }
      else {
        this.numRows = this.rows.length;
      }
    }
    else {
      sendOutgoingResultSet();
    }
  }
  
  public void setResultSetSender(ProcedureSender sender) {
     this.sender=sender;
  }

  public boolean checkRowPosition(int isType) throws StandardException {
 
    throw new UnsupportedOperationException("Not supported yet");
  }

  public void cleanUp(boolean cleanupOnError) throws StandardException {
    throw new UnsupportedOperationException("Not supported yet");
    
  }

  public void clearCurrentRow() {
    throw new UnsupportedOperationException("Not supported yet");
    
  }

  public void close(boolean cleanupOnError) throws StandardException {   
    
  }

  public void finish() throws StandardException {
    throw new UnsupportedOperationException("Not supported yet");
    
  }

  public ExecRow getAbsoluteRow(int row) throws StandardException {
    if(isLegalPosition(row)) {
        return generateExecRow(row);
    }
    return null;
  }

  public Activation getActivation() {
    return this.activation;
  }

  public ResultSet getAutoGeneratedKeysResultset() {
    throw new UnsupportedOperationException("Not supported yet");
  }

  /**
   * @see ResultSet#hasAutoGeneratedKeysResultSet
   */
  @Override
  public boolean hasAutoGeneratedKeysResultSet() {
    throw new UnsupportedOperationException("Not supported yet");
  }

  /**
   * @see ResultSet#flushBatch
   */
  @Override
  public void flushBatch() {
    throw new UnsupportedOperationException("unexpected invocation");
  }
  
  /**
   * @see ResultSet#closeBatch
   */
  @Override
  public void closeBatch() throws StandardException {
    throw new UnsupportedOperationException("unexpected invocation");
  }

  public Timestamp getBeginExecutionTimestamp() {
    throw new UnsupportedOperationException("Not supported yet");
  }

  public String getCursorName() {
    return null;
  }

  public Timestamp getEndExecutionTimestamp() {
    throw new UnsupportedOperationException("Not supported yet");
  }

  public long getExecuteTime() {
    throw new UnsupportedOperationException("Not supported yet");
  }

  public ExecRow getFirstRow() throws StandardException {
    if(this.isEmpty()) {
       return null;
    }
    return generateExecRow(0);
  }

  public ExecRow getLastRow() throws StandardException {
    if(this.isEmpty()) {
      return null;
    }
    return generateExecRow(this.numRows-1);
    
  }

  public ExecRow getNextRow() throws StandardException {
    
    if(this.afterLast) {
       return null;
    }
    
    int nextPosition=this.position+1;
    
    if(isLegalPosition(nextPosition)){
      return generateExecRow(nextPosition);  
    }    
    if(nextPosition>=this.numRows) {
        this.setAfterLastRow();       
    }    
    return null;
    
  }

  public ExecRow getPreviousRow() throws StandardException {
    if(this.beforeFirst) {
      return null;
   }
   
   int nextPosition=this.position-11;
   
   if(isLegalPosition(nextPosition)){
     return generateExecRow(nextPosition);  
   }    
   if(nextPosition<0) {
       this.setBeforeFirstRow();       
   }    
   return null;
   
  }

  public ExecRow getRelativeRow(int row) throws StandardException {
    int nextPosition=this.position+row;
    if(isLegalPosition(nextPosition)) {
       return this.generateExecRow(nextPosition);
    }
    if(nextPosition<0) {
       this.setBeforeFirstRow();
       return null;
    }
    if(nextPosition>=this.numRows) {
      this.setAfterLastRow();
    }
    return null;
  }

  public int getRowNumber() {
    
    return this.numRows;
  }

  public NoPutResultSet[] getSubqueryTrackingArray(int numSubqueries) {
    throw new UnsupportedOperationException("Not supported yet");
  }

  public long getTimeSpent(int type, int timeType) {
    throw new UnsupportedOperationException("Not supported yet");
  }

  public SQLWarning getWarnings() {
    //throw new UnsupportedOperationException("Not supported yet");
    return null;
  }

  public boolean isClosed() {   
    return false;
  }

  public int modifiedRowCount() {
    throw new UnsupportedOperationException("Not supported yet");
  }

  public void open() throws StandardException {    
    
  }

  public boolean returnsRows() {
    
    return true;
  }

  public ExecRow setAfterLastRow() throws StandardException {

    this.position = this.numRows;
    beforeFirst = false;
    afterLast = true;

    return null;
  }

  public ExecRow setBeforeFirstRow() throws StandardException {

    this.position = -1;
    this.beforeFirst = true;
    this.afterLast = false;

    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getBatchSize() {
    return this.batchSize;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setBatchSize(int size) {
    this.batchSize = size;
  }

  private ExecRow generateExecRow(int index) {
    
    List<Object> row=(List<Object>)this.rows[index];
    this.position=index;
    this.afterLast=false;
    this.beforeFirst=false;
    return new ObjectArrayRow(row.toArray(), null);
  }
  
  private boolean isEmpty()   
  {
    if(this.numRows==0) {
      return true;
    } 
    return false;
  }
  
  private boolean isLegalPosition(int index) {
    if(this.numRows==0 || index>=this.numRows || index<0) {
        return false;
    } 
    return true;
  } 
  /***
   * Get the current number of rows in the queue.
   * @return
   */
  public int getCurrentNumRows() {
      return this.rowsList.size();
  }

  @Override
  public void accept(ResultSetStatisticsVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public UUID getExecutionPlanID() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void markLocallyExecuted() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void resetStatistics() {
    // TODO Auto-generated method stub
    
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isDistributedResultSet() {
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean addLockReference(GemFireTransaction tran) {
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean releaseLocks(GemFireTransaction tran) {
    return false;
  }

  @Override
  public void checkCancellationFlag() throws StandardException {
    if (this.activation != null) {
      this.activation.checkCancellationFlag();
    }
  }
}
