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

import java.util.List;

import com.pivotal.gemfirexd.procedure.IncomingResultSet;
import com.pivotal.gemfirexd.procedure.ProcedureProcessorContext;
import com.pivotal.gemfirexd.procedure.ProcedureResultProcessor;

public class DefaultProcedureResultProcessor implements
    ProcedureResultProcessor {
  private ProcedureProcessorContext context;

  public DefaultProcedureResultProcessor() {
    
  }
  public void close() {
    

  }

  public boolean getMoreResults(int nextResultSetNumber) {
  
    
    return false;
  }

  public List<Object> getNextResultRow(int resultSetNumber)
      throws InterruptedException {
    IncomingResultSet[] resultSets=this.context.getIncomingResultSets(resultSetNumber);
    for(int i=0; i<resultSets.length; i++) {
       IncomingResultSetImpl incomingRS=(IncomingResultSetImpl)resultSets[i];
       if(incomingRS.isFinished()) {
         continue;
       }
       List<Object> row=resultSets[i].takeRow();
       if(row!=IncomingResultSet.END_OF_RESULTS) {
         return row;
       }
       incomingRS.finish();
    }
    return null;
  }
/*****
 * get a group of the out parameters from  a node which has sent back them.   
 */  
  public Object[] getOutParameters() throws InterruptedException {
    
    IncomingResultSet[]  resultSets=this.context.getIncomingOutParameters();
    if(resultSets==null) {
       return null;
    }
    
    for(int i=0; i<resultSets.length; ++i) {
      List<Object> values=resultSets[i].peekRow();
      if(values==null || values==IncomingResultSet.END_OF_RESULTS){
        continue;
      }
      return values.toArray();     
    }
    return resultSets[0].takeRow().toArray();
    
  }

  public void init(ProcedureProcessorContext context) {
       this.context=context;

  }

}
