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
package sql.sqlDAP;

import hydra.Log;

import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;

import sql.SQLTest;
import util.TestException;

import com.pivotal.gemfirexd.procedure.IncomingResultSet;
import com.pivotal.gemfirexd.procedure.ProcedureProcessorContext;
import com.pivotal.gemfirexd.procedure.ProcedureResultProcessor;

public class CustomProcessor implements ProcedureResultProcessor {
  private ProcedureProcessorContext context;
  
  @Override
  public void close() {
    this.context = null;
  }

  @Override
  public List<Object> getNextResultRow(int resultSetNumber)
      throws InterruptedException {
    if (resultSetNumber == 1) return test(1);
    else return test(resultSetNumber);
  }

  @Override
  public Object[] getOutParameters() throws InterruptedException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void init(ProcedureProcessorContext context) {
    this.context = context;
  }
  
  private List<Object> test(int num) throws InterruptedException{
    //Log.getLogWriter().info("in custom processor");
    System.out.println("in custom processor");
    IncomingResultSet[] inSets = context.getIncomingResultSets(num);
    //Log.getLogWriter().info("custom process result");
    if (inSets.length != SQLTest.numOfStores) {
      throw new TestException("does not get results set from all the nodes");
    }
    for (IncomingResultSet inSet : inSets) {
      
      List<Object> nextRow = inSet.waitPeekRow();
      if (nextRow == IncomingResultSet.END_OF_RESULTS) continue;
      
      List<Object> takeRow = inSet.takeRow();
      for (Object o: takeRow) {
        //Log.getLogWriter().info(o + ",");
        System.out.println(o + ",");
      }
      return takeRow;
    }
    return null;
  }

}
