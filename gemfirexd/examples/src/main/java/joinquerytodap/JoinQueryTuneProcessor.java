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
package examples.joinquerytodap;

import java.util.List;
import java.util.logging.Logger;

import com.pivotal.gemfirexd.procedure.IncomingResultSet;
import com.pivotal.gemfirexd.procedure.ProcedureProcessorContext;
import com.pivotal.gemfirexd.procedure.ProcedureResultProcessor;

public class JoinQueryTuneProcessor implements ProcedureResultProcessor {

  private ProcedureProcessorContext context;
  private Logger logger;
  private IncomingResultSet[] resultSets;
  private int currentIndex = 0;
  private int totalResultSets;
  
  public void init(ProcedureProcessorContext context) {
    this.context = context;
    this.logger = Logger.getLogger("com.pivotal.gemfirexd");
  }

  public Object[] getOutParameters() throws InterruptedException {
    logger.info("JoinQueryTuneProcessor::getOutParameters called");
    return null;
  }

  public List<Object> getNextResultRow(int resultSetNumber)
      throws InterruptedException {
    if (this.resultSets == null) {
      this.resultSets = this.context.getIncomingResultSets(resultSetNumber);
      this.totalResultSets = this.resultSets.length;
      logger.info("JoinQueryTuneProcessor::getNextResultRow totalResultSets=" + this.totalResultSets);
    }

    for (;;) {      
      if (this.currentIndex >= this.totalResultSets) {
        logger.info("JoinQueryTuneProcessor::getNextResultRow returing as currentIndex is " + this.currentIndex + " which is >= this.totalResultSets");
        return null;
      }

      IncomingResultSet currResultSet = this.resultSets[currentIndex];
      List<Object> row = currResultSet.takeRow();

      if (row != IncomingResultSet.END_OF_RESULTS) {
        return row;
      }
      else {
        this.currentIndex++;
      }
    } // end of for(;;) 
  }

  public void close() {
    this.context = null;
    this.logger = null;
    this.resultSets = null;
  }
}
