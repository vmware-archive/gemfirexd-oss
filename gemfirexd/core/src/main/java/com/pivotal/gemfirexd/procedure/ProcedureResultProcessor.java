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
package com.pivotal.gemfirexd.procedure;

import java.util.List;

public interface ProcedureResultProcessor {
  
  /**
   * Initialize this processor.
   */
  void init(ProcedureProcessorContext context);
  
  
  /**
   * Provide the out parameters for this procedure to the client as an
   * Object[].
   *
   * @throws InterruptedException if interrupted while waiting to receive
   *         data.
   */
  Object[] getOutParameters() throws InterruptedException;
  
  
  /**
   * Provide the next row for result set number resultSetNumber.
   * The processor should do whatever processing is required on the
   * incoming data to provide the next row.
   *
   * Return the next row of the result set specified by resultSetNumber,
   * or null if there are no more rows in this result set.
   *
   * @param resultSetNumber the 1-based result set number for the row being
   *        requested
   * @throws InterruptedException if interrupted while waiting to receive
   *         data.
   *
   *
   * @throws InterruptedException if interrupted while waiting to receive
   *         data.
   */
  List<Object> getNextResultRow(int resultSetNumber)
  throws InterruptedException;
  
  /**
   * Called by GemFireXD when this statement is closed.
   */
  void close();
  
}
