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

package hydratest;

import hydra.*;

/**
 *  Example tasks for testing hydra task batching capabilities.
 */

public class BatchClient {

  public static void tryItOutBatch() { 
    int totalIterations = TestConfig.tab().intAt( BatchPrms.totalIterations );
    int batchSize = TestConfig.tab().intAt( BatchPrms.batchSize );

    BatchClient c = new BatchClient();
    c.tryItOutBatchWork( totalIterations, batchSize );
  }
  public static void tryItOutBatch2() { 
    int totalIterations = TestConfig.tab().intAt( BatchPrms.totalIterations2 );
    int batchSize = TestConfig.tab().intAt( BatchPrms.batchSize2 );

    BatchClient c = new BatchClient();
    c.tryItOutBatchWork( totalIterations, batchSize );
  }
  private void tryItOutBatchWork( int totalIterations, int batchSize ) {
    int count = getCount();
    for ( int i = 0; i < batchSize; i++ ) {
      MasterController.sleepForMs( 1000 );
      ++count;
      if ( count == totalIterations ) {
        Log.getLogWriter().info( "Done: count is " + count );
        throw new StopSchedulingTaskOnClientOrder();
      }
    }
    Log.getLogWriter().info( "Not done: count is " + count );
    setCount( count );
  }
  private int getCount() {
    Integer n = (Integer) localcount.get();
    if ( n == null ) {
      n = new Integer(0);
      localcount.set( n );
    }
    return n.intValue();
  }
  private void setCount( int n ) {
    localcount.set( new Integer( n ) );
  }
  private static HydraThreadLocal localcount = new HydraThreadLocal();
}
