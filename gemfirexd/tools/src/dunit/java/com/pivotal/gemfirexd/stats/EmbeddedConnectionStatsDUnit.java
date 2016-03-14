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
package com.pivotal.gemfirexd.stats;

import java.sql.Connection;
import java.util.Properties;

import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.stats.ConnectionStats;
import io.snappydata.test.util.TestException;

/**
 * Test class for connection stats.
 * 
 * @author rdubey
 * 
 */
@SuppressWarnings("serial")
public class EmbeddedConnectionStatsDUnit extends DistributedSQLTestBase {

  public EmbeddedConnectionStatsDUnit(String name) {
    super(name);
  }

  /**
   * Test the three basic embedded connection stats. The test create
   * 100 connections and verifies that the stats are maintained properly.
   * 
   * @see com.pivotal.gemfirexd.internal.engine.stats.ConnectionStats
   * @throws Exception on failure.
   */
  public void testConnectionStats() throws Exception {

    Properties info = new Properties();
    info.setProperty("gemfire.enable-time-statistics", "true");

    startClientVMs(1, 0, null, info);
    startServerVMs(2, 0, null);

    DistributedSystem sys = Misc.getGemFireCache().getDistributedSystem();
    StatisticsType st = sys.findType(ConnectionStats.name);
    Statistics[] stats = sys.findStatisticsByType(st);

    int totalConnections = 100;
    // Create 100 connections
    
    for (int i = 1 ; i <= totalConnections; i++) {
      Connection conn = TestUtil.getConnection(info);
      for (Statistics s : stats ) {
        long numPeerConnections = s.getLong("peerConnectionsOpened");
        long peerConnectionsAttempted = s.getLong("peerConnectionsAttempted");
        long numNestedConnections = s.getLong("nestedConnectionsOpened");
        long numInternalConnections = s.getLong("internalConnectionsOpened");
        
        
        getLogWriter().info("numPeerConnections=" + numPeerConnections);
        getLogWriter().info("peerConnectionsAttempted=" + peerConnectionsAttempted);
        getLogWriter().info("numNestedConnections=" + numNestedConnections);
        getLogWriter().info("numInternalConnections=" + numInternalConnections);
        
        long connectionTime = s.getLong("peerConnectionsOpenTime");
        // sjigyasu: startServerVMs creates one extra connection, so consider one more.
        assertEquals("Number of connections doesnt match ",(i + 1), numPeerConnections + numNestedConnections);
        //after the connection is done connections in progress should
        // be equal to connections done.
        assertEquals("Connections in progress should equal connections " +
        		"done",numPeerConnections, peerConnectionsAttempted);
        if( connectionTime <=0) {
          throw new TestException("Time taken to connect should never be " +
          		"less than or equal to zero");
        }
        if( connectionTime > ( 5 * (1000 * 1000 * 1000)) ) {
          throw new TestException("Time taken to connect is too long, beyond 5 seconds... " + connectionTime + " nanos");
        }
      }
      conn.close();
    }
    
   for (Statistics s : stats ) {
      long numConnections = s.getLong("peerConnectionsOpened");
      long connectionsInProgress = s.getLong("peerConnectionsAttempted");
      
      getLogWriter().info("Number of connections : " + numConnections);
      getLogWriter().info("Number of connections in progress "+connectionsInProgress);
      assertEquals("Number of connections doesnt match ",totalConnections+1, numConnections);
      assertEquals("Connections in progress and connections done " +
      		"should be equal", 0, (connectionsInProgress - numConnections));
    }
   stopVMNums(1, -2);
  }
}
