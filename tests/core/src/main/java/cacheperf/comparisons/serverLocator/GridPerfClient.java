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
package cacheperf.comparisons.serverLocator;

import hydra.DistributedSystemHelper;
import hydra.HydraThreadLocal;
import hydra.MasterController;
import hydra.DistributedSystemHelper.Endpoint;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.List;

import cacheperf.CachePerfClient;
import cacheperf.CachePerfPrms;

import com.gemstone.gemfire.cache.client.internal.locator.ClientConnectionRequest;
import com.gemstone.gemfire.distributed.internal.tcpserver.TcpClient;

/**
 * @author dsmith
 *
 */
public class GridPerfClient extends CachePerfClient {
  private static final HydraThreadLocal threadLocalInstance = new HydraThreadLocal();
  private String[] groups;
  private int timeout;
  
  public GridPerfClient() {
    groups = GridPerfPrms.getServerGroups();
    timeout = GridPerfPrms.getRequestTimeout();
  }
  
  public static GridPerfClient getInstance() throws IOException {
    GridPerfClient instance = (GridPerfClient) threadLocalInstance.get();
    if(instance == null) {
      instance = new GridPerfClient();
      threadLocalInstance.set(instance);
    }
    
    instance.initialize(CONNECTS);
    return instance;
  }
  
  /**
   *  TASK make requests to the server locator
   * @throws IOException 
   * @throws ClassNotFoundException 
   */
  public static void requestServerLocationTask() throws IOException, ClassNotFoundException {
    getInstance().requestLocation();
  }
  private int batchCount;
  private int count;
  private int keyCount;
  private int iterationsSinceTxEnd;
  private void requestLocation() throws IOException, ClassNotFoundException {
    List contacts = DistributedSystemHelper.getContacts();
    Endpoint endpoint = (Endpoint) contacts.get(ttgid % contacts.size());
    InetAddress host = InetAddress.getByName(endpoint.getHost());
    int port = endpoint.getPort();
    boolean batchDone = false;
    do {
      int n = 1;
      if ( this.sleepBeforeOp ) {
        MasterController.sleepForMs( CachePerfPrms.getSleepMs() );
        n = CachePerfPrms.getSleepOpCount();
      }
      for ( int j = 0; j < n; j++ ) {
        executeTaskTerminator();   // commits at task termination
        executeWarmupTerminator(); // commits at warmup termination
        requestLocation(host, port, j);
        ++this.batchCount;
        ++this.count;
        ++this.keyCount;
        ++this.iterationsSinceTxEnd;
        batchDone = executeBatchTerminator(); // commits at batch termination
      }
    } while (!batchDone);
  }
  private void requestLocation(InetAddress host, int port, int i) throws IOException, ClassNotFoundException {
    String group  = groups == null ? null : groups[(groups.length  + ttgid) % i];
    long start = this.statistics.startConnect();
    ClientConnectionRequest request = new ClientConnectionRequest(Collections.EMPTY_SET , group);
    TcpClient.requestToServer(host, port, request, timeout);
    this.statistics.endConnect(start, true, this.histogram);
  }  
}
