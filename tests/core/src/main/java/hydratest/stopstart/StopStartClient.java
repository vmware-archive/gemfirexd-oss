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

package hydratest.stopstart;

import com.gemstone.gemfire.LogWriter;
import hydra.*;

/**
 *  A client that tests the hydra {@link hydra.ClientVmMgr} API.
 */

public class StopStartClient {

  /**
   *  TASK to test ability of stop methods to detect and prevent deadlock.
   */
  public static void stopDeadlockTask() throws ClientVmNotFoundException {

    try {
      ClientVmInfo info = ClientVmMgr.stop(
        "testing deadlock detection",
         ClientVmMgr.MEAN_EXIT, ClientVmMgr.IMMEDIATE
      );
      throw new HydraInternalException( "Test failed to detect deadlock" );

    } catch( IllegalArgumentException e ) {
      if ( e.getMessage().indexOf( "deadlock" ) != -1 ) { // detected deadlock
        log().info( e.getMessage() );
      } else {
        throw e; // some other problem
      }
    }
    try {
      Integer vmid = new Integer( RemoteTestModule.getMyVmid() );
      ClientVmInfo info = ClientVmMgr.stop(
        "testing deadlock detection", ClientVmMgr.NICE_EXIT,
         ClientVmMgr.NEVER, new ClientVmInfo( vmid, null, null )
      );
      throw new HydraInternalException( "Test failed to detect deadlock" );

    } catch( IllegalArgumentException e ) {
      if ( e.getMessage().indexOf( "deadlock" ) != -1 ) { // detected deadlock
        log().info( e.getMessage() );
      } else {
        throw e; // some other problem
      }
    }
  }

  /**
   *  TASK to test {@link hydra.ClientVmMgr#stop(String)}.
   */
  public static void stopTask() throws ClientVmNotFoundException {

    ClientVmInfo info = ClientVmMgr.stop(
      "testing synchronous mean kill on myself"
    );
    String msg = "Test failed to do synchronous mean kill on me: " + info;
    throw new HydraInternalException( msg );
  }

  /**
   *  TASK to test {@link hydra.ClientVmMgr#stop(String)}.
   */
  public static void stopRegExTask() throws ClientVmNotFoundException {
    String clientName = TestConfig.tab().stringAt(StopStartPrms.clientName);
    ClientVmInfo info = ClientVmMgr.stop(
      "testing synchronous mean kill using client name",
      ClientVmMgr.MEAN_KILL, ClientVmMgr.NEVER,
      new ClientVmInfo(null, clientName, null)
    );
  }

  /**
   *  TASK to test {@link hydra.ClientVmMgr#stopAsync(String)}.
   */
  public static void stopAsyncTask() throws ClientVmNotFoundException {

    ConfigHashtable tab = TestConfig.tab();
    long totalIterations = tab.longAt( StopStartPrms.totalIterations );
    long stopIteration = tab.longAt( StopStartPrms.stopIteration );

    for ( int i = 0; i < totalIterations; i++ ) {
      log().info( "iteration: " + i );
      if ( i == stopIteration ) {
        log().info( "invoking stopAsync" );
        ClientVmInfo info = ClientVmMgr.stopAsync(
          "stop iteration: " + stopIteration + " of " + totalIterations,
           ClientVmMgr.MEAN_KILL, ClientVmMgr.IMMEDIATE
        );
      }
    }
  }

  /**
   *  TASK to test {@link hydra.ClientVmMgr#stop(String,int,int)} with various
   *  stop and start modes.
   */
  public static void stopVariousTask() throws ClientVmNotFoundException {

    ConfigHashtable tab = TestConfig.tab();

    int stopMode =
        ClientVmMgr.toStopMode( tab.stringAt( StopStartPrms.stopMode ) );
    int startMode =
        ClientVmMgr.toStartMode( tab.stringAt( StopStartPrms.startMode ) );

    ClientVmInfo info = ClientVmMgr.stop(
      "testing synchronous stop on myself", stopMode, startMode
    );
    String msg = "Test failed to do synchronous stop on me: " + info;
    throw new HydraInternalException( msg );
  }

  /**
   *  TASK to test {@link hydra.ClientVmMgr#stop(String,int,int)} with various
   *  stop and start modes.
   */
  public static void stopVariousAsyncTask() throws ClientVmNotFoundException {

    ConfigHashtable tab = TestConfig.tab();

    int stopMode =
        ClientVmMgr.toStopMode( tab.stringAt( StopStartPrms.stopMode ) );
    int startMode =
        ClientVmMgr.toStartMode( tab.stringAt( StopStartPrms.startMode ) );

    long totalIterations = tab.longAt( StopStartPrms.totalIterations );
    long stopIteration = tab.longAt( StopStartPrms.stopIteration );

    for ( int i = 0; i < totalIterations; i++ ) {
      log().info( "iteration: " + i );
      if ( i == stopIteration ) {
        log().info( "invoking stopAsync" );
        ClientVmInfo info = ClientVmMgr.stopAsync(
          "stop iteration: " + stopIteration + " of " + totalIterations,
           stopMode, startMode
        );
      }
    }
  }

  /**
   *  TASK to test {@link hydra.ClientVmMgr#start(String)}.
   */
  public static void startTask() throws ClientVmNotFoundException {

    while( true ) {
      try {
        ClientVmInfo info = ClientVmMgr.start(
          "testing synchronous start on any stopped vm"
        );
        log().info( "This should appear after start has completed." );
        break;
      } catch( ClientVmNotFoundException e ) {
        log().info( "No vm is available for start" );
        MasterController.sleepForMs( 250 );
      }
    }
  }

  private static LogWriter log() {
    return Log.getLogWriter();
  }
}
