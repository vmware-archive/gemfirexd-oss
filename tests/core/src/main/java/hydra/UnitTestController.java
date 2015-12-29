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

package hydra;

//import com.gemstone.gemfire.LogWriter;

//import java.io.*;
import java.rmi.RemoteException;
import java.util.Iterator;
import java.util.Vector;

import swarm.Swarm;
import swarm.TestType;
import swarm.UnitTestObserver;
import dunit.impl.DUnitBB;

/** 
*  This class acts as the controller for unit tests.  It executes one unit
*  test at a time in the {@link #NAME} client vm.  Each unit test can
*  use the dunit test framework to invoke methods in other client vms.
*/

public class UnitTestController {

  public static final String NAME = "controller";

  private static boolean hasFailureOccured = false;
  private static final String GlobalUnitTestResult = "GlobalUnitTestResult";
  private static final UnitTestObserver observer = Swarm.getUnitTestObserver();

  /**
   *  INITTASK executed in the unit test locator JVMs.
   */
  public static void createAndStartLocator() {
    DistributedSystemHelper.createLocator();
    DistributedSystemHelper.startLocatorAndAdminDS();
  }

  /**
   *  CLOSETASK executed in the unit test locator JVMs.
   */
  public static void stopLocator() {
    DistributedSystemHelper.stopLocator();
  }

  /**
   *  INITTASK executed in the unit test controller VM.
   */
  public static void scheduleUnitTests() {

    // Initialize information about the hosts and all
    try {
      dunit.impl.HostImpl.initialize();
    } catch( RemoteException e ) {
      throw new HydraRuntimeException( "While initializing HostImpl...", e );
    }
    setUnitTestResult(Boolean.TRUE, false);
    // Run the unit tests
    boolean UnitTestResult = true;
    final Vector unitTests = TestConfig.getInstance().getUnitTests();
    observer.testTypeDetected(TestType.DUNIT);
    observer.totalTestCountDetected(unitTests.size());
    Log.getLogWriter().info("sssControllerUnitTestsCount:"+unitTests.size());
    for ( Iterator i = unitTests.iterator(); i.hasNext(); ) {
      TestTask unitTest = (TestTask) i.next();
      if (isTestAvailable(unitTest)) {
        UnitTestResult = scheduleUnitTest( unitTest ) && UnitTestResult;
        observer.incCurrentTestCount();
        Log.getLogWriter().info("sssCURRENT TEST COUNT:"+Swarm.getCurrentUnitTestCount()+"/"+Swarm.getTotalUnitTestCount());
      }
    }
    if ( ! UnitTestResult ) {
      if ( ! hasFailureOccured ) {
        hasFailureOccured = true;
        setUnitTestResult(Boolean.FALSE, true);
      }
    }
  }

  /**
   * Returns true if this VM has acquired the right to run this TestTask
   */
  private static boolean isTestAvailable(TestTask unitTest) {
    try {
      DUnitBB.getBB().getSharedLock().lock();
      String unitTestAsString = unitTest.toString();
      if (DUnitBB.getBB().getSharedMap().containsKey(unitTestAsString)) {
        return false;
      } else {
        DUnitBB.getBB().getSharedMap().put(unitTestAsString, Boolean.TRUE);
      }
    }finally {
      DUnitBB.getBB().getSharedLock().unlock();
    }
    return true;
  }

  /**
   * This method provides a mechanism to track individual UNITTASK failures 
   * without halting the entire hydra run prematurely.
   * @param status the Boolean value to set the UnitTestResult to 
   * @param overwrite allow or disallow overwriting of a value, prevents a race
   */
  static void setUnitTestResult(Boolean status, boolean overwrite) {
    try {
      DUnitBB.getBB().getSharedLock().lock();
      if ( ! overwrite ) {
        Boolean previous = (Boolean) DUnitBB.getBB().getSharedMap().get(GlobalUnitTestResult);
        if ( previous != null ) {
          return;
        }
      }
      
      DUnitBB.getBB().getSharedMap().put(GlobalUnitTestResult, status);
    }finally {
      DUnitBB.getBB().getSharedLock().unlock();
    }
  }

  static Boolean getUnitTestResult() {
    Boolean result = Boolean.FALSE;
    try {
      DUnitBB.getBB().getSharedLock().lock();
      result = (Boolean) DUnitBB.getBB().getSharedMap().get(GlobalUnitTestResult);
    }finally {
      DUnitBB.getBB().getSharedLock().unlock();
    }
    return result;
  }

  /**
   * Returns <code>true</code> if <code>task</code> passed.
   */
  private static boolean scheduleUnitTest( TestTask task ) {
    String hostname = RemoteTestModule.getMyHost(); 
    String lhostname = RemoteTestModule.getMyLogicalHost(); 
    Log.getLogWriter().info( hostname + " aka " + lhostname 
                             + ": Running unit test: " + task.toShortString());
    TestTaskResult result = task.execute();

    // print result
    task.addElapsedTime(result.getElapsedTime());
    task.logUnitTestResult( result );

    return !result.getErrorStatus();
  }

  public static void checkUnitTestResults() {
    boolean passed = getUnitTestResult().booleanValue();
    if (! passed) {
      throw new UnitTestFailure();
    }
  }

  public static class UnitTestFailure extends RuntimeException {

    public UnitTestFailure() {
      super("One or more DUnit tests failed.  Please see " +
            "the \"failures\" directory for further details.");
    }
  }
}
