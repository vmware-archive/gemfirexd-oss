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

package newWan;

import java.util.Set;

import hydra.GsRandom;
import hydra.RemoteTestModule;
import hydra.TestConfig;
import hydra.blackboard.Blackboard;
import util.OperationCountersBB;
import util.TestException;
import util.TestHelper;

import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.internal.cache.wan.parallel.ParallelGatewaySenderImpl;
import com.gemstone.gemfire.internal.cache.wan.serial.SerialGatewaySenderImpl;

import cq.CQUtilBB;

/**
 * Manages the blackboard for tests in this package.
 */

public class WANBlackboard extends Blackboard {

  private static WANBlackboard blackboard;
  public static String LOCATORS_MAP = "locatorMap";
  
  public static int currentEntry_valid; // for the security wan test for the valid credential
  public static int currentEntry_invalid; // for the security wan test for the invalid credential
  
  public static int currentEntry_writer; //writer for security tests
  public static int currentEntry_reader; //reader for security tests

  public static int operation_counter; // operation counter for load based test
  
  public static int NUM_CQ; //number of cq created
  /**
   *  Zero-arg constructor for remote method invocations.
   */
  public WANBlackboard() {
  }
  /**
   *  Creates a WAN blackboard using the specified name and transport type.
   */
  public WANBlackboard( String name, String type ) {
    super( name, type, WANBlackboard.class );
  }
  /**
   *  Creates a WAN blackboard named "WAN" using RMI for transport.
   */
  public static synchronized WANBlackboard getInstance() {
    if (blackboard == null) {
      blackboard = new WANBlackboard("WAN", "RMI");
    }
    return blackboard;
  }
  
  /**
   *  printBlackboard: ENDTASK to print contents of blackboard
   */
  public static void printBlackboard() {
    hydra.Log.getLogWriter().info("Printing WAN Blackboard contents");
    WANBlackboard bb =   getInstance();
    bb.print();
    TestHelper.checkForEventError(bb);
    //check of CQUtilBB for CQ
    TestHelper.checkForEventError(CQUtilBB.getBB());
  }
  
  public static synchronized void throwException(String msg){
    if(getInstance().getSharedMap().get(TestHelper.EVENT_ERROR_KEY) == null){
      getInstance().getSharedMap().put(TestHelper.EVENT_ERROR_KEY, msg + " in " + getMyUniqueName() + " " + TestHelper.getStackTrace());
    }
    
    throw new TestException(msg);
  }
 
  /**
   *  Uses RemoteTestModule information to produce a name to uniquely identify
   *  a client vm (vmid, clientName, host, pid) for the calling thread
   */
  public static String getMyUniqueName() {
    StringBuffer buf = new StringBuffer( 50 );
    buf.append("vm_" ).append(RemoteTestModule.getMyVmid());
    buf.append( "_" ).append(RemoteTestModule.getMyClientName());
    buf.append( "_" ).append(RemoteTestModule.getMyHost());
    buf.append( "_" ).append(RemoteTestModule.getMyPid());
    return buf.toString();
  }
  
  public static synchronized Long increamentAndReadKeyCounter(String regionName, int size){
    WANBlackboard bb = getInstance();
    
    bb.getSharedLock().lock();
    Long keyCounter = (Long)bb.getSharedMap().get(regionName);
    keyCounter = (keyCounter == null) ? new Long(size) : keyCounter + size ;    
    bb.getSharedMap().put(regionName, keyCounter);
    bb.getSharedLock().unlock();
    
    return keyCounter;
  }  
}

