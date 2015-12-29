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
package admin;

import hydra.*;
import hydra.blackboard.*;
import event.*;
import java.util.*;
import util.*;
import java.io.File;
import java.util.Properties;

//import com.gemstone.gemfire.admin.*;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.Locator;
//import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
//import com.gemstone.gemfire.internal.Assert;


/**
 * start and stop distribution locators in the current VM.
 *
 * @author Jean Farris
 * @since 4.0
 */ 
public class LocatorTest {

// TBD startLocator with bind address

  /* The singleton instance of LocatorTest in this VM */  
  static protected LocatorTest locatorTest;

 /**
   * Start a Locator in the current VM.
   * Assumes that locator agent has not been started by Hydra. 
   * (set hydra.Prms-startLocatorAgentsBeforeTest to false in conf file)
   * 
   */
  
  public static void HydraTask_startLocator() {

      if (locatorTest == null) {
	  locatorTest = new LocatorTest();
	  locatorTest.startLocator();
      }
  }

  protected void startLocator() {

    int numLocatorsRunningInVM = Locator.getLocators().size();

    int port = PortHelper.getRandomPort();

    // get system properties to use when starting Locator (includes mcast settings)
    String gemfireName = System.getProperty( GemFirePrms.GEMFIRE_NAME_PROPERTY );
    GemFireDescription gfd = TestConfig.getInstance().getGemFireDescription( gemfireName );
    Properties props = gfd.getDistributedSystemProperties();

    File logFile = new File("locator-" + port + ".log");

    // start the locator
    Locator locator;
    try {
      locator = Locator.startLocator(port, logFile);
    } catch (Exception e) {
      String s = "Problem starting nonHydra Locator in application VM";
      throw new AdminTestException (s, e);
    }
    int expectedNumLocators = numLocatorsRunningInVM + 1;
    if (Locator.getLocators().size() != expectedNumLocators) {
	throw new AdminTestException ("Expected " + expectedNumLocators +
           " locators but found " + Locator.getLocators().size());
    }  
    // required format for "locators" property
    String host = HostHelper.getLocalHost();
    String locatorStr = host + "[" + port + "]";

    // save until ready to connect, 
    // map will contain locators strings for locators started in all VMs
    Blackboard bb = AdminBB.getInstance();
    Integer pid = new Integer(ProcessMgr.getProcessId());
    bb.getSharedMap().put(locatorStr, pid);
    Log.getLogWriter().info("Locator started: " + locatorStr);
  }


 /**
   * set the connection managed by hydra so the VM will be connected
   * to the system configured with the locators started by this test.  Otherwise
   * the subsequent test tasks will connect to the default system defined by
   * hydra which has just one locator.
   */
    public static void HydraTask_setConnection () {

      if (locatorTest == null) {
	  locatorTest = new LocatorTest();
      }
      locatorTest.setConnection();
    }

    protected void setConnection() {

      // Get the individual locator strings "<host>[<port>]" from the shared map and 
      // combine into one comma delimited string as required for Gemfire locators
      // property.	
	String currLocators = "";
	Blackboard bb = AdminBB.getInstance();
	Map locatorsMap = bb.getSharedMap().getMap();
	Set locators = locatorsMap.keySet();
	if (locators.size() == 0) {
	    String s = "found no locators in shared map";
	    throw new AdminTestException (s);
	}
	else {
	    Iterator it = locators.iterator();
	    currLocators = (String)it.next();
	    while (it.hasNext()) {
		currLocators += ("," + (String)it.next());
	    }
	}
	// Get the system properties as defined by hydra, set the "locators" and
        // then connect using these properties.  Subsequent test tasks
        // (which connect through DistributedConnectionMgr) will use this same connection.  
	// Note the Hydra system configuration has not been changed, i.e. Hydra does not 
        // know about the newly started locators.
	String gemfireName = System.getProperty( GemFirePrms.GEMFIRE_NAME_PROPERTY );
	if ( gemfireName == null ) {
	    throw new AdminTestException ( "Could not get GEMFIRE_NAME_PROPERTY" );
	}
	GemFireDescription gfd = TestConfig.getInstance().getGemFireDescription( gemfireName );
        Properties gfdProps = gfd.getDistributedSystemProperties();

	gfdProps.setProperty("locators", currLocators);
	Log.getLogWriter().info("Added locator, locators: " + currLocators);

        DistributedConnectionMgr.disconnect();
        DistributedSystem system = DistributedConnectionMgr.connect(gemfireName, gfdProps);
    }
	



  /** STOP
   * Tests that we can get the locator running in this VM and
   * then stop it.
   */

  public static void HydraTask_stopLocator() {


      if (locatorTest == null) {
	  locatorTest = new LocatorTest();
      }      
      locatorTest.stopLocator();

  }

  protected void stopLocator() {
    
    Locator locator = null;
    // get the Locators running in this VM
    Collection locators = Locator.getLocators();

    // cannot iterate over collection to close locators
    // because that would modify the collection 
    Object locatorArray[] = locators.toArray();
    for (int i = 0; i < locators.size(); i++){
	locator = (Locator)locatorArray[i];
	locator.stop();
	Log.getLogWriter().info("Stopped locator: " + locator.toString());
    }

    // verify locators stopped
    locators = Locator.getLocators();
    if (locators.size() !=0) {
      String s = "Expected 0 locators in VM but found: " + locators.size();
      throw new AdminTestException (s);
    }
  }

  public static void HydraTask_clearSharedMap() {

    Blackboard bb = AdminBB.getInstance();
    bb.getSharedMap().clear();
  }




  /** VALIDATE
   * Verify locator running in this VM
   */

  public static void HydraTask_validateLocator() {

      locatorTest.validateLocator();

  }
  protected void validateLocator() {
    
    // Verify number of locators started for this VM
    // matches the number running in the VM.
    Blackboard bb = AdminBB.getInstance();
    SharedMap map = bb.getSharedMap();

    Collection locators = Locator.getLocators();
    int numLocatorsRunningInVm = locators.size();
    int numLocatorsStartedInVm = 0;
    int currPid = ProcessMgr.getProcessId();

    Collection locatorVmPids = map.getMap().values();
   
    Iterator it = locatorVmPids.iterator();
    while (it.hasNext()){
	Integer pid = (Integer)it.next();
	if (currPid == pid.intValue()){
	  numLocatorsStartedInVm++;
	}
    }
    if (numLocatorsStartedInVm != numLocatorsRunningInVm) {
	  String s = "Shared Map of locators doesn't match VM locators for PID: " + currPid;
	  throw new AdminTestException(s);
	}
  }
 	

  /**
   * ENDTASK to verify region data after test has completed.
   * The locator running when the data was created has been shut
   * down (it was in the VM used for INITTASKS and TASKS), so
   * we need to start a locator in the ENDTASK VM and then 
   * validate the region data.
   */
  public static void eventTest_validateRegion() {
    if (locatorTest == null) {
	  locatorTest = new LocatorTest();
	  locatorTest.validateRegion();
      }
  }

    /* If no locator is running (in this VM) start one.
     * Starting a locator for each VM, means when one VM
     * shutsdown (at completion of end task) the other VM
     * should be able to complete the end task.
     */

  protected void validateRegion() {

   try {
	startLocator();
	setConnection();
    } catch (Exception e) {
	throw new TestException ("Unable to start locator", e);
    }

    // validate region data created by EventTest  
    try {
      EventTest.HydraTask_iterate();
    }
    catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    }
    catch (Throwable e) {
      String s = "validate_region failed to perform EventTest.HydraTask_iterate()";
      throw new HydraRuntimeException (s, e);
    }

  }


}


