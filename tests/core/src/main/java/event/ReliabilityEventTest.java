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
package event;
                                                                                   
import roles.RolesBB;
import java.util.*;
import util.*;
import hydra.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.*;
                                                                                   
/**
 * A version of the <code>event.EventTest</code> that performs operations
 * and handles the RegionRoleExceptions thrown when reliability is lost.
 *
 * @see com.gemstone.gemfire.cache.RegionRoleException
 * @see com.gemstone.gemfire.cache.RegionDistributionException
 * @see com.gemstone.gemfire.cache.RegionAccessException
 *
 * @author lhughes
 * @since 5.0
 */
public class ReliabilityEventTest extends event.EventTest { 

//===================================================================================
// override methods
// Equivalent to event.EventTest methods, but handles RegionAccessExceptions
//===================================================================================

public synchronized static void HydraTask_initialize() {
   if (eventTest == null) {
      eventTest = new ReliabilityEventTest();
      eventTest.initialize();
   }
}

public static void HydraTask_doEntryOperations() {
   // ensure that we get a handle on a region (that we don't attempt during roleLoss
   Region rootRegion = null;
   do {
      rootRegion = CacheUtil.getCache().getRegion(eventTest.regionName);
      MasterController.sleepForMs(3000);
   } while (rootRegion == null);
   eventTest.doEntryOperations(rootRegion);
}

protected void addObject(Region aRegion, boolean aBoolean) {
   try {
     super.addObject(aRegion, aBoolean);
   } catch (RegionRoleException e) {
     handleRegionRoleException(e);
   }
}

protected void invalidateObject(Region aRegion, boolean isLocalInvalidate) {
   try {
     super.invalidateObject(aRegion, isLocalInvalidate);
   } catch (RegionRoleException e) {
     handleRegionRoleException(e);
   }
}
                                                                                   
protected void destroyObject(Region aRegion, boolean isLocalDestroy) {
   try {
     super.destroyObject(aRegion, isLocalDestroy);
   } catch (RegionRoleException e) {
     handleRegionRoleException(e);
   }
}
                                                                                   
protected void updateObject(Region aRegion) {
   try {
     super.updateObject(aRegion);
   } catch (RegionRoleException e) {
     handleRegionRoleException(e);
   }
}

protected void readObject(Region aRegion) {
   try {
     super.readObject(aRegion);
   } catch (RegionRoleException e) {
     handleRegionRoleException(e);
   }
}

protected void handleRegionRoleException(RegionRoleException e) {
  String eName = null;
  Set missingRoles = null;
  long count;
  if (e instanceof RegionDistributionException) {
    eName = "RegionDistributionException";
    missingRoles = ((RegionDistributionException)e).getFailedRoles();
    count = RolesBB.getBB().getSharedCounters().incrementAndRead(RolesBB.regionDistributionExceptions);
    Log.getLogWriter().info("After incrementing counter, regionDistributionExceptions = " + count);
  } else if (e instanceof RegionAccessException) {
    eName = "RegionAccessException";
    missingRoles = ((RegionAccessException)e).getMissingRoles();
    count = RolesBB.getBB().getSharedCounters().incrementAndRead(RolesBB.regionAccessExceptions);
    Log.getLogWriter().info("After incrementing counter, regionAccessExceptions = " + count);
  } else {
    throw new TestException("RegionRoleException: " + e + " not supported by ReliabilityEventTest");
  }

  // This test only expects the server to be lost/gained
  for (Iterator it = missingRoles.iterator(); it.hasNext(); ) {
    Role aRole = (Role)it.next();
    if (!aRole.getName().equals("server")) {
      throw new TestException(eName + " reports role " + aRole.getName() + " missing, but we only expect the role of server to be lost/gained");
    }
  }

  Boolean regionAvailable = (Boolean)RolesBB.getBB().getSharedMap().get(RolesBB.RegionAvailablePrefix + System.getProperty( "clientName" ));
  if (regionAvailable == null || regionAvailable.equals(Boolean.FALSE)) {
  Log.getLogWriter().fine("caught "+ eName + e + " expected with current role loss; continuing test");
  } else {
  // todo@lhughes - uncomment once reinitialize lockup at startup fixed
  //throw new TestException("caught " + eName + ", but no loss of roles detected/reported", e);
}
}

}
