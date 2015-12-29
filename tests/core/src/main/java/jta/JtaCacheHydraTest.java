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
package jta;

import java.util.*;

import hydra.*;

import util.*;

import com.gemstone.gemfire.cache.*;

import javax.naming.Context;
import javax.transaction.UserTransaction;
import javax.transaction.RollbackException;

/** Test that concurrently performs cache-related operations (get, put (create),
 *  and update) within a (JNDIContext) transactional context.  
 *  
 *  During initialization, the cache (and a region, root), are created 
 *  using cachejta.xml (which defines the JNDIContext).  Then numRandomRootRegions
 *  are created (createRootRegions) using the RegionPrms values.   If 0,
 *  then a single region, jtaRegion, is defined using the RegionPrms.
 *
 *  numberOfEvents operations are executed within a single UserTransaction and
 *  then committed.  If the commit fails, the transaction is subsequently
 *  rolled back.
 *
 *  There is no validation in the test, however counters are maintained and
 *  displayed as an ENDTASK to ensure that work was done.  Commits are allowed
 *  to fail (logging that an exception occurred + stack trace), but this does
 *  not cause the test to fail.
 * 
 * @author nandk
 */

public class JtaCacheHydraTest {

  /**
   * init Task for Hydra test
   */
  public synchronized static void initTask() {
    Cache cache = CacheHelper.getCache();
    if (cache == null) {
      try {
        cache = CacheHelper.createCacheFromXml(JtaPrms.getCacheXmlFile());
      } catch (Exception e) {
        throw new TestException("Error in Cache initialization in initTask " + TestHelper.getStackTrace(e));
      }
    }
    JtaCacheTestUtil.createRootRegions();
  }

  /**
   * Test Task for Random Region Hydra test
   */
  public static void testTaskRandomRegion() {
    UserTransaction utx = null;
    try {
      Cache cache = CacheHelper.getCache();
      Context ctx = cache.getJNDIContext();
      utx = (UserTransaction) ctx.lookup("java:/UserTransaction");
      Log.getLogWriter().info("Beginning Transaction ");
      String regionName = RegionHelper.getRegionDescription(ConfigPrms.getRegionConfig()).getRegionName();
      Region region = RegionHelper.getRegion(regionName);

      utx.begin();
      (new JtaCacheHydraTest()).testTask(region, TestConfig.tab().intAt(JtaPrms.numberOfEvents));
      utx.commit();
      Log.getLogWriter().info("Committed Transaction ");
    } catch (RollbackException r) {
      Log.getLogWriter().info("Transaction did not commit successfully, rolled back ", r);
    } catch (Exception e) {
      Log.getLogWriter().error("Exception caught in testTaskRandomRegion", e);
      if (utx != null) {
        try {
          utx.rollback();
          Log.getLogWriter().info("Transaction explicitly RolledBack");
        } catch (Exception ex) {
          throw new TestException("Error in Transaction on rollback in testTask " + TestHelper.getStackTrace(ex));
        }
      }
      throw new TestException("Error in testTask " + TestHelper.getStackTrace(e));
    }
  }

  /**
   * Test Task for Replicate (mirrored) Region Hydra test
   */
  public static void testTaskReplicateRegion() {
    UserTransaction utx = null;
    try {
      Cache cache = CacheHelper.getCache();
      Context ctx = cache.getJNDIContext();
      utx = (UserTransaction) ctx.lookup("java:/UserTransaction");
      Log.getLogWriter().info("Beginning Transaction ");

      String regionName = RegionHelper.getRegionDescription(ConfigPrms.getRegionConfig()).getRegionName();
      Region region = RegionHelper.getRegion(regionName);

      utx.begin();
      (new JtaCacheHydraTest()).testTask(region, TestConfig.tab().intAt(JtaPrms.numberOfEvents));
      utx.commit();
      Log.getLogWriter().info("Committed Transaction ");
    } catch (RollbackException r) {
      Log.getLogWriter().info("Transaction did not commit successfully, rolled back ",  r);
    } catch (Exception e) {
      Log.getLogWriter().error("Exception caught in testTaskReplicateRegion", e);
      if (utx != null) {
        try {
          utx.rollback();
          Log.getLogWriter().info("Transaction explicitly RolledBack");
        } catch (Exception ex) {
          throw new TestException( "Error in Transaction on rollback in testTask " + TestHelper.getStackTrace(ex));
        }
      }
      throw new TestException("Error in testTask " + TestHelper.getStackTrace(e));
    }
  }

  /**
   * Test Task for the Multiple Random Region Hydra Test.
   *  
   */
  public static void testTaskMultipleRandomRegion() {
    UserTransaction utx = null;
    try {
      Cache cache = CacheHelper.getCache();
      Context ctx = cache.getJNDIContext();
      utx = (UserTransaction) ctx.lookup("java:/UserTransaction");
      Iterator itr =cache.rootRegions().iterator();
      for (int i = 0; i < TestConfig.tab().intAt(JtaPrms.numberOfRandomRegions); i++) {
        Region region = (Region)itr.next();
        Log.getLogWriter().info("Beginning Transaction ");
        utx.begin();
        (new JtaCacheHydraTest()).testTask(region, TestConfig.tab().intAt(JtaPrms.numberOfEvents));
        utx.commit();
        Log.getLogWriter().info("Committed Transaction ");
      }
    } catch (RollbackException r) {
      Log.getLogWriter().info("Transaction did not commit successfully, rolled back ", r);
    } catch (Exception e) {
      Log.getLogWriter().error("Exception caught in testTaskMultipleRandomRegion", e);
      if (utx != null) {
        try {
          utx.rollback();
          Log.getLogWriter().info("Transaction explicitly RolledBack");
        }
        catch (Exception ex) {
          throw new TestException("Error in Transaction on rollback in testTask " + TestHelper.getStackTrace(ex));
        }
      }
      throw new TestException("Error in testTask " + TestHelper.getStackTrace(e));
    }
  }

  /**
   * Test Task for the Multiple Replicate (mirrored) Region Hydra Test.
   *  
   */
  public static void testTaskMultipleReplicateRegion() {
    UserTransaction utx = null;
    try {
      Cache cache = CacheHelper.getCache();
      Context ctx = cache.getJNDIContext();
      utx = (UserTransaction) ctx.lookup("java:/UserTransaction");
      for (Iterator itr =cache.rootRegions().iterator(); itr.hasNext(); ) {
        Region region = (Region)itr.next();
        // skip base region (dataPolicy=normal) from createCacheFromXml
        if (region.getName().equals("root")) continue;

        Log.getLogWriter().info("Beginning Transaction ");
        utx.begin();
        (new JtaCacheHydraTest()).testTask(region, TestConfig.tab().intAt(JtaPrms.numberOfEvents));
        utx.commit();
        Log.getLogWriter().info("Committed Transaction on region " + region.getName());
      }
    } catch (RollbackException r) {
      Log.getLogWriter().info("Transaction did not commit successfully, rolled back ", r);
    } catch (Exception e) {
      Log.getLogWriter().error("Exception caught in testTaskMultipleMirroredRegion", e);
      if (utx != null) {
        try {
          utx.rollback();
          Log.getLogWriter().info("Transaction explicitly RolledBack");
        }
        catch (Exception ex) {
          throw new TestException("Error in Transaction on rollback in testTask " + TestHelper.getStackTrace(ex));
        }
      }
      throw new TestException("Error in testTask " + TestHelper.getStackTrace(e));
    }
  }

  /*
   * testTask is called by the various testTask of Hydra test. @param region
   * @param total
   */
  private void testTask(Region region, int total) {
    (new JtaCacheTestUtil()).doRandomOperation(region, total);
  }

  /**
   * End Task for the Hydra Test
   */
  public static void endTask() {
    JtaBB.getBB().printSharedCounters();
  }
}
