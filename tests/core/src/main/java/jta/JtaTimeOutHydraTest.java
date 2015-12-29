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

import com.gemstone.gemfire.cache.*;

import javax.naming.Context;
import javax.transaction.UserTransaction;
import javax.transaction.RollbackException;

import util.*;

import hydra.*;

/**
 * @author nandk
 *  
 */
public class JtaTimeOutHydraTest {

  /**
   * Init Task 
   */
  public synchronized static void initTask() {
    Cache cache = CacheHelper.getCache();
    if (cache == null) {
      try {
        cache = CacheHelper.createCacheFromXml(JtaPrms.getCacheXmlFile());
      } catch (Exception e) {
        throw new TestException("Error in Cache Initialiaation in initTask " + TestHelper.getStackTrace(e));
      }
    }
  }

  /**
   * Test Task for 
   */
  public static void testTask() {
    boolean exceptionOccurred = false;
    UserTransaction utx = null;
    try {
      Cache cache = CacheHelper.getCache();
      Context ctx = cache.getJNDIContext();
      utx = (UserTransaction) ctx.lookup("java:/UserTransaction");
      Log.getLogWriter().info("Starting Transaction ");
      int txnTimeOut = new Random().nextInt(10)+1;
      int sleepTime = (new Random().nextInt(11)+1) * 1000;
      Log.getLogWriter().info("Txn TimeOut =" + txnTimeOut + " sleepTime " + sleepTime);
      utx.begin();
      utx.setTransactionTimeout(txnTimeOut);
      Thread.sleep(sleepTime);
      try {
        utx.commit();
      } catch (Exception e) {
        if (txnTimeOut > (sleepTime/1000)) {
          Log.getLogWriter().info("Commit threw", e);
          throw new Exception("Commit threw " + e);
        } else {     // tx timed out before commit executed
          exceptionOccurred = true;
        }
      }
      if (txnTimeOut < (sleepTime/1000) && !exceptionOccurred) { 
         throw new Exception("Expected (transaction timeout) Exception did not occur"); 
      }
      Log.getLogWriter().info("Successfully Committed Transaction ");
    } catch (Exception e) {
         Log.getLogWriter().info("Unexpected Exception in testTask", e);
         throw new TestException("Unexpected Exception in testTask " + TestHelper.getStackTrace(e));
    }
  }
}
