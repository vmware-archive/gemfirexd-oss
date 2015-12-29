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
/**
 * 
 */
package pdx.parReg;

import hydra.Log;
import hydra.TestConfig;

import java.util.Properties;

import parReg.ParRegTest;
import pdx.PdxInstanceCreator;
import pdx.PdxTest;
import util.BaseValueHolder;
import util.RandomValues;
import util.TestException;
import util.ValueHolderPrms;

import com.gemstone.gemfire.cache.CacheLoader;
import com.gemstone.gemfire.cache.CacheLoaderException;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.LoaderHelper;

/**
 * @author lynn
 *
 */
public class PdxPRCacheLoader implements CacheLoader, Declarable {

  // for efficiency reasons, save the versioned class loader for reuse on each
  // class loader call; this is documented as the recommended approach
  private static ClassLoader versionedCL = null;

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.CacheCallback#close()
   */
  public void close() {
    Log.getLogWriter().info("In PdxPRCacheLoader, close");
  }

  /* To create an instance of a versioned domain object in a class loader,
   * it must be on the class path. A customer might put the class path on
   * the vm itself. For testing purposes, we don't want it on the vm itself
   * so we can fail if the product deserializes when it shouldn't. For this
   * reason, this class loader sets the thread's class loader to include
   * a versioned class path, then resets it at the end so this thread (a
   * product thread) can continue running without versioned objects in it's
   * class loader. 
   */
  public Object load(LoaderHelper helper) throws CacheLoaderException {
    Log.getLogWriter().info("In " + this.getClass().getName());
    if (TestConfig.tab().getRandGen().nextInt(1, 100) <= 35) {
      return createObjWithClassPath(helper);
    } else {
      Object anObj = PdxInstanceCreator.getVersionedValueHolder(
          TestConfig.tab().stringAt(ValueHolderPrms.objectType), (String)(helper.getKey()), new RandomValues());
      Log.getLogWriter().info("In " + this.getClass().getName() + " for key " + helper.getKey() + ", returning " + anObj);
      if (anObj == null) { // can happen during HA if cache is null when getting PdxInstance
                           // with PdxInstanceCreator
         anObj = createObjWithClassPath(helper); // get attempt to get an object the other way
      } 
      return anObj;
    }
  }

  /** Create an object by installing a versioned classpath
   * 
   * @param helper The LoaderHelper passed to the public load(...) call
   * @return The Object to return from the load(...) call
   * @throws TestException If the classpath cannot be installed correctly
   */
  private Object createObjWithClassPath(LoaderHelper helper)
      throws TestException {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    Log.getLogWriter().info("cl is " + cl + ", versionedCL is " + versionedCL);
    if (versionedCL == null) {
      versionedCL = PdxTest.initClassLoader(); // add versioned objects to class loader
      Log.getLogWriter().info("versiondCL assigned: " + versionedCL);
      if (versionedCL == null) {
        String errStr = "Test problem, did not expect PdxTest.initClassLoader to return null";
        Log.getLogWriter().info(errStr);
        throw new TestException(errStr);
      }
    }
    try {
      Log.getLogWriter().info("Setting contextClassLoader to " + versionedCL);
      Thread.currentThread().setContextClassLoader(versionedCL);
      BaseValueHolder anObj = ParRegTest.testInstance.getValueForKey(helper.getKey());
      Log.getLogWriter().info("In " + this.getClass().getName() + " for key " + helper.getKey() + ", returning " + anObj);
      return anObj;
    } finally {
      Log.getLogWriter().info("Setting class loader to original: " + cl);
      Thread.currentThread().setContextClassLoader(cl);
    }
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.Declarable#init(java.util.Properties)
   */
  public void init(Properties props) {
    
    
  }

}
