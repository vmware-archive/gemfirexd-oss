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
package pdx;

import hydra.CacheHelper;
import hydra.Log;
import hydra.TestConfig;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import util.NameFactory;
import util.RandomValues;
import util.TestException;
import util.TestHelper;
import util.ValueHolderPrms;

import com.gemstone.gemfire.pdx.PdxInstance;
import com.gemstone.gemfire.pdx.PdxInstanceFactory;

/** Class to use GemFire's PdxInstanceFactory to create PdxInstances representing pdx domain
 *  objects used in testing. The pdx domain objects used in testing are those versioned classes
 *  defined in <gemfireCheckOutRooDir>/testsVersions.
 * @author lynn
 *
 */
public class PdxInstanceCreator {

  /** This method creates a PdxInstance representing either a version1 or version2 flavor
   *  of a versioned ValueHolder
   *  
   * @param className The class name to use to create the PdxInstance; this could be
   *                  an instance of VersionedValueHolder or PdxVersionedValueHolder for example. 
   * @param key The key which encode the base value
   * @param rv RandomValues for the extraObject (payload)
   */
  public static PdxInstance getVersionedValueHolder(String className, String key, RandomValues rv) {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      ClassLoader versionCL = PdxTest.initClassLoader();

      // now use reflection to call method to get a PdxInstance
      Class aClass;
      try {
        long base = NameFactory.getCounterForName(key);
        aClass = Class.forName("util.VersionedValueHolder", true, Thread.currentThread().getContextClassLoader());
        Method meth = aClass.getDeclaredMethod("getPdxInstance", String.class, long.class, RandomValues.class);
        PdxInstance retObj = (PdxInstance) meth.invoke(null, className, base, rv);
        return retObj;
      } catch (ClassNotFoundException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      } catch (SecurityException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      } catch (NoSuchMethodException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      } catch (IllegalArgumentException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      } catch (IllegalAccessException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      } catch (InvocationTargetException e) {
        throw new TestException(TestHelper.getStackTrace(e.getTargetException()));
      }
    } finally {
      Log.getLogWriter().info("Setting class loader to previous: " + cl);
      Thread.currentThread().setContextClassLoader(cl);
    }
  }

}
