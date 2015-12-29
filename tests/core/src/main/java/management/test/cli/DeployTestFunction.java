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
package management.test.cli;

import hydra.CacheHelper;
import hydra.Log;
import hydra.RemoteTestModule;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import util.BaseValueHolder;
import util.NameFactory;
import util.RandomValues;
import util.TestException;
import util.TestHelper;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.internal.ClassPathLoader;

/**
 * @author lynng
 *
 */
public class DeployTestFunction extends FunctionAdapter {

  // defines tasks for the function to do; these are passed to the function as an argument
  public static final String VERIFY_CLASS_AVAILABILITY = "verifyClassAvailability";

  private static final long serialVersionUID = 1L;

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.execute.FunctionAdapter#execute(com.gemstone.gemfire.cache.execute.FunctionContext)
   */
  @Override
  public void execute(FunctionContext context) {
    // retrieve and log function arguments
    List arguments = (ArrayList)(context.getArguments());
    Object initiatingThreadID = arguments.get(0);
    String task = (String)arguments.get(1);
    Log.getLogWriter().info("In execute with context " + context + " initiated in hydra thread thr_" + initiatingThreadID + "_; fcn task is " + task);

    if (task.equals(VERIFY_CLASS_AVAILABILITY)) {
      List<String> expectAvailable = (List)arguments.get(2);
      List<String> expectUnavailable = (List)arguments.get(3);
      Log.getLogWriter().info("Expect available: " + Arrays.asList(expectAvailable));
      Log.getLogWriter().info("Expect unavailable: " + Arrays.asList(expectUnavailable));

      // Test that the classes we expect to see are available
      Set<Region<?, ?>> allRegions = getAllRegions();
      RandomValues rv = new RandomValues();
      for (String className: expectAvailable) {
        Log.getLogWriter().info("Attempting to create instance of " + className + "; expect this to succeed");
        String key = NameFactory.getNextPositiveObjectName();
        Object newObj = null;
        try {
          newObj = createValueHolderInstance(className, key, rv);
        } catch (ClassNotFoundException e) {
          throw new TestException("Got unexpected " + TestHelper.getStackTrace(e));
        }
        String newObjStr = TestHelper.toString(newObj);
        Log.getLogWriter().info("Successfully created " + newObjStr);
        for (Region aRegion: allRegions) {
          Log.getLogWriter().info("Putting " + key + ", " + newObjStr + " into " + aRegion.getFullPath());
          aRegion.put(key, newObj);
        }
      }

      // Test that the classes we expect to not see are unavailable
      String key = NameFactory.getNextPositiveObjectName();
      for (String className: expectUnavailable) {
        Log.getLogWriter().info("Attempting to create instance of " + className + "; expect this to fail");
        try {
          Object newObj = createValueHolderInstance(className, key, rv);
          throw new TestException("Expected to not find " + className + " but was able to create " + TestHelper.toString(newObj));
        } catch (ClassNotFoundException e) {
          Log.getLogWriter().info("Test got expected exception for " + className + ": " + e);
        }
      }
      context.getResultSender().lastResult("Validation was successful for vm_" + RemoteTestModule.getMyVmid());
    } else {
      throw new TestException("Unknown task specified for function: " + task);
    }
  }

  /** Return a Set of all Regions defined in the cache
   * 
   * @return The set of all Regions defined in the cache
   */
  protected Set<Region<?, ?>> getAllRegions() {
    Set<Region<?, ?>> regionSet = new HashSet(CacheHelper.getCache().rootRegions());
    Set<Region<?, ?>> rootRegions = new HashSet(regionSet);
    for (Region aRegion: rootRegions) {
      regionSet.addAll(aRegion.subregions(true));
    }
    if (regionSet.size() == 0) {
      throw new TestException("Test problem; expected regions to be defined");
    }
    return regionSet;
  }

  /** Use reflection to create a new instance of a BaseValueHolder that 
   *  has a constructor with args (String, RandomValues) and is not available
   *  on the classpath currently because it wa compiled outside the <checkoutDir>/tests 
   *  directory.
   *
   * @param className The specific class to create (really any class with constructor
   *         with args (String, RandomValues).
   * @param key The String argument to the constructor.
   * @param rv The RandomValues instance to pass to the constructor.
   * 
   * @return A new instance of className.
   * 
   * @throws NoSuchMethodException 
   * @throws SecurityException 
   * @throws IllegalAccessException 
   * @throws InstantiationException 
   * @throws IllegalArgumentException 
   */
  public static BaseValueHolder createValueHolderInstance(String className, String key, RandomValues rv) throws ClassNotFoundException {
    Class aClass;
    try {
      aClass = ClassPathLoader.getLatest().forName(className);
      Constructor constructor = aClass.getConstructor(String.class, RandomValues.class);
      Object newObj = constructor.newInstance(key, rv);
      return (BaseValueHolder)newObj;
    } catch (InvocationTargetException e) {
      throw new TestException(TestHelper.getStackTrace(e.getTargetException()));
    } catch (SecurityException e) {
      throw new TestException(TestHelper.getStackTrace(TestHelper.getStackTrace(e)));
    } catch (NoSuchMethodException e) {
      throw new TestException(TestHelper.getStackTrace(TestHelper.getStackTrace(e)));
    } catch (IllegalArgumentException e) {
      throw new TestException(TestHelper.getStackTrace(TestHelper.getStackTrace(e)));
    } catch (InstantiationException e) {
      throw new TestException(TestHelper.getStackTrace(TestHelper.getStackTrace(e)));
    } catch (IllegalAccessException e) {
      throw new TestException(TestHelper.getStackTrace(TestHelper.getStackTrace(e)));
    }
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.execute.FunctionAdapter#getId()
   */
  @Override
  public String getId() {
    return this.getClass().getName();
  }

}
