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
package pdx;

import com.gemstone.gemfire.SerializationException;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.client.ServerOperationException;
import com.gemstone.gemfire.pdx.PdxInstance;
import com.gemstone.gemfire.pdx.PdxSerializer;
import com.gemstone.gemfire.pdx.ReflectionBasedAutoSerializer;

import hydra.CacheHelper;
import hydra.Log;

import java.util.ArrayList;
import java.util.List;

import util.PdxTestSerializer;
import util.TestException;
import util.TestHelper;

/**
 * @author lynn
 *
 */
public class PdxTestVersionHelper {

  /** If pdxReadSerialized is false, then allow the given
   *  Exception if it is the last of a chain of caused by's starting with
   *  a top level exception of SerializationException 
   *  or ServerOperationException and ending with a ClassNotFoundException.
   *  
   * @param e The exception to check.
   */
  public static void handleException(RuntimeException e) {
    boolean pdxReadSerialized = false;
    Cache aCache = CacheHelper.getCache();
    if (aCache != null) { // could be null during a nice_kill
      pdxReadSerialized = aCache.getPdxReadSerialized();
    }
    // if pdxReadSerialized is false, then we can allow certain exceptions
    // caused by a ClassNotFoundException
    if (!pdxReadSerialized) {
      if ((e instanceof SerializationException) ||
          (e instanceof ServerOperationException)) {
        List<Throwable> causeChain = new ArrayList();
        causeChain.add(e);
        Throwable cause = e.getCause();
        while (cause != null) {
          causeChain.add(cause);
          cause = cause.getCause();
        }
        Throwable lastCause = causeChain.get(causeChain.size() -1);
        if (lastCause instanceof ClassNotFoundException) { // ok, we will allow this
          String causeChainStr = "";
          for (int i = 0; i < causeChain.size(); i++) {
            Throwable t = causeChain.get(i);
            if (i == 0) {
              causeChainStr = causeChainStr + t.getClass().getName();
            } else {
              causeChainStr = " " + causeChainStr + " caused by " + t.getClass().getName();
            }
          }
          Log.getLogWriter().info("Got expected ClassNotFoundException for " + lastCause.getMessage() +
              " with this exception chain: " + causeChainStr);
          return;
        }
      }
    }
    throw e;
  }

  /** Given an object that can be a PdxInstance, return either the object or 
   *  the base object represented by the PdxInstance.
   * @param anObj The object that could be a PdxInstance
   * @return anObj, or if anObj is a PdxInstance return its base object.
   */
  public static Object toBaseObject(Object anObj) {
    boolean expectPdxInstance = true;
    Cache aCache = CacheHelper.getCache();
    if (aCache != null) { // could be null during a nice_kill
      expectPdxInstance = aCache.getPdxReadSerialized();
    }
    if (anObj instanceof PdxInstance) {
      if (!expectPdxInstance) {
        throw new TestException("Did not expect a PdxInstance: " + anObj);
      }
      PdxInstance pdxInst = (PdxInstance)anObj;
      Object baseObj = pdxInst.getObject();
      Log.getLogWriter().info("Obtained " + baseObj + " from PdxInstance " + pdxInst);
      return baseObj;
    } else if (anObj == null) {
      return anObj;
    } else {
      // even if we expect PdxInstances, we might get a domain object if it
      // happens to be deserialized already in the cache
      return anObj;
    }
  }

  /** Method used by hydra to return a properly configured instance of PdxSerializer
   * 
   * @return An instance of PdxSerializer to be set in the cache.
   */
  public static PdxSerializer instantiatePdxSerializer() {
    String className = PdxPrms.getPdxSerializerClassName();
    if (className.equals(ReflectionBasedAutoSerializer.class.getName())) {
      List<String> aList = new ArrayList();
      aList.add("util.VersionedValueHolder#identity=myValue");
      aList.add(".*");
      ReflectionBasedAutoSerializer mySerializer = new ReflectionBasedAutoSerializer(aList);
      Log.getLogWriter().info("Created " + mySerializer.getClass().getName() + " with args: " + aList);
      return mySerializer;
    } else if (className.equals(PdxTestSerializer.class.getName())) {
      Log.getLogWriter().info("Created " + PdxTestSerializer.class.getName());
      return new PdxTestSerializer();
    } else {
      throw new TestException("Test problem: don't know about " + className);
    }
  }
  
  public static void doEnumValidation(PdxInstance pdxInst)
      throws TestException {
    boolean doEnumValidation = PdxPrms.getDoEnumValidation();
    if (!doEnumValidation) {
       return; // we are running in a 661 mode in a 662 jvm; 661 does not have pdx enums
               // or are converting from 661 to 662 (this might be a 662 jvm, but it could
               // do a get from a 661 jvm, then the returned object will not be in 662 form
               // for enums
    } 
    if (pdxInst.isEnum()) {
      throw new TestException("isEnum() on " + pdxInst + " returned true");
    }
    String className = pdxInst.getClassName();
    if (!className.equals("util.VersionedValueHolder") &&
        !className.equals("util.PdxVersionedValueHolder")) {
      throw new TestException("getClassName() on " + pdxInst + " returned " + className);
    }
    Object enumObj = pdxInst.getField("aDay");
    if (enumObj != null) {
      if (!(enumObj instanceof PdxInstance)) {
        throw new TestException("Expected enum field aDay to be a PdxInstance but it is " + TestHelper.toString(enumObj));
      }
      PdxInstance enumPdxInst = (PdxInstance)enumObj;
      className = enumPdxInst.getClassName();
      if (!className.equals("util.VersionedValueHolder$Day")) {
        throw new TestException("getClassName() on " + enumPdxInst + " returned " + className);
      }
    }
   }

}
