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
/** Class containing pdx tests.
 * 
 */
package pdx;

import com.gemstone.gemfire.GemFireRethrowable;
import com.gemstone.gemfire.SerializationException;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.pdx.PdxInstance;
import com.gemstone.gemfire.pdx.PdxReader;
import com.gemstone.gemfire.pdx.PdxSerializer;
import com.gemstone.gemfire.pdx.PdxWriter;

import hydra.CacheHelper;
import hydra.Log;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.Map;

import objects.Portfolio;

import parReg.query.NewPortfolio;
import util.QueryObject;
import util.RandomValues;
import util.TestException;
import util.TestHelper;
import util.BaseValueHolder;

/**
 * @author lynn
 *
 */
public class PdxTest {
  
  // system property to determine if the test is running with 662 (or later) behavior or 661 behavior
  public static boolean pdx661Behavior;
  
  // used to make all threads in a single jvm use the same version (either version1 or version2); used with 661 behavior (ie pdx661Behavior is true)
  public static ClassLoader fixedLoader = null;
  
  // used to cache the two versioned class loaders available to the test; used with 662 and beyond behavior (ie pdx661Behavior is false)
  public static ClassLoader version1Loader = null;
  public static ClassLoader version2Loader = null;
  
  static {
    String propValue = System.getProperty("gemfire.loadClassOnEveryDeserialization");
    Log.getLogWriter().info("loadClassOnEveryDeserialization is " + propValue);
    if (propValue == null) { // not set; default to 662 (and later) behavior
      pdx661Behavior = false;
    } else {
      pdx661Behavior = propValue.equals("true");
    }
    Log.getLogWriter().info("pdx661Behavior is " + pdx661Behavior);
  }
  
  /** If PdxPrms.initClassLoader is true, then randomly choose a versioned
   *  class path and create and install a class loader for it on this thread.
   *  
   * @return The installed class loader (which includes a versioned class path)
   *         or null if this call did not install a new class loader.
   */
  public static ClassLoader initClassLoader() {
    if (PdxPrms.getInitClassLoader()) { // want to set a class loader on this thread
      if (pdx661Behavior) { // 661 behavior; threads in a single jvm can have different versions
        return setRandomLoader();
      } else { // 662 and later behavior; all threads in a single jvm should have the same version
        return setFixedLoader();
      }
    } else {
      return null;
    }
  }
  
  /** Set the classLoader to either version1 classes or version2 classes on a per thread basis.
   *  This means that within a single jvm, one thread could be running with version1 and another
   *  thread could be running with version2.
   *
   * @return The installed class loader (which includes a versioned class path)
   *         or null if this call did not install a new class loader.
   */
  private static ClassLoader setRandomLoader() {
    // lazily initialize version1Loader and version2Loader
    if (version1Loader == null) {
      synchronized (PdxTest.class) {
        if (version1Loader == null) {
          version1Loader = createClassLoader(1);
        }
      }
    }
    if (version2Loader == null) {
      synchronized (PdxTest.class) {
        if (version2Loader == null) {
          version2Loader = createClassLoader(2);
        }
      }
    }
    long counter = PdxBB.getBB().getSharedCounters().incrementAndRead(PdxBB.pdxClassPathIndex);
    long version = (counter % 2) + 1;
    ClassLoader clToInstall = null;
    if (version == 1) {
      clToInstall = version1Loader;
    } else {
      clToInstall = version2Loader;
    }
    Log.getLogWriter().info("Setting class loader with " + clToInstall);
    Thread.currentThread().setContextClassLoader(clToInstall); 
    return clToInstall;
  }
  
  /** Set the classLoader to either version1 classes or version2 classes on a per jvm basis.
   *  This means that all threads in a given jvm will all use either version1 or version2
   *
   * @return The installed class loader (which includes a versioned class path)
   *         or null if this call did not install a new class loader.
   */
  private static ClassLoader setFixedLoader() {
    if (fixedLoader == null) {
      synchronized (PdxTest.class) {
        if (fixedLoader == null) {
          long counter = PdxBB.getBB().getSharedCounters().incrementAndRead(PdxBB.pdxClassPathIndex);
          long version = (counter % 2) + 1;
          fixedLoader = createClassLoader(version);
          Log.getLogWriter().info("Setting class loader for all threads in this jvm to " + fixedLoader);
          Thread.currentThread().setContextClassLoader(fixedLoader);
          return fixedLoader;
        }
      }
    }
    Log.getLogWriter().info("Setting previously created class loader with " + fixedLoader);
    Thread.currentThread().setContextClassLoader(fixedLoader);
    return fixedLoader;
  }
  
  /** Create and return (but don't install) a class loader for the given version (either 1
   *  for a version1 loader or 2 for a version2 loader.
   *  
   * @param version Either 1 or 2 to indicate a class loader for version1 or version2
   * @return The new ClassLoader.
   */
  public static ClassLoader createClassLoader(long version) {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    cl = ClassLoader.getSystemClassLoader();
    String alternateVersionClassPath = System.getProperty("JTESTS") +
        File.separator + ".." + File.separator + ".." + File.separator +
        "testsVersions" + File.separator + "version" + version + File.separator + "classes/";
    ClassLoader newClassLoader = null;
    try {
      newClassLoader = new URLClassLoader(new URL[]{new File(alternateVersionClassPath).toURL()}, cl);
      Log.getLogWriter().info("Created (but not yet installing) class loader with " + alternateVersionClassPath
          + ": " + newClassLoader);
    } catch (MalformedURLException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
    return newClassLoader;
  }
  
  /** Use reflection to create a new instance of a versioned class whose
   *  name is specified by className.
   *  
   *  Since versioned classes are compled outside outside the <checkoutDir>/tests 
   *  directory, code within <checkoutDir>/tests cannot directly reference
   *  versioned classes, however the versioned class should be available at 
   *  runtime if the test has installed the correct class loader.
   *
   * @param className The name of the versioned class to create. 
   * @return A new instance of className.
   */
  public static Object getVersionedInstance(String className) {
    try {
      Class aClass = Class.forName(className, true, Thread.currentThread().getContextClassLoader());
      Constructor constructor = aClass.getConstructor();
      Object newObj = constructor.newInstance();
      return newObj;
    } catch (ClassNotFoundException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (SecurityException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (NoSuchMethodException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (IllegalArgumentException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (InstantiationException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (IllegalAccessException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (InvocationTargetException e) {
      throw new TestException(TestHelper.getStackTrace(e.getTargetException()));
    }
  }
  
  /** Use reflection to create a new instance of VersionedValueHolder or
   *  a subclass assuming it has a constructor with args (String, RandomValues). 
   *  Since versioned classes are compiled outside the <checkoutDir>/tests 
   *  directory, code within <checkoutDir>/tests cannot directly reference
   *  the them, however the these classes should be available at runtime if 
   *  the test has installed the correct class loader.
   *
   * @param className The specific class to create (really any class with constructor
   *         with args (String, RandomValues).
   * @param key The String argument to the constructor.
   * @param rv The RandomValues instance to pass to the constructor.
   * @return A new instance of className.
   */
  public static BaseValueHolder getVersionedValueHolder(String className, String key, RandomValues rv) {
    Class aClass;
    try {
      aClass = Class.forName(className, true, Thread.currentThread().getContextClassLoader());
      Constructor constructor = aClass.getConstructor(String.class, RandomValues.class);
      Object newObj = constructor.newInstance(key, rv);
      return (BaseValueHolder)newObj;
    } catch (ClassNotFoundException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (SecurityException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (NoSuchMethodException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (IllegalArgumentException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (InstantiationException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (IllegalAccessException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (InvocationTargetException e) {
      throw new TestException(TestHelper.getStackTrace(e.getTargetException()));
    }
  }
  
  /** Use reflection to create a new instance of VersionedValueHolder or
   *  a subclass assuming it has a constructor with args (Object, RandomValues). 
   *  Since versioned classes are compiled outside the <checkoutDir>/tests 
   *  directory, code within <checkoutDir>/tests cannot directly reference
   *  the them, however the these classes should be available at runtime if 
   *  the test has installed the correct class loader.
   *
   * @param className The specific class to create (really any class with constructor
   *         with args (String, RandomValues).
   * @param anObj The Object argument to the constructor.
   * @param rv The RandomValues instance to pass to the constructor.
   * @return A new instance of className.
   */

  public static BaseValueHolder getVersionedValueHolder(String className, Object anObj, RandomValues rv) {
    Class aClass;
    try {
      aClass = Class.forName(className, true, Thread.currentThread().getContextClassLoader());
      Constructor constructor = aClass.getConstructor(Object.class, RandomValues.class);
      Object newObj = constructor.newInstance(anObj, rv);
      return (BaseValueHolder)newObj;
    } catch (ClassNotFoundException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (SecurityException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (NoSuchMethodException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (IllegalArgumentException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (InstantiationException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (IllegalAccessException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (InvocationTargetException e) {
      throw new TestException(TestHelper.getStackTrace(e.getTargetException()));
    }
  }

  /** Use reflection to create a new instance of PdxVersionedNewPortfolio. 
   *
   * @param name Argument for the constructor.
   * @param index Argument for the constructor.
   * @return A new instance of parReg.query.PdxVersionedNewPortfolio
   */
  public static NewPortfolio getVersionedNewPortfolio(String className, String name, int index) {
    Class aClass;
    try {
      aClass = Class.forName(className, true, Thread.currentThread().getContextClassLoader());
      Constructor constructor = aClass.getConstructor(String.class, int.class);
      Object newObj = constructor.newInstance(name, index);
      return (NewPortfolio)newObj;
    } catch (ClassNotFoundException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (SecurityException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (NoSuchMethodException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (IllegalArgumentException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (InstantiationException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (IllegalAccessException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (InvocationTargetException e) {
      throw new TestException(TestHelper.getStackTrace(e.getTargetException()));
    }
  }
  
  /** Use reflection to create a new instance of PdxVersionedQueryObject. 
  *
  * @return A new instance of parReg.query.PdxVersionedNewPortfolio
  */
 public static QueryObject getVersionedQueryObject(String className, long base, int valueGeneration, int byteArraySize, int levels) {
   Class aClass;
   try {
     aClass = Class.forName(className, true, Thread.currentThread().getContextClassLoader());
     Constructor constructor = aClass.getConstructor(long.class, int.class, int.class, int.class);
     Object newObj = constructor.newInstance(base, valueGeneration, byteArraySize, levels);
     return (QueryObject)newObj;
   } catch (ClassNotFoundException e) {
     throw new TestException(TestHelper.getStackTrace(e));
   } catch (SecurityException e) {
     throw new TestException(TestHelper.getStackTrace(e));
   } catch (NoSuchMethodException e) {
     throw new TestException(TestHelper.getStackTrace(e));
   } catch (IllegalArgumentException e) {
     throw new TestException(TestHelper.getStackTrace(e));
   } catch (InstantiationException e) {
     throw new TestException(TestHelper.getStackTrace(e));
   } catch (IllegalAccessException e) {
     throw new TestException(TestHelper.getStackTrace(e));
   } catch (InvocationTargetException e) {
     throw new TestException(TestHelper.getStackTrace(e.getTargetException()));
   }
 }

  /** Given an object (assumed to be either a ValueHolder, PdxInstance or null)
   *  return either the ValueHolder or the ValueHolder represented by the
   *  PdxInstance or null. If the object is not one of these types, throw
   *  a TestException.
   * @param anObj The ValueHolder, PdxInstance or null
   * @return The ValueHolder or the ValueHolder represented by the PdxInstance
   */
  public static BaseValueHolder toValueHolder(Object anObj) {
    boolean expectPdxInstance = true;
    Cache aCache = CacheHelper.getCache();
    if (aCache != null) { // could be null during a nice_kill
      expectPdxInstance = aCache.getPdxReadSerialized();
    }
    if (anObj instanceof BaseValueHolder) {
      // even if we expect PdxInstances, we might get a domain object if it
      // happens to be deserialized already in the cache
      return (BaseValueHolder)anObj;
    } else if (anObj instanceof PdxInstance) {
      if (!expectPdxInstance) {
        throw new TestException("Did not expect a PdxInstance: " + anObj);
      }
      PdxInstance pdxInst = (PdxInstance)anObj;
      PdxTestVersionHelper.doEnumValidation(pdxInst);
      Object backingObj = pdxInst.getObject();
      if (backingObj instanceof BaseValueHolder) {
        Log.getLogWriter().info("Obtained " + backingObj + " from PdxInstance " + pdxInst);
        return (BaseValueHolder)backingObj;
      } else {
        throw new TestException("Expected " + pdxInst + " to represent an instance of ValueHolder, but getObject() is " +
            TestHelper.toString(backingObj));
      }
    } else if (anObj == null){
      return null;
    } else {
      throw new TestException("Expected " + anObj + " to be a PdxInstance or a ValueHolder");
    }
  }

  /** Return a map of all fields (including inherited fields) of anObject.
   *  
   * @param anObject The object with fields to include in the return map.
   * @return Map of field name (key) and field value (map value) for all
   *         fields for anObject. In addition, the map contains the key
   *         "className" and value (String), the class name for anObject.
   */
  public static Map<String, Object> getFieldMap(Object anObject) {
    Map<String, Object> fieldMap = new HashMap();
    Class aClass = anObject.getClass();
    fieldMap.put("className", aClass.getName());
    Map<String, Field> actualFields = getAllFields(anObject);
    for (String fieldName: actualFields.keySet()) {
      Field aField = actualFields.get(fieldName);
      if (!Modifier.isFinal(aField.getModifiers()) && !Modifier.isTransient(aField.getModifiers())) {
        Object fieldValue = null;
        try {
          fieldValue = aField.get(anObject);
        } catch (IllegalArgumentException e) {
          throw new TestException(TestHelper.getStackTrace(e));
        } catch (IllegalAccessException e) {
          throw new TestException(TestHelper.getStackTrace(e));
        }
        if (fieldMap.containsKey(fieldName)) {
          throw new TestException("test problem: field " + fieldName + " already exists");
        }
        if (aField.getType().isEnum() && (fieldValue != null)) {
          EnumHolder holder = new EnumHolder(aField.getType().getName(), fieldValue.toString());
          fieldMap.put(fieldName, holder);
        } else {
          fieldMap.put(fieldName, fieldValue);
        }
      }
    }
    Log.getLogWriter().info("created fieldMap " + fieldMap + " for object " + anObject);
    return fieldMap;
  }
  
  /** Return an object with fields restored to the values in the map
   *  (obtained from getFieldMap(Object).
   *  
   * @param fieldMap Map of field name (key) and field value (map value) for all
   *         fields for anObject. In addition, the map contains the key
   *         "className" and value (String), the class name for anObject.
   * @return An object with fields set to the values in fieldMap
   */
  public static Object restoreFromFieldMap(Map<String, Object> fieldMap) {
    String className = (String)fieldMap.get("className");
    Object newInstance = getVersionedInstance(className);
    Map<String, Field> actualFieldMap = getAllFields(newInstance);
    for (Object fieldName: fieldMap.keySet()) {
      Object fieldValue = fieldMap.get(fieldName);
      Field aField = actualFieldMap.get(fieldName);
      if (aField == null) {
        Log.getLogWriter().info("Did not restore " + fieldName + ", it exists in fieldMap, but not in actual Fields");
      } else {
        try {
          if (fieldValue instanceof EnumHolder) {
            EnumHolder holder = (EnumHolder) fieldValue;
            String enumClassName = holder.getClassName();
            String enumValue = holder.getEnumValue();
            Class enumClass;
            try {
              enumClass = Class.forName(enumClassName, true, Thread.currentThread().getContextClassLoader());
            } catch (ClassNotFoundException e) {
              throw new TestException(TestHelper.getStackTrace(e));
            }
            Object anObj = Enum.valueOf(enumClass, enumValue);
            aField.set(newInstance, anObj);
          } else {
            aField.set(newInstance, fieldValue);
          }
        } catch (IllegalArgumentException e) {
          throw new TestException(TestHelper.getStackTrace(e));
        } catch (IllegalAccessException e) {
          throw new TestException(TestHelper.getStackTrace(e));
        }
      }
    }
    Log.getLogWriter().info("restoreFromFieldMap returning " + newInstance +
        " restored from " + fieldMap);
    return newInstance;
  }

  /** Return a Map containing key fieldName and value Field for anObject, 
   *  including inherited fields
   * 
   * @param anObject Return a Map of fields for this object.
   * @return Map containing key (String) fieldName and value(Field).
   */
  private static Map<String, Field> getAllFields(Object anObject) {
    Map<String, Field> aMap = new HashMap();
    Class aClass = anObject.getClass();
    while (aClass != null) {
      Field[] fields = aClass.getDeclaredFields();
      for (Field aField: fields) {
        String fieldName = aField.getName();
        aMap.put(fieldName, aField);
      }
      aClass = aClass.getSuperclass();
    }
    //Log.getLogWriter().info("getAllFields returning " + aMap.keySet());
    return aMap;
  }

  /* Method to get the oldValue from an event, allowing ClassNotFoundExceptions.
   */
  public static String getOldValueStr(EntryEvent eEvent) {
    try {
      Object value = eEvent.getOldValue();
      if (value instanceof PdxInstance) {
        try {
          Object backingObj = ((PdxInstance)value).getObject();
          return "PdxInstance representing: " + TestHelper.toString(backingObj);
        } catch (SerializationException e) {
          Throwable lastCause = TestHelper.getLastCausedBy(e);
          if (lastCause instanceof ClassNotFoundException) {
            return "<PdxInstance could not be obtained due to: " + e.getMessage() + ">";
          }
          throw new TestException(TestHelper.getStackTrace(e));
        }
      } else {
        return TestHelper.toString(value);
      }
    } catch (SerializationException e) {
      // find the last non-null caused by
      Throwable lastCause = TestHelper.getLastCausedBy(e);
      if (lastCause instanceof ClassNotFoundException) {
        return "<Class " + lastCause.getMessage() + " was not found>";
      }
      throw e;
    }
  }

  /* Method to get the newValue from an event, allowing ClassNotFoundExceptions.
   */
  public static String getNewValueStr(EntryEvent eEvent) {
    try {
      Object value = eEvent.getNewValue();
      if (value instanceof PdxInstance) {
        try {
          Object backingObj = ((PdxInstance)value).getObject();
          return "PdxInstance representing: " + TestHelper.toString(backingObj);
        } catch (SerializationException e) {
          Throwable lastCause = TestHelper.getLastCausedBy(e);
          if (lastCause instanceof ClassNotFoundException) {
            return "<PdxInstance representing " + e.getMessage() + " but class could not be found>";
          }
          throw new TestException(TestHelper.getStackTrace(e));
        }
      } else {
        return TestHelper.toString(value);
      }
    } catch (SerializationException e) {
      // find the last non-null caused by
      Throwable lastCause = TestHelper.getLastCausedBy(e);
      if (lastCause instanceof ClassNotFoundException) {
        return "<Class " + lastCause.getMessage() + " was not found>";
      }
      throw e;
    }
  }

  /** Using reflection, invoke the instance method myToData on anObj passing
   *  writer as an argument.
   * @param anObj The object to receive the myToData method invocation.
   * @param writer The object to use as the argument to _toData
   */
  public static void invokeToData(Object anObj, PdxWriter writer) throws GemFireRethrowable {
    Class aClass = anObj.getClass();
    Method aMethod;
    try {
      aMethod = aClass.getMethod("myToData", new Class[] {PdxWriter.class});
      aMethod.invoke(anObj, new Object[] {writer});
    } catch (SecurityException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (NoSuchMethodException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (IllegalArgumentException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (IllegalAccessException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (InvocationTargetException e) {
      Throwable rootExc = e.getTargetException();
      if (rootExc instanceof GemFireRethrowable) {
        throw (GemFireRethrowable)rootExc;
      }
      if (rootExc instanceof RuntimeException) { // let test handle it
        throw (RuntimeException)rootExc;
      }
      if (rootExc instanceof Error) { // let test handle it
        throw (Error)rootExc;
      }
      throw new TestException(TestHelper.getStackTrace(e.getTargetException()));
    }
  }
  
  /** Using reflection, invoke the instance method myFromData on anObj passing
   *  reader as an argument.
   * @param anObj The object to receive the myFromData method invocation.
   * @param writer The object to use as the argument to _fromData
   * @throws Throwable 
   */
  public static void invokeFromData(Object anObj, PdxReader reader) {
    Class aClass = anObj.getClass();
    Method aMethod;
    try {
      aMethod = aClass.getMethod("myFromData", new Class[] {PdxReader.class});
      aMethod.invoke(anObj, new Object[] {reader});
    } catch (SecurityException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (NoSuchMethodException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (IllegalArgumentException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (IllegalAccessException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (InvocationTargetException e) {
      Throwable baseException = e.getTargetException();
      if (baseException instanceof Error) {
        throw (Error)baseException;
      } else if (baseException instanceof RuntimeException) {
        throw (RuntimeException)baseException;
      } else {
        throw new TestException(TestHelper.getStackTrace(baseException));
      }
    }
  }

  /** Return the toString() representation of the field fieldName from the given object anObj
   *  or null.
   * 
   * @param anObj The object that contains the field fieldName
   * @param fieldName Return the toString() value of fieldName in anObj.
   * @return The value of fieldName in anObj as a String
   */
  public static String getFieldValue(Object anObj, String fieldName) throws NoSuchFieldException {
    Field aField;
    try {
      aField = anObj.getClass().getField(fieldName);
      Object value = aField.get(anObj);
      if (value == null) {
        return null;
      } else {
        return value.toString();
      }
    } catch (SecurityException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (IllegalArgumentException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (IllegalAccessException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }

}
