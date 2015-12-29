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
package util; 

import hydra.CacheHelper;
import hydra.Log;
import hydra.TestConfig;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import pdx.PdxPrms;
import pdx.PdxTest;
import util.BaseValueHolder;
import util.NameFactory;
import util.RandomValues;
import util.TestException;
import util.TestHelper;
import util.ValueHolderPrms;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.pdx.PdxInstance;
import com.gemstone.gemfire.pdx.PdxInstanceFactory;
import com.gemstone.gemfire.pdx.PdxReader;
import com.gemstone.gemfire.pdx.PdxWriter;

public class VersionedValueHolder extends util.BaseValueHolder {
  static final int nullFieldFrequency = 10;  // create one out of nullFieldFrequency has null fields
  
  // add an enum field for enum testing
  public enum Day {
    Monday, Tuesday, Wednesday, Thursday, Friday
  }
  public Day aDay;
  static transient List dayList;
  
  static {
    Log.getLogWriter().info("In static initializer for testsVersions/version1/util.VersionedValueHolder");
    dayList = new ArrayList();
    dayList.addAll(EnumSet.allOf(Day.class));
  }
  
  /** No arg constructor
   * 
   */
  public VersionedValueHolder() {
    myVersion = "version1: util.VersionedValueHolder";
  }

  /** Create a new instance of VersionedValueHolder. The new instance will have
   *  a {@link Long} for myValue, using the nameFactoryName to extract the long.
   *  If ValueHolderPrms.useExtraObject is set, the extraObject field
   *  will be filled in using randomValues.
   * 
   *  @param nameFactoryName - A name from {@link NameFactory}, used to extract
   *         a value for the field myValue.
   *  @param randomValues - Used to generate an extraObject, if needed.
   *         If useExtraObject is false, this can be null
   *
   * @see NameFactory#getCounterForName
   */
  public VersionedValueHolder(String nameFactoryName, RandomValues randomValues) {
     myVersion = "version1: util.VersionedValueHolder";
     long keyIndex = NameFactory.getCounterForName(nameFactoryName);
     this.myValue = new Long(keyIndex);
     boolean useNulls = ((Long)(this.myValue) % nullFieldFrequency) == 0;
     if (useNulls) {
        this.aDay = null;
     } else {
        this.aDay = (Day)(getDayForBase((int)keyIndex));
     }
     if (TestConfig.tab().booleanAt(ValueHolderPrms.useExtraObject)) {
        this.extraObject = randomValues.getRandomObjectGraph();
     }
  }
  
  /** 
   *  Create a new instance of BaseValueHolder. The new BaseValueHolder will
   *  have <code>anObj</code> placed as its <code>myValue</code> field
   *  and, if desired, extraObject will be filled in using
   *  <code>randomValues</code> if {@link
   *  ValueHolderPrms#useExtraObject} is set to true.
   * 
   *  @param anObj - Used for myValue.
   *  @param randomValues - Used to generate extraObject, if needed.
   */
    public VersionedValueHolder(Object anObj, RandomValues randomValues) {
      myVersion = "version1: util.VersionedValueHolder";
      this.myValue = anObj;
      boolean useNulls = ((new Long("" + this.myValue)) % nullFieldFrequency) == 0;
      if (useNulls) {
         this.aDay = null;
      } else {
         this.aDay = (Day)(getDayForBase((new Integer("" + anObj)).intValue()));
      }
      if (TestConfig.tab().booleanAt(ValueHolderPrms.useExtraObject)) {
        this.extraObject = randomValues.getRandomObjectGraph();
      }
    }
  
  /** Using the current values in this VersionedValueHolder, return a new
   *  VersionedValueHolder that has the same "value" in <code>myValue</code> and
   *  a new <code>extraObject</code> if extraObject is used. For
   *  example, if <code>this.myValue</code> contains a Long of value 34, the new
   *  VersionedValueHolder will also have a myValue of 34, but it may be a
   *  String, an Integer, a BigInteger, etc.
   *
   *  @return A new instance of VersionedValueHolder
   *
   *  @throws TestException
   *          If we cannot find an alternative value for this value in
   *          this <code>VersionedValueHolder</code>.
   */
  @Override
  public BaseValueHolder getAlternateValueHolder(RandomValues randomValues) {
     BaseValueHolder vh = new VersionedValueHolder(this.myValue, randomValues);
     if (myValue instanceof Long) {
        vh.myValue = myValue.toString();
     } else if (myValue instanceof String) {
        vh.myValue = new Integer((String)myValue);
     } else if (myValue instanceof Integer) {
        vh.myValue = new BigInteger(myValue.toString());
     } else if (myValue instanceof BigInteger) {
        vh.myValue = new Long(myValue.toString());
     } else {
        throw new TestException("Cannot get replace value for myValue in " + TestHelper.toString(vh));
     }
     return vh;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return this.getClass().getName() + " [myVersion=" + this.myVersion + 
       ", myValue=" + TestHelper.toString(this.myValue) + 
       ", extraObject=" + TestHelper.toString(this.extraObject) + 
       ", modVal=" + this.modVal + 
       ", aDay=" + this.aDay + "]";
  }
  
  /** Verify the fields of this instance
   * 
   * @param base The expected base value used to give the fields values. 
   */
  @Override
  public void verifyMyFields(long base) {
    verifyMyFields(null, base);
  }
  
  /** Verify the fields of this instance
   * 
   * @param String The expected value for myValue for knownKeys style tests (a String
   *        of the form "update_N".
   */
  @Override
  public void verifyMyFields(String updatedMyValue, long base) {
    Log.getLogWriter().info("In version1 verifyMyFields, checking pdx fields with base " +
        base + " for: " + this);
    if (this.myVersion == null) {
      throw new TestException("Unexpected null myVersion field: " + this);
    }
    String expectedMyValueField = "" + base;
    if (updatedMyValue != null) {
      expectedMyValueField = updatedMyValue;
    }

    // This code is in version1; if the myVersion field was version2 (meaning
    // that version2 originally created this instance), then we can't see version2's
    // additional fields here in version1
    StringBuffer errStr = new StringBuffer();
    // check the fields common to both version1 and version2
    if (this.myValue == null) {
      errStr.append("Expected myValue " + this.myValue + " to be " + expectedMyValueField + "\n");
    } else {
      if (!(this.myValue.toString().equals(expectedMyValueField))) {
        errStr.append("Expected myValue " + this.myValue + " to be " + expectedMyValueField + "\n");
      }
    }
    if (TestConfig.tab().booleanAt(ValueHolderPrms.useExtraObject)) {
      if (this.extraObject == null) {
        errStr.append("Expected extraObject to be non-null, but it is is " + TestHelper.toString(this.extraObject) + "\n");
      }
    } else {
      if (this.extraObject != null) {
        errStr.append("Expected extraObject to be null, but it is " + TestHelper.toString(this.extraObject) + "\n");
      }
    }
    // modVal is unused and should always be null
    if (this.modVal != null) {
      errStr.append("Expected modVal to be null, but it is " + TestHelper.toString(this.modVal) + "\n");
    }
    boolean useNulls = (base % nullFieldFrequency) == 0;
    Day expectedDay = null;
    if (!useNulls) {
      expectedDay = (Day)(dayList.get((int)base % dayList.size()));
    }
    
    if ((aDay == null) || (expectedDay == null)) {
      if (expectedDay != aDay) {
         errStr.append("Expected aDay " + aDay + " to be " + expectedDay + "\n");
      }
    } else if (!(aDay.toString().equals(expectedDay.toString()))) { // use toString to make sure we really have the correct day
    	errStr.append("Expected aDay " + aDay + " to be " + expectedDay + "\n");
    }

    if (errStr.length() > 0) {
      throw new TestException(errStr.toString() + "\n" + this);
    }
  }
  
  /* 
   */
  public void myToData(PdxWriter writer) {
    Log.getLogWriter().info("In testsVersions/version1/util.VersionedValueHolder.myToData: " + this +
       (PdxPrms.getLogStackTrace() ? ("\n" + TestHelper.getStackTrace()) : ""));
    writer.writeString("myVersion", myVersion);

    // original fields
    writer.writeObject("myValue", myValue);
    writer.writeObject("extraObject", extraObject);
    writer.writeObject("modVal", modVal);
    writer.writeObject("aDay", aDay);
    
    writer.markIdentityField("myValue");
  }

  /* 
   */
  public void myFromData(PdxReader reader) {
    myVersion = reader.readString("myVersion");
    Log.getLogWriter().info("In testsVersions/version1/util.VersionedValueHolder.myFromData with myVersion: " + myVersion);

    // original fields
    myValue = reader.readObject("myValue");
    extraObject = reader.readObject("extraObject");
    modVal = (Integer)reader.readObject("modVal");
    aDay = (Day)reader.readObject("aDay");   
 
    Log.getLogWriter().info("After reading fields in fromData: " + this + 
        (PdxPrms.getLogStackTrace() ? ("\n" + TestHelper.getStackTrace()) : ""));
  }

  /** Return a PdxInstance of the given className
   * 
   * @param className The class of the object represented by the PdxInstance
   * @param base The base to use to calculate field values.
   * @param rv Random values used for the payload (extraObject) field
   * @return A PdxInstance.
   */
  public static PdxInstance getPdxInstance(String className, long base, RandomValues rv) {
    Log.getLogWriter().info("Creating PdxInstance for " + className + " with PdxInstanceFactory in version1...");
    Cache theCache = CacheHelper.getCache();
    if (theCache == null) { // can happen during HA tests
      return null;
    }
    boolean pdxReadSerialized = theCache.getPdxReadSerialized();
    PdxInstanceFactory fac = theCache.createPdxInstanceFactory(className);
    fac.writeString("myVersion", "version1: " + className);
    fac.writeObject("myValue", base);
    if (TestConfig.tab().booleanAt(ValueHolderPrms.useExtraObject)) {
      fac.writeObject("extraObject", rv.getRandomObjectGraph());
    }
    if (pdxReadSerialized) {
      Day dayValue = getDayForBase(base);
      if (dayValue == null) {
        fac.writeObject("aDay", null);
      } else {
        fac.writeObject("aDay", theCache.createPdxEnum("util.VersionedValueHolder$Day", getDayForBase(base).toString(), getDayForBase(base).ordinal()));
      }
    } else {
      fac.writeObject("aDay", getDayForBase(base));
    }
    
    // set identity fields
    fac.markIdentityField("myValue");
    
    PdxInstance pdxInst = fac.create();
    try {
       Log.getLogWriter().info("Created " + pdxInst + " with PdxInstanceFactory representing " + TestHelper.toString(pdxInst.getObject()));
    } catch (CacheClosedException e) {
       // during HA tests the cache could be closed while calling getObject()
       Log.getLogWriter().info("Created " + pdxInst + " with PdxInstanceFactory");
    }
    return pdxInst;
  }
  
  /** Return the appropriate enum Day value given the base
   * 
   * @param base Index to base the Day calculation on.
   * @return A value of Day.
   */
  public static Day getDayForBase(long base) {
    if ((base % nullFieldFrequency) == 0) {
      return null;
    } else {
      Day aDay = (Day)(dayList.get((int)base % dayList.size()));
      return aDay;
    }
  }
  
}
