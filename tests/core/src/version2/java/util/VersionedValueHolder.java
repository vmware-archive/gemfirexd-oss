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
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import pdx.PdxPrms;
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
  static final int MAX_COLLECTION_SIZE = 12;
  static final int nullFieldFrequency = 10;  // create one out of nullFieldFrequency has null fields
  
  // version2 Day enum adds Sunday and Saturday to Day enum in version1
  // Gemfire only supports adding new enum values to the end, not at the beginning so Saturday and Sunday
  // must come after Friday
  public enum Day {
    Monday, Tuesday, Wednesday, Thursday, Friday, Saturday, Sunday
  }
  static transient List dayList;
  
  // version2 enum that is not present in version1
  public enum Instrument {
    Cello, Violin, Viola, Clarinet, Oboe, Trumpet, Flute, Bassoon, Timpani, Piano
  }
  static transient List instrumentList;
  
  static {
    Log.getLogWriter().info("In static initializer for testsVersions/version2/util.VersionedValueHolder");
    dayList = new ArrayList();
    dayList.addAll(EnumSet.range(Day.Monday, Day.Friday)); // to ensure that these days are not mixed up with version1 days
    instrumentList = new ArrayList();
    instrumentList.addAll(EnumSet.allOf(Instrument.class));
  }
  //===========================================================================
  //define additional values and fields for pdx testing

  // additional fields from tests.util.BaseValueHolder
  public Day aDay;
  public Instrument anInstrument;
  public char aChar;
  public boolean aBoolean;
  public byte aByte;
  public short aShort;
  public int anInt;
  public long aLong;
  public float aFloat;
  public double aDouble;
  public Date aDate;
  public String aString;
  public Object anObject;
  public Map aMap;
  public Collection aCollection;
  public boolean[] aBooleanArray;
  public char[] aCharArray;
  public byte[] aByteArray;
  public short[] aShortArray;
  public int[] anIntArray;
  public long[] aLongArray;
  public float[] aFloatArray;
  public double[] aDoubleArray;
  public String[] aStringArray;
  public Object[] anObjectArray;
  public byte[][] anArrayOfByteArray;
  public Day[] aDayArray;
  public Instrument[] anInstrumentArray;

  /** No arg constructor
   * 
   */
  public VersionedValueHolder() {
    myVersion = "version2: util.VersionedValueHolder";
  }

  /** Create a new instance of VersionedValueHolder. The new instance will have
   *  a {@link Long} for myValue, using the nameFactoryName to extract the long.
   *  If ValueHolderPrms.useExtraObject is set, the extraObject field
   *  will be filled in using randomValues.
   * 
   *  @param nameFactoryName - A name from {@link NameFactory}, used to extract
   *         a value for the VersionedValueHolder myValue.
   *  @param randomValues - Used to generate an extraObject, if needed.
   *         If useExtraObject is false, this can be null
   *
   * @see NameFactory#getCounterForName
   */
  public VersionedValueHolder(String nameFactoryName, RandomValues randomValues) {
    myVersion = "version2: util.VersionedValueHolder";
    this.myValue = new Long(NameFactory.getCounterForName(nameFactoryName));
    if (TestConfig.tab().booleanAt(ValueHolderPrms.useExtraObject)) {
      this.extraObject = randomValues.getRandomObjectGraph();
    }
    fillInBaseValues((Long)(this.myValue));
  }
  
/** 
 *  Create a new instance of VersionedValueHolder. The new VersionedValueHolder will
 *  have <code>anObj</code> placed as its <code>myValue</code> field
 *  and, if desired, extraObject will be filled in using
 *  <code>randomValues</code> if {@link
 *  ValueHolderPrms#useExtraObject} is set to true.
 * 
 *  @param anObj - Used for myValue.
 *  @param randomValues - Used to generate extraObject, if needed.
 */
  public VersionedValueHolder(Object anObj, RandomValues randomValues) {
    myVersion = "version2: util.VersionedValueHolder";
    this.myValue = anObj;
    if (TestConfig.tab().booleanAt(ValueHolderPrms.useExtraObject)) {
      this.extraObject = randomValues.getRandomObjectGraph();
    }
    long base = new Long("" + anObj);
    fillInBaseValues(base);
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

  /** Init fields using base as appropriate, but does not
   *  initialize aQueryObject field.
   *
   *  @param base The base value for all fields. Fields are all set
   *              to a value using base as appropriate.
   */
  public void fillInBaseValues(long base) {
    boolean useNulls = (base % nullFieldFrequency) == 0;
    RandomValues rv = new RandomValues();
    aChar = (char)base;
    aBoolean = true;
    aByte = (byte)base;
    aShort = (short)base;
    anInt = (int)base;
    aLong = base;
    aFloat = base;
    aDouble = base;
    if (useNulls) {
      aDay = null;
      anInstrument = null;
      anObject = null;
      aMap = null;
      aCollection = null;
      aDate = null;
      // anEnum = null; cannot call writeEnum with null per product restrictions for null enum
      aString = null;
      aBooleanArray = null;
      aCharArray = null;
      aByteArray = null;
      aShortArray = null;
      anIntArray = null;
      aLongArray = null;
      aFloatArray = null;
      aDoubleArray = null;
      aStringArray = null;
      anObjectArray = null;
      anArrayOfByteArray = null;
      aDayArray = null;
      anInstrumentArray = null;
    } else {
      int desiredCollectionSize = (int)(base % MAX_COLLECTION_SIZE);
      aDay = getDayForBase(base);
      anInstrument = getInstrumentForBase(base);
      aDate = new Date(base);
      aString = "" + base;
      anObject = rv.getRandomObject();
      aMap = new HashMap();
      aCollection = new ArrayList();
      aBooleanArray = new boolean[desiredCollectionSize];
      aCharArray = new char[desiredCollectionSize];
      aByteArray = new byte[desiredCollectionSize];
      aShortArray = new short[desiredCollectionSize];
      anIntArray = new int[desiredCollectionSize];
      aLongArray = new long[desiredCollectionSize];
      aFloatArray = new float[desiredCollectionSize];
      aDoubleArray = new double[desiredCollectionSize];
      aStringArray = new String[desiredCollectionSize];
      anObjectArray = new Object[desiredCollectionSize];
      anArrayOfByteArray = new byte[desiredCollectionSize][desiredCollectionSize];
      aDayArray = new Day[desiredCollectionSize];
      anInstrumentArray = new Instrument[desiredCollectionSize];
      if (desiredCollectionSize > 0) {
        for (int i = 0; i < desiredCollectionSize-1; i++) {
          aMap.put(i, i);
          aCollection.add(i);
          aBooleanArray[i] = (i%2) == 0;
          aCharArray[i] = (char)(i);
          aByteArray[i] = (byte)(i);
          aShortArray[i] = (short)i;
          anIntArray[i] = i;
          aLongArray[i] = i;
          aFloatArray[i] = i;
          aDoubleArray[i] = i;
          aStringArray[i] = "" + i;
          anObjectArray[i] = rv.getRandomObject();
          anArrayOfByteArray[i] = aByteArray;
          aDayArray[i] = getDayForBase(i);
          anInstrumentArray[i] = getInstrumentForBase(i);
        }
        // now add the last value, using base if possible
        aMap.put(base, base);
        aCollection.add(base);
        aBooleanArray[desiredCollectionSize-1] = true;
        aCharArray[desiredCollectionSize-1] = (char)base;
        aByteArray[desiredCollectionSize-1] = (byte)base;
        aShortArray[desiredCollectionSize-1] = (short)base;
        anIntArray[desiredCollectionSize-1] = (int)base;
        aLongArray[desiredCollectionSize-1] = base;
        aFloatArray[desiredCollectionSize-1] = base;
        aDoubleArray[desiredCollectionSize-1] = base;
        aStringArray[desiredCollectionSize-1] = "" + base;
        anObjectArray[desiredCollectionSize-1] = rv.getRandomObject();
        aDayArray[desiredCollectionSize-1] = null;
        anInstrumentArray[desiredCollectionSize-1] = null;
      }
    }
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
   * @param updatedMyValue For knownKeys style tests, the myValue field is of the form
   *        "updated_N", where N is the base. If this is null, then this is not a
   *        knownKeys style test and myValue should be equal to the base (rather than
   *        "updated_<base>").
   * @param base The expected base value used to give the fields values. 
   */
  @Override
  public void verifyMyFields(String updatedMyValue, long base) {
    if (this.myVersion == null) {
      throw new TestException("Unexpected null myVersion field: " + this);
    }
    String expectedMyValueField = "" + base;
    if (updatedMyValue != null) {
      expectedMyValueField = updatedMyValue;
    }
    Log.getLogWriter().info("In version2 verifyMyFields, checking pdx fields with base " + base +
        " for: " + this);

    
    // Create an instance of VersionedValueHolder to use some of its fields to 
    // compare to "this"; just by creating a VersionedValueHolder, the fields
    // have the default value (for cases where we need to confirm they have
    // the default value), or they can be filled in and used for comparison
    VersionedValueHolder expected = new VersionedValueHolder(); // contains default field values
    
    // now set the fields of expected appropriately to use for comparison
    // Note: this.myVersion refers to the version that created the object
    if (myVersion.indexOf("version2") >= 0) { 
      // This thread (version2) is the same version as this.myVersion;
      // since the myVersion field refers to version2, that means that this instance
      // was created in a version2 thread, thus it should contain field values
      // based on the "base" argument
      expected.fillInBaseValues(base);
    } else {
      // this.myVersion refers to a version other than this thread's version (version2);
      // At this point, the only other version this test supports is version1.
      // Since this is running in a thread that is using version2 instances, the
      // extra version2 fields are present here, but should all have default values.
      // The variable "expected", already contains default value just by instantiating it.
      // Now set other fields as appropriate for the toString comparison below.
      Log.getLogWriter().info("Expecting default values for extra fields");
    }
    
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
    // modVal is unused
    
    // check the fields present in version2
    verifyField("aDay", this.aDay, expected.aDay);
    verifyField("anInstrument", this.anInstrument, expected.anInstrument);
    if (this.aChar != expected.aChar) {
      errStr.append("Expected aChar " + (int)this.aChar + " to be " + (int)expected.aChar + "\n");
    }
    if (this.aBoolean != expected.aBoolean) {
      errStr.append("Expected aBoolean " + this.aBoolean + " to be " + expected.aBoolean + "\n");
    }
    if (this.aByte != expected.aByte) {
      errStr.append("Expected aByte " + this.aByte + " to be " + expected.aByte + "\n");
    }
    if (this.aShort != expected.aShort) {
      errStr.append("Expected aShort " + this.aShort + " to be " + expected.aShort + "\n");
    }
    if (this.anInt != expected.anInt) {
      errStr.append("Expected anInt " + this.anInt + " to be " + expected.anInt + "\n");
    }
    if (this.aLong != expected.aLong) {
      errStr.append("Expected aLong " + this.aLong + " to be " + expected.aLong + "\n");
    }
    if (this.aFloat != expected.aFloat) {
      errStr.append("Expected aFloat " + this.aFloat + " to be " + expected.aFloat + "\n");
    }
    if (this.aDouble != expected.aDouble) {
      errStr.append("Expected aDouble " + this.aDouble + " to be " + expected.aDouble + "\n");
    }
    errStr.append(verifyField("aDate", this.aDate, expected.aDate));
    errStr.append(verifyField("aString", this.aString, expected.aString));
    // anObject is randomly generated, so it won't match our expected except in null case
    if (expected.anObject == null) {
      if (this.anObject != null) {
        throw new TestException("Expected anObject to be null but it is " + TestHelper.toString(this.anObject) + "\n");
      }
    } else {
      if (this.anObject == null) {
        throw new TestException("Expected anObject to be non-null, but it is " + this.anObject + "\n");
      }
    }
    errStr.append(verifyField("aMap", this.aMap, expected.aMap));
    errStr.append(verifyField("aCollection", this.aCollection, expected.aCollection));
    if (!Arrays.equals(this.aBooleanArray, expected.aBooleanArray)) {
      errStr.append("Expected aBooleanArray " + Arrays.toString(this.aBooleanArray)+ " to be " + 
                    Arrays.toString(expected.aBooleanArray) + "\n");
    }
    if (!Arrays.equals(this.aCharArray, expected.aCharArray)) {
      errStr.append("Expected aCharArray " + Arrays.toString(this.aCharArray)+ " to be " + 
                    Arrays.toString(expected.aCharArray) + "\n");
    }
    if (!Arrays.equals(this.aByteArray, expected.aByteArray)) {
      errStr.append("Expected aByteArray " + Arrays.toString(this.aByteArray)+ " to be " + 
                    Arrays.toString(expected.aByteArray) + "\n");
    }
    if (!Arrays.equals(this.aShortArray, expected.aShortArray)) {
      errStr.append("Expected aShortArray " + Arrays.toString(this.aShortArray)+ " to be " + 
                    Arrays.toString(expected.aShortArray) + "\n");
    }
    if (!Arrays.equals(this.anIntArray, expected.anIntArray)) {
      errStr.append("Expected aIntArray " + Arrays.toString(this.anIntArray)+ " to be " + 
                    Arrays.toString(expected.anIntArray) + "\n");
    }
    if (!Arrays.equals(this.aLongArray, expected.aLongArray)) {
      errStr.append("Expected aLongArray " + Arrays.toString(this.aLongArray)+ " to be " + 
                    Arrays.toString(expected.aLongArray) + "\n");
    }
    if (!Arrays.equals(this.aFloatArray, expected.aFloatArray)) {
      errStr.append("Expected aFloatArray " + Arrays.toString(this.aFloatArray)+ " to be " + 
                    Arrays.toString(expected.aFloatArray) + "\n");
    }
    if (!Arrays.equals(this.aDoubleArray, expected.aDoubleArray)) {
      errStr.append("Expected aDoubleArray " + Arrays.toString(this.aDoubleArray)+ " to be " + 
                    Arrays.toString(expected.aDoubleArray) + "\n");
    }
    if (!Arrays.equals(this.aStringArray, expected.aStringArray)) {
      errStr.append("Expected aStringArray " + Arrays.toString(this.aStringArray)+ " to be " + 
                    Arrays.toString(expected.aStringArray) + "\n");
    }
    // anObjectArray's contents is randomly generated, so it won't match our expected except in null case
    // but the size of expected should match
    if (expected.anObjectArray == null) {
      if (this.anObjectArray != null) {
        throw new TestException("Expected anObjectArray to be null but it is " + TestHelper.toString(this.anObjectArray) + "\n");
      }
    } else {
      if (this.anObjectArray == null) {
        throw new TestException("Expected anObjectArray to be non-null, but it is " + this.anObject + "\n");
      }
      if (expected.anObjectArray.length != this.anObjectArray.length) {
        throw new TestException("Expected length of anObjectArray to be " + expected.anObjectArray.length +
            ", but it is " + this.anObjectArray.length + "; " + TestHelper.toString(this.anObjectArray) + "\n");
      }
    }
    if (!Arrays.deepEquals(this.anArrayOfByteArray, expected.anArrayOfByteArray)) {
      errStr.append("Expected anArrayOfByteArray " + Arrays.deepToString(this.anArrayOfByteArray)+ " to be " + 
                    Arrays.deepToString(expected.anArrayOfByteArray) + "\n");
    }
    if (!arraysAreEqual(this.aDayArray, expected.aDayArray)) {
      errStr.append("Expected aDayArray " + Arrays.deepToString(this.aDayArray)+ " to be " + 
                    Arrays.deepToString(expected.aDayArray) + "\n");
    }
    if (!arraysAreEqual(this.anInstrumentArray, expected.anInstrumentArray)) {
      errStr.append("Expected anInstrumentArray " + Arrays.deepToString(this.anInstrumentArray) + " to be " +
                    Arrays.deepToString(expected.anInstrumentArray)+ "\n"); 
    }
    
    if (errStr.length() > 0) {
      throw new TestException(errStr.toString());
    }
  }
  
  /** Compare two arrays with a toString on each element
   * 
   * @param arr1 An Array of Objects to compare.
   * @param arr2 Another Array of Objects to compare.
   * @return True if the value of toString on each corresponding element of arr1 and arr2 is equal,
   *         false otherwise. 
   */
  private boolean arraysAreEqual(Object[] arr1, Object[] arr2) {
    if ((arr1 == null) || (arr2 == null)) {
      return arr1 == arr2;
    }
    if (arr1.length != arr2.length) {
      return false;
    }
    for (int i = 0; i < arr1.length; i++) {
      if ((arr1[i] == null) || (arr2[i] == null)) {
        if (arr1[i] != arr2[i]) {
          return false;
        }
      } else {
        if (!(arr1[i].toString().equals(arr2[i].toString()))) {
          return false;
        }
      }
    }
    return true;
  }

private String verifyField(String fieldName, Object actual, Object expected) {
    if (actual == null) {
      if (expected != null) {
        return "Expected " + fieldName + " " + TestHelper.toString(actual) + " to be " + TestHelper.toString(expected) + "\n";
      }
    } else if (!actual.equals(expected)) {
      return "Expected " + fieldName + " " + TestHelper.toString(actual) + " to be " + TestHelper.toString(expected) + "\n";
    }
    return "";
  }

  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return this.getClass().getName() + " [myVersion=" + this.myVersion + 
      ", aDay=" + this.aDay +
      ", anInstrument=" + this.anInstrument +
      ", aChar=" + (int)this.aChar + 
      ", aBoolean=" + this.aBoolean + 
      ", aByte=" + this.aByte +
      ", aShort=" + this.aShort + 
      ", anInt=" + this.anInt + 
      ", aLong=" + this.aLong + 
      ", aFloat=" + this.aFloat + 
      ", aDouble=" + this.aDouble +
      ", aDate=" + this.aDate + 
      ", aString=" + this.aString + 
      ", anObject=" + TestHelper.toString(this.anObject) +
      ", aMap=" +
        ((aMap == null) ? "null" : ("<" + this.aMap.getClass().getName() + " of size " + this.aMap.size() + ">")) +
      ", aCollection=" +
        ((aCollection == null) ? "null" : ("<" + this.aCollection.getClass().getName() + " of size " + this.aCollection.size() + ">")) +
      ", aBooleanArray=" + 
        ((aBooleanArray == null) ? "null" : "<array of size " + aBooleanArray.length + ">") +
      ", aCharArray=" + 
        ((aCharArray == null) ? "null" : "<array of size " + aCharArray.length + ">") +
      ", aByteArray=" + 
        ((aByteArray == null) ? "null" : "<array of size " + aByteArray.length + ">") +
      ", aShortArray=" +
        ((aShortArray == null) ? "null" : "<array of size " + aShortArray.length + ">") +
      ", aIntArray=" + 
        ((anIntArray == null) ? "null" : "<array of size " + anIntArray.length + ">") +
      ", aLongArray=" + 
        ((aLongArray == null) ? "null" : "<array of size " + aLongArray.length + ">") +
      ", aFloatArray=" + 
        ((aFloatArray == null) ? "null" : "<array of size " + aFloatArray.length + ">") +
      ", aDoubleArray=" + 
        ((aDoubleArray == null) ? "null" : "<array of size " + aDoubleArray.length + ">") +
      ", aStringArray=" + 
        ((aStringArray == null) ? "null" : "<array of size " + aStringArray.length + ">") +
      ", aObjectArray=" + 
        ((anObjectArray == null) ? "null" : TestHelper.toString(anObjectArray) + ">") +
      ", anArrayOfByteArray=" + 
        ((anArrayOfByteArray == null) ? "null" : "<array of size " + anArrayOfByteArray.length + ">") +
      ", aDayArray=" + 
        ((aDayArray == null) ? "null" : "<array of size " + aDayArray.length + ">") +
      ", anInstrumentArray=" + 
        ((anInstrumentArray == null) ? "null" : "<array of size " + anInstrumentArray.length + ">") +

      ", myValue=" + TestHelper.toString(this.myValue) +
      ", extraObject=" + TestHelper.toString(this.extraObject) +
      ", modVal=" + this.modVal + "]";
  }

  public void myToData(PdxWriter out) {
    Log.getLogWriter().info("In testsVersions/version2/util.VersionedValueHolder.myToData: " + this
        + (PdxPrms.getLogStackTrace() ? ("\n" + TestHelper.getStackTrace()) : ""));
    out.writeString("myVersion", myVersion);

    // original fields
    out.writeObject("myValue", myValue);
    out.writeObject("extraObject", extraObject);
    out.writeObject("modVal", modVal);

    // extra fields for this version
    out.writeObject("aDay", aDay);
    out.writeObject("anInstrument", anInstrument);
    out.writeChar("aChar", aChar);
    out.writeBoolean("aBoolean", aBoolean);
    out.writeByte("aByte", aByte);
    out.writeShort("aShort", aShort);
    out.writeInt("anInt", anInt);
    out.writeLong("aLong", aLong);
    out.writeFloat("aFloat", aFloat);
    out.writeDouble("aDouble", aDouble);
    out.writeDate("aDate", aDate);
    out.writeString("aString", aString);
    out.writeObject("anObject", anObject);
    out.writeObject("aMap", aMap);
    out.writeObject("aCollection", aCollection);
    out.writeBooleanArray("aBooleanArray", aBooleanArray);
    out.writeCharArray("aCharArray", aCharArray);
    out.writeByteArray("aByteArray", aByteArray);
    out.writeShortArray("aShortArray", aShortArray);
    out.writeIntArray("anIntArray", anIntArray);
    out.writeLongArray("aLongArray", aLongArray);
    out.writeFloatArray("aFloatArray", aFloatArray);
    out.writeDoubleArray("aDoubleArray", aDoubleArray);
    out.writeStringArray("aStringArray", aStringArray);
    out.writeObjectArray("anObjectArray", anObjectArray);
    out.writeArrayOfByteArrays("anArrayOfByteArray", anArrayOfByteArray);
    out.writeObjectArray("aDayArray", aDayArray);
    out.writeObjectArray("anInstrumentArray", anInstrumentArray);
    
    out.markIdentityField("myValue");
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.pdx.PdxSerializable#fromData(com.gemstone.gemfire.pdx.PdxReader)
   */
  public void myFromData(PdxReader in) {
    myVersion = in.readString("myVersion");
    Log.getLogWriter().info("In testsVersions/version2/util.VersionedValueHolder.myFromData with myVersion: " + myVersion);

    // original fields
    myValue = in.readObject("myValue");
    extraObject = in.readObject("extraObject");
    modVal = (Integer)in.readObject("modVal");

    // extra fields for this version
    aDay = (Day)in.readObject("aDay");
    anInstrument = (Instrument)in.readObject("anInstrument");
    aChar = in.readChar("aChar");
    aBoolean = in.readBoolean("aBoolean");
    aByte = in.readByte("aByte");
    aShort = in.readShort("aShort");
    anInt = in.readInt("anInt");
    aLong = in.readLong("aLong");
    aFloat = in.readFloat("aFloat");
    aDouble = in.readDouble("aDouble");
    aDate = in.readDate("aDate");
    aString = in.readString("aString");
    anObject = in.readObject("anObject");
    aMap = (Map)in.readObject("aMap");
    aCollection = (Collection)in.readObject("aCollection");
    aBooleanArray = in.readBooleanArray("aBooleanArray");
    aCharArray = in.readCharArray("aCharArray");
    aByteArray = in.readByteArray("aByteArray");
    aShortArray = in.readShortArray("aShortArray");
    anIntArray = in.readIntArray("anIntArray");
    aLongArray = in.readLongArray("aLongArray");
    aFloatArray = in.readFloatArray("aFloatArray");
    aDoubleArray = in.readDoubleArray("aDoubleArray");
    aStringArray = in.readStringArray("aStringArray");
    anObjectArray = in.readObjectArray("anObjectArray");
    anArrayOfByteArray = in.readArrayOfByteArrays("anArrayOfByteArray");
    aDayArray = (Day[]) in.readObjectArray("aDayArray");
    anInstrumentArray = (Instrument[]) in.readObjectArray("anInstrumentArray");

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
    Log.getLogWriter().info("Creating PdxInstance for " + className + " with PdxInstanceFactory in version2...");
    VersionedValueHolder vvh = new VersionedValueHolder(base, rv);
    vvh.myVersion = "version2: " + className;
    
    // now vvh contains all the fields and values we want to write to the factory
    Cache theCache = CacheHelper.getCache();
    if (theCache == null) { // can happen during HA tests
      return null;
    }
    boolean pdxReadSerialized = theCache.getPdxReadSerialized();

    PdxInstanceFactory fac = theCache.createPdxInstanceFactory(className);
    fac.writeString("myVersion", vvh.myVersion);
    fac.writeObject("myValue", vvh.myValue);
    fac.writeObject("extraObject", vvh.extraObject);
    if (pdxReadSerialized) {
      if (vvh.aDay == null) {
        fac.writeObject("aDay", null);
      } else {
        fac.writeObject("aDay", theCache.createPdxEnum("util.VersionedValueHolder$Day", vvh.aDay.toString(), vvh.aDay.ordinal()));
      }
      if (vvh.anInstrument == null) {
        fac.writeObject("anInstrument", null);
      } else {
        fac.writeObject("anInstrument", theCache.createPdxEnum("util.VersionedValueHolder$Instrument", vvh.anInstrument.toString(), vvh.anInstrument.ordinal()));
      }
    } else {
      fac.writeObject("aDay", vvh.aDay);
      fac.writeObject("anInstrument", vvh.anInstrument);
    }
    fac.writeChar("aChar", vvh.aChar);
    fac.writeBoolean("aBoolean", vvh.aBoolean);
    fac.writeByte("aByte", vvh.aByte);
    fac.writeShort("aShort", vvh.aShort);
    fac.writeInt("anInt", vvh.anInt);
    fac.writeLong("aLong", vvh.aLong);
    fac.writeFloat("aFloat", vvh.aFloat);
    fac.writeDouble("aDouble", vvh.aDouble);
    fac.writeDate("aDate", vvh.aDate);
    fac.writeString("aString", vvh.aString);
    fac.writeObject("anObject", vvh.anObject);
    fac.writeObject("aMap", vvh.aMap);
    fac.writeObject("aCollection", vvh.aCollection);
    fac.writeBooleanArray("aBooleanArray", vvh.aBooleanArray);
    fac.writeCharArray("aCharArray", vvh.aCharArray);
    fac.writeByteArray("aByteArray", vvh.aByteArray);
    fac.writeShortArray("aShortArray", vvh.aShortArray);
    fac.writeIntArray("anIntArray", vvh.anIntArray);
    fac.writeLongArray("aLongArray", vvh.aLongArray);
    fac.writeFloatArray("aFloatArray", vvh.aFloatArray);
    fac.writeDoubleArray("aDoubleArray", vvh.aDoubleArray);
    fac.writeStringArray("aStringArray", vvh.aStringArray);
    fac.writeObjectArray("anObjectArray", vvh.anObjectArray);
    fac.writeArrayOfByteArrays("anArrayOfByteArray", vvh.anArrayOfByteArray);
    fac.writeObjectArray("aDayArray", vvh.aDayArray);
    fac.writeObjectArray("anInstrumentArray", vvh.anInstrumentArray);
    
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
      Day aDay = (Day)(dayList.get((int) (base % dayList.size())));
      return aDay;
  }
  
  /** Return the appropriate enum Instrument value given the base
   * 
   * @param base Index to base the Instrument calculation on.
   * @return A value of Instrument.
   */
  public static Instrument getInstrumentForBase(long base) {
    Instrument anInstrument = (Instrument)(instrumentList.get((int)base % instrumentList.size()));
    return anInstrument;
  }
}
