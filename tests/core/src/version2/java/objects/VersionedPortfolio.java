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
package objects;

import com.gemstone.gemfire.pdx.PdxReader;
import com.gemstone.gemfire.pdx.PdxWriter;

import hydra.Log;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import util.TestException;
import util.TestHelper;

/**
 * @author lynn
 *
 */
public class VersionedPortfolio extends Portfolio {
 
  public enum Day {
    Sunday, Monday, Tuesday, Wednesday, Thursday, Friday, Saturday
  }

  // Add extra fields
  public int pdxID;
  public String pdxType;
  public String pdxStatus;
  public Day aDay;

  static transient List dayList;
  
  static {
    Log.getLogWriter().info("In static initializer for testsVersions/version2/objects.VersionedPortfolio");
    dayList = new ArrayList();
    dayList.addAll(EnumSet.allOf(Day.class));
  }
  
  public VersionedPortfolio() {
    super();
    myVersion = "testsVersions/version2/objects.VersionedPortfolio";
    pdxID = 0;
    pdxStatus = "";
    pdxType = "";
    aDay = null;
  }
  
  /** Create a Map of the fields of this instance, plus other information
   *  to be used to write the information to the blackboard.
   */
  public Map createPdxHelperMap() {
    Map fieldMap = new HashMap();
    fieldMap.put("myVersion", myVersion);
    fieldMap.put("className", this.getClass().getName());
    fieldMap.put("ID", ID);
    fieldMap.put("type", type);
    fieldMap.put("status", status);
    fieldMap.put("pdxID", pdxID);
    fieldMap.put("pdxType", pdxType);
    fieldMap.put("pdxStatus", pdxStatus);
    if (aDay == null) {
      fieldMap.put("aDay", null);
    } else {
      fieldMap.put("aDay", aDay.toString());
    }
    Log.getLogWriter().info("created map in version2: " + fieldMap);
    return fieldMap;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return this.getClass().getName() + " (toString from version2) [" +
        "myVersion=" + this.myVersion + 
        ", ID=" + this.ID + 
        ", type=" + this.type +
        ", status=" + this.status + 
        ", payload=" + this.payload + 
        ", pdxID=" + this.pdxID + 
        ", pdxType=" + this.pdxType + 
        ", pdxStatus=" + this.pdxStatus + 
        ", aDay=" + this.aDay + "]";
  }
  
  public void init( int i ) {
    super.init(i);
    this.pdxID = i;
    pdxStatus = i % 2 == 0 ? "active" : "inactive";
    pdxType = "type" + (i % 3);
    if ((i % 10) == 0) {
      aDay = null;
    } else {
      aDay = getDayForBase(i);
    }
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.pdx.PdxSerializable#toData(com.gemstone.gemfire.pdx.PdxWriter)
   */
  public void myToData(PdxWriter writer) {
    Log.getLogWriter().info("In testsVersions/version2/objects.VersionedPortfolio.myToData: " + this);
    writer.writeString("myVersion", myVersion);
    writer.writeInt("ID", ID);
    writer.writeString("type", type);
    writer.writeString("status", status);
    writer.writeByteArray("payload", payload);
    writer.writeInt("pdxID", pdxID);
    writer.writeString("pdxType", pdxType);
    writer.writeString("pdxStatus", pdxStatus);
    writer.writeObject("aDay", aDay);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.pdx.PdxSerializable#fromData(com.gemstone.gemfire.pdx.PdxReader)
   */
  public void myFromData(PdxReader reader) {
    myVersion = reader.readString("myVersion");
    Log.getLogWriter().info("In testsVersions/version2/objects.VersionedPortfolio.myFromData with myVersion: " + myVersion);
    ID = reader.readInt("ID");
    type = reader.readString("type");
    status = reader.readString("status");
    payload = reader.readByteArray("payload");
    pdxID = reader.readInt("pdxID");
    pdxType = reader.readString("pdxType");
    pdxStatus = reader.readString("pdxStatus");
    aDay = (Day)(reader.readObject("aDay"));
    Log.getLogWriter().info("After reading fields in fromData: " + this);
    
    // consistency check
    if (myVersion.indexOf("version1") >= 0) { // this object was created with version1 and couldn't fill out fields defined in this version so we expect them to be default values
      if (pdxID != 0) { 
        throw new TestException("Expected pdxID to be 0, but it is " + this);
      }
      if (pdxType != null) {
        throw new TestException("Expected pdxType to be null, but it is " + this);
      }
      if (pdxStatus != null) {
        throw new TestException("Expected pdxStatus to be null, but it is " + this);
      }
      if (aDay != null) {
        throw new TestException("Expected aDay to be null, but it is " + this);
      }
    } else { // expect fields to have values
      // pdxID can be 0; this value is allowed and used in the tests
      if (pdxID < 0) { 
        throw new TestException("Expected pdxID to have a non-zero value, but it is " + this);
      }
      String expected = "type" + (ID % 3);
      if (pdxType == null) {
        throw new TestException("Expected pdxType to be " + expected + " but it is " + this);
      }
      if (!pdxType.equals(expected)) {
        throw new TestException("Expected pdxType to be " + expected + ", but it is " + this);
      }
      expected = ID % 2 == 0 ? "active" : "inactive";
      if (pdxStatus == null) {
        throw new TestException("Expected pdxType to be " + expected + " but it is " + this);
      }

      if (!pdxStatus.equals(expected)) {
        throw new TestException("Expected pdxStatus to be " + expected + " but it is " + this);
      }
      if ((ID % 10) == 0) { // expect aDay to be null
        if (aDay != null) {
          throw new TestException("Expected aDay to be null, but it is " + this);
        }
      } else { // expect aDay to not be null
        if (aDay == null) {
          throw new TestException("Expected aDay to be non-null, but it is " + this);
        }
        Day expectedDay = getDayForBase(ID);
        if (!aDay.toString().equals(expectedDay.toString())) {
          throw new TestException("Expected aDay to be expectedDay, but it is " + this);
        } 
      }
    }
  }
  
  /** Restore the fields of this instance using the values of the Map, created
   *  by createPdxHelperMap()
   */
  public void restoreFromPdxHelperMap(Map aMap) {
    super.restoreFromPdxHelperMap(aMap);
    Log.getLogWriter().info("now restoring pdx fields from map");
    this.pdxID = (Integer)aMap.get("pdxID");
    this.pdxType = (String)aMap.get("pdxType");
    this.pdxStatus = (String)aMap.get("pdxStatus");
    Object dayField = aMap.get("aDay");
    if (dayField == null) {
      this.aDay = null;
    } else {
      this.aDay = Day.valueOf((String)dayField);
    }
    Log.getLogWriter().info("returning instance from map in testsVersions/version2/objects.VersionedPortfolio: " + this);
  }

  /** Return the appropriate enum Day value given the base
   * 
   * @param base Index to base the Day calculation on.
   * @return A value of Day.
   */
  public static Day getDayForBase(int base) {
      Day aDay = (Day)(dayList.get((base % dayList.size())));
      return aDay;
  }
}
