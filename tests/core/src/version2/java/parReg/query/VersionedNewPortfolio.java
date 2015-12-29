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
package parReg.query;

import com.gemstone.gemfire.pdx.PdxReader;
import com.gemstone.gemfire.pdx.PdxWriter;

import hydra.Log;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import pdx.PdxPrms;

/**
 * @author lynn
 *
 */
public class VersionedNewPortfolio extends NewPortfolio {

  public enum Day {
    Sunday, Monday, Tuesday, Wednesday, Thursday, Friday, Saturday
  }
  
  protected int pdxId = 0;           
  protected String pdxName = "name";        
  protected String pdxStatus = "status";
  protected String pdxType = "type";
  protected Map pdxPositions = new HashMap();
  public String pdxUndefinedTestField = null;
  public Day aDay;
  
  static transient List dayList;
  
  static {
    Log.getLogWriter().info("In static initializer for testsVersions/version2/objects.VersionedPortfolio");
    dayList = new ArrayList();
    dayList.addAll(EnumSet.range(Day.Monday, Day.Friday));
  }
  
  public VersionedNewPortfolio() {
    myVersion = "testsVersions/version2/parReg.query.VersionedNewPortfolio";
  }
  public VersionedNewPortfolio(String name, int id) {
    super(name, id);

    
    this.pdxName = name;
    this.pdxId = id;
    
    this.pdxStatus = id % 2 == 0 ? "active" : "inactive";
    this.pdxType = "type" + (id % NUM_OF_TYPES);
    this.aDay = getDayForBase(id);
    
    setPositions();
  }
  
  private void setPositions() {
    int numOfPositions = rng.nextInt(MAX_NUM_OF_POSITIONS);
    if (numOfPositions == 0) 
      numOfPositions++;
     
    int secId =  rng.nextInt(NUM_OF_SECURITIES);
    
    for (int i=0; i < numOfPositions; i++) {
      Properties props = getProps();
      
//    secId needs to be UNIQUE in one portfolio, keep track MAX_NUM_OF_POSITIONS and NUM_OF_SECURITIES
      secId += i * 7;                    
      if (secId > NUM_OF_SECURITIES)
        secId -= NUM_OF_SECURITIES;
      props.setProperty("secId", new Integer(secId).toString());
      
      Position pos = new Position();
      pos.init(props);
      this.positions.put(pos.getSecId(), pos);
    }
  }

  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return this.getClass().getName() + " [pdxId=" + this.pdxId + ", pdxName="
        + this.pdxName + ", pdxStatus=" + this.pdxStatus + ", pdxType="
        + this.pdxType + ", pdxPositions=" + this.pdxPositions
        + ", pdxUndefinedTestField=" + this.pdxUndefinedTestField
        + ", aDay=" + this.aDay
        + ", myVersion=" + this.myVersion + ", id=" + this.id + ", name="
        + this.name + ", status=" + this.status + ", type=" + this.type
        + ", positions=" + this.positions + ", undefinedTestField="
        + this.undefinedTestField + "]";
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.pdx.PdxSerializable#toData(com.gemstone.gemfire.pdx.PdxWriter)
   */
  public void myToData(PdxWriter writer) {
    if (PdxPrms.getLogToAndFromData()) {
      Log.getLogWriter().info("In testsVersions/version2/parReg.query.VersionedNewPortfolio.myToData: " + this);
    }
    writer.writeString("myVersion", myVersion);
    writer.writeInt("id", id);
    writer.writeString("name", name);
    writer.writeString("status", status);
    writer.writeString("type", type);
    writer.writeObject("positions", positions);
    writer.writeString("undefinedTestField", undefinedTestField);
    writer.writeInt("pdxId", pdxId);
    writer.writeString("pdxName", pdxName);
    writer.writeString("pdxStatus", pdxStatus);
    writer.writeString("pdxType", pdxType);
    writer.writeObject("pdxPositions", pdxPositions);
    writer.writeString("pdxUndefinedTestField", pdxUndefinedTestField);
    writer.writeObject("aDay", aDay);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.pdx.PdxSerializable#fromData(com.gemstone.gemfire.pdx.PdxReader)
   */
  public void myFromData(PdxReader reader) {
    myVersion = reader.readString("myVersion");
    if (PdxPrms.getLogToAndFromData()) {
      Log.getLogWriter().info("In testsVersions/version2/parReg.query.VersionedNewPortfolio.myFromData with myVersion: " + myVersion);
    }
    id = reader.readInt("id");
    name = reader.readString("name");
    status = reader.readString("status");
    type = reader.readString("type");
    positions = (Map)reader.readObject("positions");
    undefinedTestField = reader.readString("undefinedTestField");
    pdxId = reader.readInt("pdxId");
    pdxName = reader.readString("pdxName");
    pdxStatus = reader.readString("pdxStatus");
    pdxType = reader.readString("pdxType");
    pdxPositions = (Map)reader.readObject("pdxPositions");
    pdxUndefinedTestField = reader.readString("pdxUndefinedTestField");
    aDay = (Day)(reader.readObject("aDay"));
    if (PdxPrms.getLogToAndFromData()) {
      Log.getLogWriter().info("After reading fields in testsVersion/version2/parReg.query.VersionedNewPortfolio.fromData: " + this);
    }
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
