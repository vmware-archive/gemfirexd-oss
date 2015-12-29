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
import com.gemstone.gemfire.pdx.PdxSerializable;
import com.gemstone.gemfire.pdx.PdxWriter;

import hydra.Log;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import pdx.PdxPrms;

/**
 * @author lynn
 *
 */
public class PdxVersionedNewPortfolio extends VersionedNewPortfolio implements PdxSerializable {
  
  public PdxVersionedNewPortfolio() {
    myVersion = "testsVersions/version2/parReg.query.PdxVersionedNewPortfolio";
  }
  
  public PdxVersionedNewPortfolio(String name, int id) {
    super(name, id);
    myVersion = "testsVersions/version2/parReg.query.PdxVersionedNewPortfolio";

    
    this.pdxName = name;
    this.pdxId = id;
    
    this.pdxStatus = id % 2 == 0 ? "active" : "inactive";
    this.pdxType = "type" + (id % NUM_OF_TYPES);
    
    setPdxPositions();
  }
  
  private void setPdxPositions() {
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
   * @see com.gemstone.gemfire.pdx.PdxSerializable#toData(com.gemstone.gemfire.pdx.PdxWriter)
   */
  public void toData(PdxWriter writer) {
    if (PdxPrms.getLogToAndFromData()) {
      Log.getLogWriter().info("In testsVersions/version2/parReg.query.PdxVersionedNewPortfolio.toData, calling myToData...");
    }
    myToData(writer);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.pdx.PdxSerializable#fromData(com.gemstone.gemfire.pdx.PdxReader)
   */
  public void fromData(PdxReader reader) {
    myVersion = reader.readString("myVersion");
    if (PdxPrms.getLogToAndFromData()) {
      Log.getLogWriter().info("In testsVersions/version2/parReg.query.PdxVersionedNewPortfolio.fromData, calling myFromData...");
    }
    myFromData(reader);
  }
  
  /** Create a map of fields and field values to use to write to the blackboard
   *  since PdxSerialiables cannot be put on the blackboard since the MasterController
   *  does not have pdx objects on its classpath. For PdxSerializables
   *  we put this Map on the blackboard instead.
   */
  @Override
  public Map createPdxHelperMap() {
    Map fieldMap = super.createPdxHelperMap();
    fieldMap.put("className", this.getClass().getName());
    fieldMap.put("pdxId", pdxId);
    fieldMap.put("pdxName", pdxName);
    fieldMap.put("pdxStatus", pdxStatus);
    fieldMap.put("pdxType", pdxType);
    fieldMap.put("pdxPositions", pdxPositions);
    fieldMap.put("pdxUndefinedTestField", pdxUndefinedTestField);
//    Log.getLogWriter().info("created map in version2/testsVersions/parReg.query.NewPortfolio: " + fieldMap);
    return fieldMap;
  }

  /** Restore the fields of this instance using the values of the Map, created
   *  by createPdxHelperMap()
   */
  @Override
  public void restoreFromPdxHelperMap(Map aMap) {
    super.restoreFromPdxHelperMap(aMap);
 //   Log.getLogWriter().info("restoring from map into " + this.getClass().getName() + ", aMap: " + aMap);
    // if map created with version2, then we have pdx fields in map
    if (((String)(aMap.get("myVersion"))).indexOf("version2") >= 0) {
      this.pdxId = (Integer)aMap.get("pdxId");
      this.pdxName = (String)aMap.get("pdxName");
      this.pdxStatus = (String)aMap.get("pdxStatus");
      this.pdxType = (String)aMap.get("pdxType");
      this.pdxPositions = (Map)aMap.get("pdxPositions");
      this.pdxUndefinedTestField = (String)aMap.get("pdxUndefinedTestField");
    }
 //   Log.getLogWriter().info("returning instance from map in tests/parReg.query.NewPortfolio: " + this);
  }

}
