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
import com.gemstone.gemfire.pdx.PdxSerializable;
import com.gemstone.gemfire.pdx.PdxWriter;

import hydra.Log;

import java.io.IOException;
import java.util.Map;

/**
 * @author lynn
 *
 */
public class PdxVersionedPortfolio extends VersionedPortfolio implements
    PdxSerializable {
  
  public PdxVersionedPortfolio() {
    myVersion = "testsVersions/version2/objects.PdxVersionedPortfolio";
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.pdx.PdxSerializable#toData(com.gemstone.gemfire.pdx.PdxWriter)
   */
  public void toData(PdxWriter writer) {
    Log.getLogWriter().info("In testsVersions/version2/objects.PdxVersionedPortfolio.toData, " +
       "calling myToData...");
    myToData(writer);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.pdx.PdxSerializable#fromData(com.gemstone.gemfire.pdx.PdxReader)
   */
  public void fromData(PdxReader reader) {
    Log.getLogWriter().info("In testsVersions/version2/objects.PdxVersionedPortfolio.fromData, " +
       "calling myFromData...");
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
    fieldMap.put("pdxID", pdxID);
    fieldMap.put("pdxType", pdxType);
    fieldMap.put("pdxStatus", pdxStatus);
    if (aDay == null) {
      fieldMap.put("aDay", null);
    } else {
      fieldMap.put("aDay", aDay.toString());
    }
    Log.getLogWriter().info("created map in version2/testsVersions/objects.PdxVersionedPortfolio: " + fieldMap);
    return fieldMap;
  }

  /** Restore the fields of this instance using the values of the Map, created
   *  by createPdxHelperMap()
   */
  @Override
  public void restoreFromPdxHelperMap(Map aMap) {
    super.restoreFromPdxHelperMap(aMap);
    Log.getLogWriter().info("restoring from map into " + this.getClass().getName() + ": " + aMap);
    this.pdxID = (Integer)aMap.get("pdxID");
    this.pdxType = (String)aMap.get("pdxType");
    this.pdxStatus = (String)aMap.get("pdxStatus");
    Object dayField = aMap.get("aDay");
    if (dayField == null) {
       this.aDay = null;
    } else {
      this.aDay = Day.valueOf((String)dayField);
    }
    Log.getLogWriter().info("returning instance from map in version2/testsVersions/objects.PdxVersionedPortfolio: " + this);
  }
}
