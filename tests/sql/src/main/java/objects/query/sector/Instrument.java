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
package objects.query.sector;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.util.Sizeof;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

public class Instrument implements DataSerializable {

  private String id; // unique
  private int sectorId;
  private Map positions;
  public static String REGION_TABLE_NAME = "INSTRUMENTS";
  public static String REGION_TABLE_SHORT_NAME = "i";
  
  /**
   * Initialize the i'th instrument.
   */
  public void init(int i) {    
    int numInstrumentsPerSector = SectorPrms.getNumInstrumentsPerSector();
    String typeName = Instrument.getInstrument(i);
    this.id = typeName;
    this.sectorId = i/numInstrumentsPerSector;
  }
  
  public static String getTableName() {
    return Instrument.REGION_TABLE_NAME;
  }

  public static String getTableAndShortName() {
    return Instrument.REGION_TABLE_NAME + " "
        + Instrument.REGION_TABLE_SHORT_NAME;
  }

  public static String getTableShortName() {
    return Instrument.REGION_TABLE_SHORT_NAME;
  }

  public static String getInstrument(int id) {
    return "Instrument" + (id % SectorPrms.getNumInstruments());
  }

  @SuppressWarnings("unchecked")
  public static String commaSeparatedStringFor(Vector fields) {
    //Vector copy = (Vector)fields.clone();
    StringBuffer sb = new StringBuffer();
    for (Iterator i = fields.iterator(); i.hasNext();) {
      String field = (String) i.next();
      if (field.equals("*")) {
        return field;
      }
      else {
        sb.append(Instrument.REGION_TABLE_SHORT_NAME + "." + field);
      }
      if (i.hasNext()) {
        sb.append(",");
      }
    }
    return sb.toString();
  }
  
  /**
   * returns a list of column names that make up the Position table
   * @return
   */
  @SuppressWarnings("unchecked")
  public static List getFields(List fieldList) {
    List fields = new ArrayList();
    if (fieldList.contains("*")) {
      fields.add("id");
      fields.add("sector_id");
    }
    else if (fieldList.contains(SectorPrms.NONE)) {
      //we don't want any fields
    }
    else {
     fields.addAll(fieldList);
    }
    return fields;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public int getSectorId() {
    return sectorId;
  }

  public void setSectorId(int sectorId) {
    this.sectorId = sectorId;
  }

  public Map getPositions() {
    return positions;
  }

  public void setPositions(Map positions) {
    this.positions = positions;
  }

  //----------------------------------------------------------------------------
  // QueryFactory: printing
  //----------------------------------------------------------------------------

  public String toString() {
    return "Instrument #" + this.id;
  }

  public int sizeof(Object o) {
    if (o instanceof Instrument) {
      Instrument obj = (Instrument) o;
      return Sizeof.sizeof(obj.id) + Sizeof.sizeof(obj.sectorId);
    }
    else {
      return Sizeof.sizeof(o);
    }
  }

  //----------------------------------------------------------------------------
  // DataSerializable
  //----------------------------------------------------------------------------

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(this.id, out);
    out.writeInt(this.sectorId);
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.id = DataSerializer.readString(in);
    this.sectorId = in.readInt();
  }


  
}
