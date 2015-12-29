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
import com.gemstone.gemfire.internal.util.Sizeof;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

public class Risk implements DataSerializable {

  private int id; // unique
  private int positionId;
  private int risk;
  public static String REGION_TABLE_NAME = "RISKS";
  public static String REGION_TABLE_SHORT_NAME = "r";

  public void init(int i) {
    this.id = i;
    this.positionId = i;
    int numRiskValues = SectorPrms.getNumRiskValues();
    risk = id % numRiskValues;
  }

  public static String getTableName() {
    return Risk.REGION_TABLE_NAME;
  }

  public static String getTableAndShortName() {
    return Risk.REGION_TABLE_NAME + " " + Risk.REGION_TABLE_SHORT_NAME;
  }

  public static String getTableShortName() {
    return Risk.REGION_TABLE_SHORT_NAME;
  }

  public static String commaSeparatedStringFor(Vector fields) {
    StringBuffer sb = new StringBuffer();
    for (Iterator i = fields.iterator(); i.hasNext();) {
      String field = (String) i.next();
      if (field.equals("*")) {
        return field;
      }
      else {
        sb.append(Risk.REGION_TABLE_SHORT_NAME + "." + field);
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
  public static List getFields(List fieldList) {
    List fields = new ArrayList();
    if (fieldList.contains ("*")) {
      fields.add("id");
      fields.add("position_id");
      fields.add("risk");
    }
    else if (fieldList.contains(SectorPrms.NONE)) {
      //we don't want any fields
    }
    else {
      fields.addAll(fieldList);
    }
    return fields;
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  //----------------------------------------------------------------------------
  // QueryFactory: printing
  //----------------------------------------------------------------------------

  public String toString() {
    return "Risk #" + this.id;
  }

  public int sizeof(Object o) {
    if (o instanceof Risk) {
      Risk obj = (Risk) o;
      return Sizeof.sizeof(obj.id) + Sizeof.sizeof(obj.positionId)
          + Sizeof.sizeof(obj.risk);
    }
    else {
      return Sizeof.sizeof(o);
    }
  }

  //----------------------------------------------------------------------------
  // DataSerializable
  //----------------------------------------------------------------------------

  public void toData(DataOutput out) throws IOException {
    out.writeInt(this.id);
    out.writeInt(this.positionId);
    out.writeInt(this.risk);
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.id = in.readInt();
    this.positionId = in.readInt();
    this.risk = in.readInt();
  }
}
