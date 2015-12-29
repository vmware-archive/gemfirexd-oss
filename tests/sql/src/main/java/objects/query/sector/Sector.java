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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import objects.query.QueryFactory;
import objects.query.QueryPrms;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.util.Sizeof;

public class Sector implements DataSerializable {

  private int id; // unique
  private String name;
  private double marketCap;
  private Map instruments;
  
  public static String REGION_TABLE_NAME = "SECTORS";
  public static String REGION_TABLE_SHORT_NAME = "s";

  public static String getTableName() {
    return Sector.REGION_TABLE_NAME;
  }

  public static String getTableAndShortName() {
    return Sector.REGION_TABLE_NAME + " " + Sector.REGION_TABLE_SHORT_NAME;
  }

  public static String getTableShortName() {
    return Sector.REGION_TABLE_SHORT_NAME;
  }

  //--------------------------------------------------------------------------
  //Accessors
  //--------------------------------------------------------------------------
  public int getId() {
    return this.id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public double getMarketCap() {
    return marketCap;
  }

  public void setMarketCap(double marketCap) {
    this.marketCap = marketCap;
  }
  
  public static String getSectorName(int sid) {
    return "IndustrySector" + sid;
  }
  
  public Map getInstruments() {
    return instruments;
  }

  public void setInstruments(Map instruments) {
    this.instruments = instruments;
  }
  
  /**
   * Initialize the i'th sector.
   */
  public void init(int i) {
    this.id = i;
    this.name = getSectorName(i);
    this.marketCap = i % SectorPrms.getNumMarketCapValues();
  }

  public static QueryFactory getQueryFactory(int api) {
    switch (api) {
      case QueryPrms.GFXD:
        return new GFXDSectorQueryFactory();
      case QueryPrms.MYSQL:
        return new MySQLSectorQueryFactory();
      case QueryPrms.MYSQLC:
        return new MySQLCSectorQueryFactory();
      case QueryPrms.RTE:
        return new SQLSectorQueryFactory();
      default:
        String s = "No factory for: " + QueryPrms.getAPIString(api);
        throw new UnsupportedOperationException(s);
    }
  }

  public static String commaSeparatedStringFor(Vector fields) {
    StringBuffer sb = new StringBuffer();
    for (Iterator i = fields.iterator(); i.hasNext();) {
      String field = (String) i.next();
      if (field.equals("*")) {
        return field;
      }
      else {
        sb.append(Sector.REGION_TABLE_SHORT_NAME + "." + field);
      }
      if (i.hasNext()) {
        sb.append(",");
      }
    }
    return sb.toString();
  }
  
  /**
   * returns a list of column names that make up the Sector table
   * @return
   */
  public static List getFields(List fieldList) {
    List fields = new ArrayList();
    if (fieldList.contains ("*")) {
      fields.add("id");
      fields.add("name");
      fields.add("market_cap");
    }
    else if (fieldList.contains(SectorPrms.NONE)) {
      //we don't want any fields
    }
    else {
      fields.addAll(fieldList);
    }
    return fields;
  }

  //----------------------------------------------------------------------------
  // QueryFactory: printing
  //----------------------------------------------------------------------------

  public String toString() {
    return "Sector #" + this.id + "=" + id + ": name=" + this.name + ": marketCap=" + marketCap;
  }

  public int sizeof(Object o) {
    if (o instanceof Sector) {
      Sector obj = (Sector) o;
      return Sizeof.sizeof(obj.id) + Sizeof.sizeof(obj.name)
          + Sizeof.sizeof(obj.marketCap);
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
    DataSerializer.writeString(this.name, out);
    out.writeDouble(this.marketCap);
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.id = in.readInt();
    this.name = DataSerializer.readString(in);
    this.marketCap = in.readDouble();
  }



}
