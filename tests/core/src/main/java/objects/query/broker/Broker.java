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
package objects.query.broker;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.util.Sizeof;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Vector;

import objects.query.QueryPrms;
import objects.query.QueryFactory;

/**
 * Represents a broker referenced by {@link Ticket}.
 */
public class Broker implements Serializable {

  public static String REGION_TABLE_NAME = "brokers";
  public static String REGION_TABLE_SHORT_NAME = "b";

  protected static String getName(int i) {
    return "Jane Doe " + i % BrokerPrms.getNumBrokerNames();
  }

  public static String getTableName() {
    return Broker.REGION_TABLE_NAME;
  }
  
  public static String getTableShortName() {
    return Broker.REGION_TABLE_SHORT_NAME;
  }
  
  public static String getTableAndShortName() {
    return Broker.REGION_TABLE_NAME + " " + Broker.REGION_TABLE_SHORT_NAME;
  }

  public static QueryFactory getQueryFactory(int api) {
    switch (api)
    {
      case QueryPrms.OQL:
        return new OQLBrokerQueryFactory();
      case QueryPrms.GFXD:
        return new GFXDBrokerQueryFactory();
      default:
        throw new UnsupportedOperationException("No factory for: " + api);
    }
  }

  public static String commaSeparatedStringFor(Vector fields) {
    StringBuffer sb = new StringBuffer();
    for (Iterator i = fields.iterator(); i.hasNext();) {
      String field = (String)i.next();
      if (field.equals("*")) {
        return field;
      } else {
        sb.append(Broker.REGION_TABLE_SHORT_NAME + "." + field);
      }
      if (i.hasNext()) {
        sb.append(",");
      }
    }
    return sb.toString();
  }

  //----------------------------------------------------------------------------
  // Fields
  //----------------------------------------------------------------------------  
  private int id; // unique
  private String name;

  //----------------------------------------------------------------------------
  // Constructors
  //----------------------------------------------------------------------------

  public Broker() {
  }

  /**
   * Initialize the i'th broker.
   */
  public void init(int i) {
    this.id = i;
    this.name = getName(i);
  }

  //----------------------------------------------------------------------------
  // Accessors
  //----------------------------------------------------------------------------

  public int getId() {
    return this.id;
  }

  public String getName() {
    return this.name;
  }

  //----------------------------------------------------------------------------
  // QueryFactory: printing
  //----------------------------------------------------------------------------

  public String toString() {
    return "Broker #" + this.id + "=" + this.name;
  }

  public int sizeof(Object o) {
    if (o instanceof Broker) {
      Broker obj = (Broker) o;
      return Sizeof.sizeof(obj.id)
           + Sizeof.sizeof(obj.name);
    } else {
      return Sizeof.sizeof(o);
    }
  }

  //----------------------------------------------------------------------------
  // DataSerializable
  //----------------------------------------------------------------------------

  public void toData(DataOutput out) throws IOException {
    out.writeInt(this.id);
    DataSerializer.writeString(this.name, out);
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.id = in.readInt();
    this.name = DataSerializer.readString(in);
  }
}
