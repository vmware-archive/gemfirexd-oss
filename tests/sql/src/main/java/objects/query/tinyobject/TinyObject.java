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
package objects.query.tinyobject;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.Delta;
import com.gemstone.gemfire.internal.util.Sizeof;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import objects.ConfigurableObject;
import objects.query.QueryFactory;
import objects.query.QueryPrms;

public class TinyObject implements DataSerializable, ConfigurableObject, Delta {
  private int id; // unique

  private int intField;

  private boolean hasDelta = false;

  public static String REGION_TABLE_NAME = "TINY_OBJECT";
  public static String REGION_TABLE_SHORT_NAME = "lo";

  public static String getTableName() {
    return TinyObject.REGION_TABLE_NAME;
  }

  public static String getTableAndShortName() {
    return TinyObject.REGION_TABLE_NAME + " "
        + TinyObject.REGION_TABLE_SHORT_NAME;
  }

  public static String getTableShortName() {
    return TinyObject.REGION_TABLE_SHORT_NAME;
  }
  public TinyObject() {
    
  }
  
  /**
   * Initialize the i'th tinyobject.
   */
  public void init(int i) {

    this.id = i;

    this.intField = i;
  }
  
  public static QueryFactory getQueryFactory(int api) {
    switch (api) {
      case QueryPrms.GFXD:
        return new GFXDTinyObjectQueryFactory();
      default:
        throw new UnsupportedOperationException("No factory for: " + api);
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
        sb.append(TinyObject.REGION_TABLE_SHORT_NAME + "." + field);
      }
      if (i.hasNext()) {
        sb.append(",");
      }
    }
    return sb.toString();
  }

  /**
   * returns a list of column names that make up the TinyObject table
   * @return
   */
  public static List getFields(List fieldList) {
    List fields = new ArrayList();
    if (fieldList.contains("*") == true) {
      fields.add("id");
      
      fields.add("intField");
    }
    else if (fieldList.contains(TinyObjectPrms.NONE)) {
      //we don't want any fields
    }
    else {
      fields = fieldList;
    }
   
    return fields;
  }

  //----------------------------------------------------------------------------
  // QueryFactory: printing
  //----------------------------------------------------------------------------

  public String toString() {
    return "TinyObject #" + this.id + "=" + id + ": intField=" + this.intField;
  }

  public int sizeof(Object o) {
    if (o instanceof TinyObject) {
      TinyObject obj = (TinyObject) o;
      return Sizeof.sizeof(obj.id) + Sizeof.sizeof(obj.intField);
    }
    else {
      return Sizeof.sizeof(o);
    }
  }

  //----------------------------------------------------------------------------
  // Delta
  //----------------------------------------------------------------------------
  
  public synchronized void update(int i) {
    setIntField(i);
    this.hasDelta = true;
  }

  public boolean hasDelta() {
    return this.hasDelta;
  }

  public void toDelta(DataOutput out)
  throws IOException {
    out.writeInt(this.intField);
  }
  
  public void fromDelta(DataInput in)
  throws IOException {
    this.intField = in.readInt();
  }
  
  //----------------------------------------------------------------------------
  // DataSerializable
  //----------------------------------------------------------------------------

  public void toData(DataOutput out) throws IOException {
    out.writeInt(this.id);
    out.writeInt(this.intField);
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.id = in.readInt();
    this.intField = in.readInt();
  }

  //--------------------------------------------------------------------------
  //Accessors
  //--------------------------------------------------------------------------
  public Integer getId() {
    return new Integer(this.id);
  }

  public int getIntField() {
    return intField;
  }

  public void setIntField(int intField) {
    this.intField = intField;
  }

  public int getIndex() {
    return id;
  }

  public void validate(int index) {
  }
}
