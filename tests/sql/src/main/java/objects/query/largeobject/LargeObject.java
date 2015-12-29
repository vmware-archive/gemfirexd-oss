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
package objects.query.largeobject;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
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

public class LargeObject implements DataSerializable, ConfigurableObject, Delta {
  private int id; // unique

  private String stringField1;
  private String stringField2;
  private String stringField3;
  private String stringField4;
  private String stringField5;
  private String stringField6;
  private String stringField7;
  private String stringField8;
  private String stringField9;
  private String stringField10;
  private String stringField11;
  private String stringField12;
  private String stringField13;
  private String stringField14;
  private String stringField15;
  private String stringField16;
  private String stringField17;
  private String stringField18;
  private String stringField19;
  private String stringField20;

  private int intField1;
  private int intField2;
  private int intField3;
  private int intField4;
  private int intField5;
  private int intField6;
  private int intField7;
  private int intField8;
  private int intField9;
  private int intField10;
  private int intField11;
  private int intField12;
  private int intField13;
  private int intField14;
  private int intField15;
  private int intField16;
  private int intField17;
  private int intField18;
  private int intField19;
  private int intField20;

  private boolean hasDelta = false;

  public static String REGION_TABLE_NAME = "LARGE_OBJECT";
  public static String REGION_TABLE_SHORT_NAME = "lo";

  public static String getTableName() {
    return LargeObject.REGION_TABLE_NAME;
  }

  public static String getTableAndShortName() {
    return LargeObject.REGION_TABLE_NAME + " "
        + LargeObject.REGION_TABLE_SHORT_NAME;
  }

  public static String getTableShortName() {
    return LargeObject.REGION_TABLE_SHORT_NAME;
  }
  public LargeObject() {
    
  }
  
  /**
   * Initialize the i'th largeobject.
   */
  public void init(int i) {

    this.id = i;

    this.stringField1 = "stringField1 " + i;
    this.stringField2 = "stringField2 " + i;
    this.stringField3 = "stringField3 " + i;
    this.stringField4 = "stringField4 " + i;
    this.stringField5 = "stringField5 " + i;
    this.stringField6 = "stringField6 " + i;
    this.stringField7 = "stringField7 " + i;
    this.stringField8 = "stringField8 " + i;
    this.stringField9 = "stringField9 " + i;
    this.stringField10 = "stringField10 " + i;
    this.stringField11 = "stringField11 " + i;
    this.stringField12 = "stringField12 " + i;
    this.stringField13 = "stringField13 " + i;
    this.stringField14 = "stringField14 " + i;
    this.stringField15 = "stringField15 " + i;
    this.stringField16 = "stringField16 " + i;
    this.stringField17 = "stringField17 " + i;
    this.stringField18 = "stringField18 " + i;
    this.stringField19 = "stringField19 " + i;
    this.stringField20 = "stringField20 " + i;

    this.intField1 = i;
    this.intField2 = i;
    this.intField3 = i;
    this.intField4 = i;
    this.intField5 = i;
    this.intField6 = i;
    this.intField7 = i;
    this.intField8 = i;
    this.intField9 = i;
    this.intField10 = i;
    this.intField11 = i;
    this.intField12 = i;
    this.intField13 = i;
    this.intField14 = i;
    this.intField15 = i;
    this.intField16 = i;
    this.intField17 = i;
    this.intField18 = i;
    this.intField19 = i;
    this.intField20 = i;
  }
  
  public static QueryFactory getQueryFactory(int api) {
    switch (api) {
      case QueryPrms.GFXD:
        return new GFXDLargeObjectQueryFactory();
      case QueryPrms.OQL:
        return new GFELargeObjectQueryFactory();
      case QueryPrms.MYSQL:
        return new MySQLLargeObjectQueryFactory();
      case QueryPrms.MYSQLC:
        return new MySQLCLargeObjectQueryFactory();
      case QueryPrms.RTE:
        return new SQLLargeObjectQueryFactory();
      case QueryPrms.GFE_GFK:
      case QueryPrms.GFE_GFK_DVD:
      case QueryPrms.GFE:
        return new GFELargeObjectQueryFactory();
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
        sb.append(LargeObject.REGION_TABLE_SHORT_NAME + "." + field);
      }
      if (i.hasNext()) {
        sb.append(",");
      }
    }
    return sb.toString();
  }

  /**
   * returns a list of column names that make up the LargeObject table
   * @return
   */
  public static List getFields(List fieldList) {
    List fields = new ArrayList();
    if (fieldList.contains("*") == true) {
      fields.add("id");
      fields.add("stringField1");
      fields.add("stringField2");
      fields.add("stringField3");
      fields.add("stringField4");
      fields.add("stringField5");
      fields.add("stringField6");
      fields.add("stringField7");
      fields.add("stringField8");
      fields.add("stringField9");
      fields.add("stringField10");
      fields.add("stringField11");
      fields.add("stringField12");
      fields.add("stringField13");
      fields.add("stringField14");
      fields.add("stringField15");
      fields.add("stringField16");
      fields.add("stringField17");
      fields.add("stringField18");
      fields.add("stringField19");
      fields.add("stringField20");
      
      fields.add("intField1");
      fields.add("intField2");
      fields.add("intField3");
      fields.add("intField4");
      fields.add("intField5");
      fields.add("intField6");
      fields.add("intField7");
      fields.add("intField8");
      fields.add("intField9");
      fields.add("intField10");
      fields.add("intField11");
      fields.add("intField12");
      fields.add("intField13");
      fields.add("intField14");
      fields.add("intField15");
      fields.add("intField16");
      fields.add("intField17");
      fields.add("intField18");
      fields.add("intField19");
      fields.add("intField20");
    }
    else if (fieldList.contains(LargeObjectPrms.NONE)) {
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
    return "LargeObject #" + this.id + "=" + id + ": stringField1="
        + this.stringField1 + ": stringField2=" + this.stringField2
        + ": stringField3=" + this.stringField4 + ": stringField4="
        + this.stringField4 + ": stringField5=" + this.stringField5
        + ": stringField6=" + this.stringField6 + ": stringField7="
        + this.stringField7 + ": stringField8=" + this.stringField8
        + ": stringField9=" + this.stringField9 + ": stringField10="
        + this.stringField10 + ": stringField11=" + this.stringField11
        + ": stringField12=" + this.stringField12 + ": stringField13="
        + this.stringField13 + ": stringField14=" + this.stringField14
        + ": stringField15=" + this.stringField15 + ": stringField16="
        + this.stringField16 + ": stringField17=" + this.stringField17
        + ": stringField18=" + this.stringField18 + ": stringField19="
        + this.stringField19 + ": stringField20=" + this.stringField20

        + ": intField1=" + this.intField1 + ": intField2=" + this.intField2
        + ": intField3=" + this.intField4 + ": intField4=" + this.intField4
        + ": intField5=" + this.intField5 + ": intField6=" + this.intField6
        + ": intField7=" + this.intField7 + ": intField8=" + this.intField8
        + ": intField9=" + this.intField9 + ": intField10=" + this.intField10
        + ": intField11=" + this.intField11 + ": intField12=" + this.intField12
        + ": intField13=" + this.intField13 + ": intField14=" + this.intField14
        + ": intField15=" + this.intField15 + ": intField16=" + this.intField16
        + ": intField17=" + this.intField17 + ": intField18=" + this.intField18
        + ": intField19=" + this.intField19 + ": intField20=" + this.intField20;

  }

  public int sizeof(Object o) {
    if (o instanceof LargeObject) {
      LargeObject obj = (LargeObject) o;
      return Sizeof.sizeof(obj.id) + Sizeof.sizeof(obj.stringField1)
          + Sizeof.sizeof(obj.stringField2) + Sizeof.sizeof(obj.stringField3)
          + Sizeof.sizeof(obj.stringField4) + Sizeof.sizeof(obj.stringField5)
          + Sizeof.sizeof(obj.stringField6) + Sizeof.sizeof(obj.stringField7)
          + Sizeof.sizeof(obj.stringField8) + Sizeof.sizeof(obj.stringField9)
          + Sizeof.sizeof(obj.stringField10) + Sizeof.sizeof(obj.stringField11)
          + Sizeof.sizeof(obj.stringField12) + Sizeof.sizeof(obj.stringField13)
          + Sizeof.sizeof(obj.stringField14) + Sizeof.sizeof(obj.stringField15)
          + Sizeof.sizeof(obj.stringField16) + Sizeof.sizeof(obj.stringField17)
          + Sizeof.sizeof(obj.stringField18) + Sizeof.sizeof(obj.stringField19)
          + Sizeof.sizeof(obj.stringField20)

          + Sizeof.sizeof(obj.intField1) + Sizeof.sizeof(obj.intField2)
          + Sizeof.sizeof(obj.intField3) + Sizeof.sizeof(obj.intField4)
          + Sizeof.sizeof(obj.intField5) + Sizeof.sizeof(obj.intField6)
          + Sizeof.sizeof(obj.intField7) + Sizeof.sizeof(obj.intField8)
          + Sizeof.sizeof(obj.intField9) + Sizeof.sizeof(obj.intField10)
          + Sizeof.sizeof(obj.intField11) + Sizeof.sizeof(obj.intField12)
          + Sizeof.sizeof(obj.intField13) + Sizeof.sizeof(obj.intField14)
          + Sizeof.sizeof(obj.intField15) + Sizeof.sizeof(obj.intField16)
          + Sizeof.sizeof(obj.intField17) + Sizeof.sizeof(obj.intField18)
          + Sizeof.sizeof(obj.intField19) + Sizeof.sizeof(obj.intField20);

    }
    else {
      return Sizeof.sizeof(o);
    }
  }

  //----------------------------------------------------------------------------
  // Delta
  //----------------------------------------------------------------------------
  
  public synchronized void update(int i) {
    setStringField3("stringField3 " + i);
    setIntField12(i);
    setIntField18(i);
    this.hasDelta = true;
  }

  public boolean hasDelta() {
    return this.hasDelta;
  }

  public void toDelta(DataOutput out)
  throws IOException {
    out.writeUTF(this.stringField3);
    out.writeInt(this.intField12);
    out.writeInt(this.intField18);
  }
  
  public void fromDelta(DataInput in)
  throws IOException {
    this.stringField3 = in.readUTF();
    this.intField12 = in.readInt();
    this.intField18 = in.readInt();
  }
  
  //----------------------------------------------------------------------------
  // DataSerializable
  //----------------------------------------------------------------------------

  public void toData(DataOutput out) throws IOException {
    out.writeInt(this.id);
    DataSerializer.writeString(this.stringField1, out);
    DataSerializer.writeString(this.stringField2, out);
    DataSerializer.writeString(this.stringField3, out);
    DataSerializer.writeString(this.stringField4, out);
    DataSerializer.writeString(this.stringField5, out);
    DataSerializer.writeString(this.stringField6, out);
    DataSerializer.writeString(this.stringField7, out);
    DataSerializer.writeString(this.stringField8, out);
    DataSerializer.writeString(this.stringField9, out);
    DataSerializer.writeString(this.stringField10, out);
    DataSerializer.writeString(this.stringField11, out);
    DataSerializer.writeString(this.stringField12, out);
    DataSerializer.writeString(this.stringField13, out);
    DataSerializer.writeString(this.stringField14, out);
    DataSerializer.writeString(this.stringField15, out);
    DataSerializer.writeString(this.stringField16, out);
    DataSerializer.writeString(this.stringField17, out);
    DataSerializer.writeString(this.stringField18, out);
    DataSerializer.writeString(this.stringField19, out);
    DataSerializer.writeString(this.stringField20, out);
    
    out.writeInt(this.intField1);
    out.writeInt(this.intField2);
    out.writeInt(this.intField3);
    out.writeInt(this.intField4);
    out.writeInt(this.intField5);
    out.writeInt(this.intField6);
    out.writeInt(this.intField7);
    out.writeInt(this.intField8);
    out.writeInt(this.intField9);
    out.writeInt(this.intField10);
    out.writeInt(this.intField11);
    out.writeInt(this.intField12);
    out.writeInt(this.intField13);
    out.writeInt(this.intField14);
    out.writeInt(this.intField15);
    out.writeInt(this.intField16);
    out.writeInt(this.intField17);
    out.writeInt(this.intField18);
    out.writeInt(this.intField19);
    out.writeInt(this.intField20);    

  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.id = in.readInt();
    this.stringField1 = DataSerializer.readString(in);
    this.stringField2 = DataSerializer.readString(in);
    this.stringField3 = DataSerializer.readString(in);
    this.stringField4 = DataSerializer.readString(in);
    this.stringField5 = DataSerializer.readString(in);
    this.stringField6 = DataSerializer.readString(in);
    this.stringField7 = DataSerializer.readString(in);
    this.stringField8 = DataSerializer.readString(in);
    this.stringField9 = DataSerializer.readString(in);
    this.stringField10 = DataSerializer.readString(in);
    this.stringField11 = DataSerializer.readString(in);
    this.stringField12 = DataSerializer.readString(in);
    this.stringField13 = DataSerializer.readString(in);
    this.stringField14 = DataSerializer.readString(in);
    this.stringField15 = DataSerializer.readString(in);
    this.stringField16 = DataSerializer.readString(in);
    this.stringField17 = DataSerializer.readString(in);
    this.stringField18 = DataSerializer.readString(in);
    this.stringField19 = DataSerializer.readString(in);
    this.stringField20 = DataSerializer.readString(in);

    this.intField1 = in.readInt();
    this.intField2 = in.readInt();
    this.intField3 = in.readInt();
    this.intField4 = in.readInt();
    this.intField5 = in.readInt();
    this.intField6 = in.readInt();
    this.intField7 = in.readInt();
    this.intField8 = in.readInt();
    this.intField9 = in.readInt();
    this.intField10 = in.readInt();
    this.intField11 = in.readInt();
    this.intField12 = in.readInt();
    this.intField13 = in.readInt();
    this.intField14 = in.readInt();
    this.intField15 = in.readInt();
    this.intField16 = in.readInt();
    this.intField17 = in.readInt();
    this.intField18 = in.readInt();
    this.intField19 = in.readInt();
    this.intField20 = in.readInt();

  }

  //--------------------------------------------------------------------------
  //Accessors
  //--------------------------------------------------------------------------
  public Integer getId() {
    return new Integer(this.id);
  }

  public String getStringField1() {
    return stringField1;
  }

  public void setStringField1(String stringField1) {
    this.stringField1 = stringField1;
  }

  public String getStringField2() {
    return stringField2;
  }

  public void setStringField2(String stringField2) {
    this.stringField2 = stringField2;
  }

  public String getStringField3() {
    return stringField3;
  }

  public void setStringField3(String stringField3) {
    this.stringField3 = stringField3;
  }

  public String getStringField4() {
    return stringField4;
  }

  public void setStringField4(String stringField4) {
    this.stringField4 = stringField4;
  }

  public String getStringField5() {
    return stringField5;
  }

  public void setStringField5(String stringField5) {
    this.stringField5 = stringField5;
  }

  public String getStringField6() {
    return stringField6;
  }

  public void setStringField6(String stringField6) {
    this.stringField6 = stringField6;
  }

  public String getStringField7() {
    return stringField7;
  }

  public void setStringField7(String stringField7) {
    this.stringField7 = stringField7;
  }

  public String getStringField8() {
    return stringField8;
  }

  public void setStringField8(String stringField8) {
    this.stringField8 = stringField8;
  }

  public String getStringField9() {
    return stringField9;
  }

  public void setStringField9(String stringField9) {
    this.stringField9 = stringField9;
  }

  public String getStringField10() {
    return stringField10;
  }

  public void setStringField10(String stringField10) {
    this.stringField10 = stringField10;
  }

  public String getStringField11() {
    return stringField11;
  }

  public void setStringField11(String stringField11) {
    this.stringField11 = stringField11;
  }

  public String getStringField12() {
    return stringField12;
  }

  public void setStringField12(String stringField12) {
    this.stringField12 = stringField12;
  }

  public String getStringField13() {
    return stringField13;
  }

  public void setStringField13(String stringField13) {
    this.stringField13 = stringField13;
  }

  public String getStringField14() {
    return stringField14;
  }

  public void setStringField14(String stringField14) {
    this.stringField14 = stringField14;
  }

  public String getStringField15() {
    return stringField15;
  }

  public void setStringField15(String stringField15) {
    this.stringField15 = stringField15;
  }

  public String getStringField16() {
    return stringField16;
  }

  public void setStringField16(String stringField16) {
    this.stringField16 = stringField16;
  }

  public String getStringField17() {
    return stringField17;
  }

  public void setStringField17(String stringField17) {
    this.stringField17 = stringField17;
  }

  public String getStringField18() {
    return stringField18;
  }

  public void setStringField18(String stringField18) {
    this.stringField18 = stringField18;
  }

  public String getStringField19() {
    return stringField19;
  }

  public void setStringField19(String stringField19) {
    this.stringField19 = stringField19;
  }

  public String getStringField20() {
    return stringField20;
  }

  public void setStringField20(String stringField20) {
    this.stringField20 = stringField20;
  }

  public int getIntField1() {
    return intField1;
  }

  public void setIntField1(int intField1) {
    this.intField1 = intField1;
  }

  public int getIntField2() {
    return intField2;
  }

  public void setIntField2(int intField2) {
    this.intField2 = intField2;
  }

  public int getIntField3() {
    return intField3;
  }

  public void setIntField3(int intField3) {
    this.intField3 = intField3;
  }

  public int getIntField4() {
    return intField4;
  }

  public void setIntField4(int intField4) {
    this.intField4 = intField4;
  }

  public int getIntField5() {
    return intField5;
  }

  public void setIntField5(int intField5) {
    this.intField5 = intField5;
  }

  public int getIntField6() {
    return intField6;
  }

  public void setIntField6(int intField6) {
    this.intField6 = intField6;
  }

  public int getIntField7() {
    return intField7;
  }

  public void setIntField7(int intField7) {
    this.intField7 = intField7;
  }

  public int getIntField8() {
    return intField8;
  }

  public void setIntField8(int intField8) {
    this.intField8 = intField8;
  }

  public int getIntField9() {
    return intField9;
  }

  public void setIntField9(int intField9) {
    this.intField9 = intField9;
  }

  public int getIntField10() {
    return intField10;
  }

  public void setIntField10(int intField10) {
    this.intField10 = intField10;
  }

  public int getIntField11() {
    return intField11;
  }

  public void setIntField11(int intField11) {
    this.intField11 = intField11;
  }

  public int getIntField12() {
    return intField12;
  }

  public void setIntField12(int intField12) {
    this.intField12 = intField12;
  }

  public int getIntField13() {
    return intField13;
  }

  public void setIntField13(int intField13) {
    this.intField13 = intField13;
  }

  public int getIntField14() {
    return intField14;
  }

  public void setIntField14(int intField14) {
    this.intField14 = intField14;
  }

  public int getIntField15() {
    return intField15;
  }

  public void setIntField15(int intField15) {
    this.intField15 = intField15;
  }

  public int getIntField16() {
    return intField16;
  }

  public void setIntField16(int intField16) {
    this.intField16 = intField16;
  }

  public int getIntField17() {
    return intField17;
  }

  public void setIntField17(int intField17) {
    this.intField17 = intField17;
  }

  public int getIntField18() {
    return intField18;
  }

  public void setIntField18(int intField18) {
    this.intField18 = intField18;
  }

  public int getIntField19() {
    return intField19;
  }

  public void setIntField19(int intField19) {
    this.intField19 = intField19;
  }

  public int getIntField20() {
    return intField20;
  }

  public void setIntField20(int intField20) {
    this.intField20 = intField20;
  }

  public int getIndex() {
    return id;
  }

  public void validate(int index) {
  }
}
