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

public class Position implements DataSerializable {

  private int id; // unique
  private int bookId;
  private String instrument;
  private int amount;
  private int synthetic;
  private String owner;
  private String symbol;
  private String str1;
  private Map risks;

  public static String REGION_TABLE_NAME = "POSITIONS";
  public static String REGION_TABLE_SHORT_NAME = "p";

  /**
   * Initialize the i'th instrument.
   */
  public void init(int i) {    
    int numPositionsPerInstrument = SectorPrms.getNumPositionsPerInstrument();
    int numBookValues = SectorPrms.getNumBookValues();
    int id = i; // unique
    this.id = id;
    this.bookId = id % numBookValues;;
    this.instrument = Instrument.getInstrument(id /numPositionsPerInstrument);
    this.amount = id;
    this.synthetic = id % 2;
    this.owner = Position.getOwner(id);
    this.symbol =  Position.getSymbol(id);
  }
  
  public static String getTableName() {
    return Position.REGION_TABLE_NAME;
  }

  public static String getTableAndShortName() {
    return Position.REGION_TABLE_NAME + " " + Position.REGION_TABLE_SHORT_NAME;
  }

  public static String getTableShortName() {
    return Position.REGION_TABLE_SHORT_NAME;
  }

  public static String getOwner(int id) {
    return "Owner" + (id % SectorPrms.getNumPositionsPerInstrument());
  }

  public static String getSymbol(int id) {
    return "SYM" + (id % SectorPrms.getNumSymbolValues());
  }

  @SuppressWarnings("unchecked")
  public static String commaSeparatedStringFor(Vector fields) {
   // Vector copy = (Vector)fields.clone();
    StringBuffer sb = new StringBuffer();
    for (Iterator i = fields.iterator(); i.hasNext();) {
      String field = (String) i.next();
      if (field.equals("*")) {
        return field;
      }
      else {
        sb.append(Position.REGION_TABLE_SHORT_NAME + "." + field);
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
      fields.add("book_id");
      fields.add("instrument");
      fields.add("amount");
      fields.add("synthetic");
      fields.add("owner");
      fields.add("symbol");
    }
    else if (fieldList.contains(SectorPrms.NONE)) {
      //we don't want any fields
    }
    else {
      fields.addAll(fieldList);
    }
    return fields;
  }

  //-------------------------------------------------------------------------
  //Accessors
  //-------------------------------------------------------------------------
  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public int getBookId() {
    return bookId;
  }

  public void setBookId(int bookId) {
    this.bookId = bookId;
  }

  public String getSymbol() {
    return symbol;
  }

  public void setSymbol(String symbol) {
    this.symbol = symbol;
  }

  public String getInstrument() {
    return instrument;
  }

  public void setInstrument(String instrument) {
    this.instrument = instrument;
  }

  public int getAmount() {
    return amount;
  }

  public void setAmount(int amount) {
    this.amount = amount;
  }

  public int getSynthetic() {
    return synthetic;
  }

  public void setSynthetic(int synthetic) {
    this.synthetic = synthetic;
  }

  public String getOwner() {
    return owner;
  }

  public void setOwner(String owner) {
    this.owner = owner;
  }

  public Map getRisks() {
    return risks;
  }

  public void setRisks(Map risks) {
    this.risks = risks;
  }





  //----------------------------------------------------------------------------
  // QueryFactory: printing
  //----------------------------------------------------------------------------

  public String toString() {
    return "Position #" + this.id;
  }

  public int sizeof(Object o) {
    if (o instanceof Position) {
      Position obj = (Position) o;
      return Sizeof.sizeof(obj.id) + Sizeof.sizeof(obj.bookId)
          + Sizeof.sizeof(obj.instrument) + Sizeof.sizeof(obj.amount);
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
    out.writeInt(this.bookId);
    DataSerializer.writeString(this.instrument, out);
    out.writeInt(this.amount);
    out.writeInt(this.synthetic);
    DataSerializer.writeString(this.owner, out);
    DataSerializer.writeString(this.symbol, out);
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.id = in.readInt();
    this.bookId = in.readInt();
    this.instrument = DataSerializer.readString(in);
    this.amount = in.readInt();
    this.synthetic = in.readInt();
    this.owner = DataSerializer.readString(in);
    this.symbol = DataSerializer.readString(in);
  }
}
