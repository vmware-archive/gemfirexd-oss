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

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.Instantiator;
import com.gemstone.gemfire.cache.util.ObjectSizer;
import com.gemstone.gemfire.internal.util.Sizeof;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Iterator;
import java.util.Vector;

/**
 * A ticket for a stock order placed by a {@link Broker}.
 * <p>
 * Implements DataSerializable for better performance and ObjectSizer
 * for better use with an memory evictor.
 */
public class BrokerTicket implements DataSerializable, ObjectSizer {

  private static NumberFormat format;
  public static String REGION_TABLE_NAME = "broker_tickets";
  public static String REGION_TABLE_SHORT_NAME = "bt";

  static {
    format = new DecimalFormat("############");
    format.setMinimumIntegerDigits(12);
  }
/*
  static {
    Instantiator.register(new Instantiator(BrokerTicket.class, (byte) 99) {
      public DataSerializable newInstance() {
        return new BrokerTicket();
      }
    });
  }
*/

  public static String getTableName() {
    return BrokerTicket.REGION_TABLE_NAME;
  }
  
  
  public static String getTableShortName() {
    return BrokerTicket.REGION_TABLE_SHORT_NAME;
  }
  
  public static String getTableAndShortName() {
    return BrokerTicket.REGION_TABLE_NAME + " " + BrokerTicket.REGION_TABLE_SHORT_NAME;
  }  
  
  public static String commaSeparatedStringFor(Vector fields) {
      StringBuffer sb = new StringBuffer();
      for (Iterator i = fields.iterator(); i.hasNext();) {
        String field = (String)i.next();
        if (field.equals("*")) {
          return field;
        } else {
          sb.append(BrokerTicket.REGION_TABLE_SHORT_NAME + "." + field);
        }
        if (i.hasNext()) {
          sb.append(",");
        }
      }
      return sb.toString();
    }

  /**
   * Returns a unique id for the i'th ticket for the given broker id and number
   * of tickets per broker.
   */
  protected static int getId(int i, int bid, int numTicketsPerBroker) {
    return bid * numTicketsPerBroker + i;
  }
  
  /**
   * Returns a price for the given ticket id and number of unique ticket prices.
   */
  protected static double getPrice(int tid, int numTicketPrices) {
    return (double)tid % numTicketPrices;
  }
  
  /**
   * Returns a quantity for the given ticket id.
   */
  protected static int getQuantity(int tid) {
    return 5; // @todo something useful here
  }
  
  /**
   * Returns a ticker for the given ticket id.
   */
  protected static String getTicker(int tid) {
    return "GEM" + format.format(tid);
  }
  
  /**
   * Returns a String for the filler fields.
   */
  protected static String getFiller(int tid) {
    return format.format(tid);
  }

  //----------------------------------------------------------------------------
  // Fields
  //----------------------------------------------------------------------------

  private int id; // unique
  private int brokerId; // an objects.Broker.id
  private double price;
  private int quantity;
  private String ticker;
  private String str01;
  private String str02;
  private String str03;
  private String str04;
  private String str05;
  private String str06;
  private String str07;
  private String str08;
  private String str09;
  private String str10;
  private String str11;
  private String str12;
  private String str13;
  private String str14;
  private String str15;

  //----------------------------------------------------------------------------
  // Constructors
  //----------------------------------------------------------------------------

  public BrokerTicket() {
  }

  /**
   * Initialize the i'th ticket for the given broker id.
   */
  public void init(int i, int bid, int numTicketsPerBroker, int numTicketPrices) {
    this.id = getId(i, bid, numTicketsPerBroker);
    this.brokerId = bid;
    this.price = getPrice(this.id, numTicketPrices);
    this.quantity = getQuantity(this.id);
    this.ticker = getTicker(this.id);
    this.str01 = getFiller(this.id);
    this.str02 = getFiller(this.id);
    this.str03 = getFiller(this.id);
    this.str04 = getFiller(this.id);
    this.str05 = getFiller(this.id);
    this.str06 = getFiller(this.id);
    this.str07 = getFiller(this.id);
    this.str08 = getFiller(this.id);
    this.str09 = getFiller(this.id);
    this.str10 = getFiller(this.id);
    this.str11 = getFiller(this.id);
    this.str12 = getFiller(this.id);
    this.str13 = getFiller(this.id);
    this.str14 = getFiller(this.id);
    this.str15 = getFiller(this.id);
  }

  //----------------------------------------------------------------------------
  // Accessors
  //----------------------------------------------------------------------------

  public int getId() {
    return this.id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public int getBrokerId() {
    return this.brokerId;
  }

  public void setBrokerId(int brokerId) {
    this.brokerId = brokerId;
  }

  public double getPrice() {
    return this.price;
  }

  public void setPrice(double price) {
    this.price = price;
  }

  public int getQuantity() {
    return this.quantity;
  }

  public void setQuantity(int quantity) {
    this.quantity = quantity;
  }

  public String getTicker() {
    return this.ticker;
  }

  public void setTicker(String ticker) {
    this.ticker = ticker;
  }

  //----------------------------------------------------------------------------
  // printing
  //----------------------------------------------------------------------------

  public String toString() {
    return "BrokerTicket #" + this.id + " brokerId= " + this.brokerId + this.ticker + " @ " + price;
  }

  //----------------------------------------------------------------------------
  // ObjectSizer
  //----------------------------------------------------------------------------

  public int sizeof(Object o) {
    if (o instanceof BrokerTicket) {
      BrokerTicket obj = (BrokerTicket) o;
      return Sizeof.sizeof(obj.id)
           + Sizeof.sizeof(obj.brokerId)
           + Sizeof.sizeof(obj.price)
           + Sizeof.sizeof(obj.quantity)
           + Sizeof.sizeof(obj.ticker)
           + Sizeof.sizeof(obj.str01)
           + Sizeof.sizeof(obj.str02)
           + Sizeof.sizeof(obj.str03)
           + Sizeof.sizeof(obj.str04)
           + Sizeof.sizeof(obj.str05)
           + Sizeof.sizeof(obj.str06)
           + Sizeof.sizeof(obj.str07)
           + Sizeof.sizeof(obj.str08)
           + Sizeof.sizeof(obj.str09)
           + Sizeof.sizeof(obj.str10)
           + Sizeof.sizeof(obj.str11)
           + Sizeof.sizeof(obj.str12)
           + Sizeof.sizeof(obj.str13)
           + Sizeof.sizeof(obj.str14)
           + Sizeof.sizeof(obj.str15);
    } else {
      return Sizeof.sizeof(o);
    }
  }

  //----------------------------------------------------------------------------
  // DataSerializable
  //----------------------------------------------------------------------------

  public void toData(DataOutput out) throws IOException {
    out.writeInt(this.id);
    out.writeInt(this.brokerId);
    out.writeDouble(this.price);
    out.writeInt(this.quantity);
    DataSerializer.writeString(this.ticker, out);
    DataSerializer.writeString(this.str01, out);
    DataSerializer.writeString(this.str02, out);
    DataSerializer.writeString(this.str03, out);
    DataSerializer.writeString(this.str04, out);
    DataSerializer.writeString(this.str05, out);
    DataSerializer.writeString(this.str06, out);
    DataSerializer.writeString(this.str07, out);
    DataSerializer.writeString(this.str08, out);
    DataSerializer.writeString(this.str09, out);
    DataSerializer.writeString(this.str10, out);
    DataSerializer.writeString(this.str11, out);
    DataSerializer.writeString(this.str12, out);
    DataSerializer.writeString(this.str13, out);
    DataSerializer.writeString(this.str14, out);
    DataSerializer.writeString(this.str15, out);
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.id = in.readInt();
    this.brokerId = in.readInt();
    this.price = in.readDouble();
    this.quantity = in.readInt();
    this.ticker = DataSerializer.readString(in);
    this.str01 = DataSerializer.readString(in);
    this.str02 = DataSerializer.readString(in);
    this.str03 = DataSerializer.readString(in);
    this.str04 = DataSerializer.readString(in);
    this.str05 = DataSerializer.readString(in);
    this.str06 = DataSerializer.readString(in);
    this.str07 = DataSerializer.readString(in);
    this.str08 = DataSerializer.readString(in);
    this.str09 = DataSerializer.readString(in);
    this.str10 = DataSerializer.readString(in);
    this.str11 = DataSerializer.readString(in);
    this.str12 = DataSerializer.readString(in);
    this.str13 = DataSerializer.readString(in);
    this.str14 = DataSerializer.readString(in);
    this.str15 = DataSerializer.readString(in);
  }
}
