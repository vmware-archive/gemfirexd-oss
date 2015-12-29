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
package wan.ml;
import java.io.*;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;

public class Trade implements DataSerializable {
  long   TradeId;
  String Trader;
  String Entity;
  String Currency;
  double Amount;
  double ContractRate;
  String CtrCurrency;
  double ctr_amt;
  double BaseUSEquiv;
  double ctr_us_equiv;
  double CcyDollarDiscountFactor;
  double CcySpotRate;
  double ctr_spot;
  String ValueDate;
  String TradeDate;
  long   timestamp;
  String counterparty;
  long gf_latency_start;

  static java.text.SimpleDateFormat fmt = new java.text.SimpleDateFormat("MM/dd/yyyy");

  public Trade(String xml) throws JDOMException, IOException  {
    // actually parse the xml here
    SAXBuilder builder = new SAXBuilder();
    Document doc = null;
    try {
        Reader string_reader = new StringReader(xml);
        doc = builder.build(string_reader);
    } catch (Exception e) {
        System.out.println("Caught exception during XML parsing of a Trade");
        e.printStackTrace();
    }

    Element root_element = doc.getRootElement ();

    TradeId                 = getIntegerElement(root_element, "TradeId");
    Trader                  = getStringElement(root_element, "Trader").trim();
    Entity                  = getStringElement(root_element, "Entity").trim();
    Currency                = getStringElement(root_element, "Currency");
    Amount                  = getDoubleElement(root_element, "Amount");
    ContractRate            = getDoubleElement(root_element, "ContractRate");
    CtrCurrency             = getStringElement(root_element, "CtrCurrency");
    //TODO The trade file is missing this property, we will need to get an update.  Use 1 for now.
    //BaseUSEquiv             = getDoubleElement(root_element, "BaseUSEquiv");
    BaseUSEquiv = 1;
    CcyDollarDiscountFactor = getDoubleElement(root_element, "CcyDollarDiscountFactor");
    CcySpotRate             = getDoubleElement(root_element, "CcySpotRate");
    TradeDate               = getStringElement(root_element, "TradeDate");
    ValueDate               = getStringElement(root_element, "ValueDate");
    counterparty      = getStringElement(root_element, "CounterParty" ).trim();

    ctr_amt = Amount*ContractRate*-1;
    ctr_us_equiv = 1.0/(ContractRate/BaseUSEquiv);
    if("USD".equals(CtrCurrency))ctr_spot = 1.0;
    else ctr_spot = 1.0/(ContractRate/CcySpotRate);
  }

  private static double getDoubleElement(Element root_element, String name){
    String temp = getStringElement(root_element, name);
    Double dtemp = new Double(temp);
    return(dtemp.doubleValue());
  }

  private static int getIntegerElement(Element root_element, String name){
    String temp = getStringElement(root_element, name);
    Integer dtemp = new Integer(temp);
    return(dtemp.intValue());
  }

  private static String getStringElement(Element root_element, String name){
    //return(root_element.getChild(name).getText());
    return(root_element.getChildText(name));
  }

  public Trade(int TradeId,
    String Entity,
    String TradeDate,
    String Trader,
    String Currency,
    double Amount,
    double ContractRate,
    String CtrCurrency,
    double BaseUSEquiv,
    double CcyDollarDiscountFactor,
    double CcySpotRate,
    String ValueDate ){
    // derived fields Ctr Amt, Ctr US Equiv, Ctr Spot EXCEPT if CtrCcy=USD,
    this.TradeId = TradeId;
    this.Trader = Trader;
    this.Entity = Entity;
    this.Currency = Currency;
    this.Amount = Amount;
    this.ContractRate = ContractRate;
    this.CtrCurrency = CtrCurrency;
    this.ctr_amt = Amount*ContractRate*-1;
    this.BaseUSEquiv = BaseUSEquiv;
    this.ctr_us_equiv = 1.0/(ContractRate/BaseUSEquiv);
    this.CcyDollarDiscountFactor = CcyDollarDiscountFactor;
    this.CcySpotRate = CcySpotRate;
    if("USD".equals(CtrCurrency))this.ctr_spot = 1.0;
    else this.ctr_spot = 1.0/(ContractRate/CcySpotRate);
    this.ValueDate = ValueDate;
    this.TradeDate = TradeDate;
    this.timestamp = System.currentTimeMillis();
  }

  public Trade(long l, String entity2, String tradeDate2, String trader2, String currency2, double d, double e, String ctrCurrency2, double f, double g, double h, String valueDate2) {
    // TODO Auto-generated constructor stub
  }

  public void dumpTrade() {
    System.out.println("Trade record");
    System.out.print(TradeId); System.out.print(" ");
    System.out.print(Trader); System.out.print(" ");
    System.out.print(Currency); System.out.print(" ");
    System.out.print(Amount); System.out.print(" ");
    System.out.print(ContractRate); System.out.print(" ");
    System.out.print(CtrCurrency); System.out.print(" ");
    System.out.print(ctr_amt); System.out.print(" ");
    System.out.print(BaseUSEquiv); System.out.print(" ");
    System.out.print(ctr_us_equiv); System.out.print(" ");
    System.out.print(CcyDollarDiscountFactor); System.out.print(" ");
    System.out.print(CcySpotRate); System.out.print(" ");
    System.out.print(ctr_spot); System.out.print(" ");
    System.out.print(TradeDate); System.out.print(" ");
    System.out.print(ValueDate); System.out.print(" ");
    System.out.println(timestamp);
  }

  public long getGFLatencyStart() {
    return gf_latency_start;
  }

  public void setGFLatencyStart() {
    gf_latency_start = System.currentTimeMillis();
  }

  public Trade() {}

  public void toData(DataOutput out) throws IOException
  {
    out.writeLong(this.TradeId);
    DataSerializer.writeString(this.Trader, out);
    DataSerializer.writeString(this.Entity, out);
    DataSerializer.writeString(this.Currency, out);
    out.writeDouble(this.Amount);
    out.writeDouble(this.ContractRate);
    DataSerializer.writeString(this.CtrCurrency, out);
    out.writeDouble(this.ctr_amt);

    out.writeDouble(this.BaseUSEquiv);
    out.writeDouble(this.ctr_us_equiv);
    out.writeDouble(this.CcyDollarDiscountFactor);
    out.writeDouble(this.CcySpotRate);
    out.writeDouble(this.ctr_spot);

    DataSerializer.writeString(this.ValueDate, out);
    DataSerializer.writeString(this.TradeDate, out);
    out.writeLong(this.timestamp);
    DataSerializer.writeString(this.counterparty, out);
    out.writeLong(this.gf_latency_start);
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException
  {
    this.TradeId = in.readLong();
    this.Trader = DataSerializer.readString(in);
    this.Entity = DataSerializer.readString(in);
    this.Currency = DataSerializer.readString(in);
    this.Amount = in.readDouble();
    this.ContractRate = in.readDouble();
    this.CtrCurrency = DataSerializer.readString(in);
    this.ctr_amt = in.readDouble();

    this.BaseUSEquiv = in.readDouble();
    this.ctr_us_equiv = in.readDouble();
    this.CcyDollarDiscountFactor = in.readDouble();
    this.CcySpotRate = in.readDouble();
    this.ctr_spot = in.readDouble();

    this.ValueDate = DataSerializer.readString(in);
    this.TradeDate = DataSerializer.readString(in);
    this.timestamp = in.readLong();
    this.counterparty = DataSerializer.readString(in);
    this.gf_latency_start = in.readLong();
  }

}

