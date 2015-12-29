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

import java.io.Reader;
import java.io.Serializable;
import java.io.StringReader;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.input.SAXBuilder;

public class MarketUpdate implements Serializable {
  public String ccy;
  public String ValueDate;
  public double value;
  public double spot;
  public double discount_factor;
  public int tickId;
  //public String wanIdentity = null;
  /**
   * We'll be more efficient with RTE publishing if we know whether to insert of update.
   */
  boolean isNew;

  public MarketUpdate(String ccy, String ValueDate, double value, double spot,
      double discount_factor, int tickId, boolean isNew ){
    this.ccy = ccy;
    this.ValueDate = ValueDate;
    this.value = value;
    this.spot = spot;
    this.discount_factor = discount_factor;
    this.tickId = tickId;
    //this.wanIdentity = wanIdentity;
    this.isNew = isNew;
  }

  public MarketUpdate ( String xml, boolean isNew ) {

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
    this.ccy          = getStringElement(root_element, "ccy");
    this.ValueDate        = getStringElement(root_element, "ValueDate");
    this.value          = getDoubleElement(root_element, "value");
    this.spot           = getDoubleElement(root_element, "spot");
    this.discount_factor    = getDoubleElement(root_element, "discount_factor");

    this.tickId         = getIntegerElement(root_element, "TickId" );
    //this.wanIdentity = wanIdentity;
    this.isNew = isNew;
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
}

