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
package org.jgroups.simple;

import java.util.*;
import java.io.*;

public class Order implements Serializable, Cloneable {

  String brokerName;
  int quantity;
  Date expiration;
  double price;

  public Order() {
     
  }

  public Order(String bName, int quant, Date expiry, double cost) {
     super();
     brokerName = bName;
     quantity = quant;
     expiration = expiry;
     price = cost;
  }

  public String getBrokerName() {
     return brokerName;
  }

  public int getQuantity() {
     return quantity;
  }

 
  public Date getExpiration() {
     return expiration;
  }

  
  public double getPrice() {
     return price;
  }

  public byte[] toBytes() throws Exception {
     ByteArrayOutputStream bos = new ByteArrayOutputStream();
     ObjectOutputStream out = new ObjectOutputStream(bos);
     out.writeObject(this);
     out.flush();
     return bos.toByteArray();
  }

  public static Order fromBytes(byte[] byteArray) throws Exception {
      ByteArrayInputStream brs = new ByteArrayInputStream(byteArray);
      ObjectInputStream in = new ObjectInputStream(brs);
      return (Order) in.readObject();
  }

}
