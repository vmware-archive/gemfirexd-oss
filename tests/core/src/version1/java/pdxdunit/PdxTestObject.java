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
package com.gemstone.gemfire.cache.query.data;

import com.gemstone.gemfire.pdx.PdxInstance;
import com.gemstone.gemfire.cache.query.data.PositionPdx;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.pdx.PdxReader;
import com.gemstone.gemfire.pdx.PdxSerializable;
import com.gemstone.gemfire.pdx.PdxWriter;

import dunit.DistributedTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class PdxTestObject implements PdxSerializable {
    protected String _ticker;
    protected int _price;
    public int id;
    public int important;
    public int selection;
    public int select;
    public Map idTickers = new HashMap();
    public HashMap positions = new HashMap();
    public PdxTestObject2 test;
    
    public PdxTestObject() {
      //GemFireCacheImpl.getInstance().getLoggerI18n().fine(new Exception("DEBUG"));
    }

    public PdxTestObject(int id, String ticker) {
      this.id = id;
      this._ticker = ticker;
      this._price = id;
      this.important = id;
      this.selection =id;
      this.select =id;
      //GemFireCacheImpl.getInstance().getLoggerI18n().fine(new Exception("DEBUG"));
      idTickers.put(id + "", ticker);
      this.test = new PdxTestObject2(id);
    }

    public PdxTestObject(int id, String ticker, int numPositions) {
      this(id, ticker);
      for (int i=0; i < numPositions; i++) {
        positions.put(id + i, new PositionPdx(ticker + ":" +  id + ":" + i , (id + 100)));
      }
    }
    
    public int getIdValue() {
      return this.id;
    }

    public String getTicker() {
      return this._ticker;
    }

    public int getPriceValue() {
      return this._price;
    }

    public HashMap getPositions(String id) {
      return this.positions;  
    }
    
    public void toData(PdxWriter out)
    {
      //System.out.println("Is serializing in WAN: " + GatewayEventImpl.isSerializingValue());
      out.writeInt("id", this.id);
      out.writeString("ticker", this._ticker);
      out.writeInt("price", this._price);
      out.writeObject("idTickers", this.idTickers);
      out.writeObject("positions", this.positions);
      out.writeObject("test", this.test);
    }

    public void fromData(PdxReader in)
    {
      //System.out.println("Is deserializing in WAN: " + GatewayEventImpl.isDeserializingValue());
      this.id = in.readInt("id");
      this._ticker = in.readString("ticker");
      this._price = in.readInt("price");
      this.idTickers = (HashMap)in.readObject("idTickers");
      this.positions = (HashMap)in.readObject("positions");
      this.test = (PdxTestObject2)in.readObject("test");
    }

    public String toString() {
      StringBuffer buffer = new StringBuffer();
      buffer
      .append("PdxTestObject [")
      .append("id=")
      .append(this.id)
      .append("; ticker=")
      .append(this._ticker)
      .append("; price=")
      .append(this._price)
      .append("]");
      return buffer.toString();
    }

    @Override
    public boolean equals(Object o){
      //DistributedTestCase.getLogWriter().info("In PdxTestObject.equals() this: " + this + " other :" + o);
      //GemFireCacheImpl.getInstance().getLoggerI18n().fine("In PdxTestObject.equals() this: " + this + " other :" + o);
      if (!(o instanceof PdxTestObject)) {
        return false;
      }
      
      PdxTestObject other = (PdxTestObject)o;
      if ((id == other.id) && (_ticker.equals(other._ticker))) {
        return true;
      } else {
        //DistributedTestCase.getLogWriter().info("NOT EQUALS");  
        return false;
      }
    }
    
    @Override
    public int hashCode(){
      //GemFireCacheImpl.getInstance().getLoggerI18n().fine("In PdxTestObject.hashCode() : " + this.id);
      return this.id;
    }

}
