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
package objects;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.gemstone.gemfire.cache.query.data.CollectionHolder;
import com.gemstone.gemfire.cache.query.data.ComparableWrapper;
import com.gemstone.gemfire.cache.query.data.PositionPdx;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.pdx.PdxReader;
import com.gemstone.gemfire.pdx.PdxSerializable;
import com.gemstone.gemfire.pdx.PdxWriter;

public class PdxPortfolio implements PdxSerializable, ConfigurableObject {

  private int ID;
  public String pkid;
  public PositionPdx position1;
  public PositionPdx position2;
  public Object[] position3;
  int position3Size;
  public String description;
  public long createTime;
  public HashMap positions = new HashMap();
  public HashMap collectionHolderMap = new HashMap();
  String type;
  public String status;
  public String [] names={"aaa","bbb","ccc","ddd"};
  public String unicodeṤtring; 
  private final long longMinValue = Long.MIN_VALUE;
  private final float floatMinValue = Float.MIN_VALUE;
  private final double doubleMinValue = Double.MIN_VALUE;
  
  public static int numInstance = 0;
  
  /*
   * public String getStatus(){ return status;
   */
  public int getID() {
    return ID;
  }
  
  public long getCreateTime() {
    return this.createTime;
  }
  
  public void setCreateTime(long time) {
    this.createTime =time;
  }
  
  public String getPk() {
    return pkid;  
  }
  public HashMap getPositions() {
    return positions;
  }

  public HashMap getPositions(String str) {
    return positions;
  }

  public HashMap getPositions(Integer i) {
    return positions;
  }

  public HashMap getPositions(int i) {
    return positions;
  }

  public PositionPdx getP1() {
    return position1;
  }

  public PositionPdx getP2() {
    return position2;
  }
  
  public HashMap getCollectionHolderMap() {
     return collectionHolderMap; 
  }
  
  public ComparableWrapper getCW(int x) {
      return new ComparableWrapper(x);
  }
  
  public boolean testMethod(boolean booleanArg) {
    return true;
  }

  public boolean isActive() {
    return status.equals("active");
  }

  public static String secIds[] = { "SUN", "IBM", "YHOO", "GOOG", "MSFT",
      "AOL", "APPL", "ORCL", "SAP", "DELL", "RHAT", "NOVL", "HP"};
  
  /* public no-arg constructor required for Deserializable */
  public PdxPortfolio() {
    this.numInstance++;
//    GemFireCacheImpl.getInstance().getLoggerI18n().fine(new Exception("DEBUG"));
  }

  public PdxPortfolio(int i) {
    this.numInstance++;
    ID = i;
    if(i % 2 == 0) {
      description = null;
    }
    else {
      description = "XXXX";
    }
    pkid = "" + i;
    status = i % 2 == 0 ? "active" : "inactive";
    type = "type" + (i % 3);
    position1 = new PositionPdx(secIds[PositionPdx.cnt % secIds.length],
        PositionPdx.cnt * 1000);
    if (i % 2 != 0) {
      position2 = new PositionPdx(secIds[PositionPdx.cnt % secIds.length],
          PositionPdx.cnt * 1000);
    }
    else {
      position2 = null;
    }
    
    positions.put(secIds[PositionPdx.cnt % secIds.length], new PositionPdx(
        secIds[PositionPdx.cnt % secIds.length], PositionPdx.cnt * 1000));
    positions.put(secIds[PositionPdx.cnt % secIds.length], new PositionPdx(
        secIds[PositionPdx.cnt % secIds.length], PositionPdx.cnt * 1000));
    
    collectionHolderMap.put("0", new CollectionHolder());
    collectionHolderMap.put("1", new CollectionHolder());
    collectionHolderMap.put("2", new CollectionHolder());
    collectionHolderMap.put("3", new CollectionHolder());
    
    unicodeṤtring = i % 2 == 0 ? "ṤṶẐ" : "ṤẐṶ";
    Assert.assertTrue(unicodeṤtring.length() == 3);
//    GemFireCacheImpl.getInstance().getLoggerI18n().fine(new Exception("DEBUG"));
  }
  
  public PdxPortfolio(int i, int j){
    this(i);
    this.position1.portfolioId = j;
    this.position3 = new Object[3];
    for (int k=0; k < position3.length; k++) {
      PositionPdx p = new PositionPdx(secIds[k], (k+1) * 1000);
      p.portfolioId = (k+1);
      this.position3[k] = p;
    }
  }
  
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof PdxPortfolio)) {
      return false;
    }
    PdxPortfolio p2 = (PdxPortfolio)o;
    return this.ID == p2.ID;
  }
  
  @Override
  public int hashCode() {
    return this.ID;
  }

  public String toString() {
    String out = "PdxPortfolio [ID=" + ID + " status=" + status + " type=" + type
        + " pkid=" + pkid + "\n ";
    Iterator iter = positions.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry entry = (Map.Entry) iter.next();
      out += entry.getKey() + ":" + entry.getValue() + ", ";
    }
    out += "\n P1:" + position1 + ", P2:" + position2;
    return out + "\n]";
  }

  /**
   * Getter for property type.S
   * 
   * @return Value of property type.
   */
  public String getType() {
    return this.type;
  }
  
  public boolean boolFunction(String strArg){
      if(strArg=="active"){
      return true;
      }
      else{
          return false;
      }
  }  //added by vikramj
  
  public int intFunction(int j) {
    return j;
  }
  
  public String funcReturnSecId(Object o){
      return ((PositionPdx)o).getSecId();
  }
  
  public long longFunction(long j) {
    return j;
  } 
  
  public float getFloatMinValue() {
    return this.floatMinValue;
  }
  
  public float getLongMinValue() {
    return this.longMinValue;
  }
  
  public double getDoubleMinValue() {
    return this.doubleMinValue;
  }
  public void fromData(PdxReader in) {
    this.ID = in.readInt("ID");
    this.pkid = in.readString("pkid");
    this.position1 = (PositionPdx)in.readObject("position1");
    this.position2 = (PositionPdx)in.readObject("position2");   
    this.positions = (HashMap)in.readObject("positions");
    this.collectionHolderMap = (HashMap)in.readObject("collectionHolderMap");
    this.type = in.readString("type");
    this.status = in.readString("status");
    this.names = in.readStringArray("names");
    this.description = in.readString("description");
    this.createTime = in.readLong("createTime");
    // Read Position3
    this.position3 = in.readObjectArray("position3");
  }
  
  public void toData(PdxWriter out) {
    out.writeInt("ID", this.ID);
    out.writeString("pkid", this.pkid);
    out.writeObject("position1", this.position1);
    out.writeObject("position2", this.position2);
    out.writeObject("positions", this.positions);
    out.writeObject("collectionHolderMap", this.collectionHolderMap);
    out.writeString("type", this.type);
    out.writeString("status", this.status);
    out.writeStringArray("names", this.names);
    out.writeString("description", this.description);
    out.writeLong("createTime", this.createTime);
    // Write Position3.
    out.writeObjectArray("position3", this.position3);
    // Identity Field.
    out.markIdentityField("ID");
  } 
  
  public void init(int i) {
    this.numInstance++;
    ID = i;
    if(i % 2 == 0) {
      description = null;
    }
    else {
      description = "XXXX";
    }
    pkid = "" + i;
    status = i % 2 == 0 ? "active" : "inactive";
    type = "type" + (i % 3);
    position1 = new PositionPdx(secIds[PositionPdx.cnt % secIds.length],
        PositionPdx.cnt * 1000);
    if (i % 2 != 0) {
      position2 = new PositionPdx(secIds[PositionPdx.cnt % secIds.length],
          PositionPdx.cnt * 1000);
    }
    else {
      position2 = null;
    }
    
    positions.put(secIds[PositionPdx.cnt % secIds.length], new PositionPdx(
        secIds[PositionPdx.cnt % secIds.length], PositionPdx.cnt * 1000));
    positions.put(secIds[PositionPdx.cnt % secIds.length], new PositionPdx(
        secIds[PositionPdx.cnt % secIds.length], PositionPdx.cnt * 1000));
    
    collectionHolderMap.put("0", new CollectionHolder());
    collectionHolderMap.put("1", new CollectionHolder());
    collectionHolderMap.put("2", new CollectionHolder());
    collectionHolderMap.put("3", new CollectionHolder());
    
    unicodeṤtring = i % 2 == 0 ? "ṤṶẐ" : "ṤẐṶ";
    Assert.assertTrue(unicodeṤtring.length() == 3);
//    GemFireCacheImpl.getInstance().getLoggerI18n().fine(new Exception("DEBUG"));
  }
  
  public void validate(int i) {
    // Nothing to do
  }
  
  public int getIndex() {
    return ID;
  }

}
