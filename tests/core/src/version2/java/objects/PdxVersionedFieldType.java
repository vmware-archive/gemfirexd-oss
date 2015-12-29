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
/**
 * 
 */
package objects;

import com.gemstone.gemfire.pdx.PdxReader;
import com.gemstone.gemfire.pdx.PdxSerializable;
import com.gemstone.gemfire.pdx.PdxWriter;

public class PdxVersionedFieldType implements PdxSerializable{
  public String myVersion;
  public int intField;
  public long longField;
  public float floatField;
  public double doubleField;
  
  public PdxVersionedFieldType (int id){
    this.myVersion = "version-2";
    this.intField = id;
    this.longField = new Integer(id+1).longValue();
    this.floatField = new Integer(id+2).floatValue();
    this.doubleField = new Integer(id+3).doubleValue();
  }
  
  public void toData(PdxWriter writer){
    writer.writeInt("intField", intField);
    writer.writeLong("longField", longField);
    writer.writeFloat("floatField", floatField);
    writer.writeDouble("doubleField", doubleField);
  }
  
  public void fromData(PdxReader reader){
    this.intField = reader.readInt("intField");
    this.longField = reader.readLong("longField");
    this.floatField = reader.readFloat("floatField");
    this.doubleField = reader.readDouble("doubleField");
  }
  
  @Override
  public String toString(){
    return this.getClass().getName() +  "[ myVersion: " + this.myVersion 
        + ", intField: " + this.intField + ", longField: " 
        + this.longField + ", floatField: " + this.floatField 
        + ", doubleField: " + this.doubleField + " ]";
  }
}