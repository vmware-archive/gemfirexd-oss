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
  public String stringField;
  public boolean booleanField;
  public char charField;
  public short shortField;
   
  public PdxVersionedFieldType(int id){
    this.myVersion = "version-1";
    this.stringField = ""+id;
    this.booleanField = id % 2 == 0 ? true : false;
    this.charField = (char)id;
    this.shortField = new Integer(id).shortValue();
  }
  
  public void toData(PdxWriter writer){
    writer.writeString("stringField", stringField);
    writer.writeBoolean("booleanField", booleanField);
    writer.writeChar("charField", charField);
    writer.writeShort("shortField", shortField);
  }
  
  public void fromData(PdxReader reader){
    this.stringField = reader.readString("stringField");
    this.booleanField = reader.readBoolean("booleanField");
    this.charField = reader.readChar("charField");
    this.shortField = reader.readShort("shortField");
  }
  
  @Override
  public String toString(){
    return this.getClass().getName() +  "[ myVersion: " + this.myVersion 
        + ", stringField: " + this.stringField + ", booleanField: " 
        + this.booleanField + ", charField: " + this.charField 
        + ", shortField: " + this.shortField + " ]";
  }
}