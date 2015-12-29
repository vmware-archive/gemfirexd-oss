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
package com.gemstone.gemfire.management.internal.cli.domain;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;


/**
 * Domain object used for Data Commands Functions
 * @author tushark
 * 
 * TODO : Implement DataSerializable
 *
 */
public class DataCommandRequest implements /*Data*/ Serializable{
  
  
  private String command;
  private String key;
  private String value;
  private boolean putIfAbsent;
  private String keyClass;
  private String valueClass;
  private String regionName;
  private String removeAllKeys;
  private boolean recursive;
  private String query;
  
  public static final String NEW_LINE = System.getProperty("line.separator");
  
  @Override
  public String toString(){
    StringBuilder sb = new StringBuilder();
    if(isGet()){
      sb.append(" Type  : Get").append(NEW_LINE);
      sb.append(" Key  : ").append(key).append(NEW_LINE);
      sb.append(" RegionName  : ").append(regionName).append(NEW_LINE);
    }else if(isLocateEntry()){
      sb.append(" Type  : Locate Entry").append(NEW_LINE);
      sb.append(" Key  : ").append(key).append(NEW_LINE);
      sb.append(" RegionName  : ").append(regionName).append(NEW_LINE);
      sb.append(" Recursive  : ").append(recursive).append(NEW_LINE);
    }else if(isPut()){
      sb.append(" Type  : Put");
      sb.append(" Key  : ").append(key).append(NEW_LINE);
      sb.append(" putIfAbsent  : ").append(putIfAbsent).append(NEW_LINE);
      sb.append(" Value  : ").append(value).append(NEW_LINE);
      sb.append(" RegionName  : ").append(regionName).append(NEW_LINE);
    }else if(isRemove()){
      sb.append(" Type  : Remove");
      sb.append(" Key  : ").append(key).append(NEW_LINE);
      sb.append(" removeAllKeys  : ").append(removeAllKeys).append(NEW_LINE);
      sb.append(" RegionName  : ").append(regionName).append(NEW_LINE);
    }else if(isSelect()){
      sb.append(" Type  : SELECT");
      sb.append(" Query  : ").append(query).append(NEW_LINE);
    }
    return sb.toString();
  }
  
  public boolean isGet(){
    if(CliStrings.GET.equals(command))
      return true;
    else return false;
  }
  
  public boolean isPut(){
    if(CliStrings.PUT.equals(command))
      return true;
    else return false;
  }
  
  public boolean isRemove(){
    if(CliStrings.REMOVE.equals(command))
      return true;
    else return false;
  }
  
  public boolean isLocateEntry(){
    if(CliStrings.LOCATE_ENTRY.equals(command))
      return true;
    else return false;
  }
  
  public boolean isSelect(){
    if(CliStrings.QUERY.equals(command))
      return true;
    else return false;
  }
  
  public String getQuery() {
    return query;
  }

  public void setQuery(String query) {
    this.query = query;
  }

  public String getCommand() {
    return command;
  }
  public String getKey() {
    return key;
  }
  public String getValue() {
    return value;
  }
  public boolean isPutIfAbsent() {
    return putIfAbsent;
  }
  public String getKeyClass() {
    return keyClass;
  }
  public String getValueClass() {
    return valueClass;
  }
  public String getRegionName() {
    return regionName;
  }
  public String getRemoveAllKeys() {
    return removeAllKeys;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public void setValue(String value) {
    this.value = value;
  }

  public void setPutIfAbsent(boolean putIfAbsent) {
    this.putIfAbsent = putIfAbsent;
  }

  public void setKeyClass(String keyClass) {
    this.keyClass = keyClass;
  }

  public void setValueClass(String valueClass) {
    this.valueClass = valueClass;
  }

  public void setRegionName(String regionName) {
    this.regionName = regionName;
  }

  public void setRemoveAllKeys(String removeAllKeys) {
    this.removeAllKeys = removeAllKeys;
  }

  public void setCommand(String command) {
    this.command = command;
  }

  public boolean isRecursive() {
    return recursive;
  }

  public void setRecursive(boolean recursive) {
    this.recursive = recursive;
  }

  //@Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(command,out);
    DataSerializer.writeString(key,out);
    DataSerializer.writeString(value,out);
    DataSerializer.writeBoolean(putIfAbsent,out);
    DataSerializer.writeString(keyClass,out);
    DataSerializer.writeString(valueClass,out);
    DataSerializer.writeString(regionName,out);
    DataSerializer.writeString(removeAllKeys,out);
    DataSerializer.writeBoolean(recursive,out);    
  }

  //@Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {    
    command = DataSerializer.readString(in);
    key = DataSerializer.readString(in);
    value = DataSerializer.readString(in);
    putIfAbsent = DataSerializer.readBoolean(in);
    keyClass = DataSerializer.readString(in);
    valueClass = DataSerializer.readString(in);
    regionName = DataSerializer.readString(in);
    removeAllKeys = DataSerializer.readString(in);
    recursive = DataSerializer.readBoolean(in);     
  }  
  
  

}
