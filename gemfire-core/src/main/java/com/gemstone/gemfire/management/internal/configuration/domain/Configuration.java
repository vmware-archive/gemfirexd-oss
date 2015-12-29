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
package com.gemstone.gemfire.management.internal.configuration.domain;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;


public class Configuration implements DataSerializable{
  
  private static final long serialVersionUID = 1L;
  private String configName;
  private String cacheXmlContent;
  private String cacheXmlFileName;
  private String propertiesFileName;
  private Properties gemfireProperties = new Properties();
  boolean isCacheXmlModified, isPropertiesChanged, isJarChanged;
  Set<String> jarNames = new HashSet<String>();
  
  public boolean isCacheXmlModified() {
    return this.isCacheXmlModified;
  }
  
  public boolean isPropertiesFileChanged() {
    return this.isPropertiesChanged;
  }
  
  //Public no arg constructor required for Deserializable
  public Configuration() {
    
  }
  
  public Configuration(String configName) {
    this.configName = configName;
    this.cacheXmlFileName = configName + ".xml";
    this.setPropertiesFileName(configName + ".properties");
  }
  
  public void resetModifiedFlags() {
    isCacheXmlModified = isPropertiesChanged = isJarChanged = false;
  }
  
  public String getCacheXmlContent() {
    return cacheXmlContent;
  }
  
  public void setCacheXmlContent(String cacheXmlContent) {
    this.isCacheXmlModified = true;
    this.cacheXmlContent = cacheXmlContent;
  }
  
  public String getCacheXmlFileName() {
    return cacheXmlFileName;
  }
  
  public void setCacheXmlFileName(String cacheXmlFileName) {
    this.cacheXmlFileName = cacheXmlFileName;
  }
  
  public Properties getGemfireProperties() {
    return gemfireProperties;
  }
  
  public void setGemfireProperties(Properties gemfireProperties) {
    this.isPropertiesChanged = true;
    this.gemfireProperties = gemfireProperties;
  }
  
  public String getConfigName() {
    return configName;
  }
  
  public void setConfigName(String configName) {
    this.configName = configName;
  }

  public String getPropertiesFileName() {
    return propertiesFileName;
  }

  public void setPropertiesFileName(String propertiesFileName) {
    this.propertiesFileName = propertiesFileName;
  }
 
  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(configName, out);
    DataSerializer.writeString(cacheXmlFileName, out);
    DataSerializer.writeString(cacheXmlContent, out);
    DataSerializer.writeString(propertiesFileName, out);
    DataSerializer.writeProperties(gemfireProperties, out);
    DataSerializer.writeBoolean(isCacheXmlModified, out);
    DataSerializer.writeBoolean(isPropertiesChanged, out);
    DataSerializer.writeBoolean(isJarChanged, out);
    DataSerializer.writeHashSet((HashSet<?>) jarNames, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.configName = DataSerializer.readString(in);
    this.cacheXmlFileName = DataSerializer.readString(in);
    this.cacheXmlContent = DataSerializer.readString(in);
    this.propertiesFileName = DataSerializer.readString(in);
    this.gemfireProperties = DataSerializer.readProperties(in);
    this.isCacheXmlModified = DataSerializer.readBoolean(in);
    this.isPropertiesChanged = DataSerializer.readBoolean(in);
    this.isJarChanged = DataSerializer.readBoolean(in); 
    this.jarNames = DataSerializer.readHashSet(in);
  }
  
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("Config Name : ").append(this.configName);
    sb.append("\nCacheXml Content : \n").append(this.cacheXmlContent);
    sb.append("\nProperties : \n").append(this.gemfireProperties.toString());
    return sb.toString();
  }
  
  @Override
  public int hashCode() {
    return configName.hashCode();
  }
  
  @Override
  public boolean equals(Object obj) {
    return super.equals(obj);
  }
}
