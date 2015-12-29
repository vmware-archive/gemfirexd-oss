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
package com.pivotal.gemfirexd.internal.tools.dataextractor.domain;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*****
 * Domain class that contains information about a server
 * @author jhuynh
 * @author bansods
 *
 */
public class ServerInfo {

  private String serverName;
  private List<String> diskStoreNames;
  private List<String> diskStoreDirectories;
  private String dataDictionaryLocation;
  private String serverDirectory;

  public static class Builder {
    private String serverName;
    private List<String> diskStoreNames;
    private List<String> diskStoreDirectories;
    private String dataDictionaryLocation;
    private String serverDirectory;

    public ServerInfo build() {
      return new ServerInfo(this);
    }

    public Builder serverName(String serverName) {
      this.serverName = serverName;
      return this;
    }

    public Builder diskStoreNames(List<String> diskStoreNames) {
      this.diskStoreNames = diskStoreNames;
      return this;
    }

    public Builder diskStoreDirs(List<String> diskStoreDirs) {
      this.diskStoreDirectories = new ArrayList<String>();
      Iterator<String> iterator = diskStoreDirs.iterator();
      while (iterator.hasNext()) {
        this.diskStoreDirectories.add(iterator.next().trim());
      }
      return this;
    }

    public Builder dataDictionaryLocation(String dataDictionaryLocation) {
      this.dataDictionaryLocation = dataDictionaryLocation;
      return this;
    }

    public Builder serverDirectory(String serverDirectory) {
      this.serverDirectory = serverDirectory;
      return this;
    }
  }

  private ServerInfo(Builder builder) {
    this.setServerName(builder.serverName);
    this.setServerDirectory(builder.serverDirectory);
    this.setDataDictionaryLocation(builder.dataDictionaryLocation);
    this.setDiskStoreDirectories(builder.diskStoreDirectories);
    this.setDiskStoreNames(builder.diskStoreNames);
  }
  
  
  public static Map<String, ServerInfo> createServerInfoList(String propFilePath) throws IOException {
    Map<String, ServerInfo> serverInfoMap = new HashMap<String, ServerInfo>();
    
    Properties props = new Properties();
    
    File file = new File(propFilePath);
    if (!file.exists()) {
      throw new FileNotFoundException("Properties file: " + propFilePath + " does not exist, please specify the properties file");
    }
    
    FileReader reader = new FileReader(file);
    try {
      props.load(reader);
      
      if (!props.isEmpty()) {
        Set<Object> keys = props.keySet();
        
        for (Object key : keys) {
          String serverName = (String)key;
          String diskStoreCSL = props.getProperty(serverName);
          String []diskDirs = diskStoreCSL.split(",");
          
          if (diskDirs != null && diskDirs.length > 0 ) {
            String serverDirectory = diskDirs[0];
            Builder builder = new Builder();
            builder.serverName(serverName);
            builder.serverDirectory(serverDirectory);
            
            //if (diskDirs.length > 1) {
              List<String> diskStoreDirs = Arrays.asList(diskDirs);//Arrays.asList(Arrays.copyOfRange(diskDirs, 1, diskDirs.length -1));
              builder.diskStoreDirs(diskStoreDirs);
            //}
            ServerInfo serverInfo = builder.build();
            serverInfoMap.put(serverName, serverInfo);
          }
        }
      }
    }
    finally {
      if (reader != null) {
        reader.close();
      }
    }
    return serverInfoMap;
  }


  public List<String> getDiskStoreNames() {
    return diskStoreNames;
  }


  public void setDiskStoreNames(List<String> diskStoreNames) {
    this.diskStoreNames = diskStoreNames;
  }


  public List<String> getDiskStoreDirectories() {
    return diskStoreDirectories;
  }


  public void setDiskStoreDirectories(List<String> diskStoreDirectories) {
    this.diskStoreDirectories = diskStoreDirectories;
  }


  public String getServerName() {
    return serverName;
  }


  public void setServerName(String serverName) {
    this.serverName = serverName;
  }


  public String getDataDictionaryLocation() {
    return dataDictionaryLocation;
  }


  public void setDataDictionaryLocation(String dataDictionaryLocation) {
    this.dataDictionaryLocation = dataDictionaryLocation;
  }


  public String getServerDirectory() {
    return serverDirectory;
  }


  public void setServerDirectory(String serverDirectory) {
    this.serverDirectory = serverDirectory;
  }
  
  
}
