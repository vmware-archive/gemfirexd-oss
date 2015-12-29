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
package com.gemstone.gemfire.management.internal.configuration.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.distributed.DistributedLockService;
import com.gemstone.gemfire.distributed.internal.locks.DLockService;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.management.internal.cli.CliUtil;
import com.gemstone.gemfire.management.internal.configuration.domain.Configuration;

/***
 * 
 * @author bansods
 *
 */
public class ConfigurationUtils {
  
  private final static String CONFIGURATION_LOCK_NAME = "configurationLock";
  private final static String CONFIGURATION_LOCK_SERVICE_NAME = "configurationLockServiceName";
  
  public static Properties readProperties(String propertiesFilePath) throws IOException {
    Properties properties = new Properties();
    FileInputStream fis = new FileInputStream(propertiesFilePath);
    properties.load(fis);
    fis.close();
    return properties;
  }
  
  /**
   * Returns an array containing the names of the subdirectories in a given directory
   * @param path Path of the directory whose subdirectories are listed
   * @return String[] names of first level subdirectories, null if no subdirectories are found or if the path is incorrect
   */
  public static String[] getSubdirectories(String path) {
    File directory = new File(path);
    return directory.list(DirectoryFileFilter.INSTANCE);
  }
  
  public static void main(String[] args) throws Exception {
    String [] paths = getSubdirectories("/kuwait1/users/bansods");
    for (String path : paths) {
      System.out.println(path);
    }
    
    Configuration configuration = new Configuration("cluster");
    writeConfig("/kuwait1/users/bansods/sd", configuration, true);
  }
  
  public static void writeConfig(String configDirPath, Configuration configuration, boolean forceWrite) throws Exception {
    File configDir = new File(configDirPath);
    if (!configDir.exists()) {
      configDir.mkdirs();
    }
    String dirPath = FilenameUtils.concat(configDirPath, configuration.getConfigName());
    File file = new File(dirPath);
    if (!file.exists()) {
      if (!file.mkdir()) {
        //TODO : Throw some exception instead of quietly returning 
        throw new Exception("Cannot create directory on this machine");
      }
    }
      
    if(configuration.isPropertiesFileChanged() || forceWrite) {
      writePropeties(dirPath, configuration);
    }
    if (configuration.isCacheXmlModified() || forceWrite) {
      writeCacheXml(dirPath, configuration);
    }
  }
  
  public static void writePropeties(String dirPath, Configuration configuration) {
    String fullPath = FilenameUtils.concat(dirPath,configuration.getPropertiesFileName());
    try {
      configuration.getGemfireProperties().store(new BufferedWriter(new FileWriter(fullPath)), "");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  /***
   * Writes the cache.xml to the file , based on Configuration
   * @param dirPath Path of the directory in which the configuration is written
   * @param configuration 
   */
  public static void writeCacheXml(String dirPath, Configuration configuration) {
    String fullPath = FilenameUtils.concat(dirPath,configuration.getCacheXmlFileName());
    try {
      FileUtils.writeStringToFile(new File(fullPath), configuration.getCacheXmlContent(), "UTF-8") ;
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  public static Map<String, Configuration> getSharedConfiguration(String configDirPath) {
    String []subdirectoryNames = getSubdirectories(configDirPath);
    Map<String, Configuration> sharedConfiguration = new HashMap<String, Configuration>();
    
    if (subdirectoryNames != null) {
      for (String subdirectoryName : subdirectoryNames) {
        String fullpath = FilenameUtils.concat(configDirPath, subdirectoryName);
        Configuration configuration = readConfiguration(subdirectoryName, fullpath);
        sharedConfiguration.put(subdirectoryName, configuration);
      }
    }
    return sharedConfiguration;
  }
  
  public static DistributedLockService createConfigLockService() {
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    return DLockService.create(CONFIGURATION_LOCK_SERVICE_NAME, cache.getDistributedSystem(), true, true, true);
  }
  
  public static boolean lockConfiguration() {
    boolean success = true;
    try {
      DistributedLockService dlockServ = DLockService.getServiceNamed(CONFIGURATION_LOCK_SERVICE_NAME);
      success = dlockServ.lock(CONFIGURATION_LOCK_NAME, 10000, 10000);
    } catch (Exception e) {
      logInfo("Error while acquiring the lock " + CliUtil.stackTraceAsString(e));
    }
    return success;
  }
  
  public static void unlockConfiguration() {
    logInfo("#SB unlocking the configuration");
    DLockService.getServiceNamed(CONFIGURATION_LOCK_SERVICE_NAME).unlock(CONFIGURATION_LOCK_NAME);
  }
  private static Configuration readConfiguration(String configName, String configDirectory) {
    Configuration configuration = new Configuration(configName);
    String cacheXmlFullPath = FilenameUtils.concat(configDirectory, configuration.getCacheXmlFileName());
    String propertiesFullPath = FilenameUtils.concat(configDirectory, configuration.getPropertiesFileName());
    configuration.setCacheXmlContent(XmlUtils.readXmlAsStringFromFile(cacheXmlFullPath));
    try {
      configuration.setGemfireProperties(readProperties(propertiesFullPath));
    } catch (IOException e) {
      e.printStackTrace();
    }
    return configuration;
  }
  
  private static void logInfo(String message) {
    LogWriter logger = getLogger();
    if (logger.infoEnabled()) {
      logger.info(message);
    }
  }
  private static void logInfo(Throwable e) {
    LogWriter logger = getLogger();
    if (logger.infoEnabled()) {
      logger.info(e);
    }
  }
  
  private static void logError(String message) {
    LogWriter logger = getLogger();
    if (logger.errorEnabled()) {
      logger.error(message);
    }
  }
  
  private static void logError(Throwable e) {
    LogWriter logger = getLogger();
    if (logger.errorEnabled()) {
      logger.error(e);
    }
  }
  
  private static LogWriter getLogger() {
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    return cache.getLogger();
  }
}
