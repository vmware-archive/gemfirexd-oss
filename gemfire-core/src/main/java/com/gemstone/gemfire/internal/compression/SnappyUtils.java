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
package com.gemstone.gemfire.internal.compression;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Properties;

import org.xerial.snappy.SnappyError;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.GemFireVersion;
import com.gemstone.gemfire.internal.LogWriterImpl;
import com.gemstone.gemfire.internal.PureLogWriter;
import com.gemstone.gemfire.internal.SharedLibrary;

/**
 * Internal utilities for manipulating where to load Snappy native library from.
 * 
 * @author Kirk Lund
 * @since 7.5
 */
public class SnappyUtils {
  public static final String SNAPPY_SYSTEM_PROPERTIES_FILE = "org-xerial-snappy.properties";
  public static final String KEY_SNAPPY_LIB_PATH = "org.xerial.snappy.lib.path";
  public static final String KEY_SNAPPY_LIB_NAME = "org.xerial.snappy.lib.name";
  public static final String KEY_SNAPPY_TEMPDIR = "org.xerial.snappy.tempdir";
  public static final String KEY_SNAPPY_USE_SYSTEMLIB = "org.xerial.snappy.use.systemlib";
  public static final String KEY_SNAPPY_DISABLE_BUNDLED_LIBS = "org.xerial.snappy.disable.bundled.libs";
  
  public static final String DISTRIBUTED_VERSION = "1.0.4.1";
  public static final String UNKNOWN_VERSION = "unknown";

  /**
   * Attempts to set system properties to have Snappy use the Snappy native library in GemFire product/lib.
   * @return array of system property names that were set if any
   */
  public static String[] setSnappySystemProperties(LogWriter logWriter) {
    String version = getNativeLibraryVersion();
    if (UNKNOWN_VERSION.equals(version) || !DISTRIBUTED_VERSION.contains(version)) {
      logWriter.info("Snappy native library version " + version + " is different than version " + DISTRIBUTED_VERSION + " distributed with GemFire.");
      return new String[]{};
    }
    
    boolean hasSnappySystemProperties = Boolean.getBoolean(SNAPPY_SYSTEM_PROPERTIES_FILE) 
        || Boolean.getBoolean(KEY_SNAPPY_LIB_PATH) 
        || Boolean.getBoolean(KEY_SNAPPY_LIB_NAME) 
        || Boolean.getBoolean(KEY_SNAPPY_TEMPDIR) 
        || Boolean.getBoolean(KEY_SNAPPY_USE_SYSTEMLIB) 
        || Boolean.getBoolean(KEY_SNAPPY_DISABLE_BUNDLED_LIBS);
    if (hasSnappySystemProperties) {
      logWriter.info("One or more Snappy system properties are already set.");
      return new String[]{};
    }
    
    String failureMessage = "Unable to find Snappy native library distributed with GemFire: ";
    
    StringBuffer sb = new StringBuffer("snappy-java-").append(DISTRIBUTED_VERSION).append("-");
    if (SharedLibrary.is64Bit()) {
      sb.append("amd64");
    } else {
      sb.append("i386");
    }
    
    String libraryName = sb.toString();
    String libraryDir = null;
    
    URL gemfireJarURL = GemFireVersion.getJarURL();
    if (gemfireJarURL == null) {
      logWriter.info(new StringBuilder(failureMessage).append("Unable to locate gemfire.jar file.").toString());
      return new String[]{};
    }       

    String gemfireJar = null;
    try {
      gemfireJar = URLDecoder.decode(gemfireJarURL.getFile(), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      // this should never happen because UTF-8 is required to be implemented
      throw new RuntimeException(e);
    }
    int index = gemfireJar.lastIndexOf("/");
    if (index == -1) {
      logWriter.info(new StringBuilder(failureMessage).append("Unable to parse gemfire.jar path.").toString());
      return new String[]{};
    }
    
    libraryDir = gemfireJar.substring(0, index+1);
    String osLibraryName = System.mapLibraryName(libraryName);
    File libraryPath = new File(libraryDir, osLibraryName);
    File parentPath = libraryPath.getParentFile();
    String canonicalDir = null;
    try {
      canonicalDir = parentPath.getCanonicalPath();
    } catch (IOException e) {
      logWriter.info(new StringBuilder(failureMessage).append("Unable to get canonical path for " + parentPath).toString());
      return new String[]{};
    }
    
    logWriter.info("Checking for Snappy native library at " + libraryPath.getPath());      
    if (libraryPath.exists()) {
      logWriter.info("Found Snappy native library at " + libraryPath);
      String snappyNativeLibraryPath = canonicalDir;
      String snappyNativeLibraryName = osLibraryName;
      System.setProperty(KEY_SNAPPY_LIB_PATH, snappyNativeLibraryPath);
      System.setProperty(KEY_SNAPPY_LIB_NAME, snappyNativeLibraryName);
      return new String[] { KEY_SNAPPY_LIB_PATH, KEY_SNAPPY_LIB_NAME };
    } else {
      logWriter.info(new StringBuilder(failureMessage).append(libraryPath).toString());
      // do nothing -- let Snappy extract the native library
      return new String[]{};
    }
  }
  
  /**
   * Returns the Snappy native library version string without loading the native library.
   * @return Snappy native library version string
   */
  public static String getNativeLibraryVersion() {
    URL versionFile = SnappyError.class.getResource("/org/xerial/snappy/VERSION");
    String version = UNKNOWN_VERSION;
    try {
      if (versionFile != null) {
        Properties versionData = new Properties();
        versionData.load(versionFile.openStream());
        version = versionData.getProperty("version", version);
        if (version.equals(UNKNOWN_VERSION)) {
          version = versionData.getProperty("VERSION", version);
        }
        version = version.trim().replaceAll("[^0-9\\.]", "");
      }
    } catch (IOException e) {
    }
    return version;
  }
  
  public static LogWriter getOrCreateLogWriter() {
    InternalDistributedSystem ids = InternalDistributedSystem.getAnyInstance();
    if (ids == null || ids.getLogWriter() == null) {
      return new PureLogWriter(LogWriterImpl.levelNameToCode("info"));
    } else {
      return ids.getLogWriter();
    }
  }
}
