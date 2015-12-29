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
package delta;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;

import java.io.File;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.Properties;

/**
 * This class is a command-line application that allows the user to run, verify
 * and experiment with the delta propagation features
 * 
 * @author GemStone Systems, Inc.
 * @since 6.1
 */
public class DeltaCacheServer {

  /** Server <code>Region</code> on which operations are performed */
  private Region currRegion;

  /** <code>Cache</code> used in this example */
 private Cache cache;

  /** The cache.xml file used to declaratively configure the cache */
  private File xmlFile = null;

  /**
   * Prints information on how this program should be used.
   */
  static void showHelp() {
    PrintStream out = System.out;

    out.println();
    out
        .println("A distributed system is created with properties loaded from a gemfire.properties file. ");
    out
        .println("  The file, server_gemfire.properties, is provided with the example. ");
    out
        .println("  You should specify this file by defining the 'gemfirePropertyFile' system property as -DgemfirePropertyFile=server_gemfire.properties");
    out
        .println("The declarative XML to use for the cache servers are server1.xml and server2.xml ");
    out.println(" To turn on cloning in the cache servers, use cloningEnabledOnServer1.xml and cloningEnabledOnServer2.xml instead\n");
    out
        .println("Usage: java -DgemfirePropertyFile=server_gemfire.properties");
    out.println("            DeltaCacheServer [server1.xml|server2.xml|cloningEnabledOnServer1.xml|cloningEnabledOnServer2.xml] \n");
  }

  /**
   * Parses the command line and runs the <code>DeltaCacheServer</code> example.
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      showHelp();
      System.exit(1);
    }
    
    String xmlFileName = args[0];

    File xmlFile = new File(xmlFileName);
    if (!xmlFile.exists()) {
      System.err.println("Supplied Cache config file " + xmlFileName
          + " does not exist");
      System.exit(1);

    }

    DeltaCacheServer runner = new DeltaCacheServer();
    runner.xmlFile = xmlFile;
    runner.initialize();
    System.exit(0);
  }

  /**
   * Initializes the <code>Cache</code> for this example program. Create a
   * root region if none is defined
   */
  private void initialize() throws Exception {
    Properties props = new Properties();
    props.setProperty("name", "DeltaCacheServer");
    if (this.xmlFile != null) {
      props.setProperty("cache-xml-file", this.xmlFile.toString());
    }

      this.cache = new CacheFactory(props).create();
      Iterator rIter = this.cache.rootRegions().iterator();
      if (rIter.hasNext()) {
        this.currRegion = (Region)rIter.next();
      }

      //  Verify that region is initialised 
      assert currRegion != null; 
      
      System.out.println("Initialized");

      while (System.in.read() != -1)
        ;

    System.exit(0);
  }
}
