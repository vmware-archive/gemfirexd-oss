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
package haOverflow;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import static com.gemstone.gemfire.cache.RegionShortcut.*;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;

import java.io.File;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.Properties;

/**
 * This class is a command-line application that allows the user to run, verify
 * and experiment with the HA Overflow features
 * 
 * @author GemStone Systems, Inc.
 * @since 5.7
 */
public class HACacheServer {

  /** Cache <code>Region</code> currently reviewed by this example */
  private Region currRegion;

  /** The cache used in the example */
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
        .println("A distributed system is created with properties loaded from your ");
    out
        .println("  gemfire.properties file.  You *should* specify alternative property");
    out
        .println("  files using -DgemfirePropertyFile=server_gemfire.properties");
    out
        .println("The declarative XML to use for building the cache can by default be");
    out.println("  server.xml");
    out.println("  ");
    out
        .println("Usage: java -DgemfirePropertyFile=server_gemfire.properties");
    out.println("            HACacheServer <cache.xml> ");
    out.println();
  }

  /**
   * Parses the command line and runs the <code>HACacheServer</code> example.
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      showHelp();
      System.out.println("Args are " + args[0] + ", " + args[1]);
      System.exit(1);
    }
    String xmlFileName = args[0];

    File xmlFile = new File(xmlFileName);
    if (!xmlFile.exists()) {
      System.err
          .println("Supplied Cache config file <cache.xml> does not exist");
      System.exit(1);

    }

    HACacheServer runner = new HACacheServer();
    runner.xmlFile = xmlFile;
    runner.initialize();
    System.exit(0);
  }

  /**
   * Initializes the <code>Cache</code> for this example program. Create a
   * root region if none is defined
   */
  void initialize() throws Exception {
    Properties props = new Properties();
    props.setProperty("name", "HAOverflowServer");
    if (this.xmlFile != null) {
      props.setProperty("cache-xml-file", this.xmlFile.toString());
    }

    // create the directory where data is going to be stored in case of
    // ha-overflow
    // File dir = new File("backupDirectory");
    // dir.mkdir();

    this.cache = new CacheFactory(props).create();
    Iterator rIter = this.cache.rootRegions().iterator();
    if (rIter.hasNext()) {
      this.currRegion = (Region)rIter.next();
    }
    else {
      /* If no root region exists, create one with default attributes */
      cache.getLogger().info("No root region in cache. Creating a root, +"
          + "'root'\nfor cache access.\n");
      RegionFactory rf = cache.createRegionFactory(REPLICATE);
      currRegion = rf.create("root");
      cache.getLogger().fine("Root region:\"" + currRegion + "\" has been created.");
    }

    cache.getLogger().info("Initialized");

    while (System.in.read() != -1)
      ;

    System.exit(0);
  }
}
