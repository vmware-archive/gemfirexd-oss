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

import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;

import java.io.File;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.Properties;

/**
 * This class is a command-line application that functions as a reciver Client
 * in the system and allows the user to run, verify and experiment with the 
 * delta propogation features
 * 
 * @author GemStone Systems, Inc.
 * @since 6.1
 */
public class DeltaReceiverClient {
  /** Client <code>Region</code> on which operations are performed */
  private Region currRegion;

  /** <code>ClientCache</code> used in this example */
  private ClientCache cache;


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
        .println("  A file, 'client_gemfire.properties' is provided with the example. "); 

    out.println("  You should specify this file by defining the 'gemfirePropertyFile' system property as -DgemfirePropertyFile=client_gemfire.properties");
    out
        .println("The declarative XML to use for building the cache is receiverClient.xml, when cloning is not enabled.\n");
    out
    .println("  To enable cloning use 'cloningEnabledOnReceiverClient.xml' instead.\n" );
    out.println("Usage: java -DgemfirePropertyFile=gemfire.properties");
    out.println("            DeltaFeederClient [receiverClient.xml|cloningEnabledOnReceiverClient.xml]");
    out.println();
  }

  /**
   * Parses the command line and runs the <code>Delta Propagation</code> example.
   */
  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
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

    DeltaReceiverClient runner = new DeltaReceiverClient();
    runner.xmlFile = xmlFile;
    runner.initialize();
    
    while (System.in.read() != -1)
      ;
    
    runner.shutdown();

    System.exit(0);
  }

  /**
   * Initializes the <code>Cache</code> for this example program.
   */
  private void initialize() throws Exception {
    Properties props = new Properties();
    if (this.xmlFile != null) {
      props.setProperty("cache-xml-file", this.xmlFile.toString());
    }

      this.cache = new ClientCacheFactory(props).create();
      Iterator rIter = this.cache.rootRegions().iterator();
      if (rIter.hasNext()) {
        this.currRegion = (Region)rIter.next();
      }
      //region supposed to get initialised at this point
      assert currRegion != null; 
      
      currRegion.getAttributesMutator().addCacheListener(
          new CacheListenerAdapter() {
            @Override
            public void afterCreate(EntryEvent event) {
              System.out.println("After Create: " + event.getNewValue());
              System.out.println("-----------");
            }
            
            @Override
            public void afterUpdate(EntryEvent event) {
              System.out.println("After Update: " + event.getNewValue());
              System.out.println("-----------");
            }
          });
      this.currRegion.registerInterest("ALL_KEYS");
      System.out.println("Region name is " + this.currRegion.getFullPath());
  }
  /**
   * closes the cache
   */
  private void shutdown() throws Exception {
    this.cache.close();
  }
}
