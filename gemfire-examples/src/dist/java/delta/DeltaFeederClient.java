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

import cacheRunner.LoggingCacheWriter;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;

import java.io.File;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.Properties;

/**
 * This class is a command-line application that functions as a feeder Client in
 * the system and allows the user to run, verify and experiment with the delta
 * propogation features
 * 
 * @author GemStone Systems, Inc.
 * @since 6.1
 */
@SuppressWarnings("synthetic-access")
public class DeltaFeederClient {

  /** Client <code>Region</code> on which operations are performed */
  private static Region currRegion;

  /** <code>ClientCache</code> used in this example */
  private ClientCache cache;

  /** The cache.xml file used to declaratively configure the cache */
  private File xmlFile = null;

  private static final int NUMBER_OF_FEEDER_THREADS = 5;
  
  private static Thread[] feedThread = new Thread[NUMBER_OF_FEEDER_THREADS];

  private static final String THREAD_NAME_SUFFIX = "FEEDER_THREAD";

  private static final String SYNC_FEED_OPTION = "synchronized";

  private static final String NESTED_FEED_OPTION = "nested";
  
  private static final String COLLECTION_FEED_OPTION = "collection";

  private static final String KEY_STRING = "_key";

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
        .println("The declarative XML to use for building the cache is feederClient.xml, when cloning is not enabled.\n");
    out
    .println("  To enable cloning use 'cloningEnabledOnFeederClient.xml' instead.\n" );
    out.println("Usage: java -DgemfirePropertyFile=gemfire.properties");
    out.println("            DeltaFeederClient [feederClient.xml|cloningEnabledOnFeederClient.xml> <simple|synchronized|nested|collection]");
    out.println();
  }

  /**
   * Parses the command line and runs the <code>Delta Propagation</code>
   * example.
   */
  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      showHelp();
      System.exit(1);
    }
    
    String xmlFileName = args[0];
    
    String option = "";
    if (args.length > 1) {
      option = args[1];
    }
    
    File xmlFile = new File(xmlFileName);
    if (!xmlFile.exists()) {
      System.err.println("Cache config file " + xmlFileName
          + " does not exist");
      System.exit(1);
    }

    DeltaFeederClient runner = new DeltaFeederClient();
    runner.xmlFile = xmlFile;
     
    runner.initialize();
    
    //  Set the class implementing the Delta interface that will be used
    if(NESTED_FEED_OPTION.compareToIgnoreCase(option) == 0) { 
      runner.doNestedDeltaFeed();
      feedThread[0].join();
    }
    else if(SYNC_FEED_OPTION.compareToIgnoreCase(option) == 0) {
      // start all the feeders first
      for (int i = 0; i < NUMBER_OF_FEEDER_THREADS; i++) {
        runner.doConcurrentDeltaFeed(i);
      }
      // wait for feeders to finish
      for (int i = 0; i < NUMBER_OF_FEEDER_THREADS; i++) {
        feedThread[i].join();
      } 
    }
    else if(COLLECTION_FEED_OPTION.compareToIgnoreCase(option) == 0) {
      runner.doCollectionDeltaFeed();
      feedThread[0].join();
    }
    else {
      runner.doSimpleDeltaFeed();
      feedThread[0].join();
    }
    
    // wait for feeder threads to complete if feeder threads are active
    runner.shutdown();

    System.exit(0);
  }

  private void doConcurrentDeltaFeed(int i) {
    feedThread[i] = new Thread(new SyncDeltaFeed());
    feedThread[i].setName(THREAD_NAME_SUFFIX + "_" + i);
    feedThread[i].start();
  }

  private void doNestedDeltaFeed() {
    feedThread[0] = new Thread(new NestedDeltaFeed());
    feedThread[0].setName(THREAD_NAME_SUFFIX + "_" + 0);
    feedThread[0].start();
  }

  private void doSimpleDeltaFeed() {
    feedThread[0] = new Thread(new SimpleDeltaFeed());
    feedThread[0].setName(THREAD_NAME_SUFFIX + "_" + 0);
    feedThread[0].start();
  }
  
  
  private void doCollectionDeltaFeed() {
    feedThread[0] = new Thread(new CollectionDeltaFeed());
    feedThread[0].setName(THREAD_NAME_SUFFIX + "_" + 0);
    feedThread[0].start();
  }
  /**
   * Initializes the <code>Cache</code> for this example program. Uses the
   * {@link LoggingCacheWriter}.
   */
  private void initialize() throws Exception {
    Properties props = new Properties();
    if (this.xmlFile != null) {
      props.setProperty("cache-xml-file", this.xmlFile.toString());
    }

      this.cache = new ClientCacheFactory(props).create();
      
      Iterator rIter = this.cache.rootRegions().iterator();
      if (rIter.hasNext()) {
        currRegion = (Region)rIter.next();
      }
      // region supposed to get initialised at this point
      assert currRegion != null; 

      System.out.println("Region name is " + currRegion.getFullPath());
  }

  private void shutdown() throws Exception {
    this.cache.close();
  }

  /*
   * Synchronized delta object feed
   */
  private class SyncDeltaFeed implements Runnable {

    public void run() {
      // create the object
      long lStart = Thread.currentThread().getId();
      double dStart = Math.random();
      SynchronizedDelta deltaObj = new SynchronizedDelta(lStart, dStart);
      System.out.println(Thread.currentThread().getName() + " create "
          + KEY_STRING + ", value=" + deltaObj);
      currRegion.put(KEY_STRING, deltaObj);
      
      // update the object
      for (int i = 0; i < 25; i++) {
        int remainder = i % 3;
        switch(remainder) {
          // change both fields
          case 0: {
            deltaObj.setLongVal(System.nanoTime());
            deltaObj.setDoubleVal(Math.random());
            System.out.println(Thread.currentThread().getName() + "  putting "
                + KEY_STRING + ", value=" + deltaObj);
            
            break;
          }
          
          // only change the int field
          case 1: {
            deltaObj.setLongVal(System.nanoTime());
            System.out.println(Thread.currentThread().getName() + " putting "
                + KEY_STRING + ", value=" + deltaObj);
            
            break;
          }
          
          // only change the double field
          case 2: {
            deltaObj.setDoubleVal(Math.random());
            System.out.println(Thread.currentThread().getName() + " putting "
                + KEY_STRING + ", value=" + deltaObj);
            break;
          }
          
          default:
            break;
        }
        
        currRegion.put(KEY_STRING, deltaObj);
        
      }
    }

  }

  /*
   * Nested delta object feed
   */
  private class NestedDeltaFeed implements Runnable {

    public void run() {
      
      // create the object
      int iStart = Integer.MAX_VALUE;
      NestedDelta.NestedType nestedDelta = new NestedDelta.NestedType();
      NestedDelta delta = new NestedDelta(iStart);
      
      currRegion.put(KEY_STRING, delta);
      
      // update the object
      for (int i = 1; i < 25; i++) {
        int remainder = i % 3;
        switch(remainder) {
          // change fields int, NestedType's boolean and int 
          case 0: {
            delta.setIntVal(i);
            nestedDelta.setIdent(i);
            nestedDelta.setSwitch(true);
            delta.setNestedDelta(nestedDelta);
            System.out.println(Thread.currentThread().getName() + " putting "
                + KEY_STRING + ", value=" + delta);
            
            break;
          }
          
          // change fields int, NestedType's boolean field
          case 1: {
            delta.setIntVal(i);
            nestedDelta.setSwitch(false);
            delta.setNestedDelta(nestedDelta);
            System.out.println(Thread.currentThread().getName() + " putting "
                + KEY_STRING + ", value=" + delta);
            
            break;
          }
          
          // change NestedType's int field
          case 2: {
            nestedDelta.setIdent(i);
            delta.setNestedDelta(nestedDelta);
            System.out.println(Thread.currentThread().getName() + " putting "
                + KEY_STRING + ", value=" + delta);
            
            break;
          }
          
          default:
            break;
        }
        
        currRegion.put(KEY_STRING, delta);
        
      }
    }
  }
  
  /*
   * Simple object feed
   */
  private class SimpleDeltaFeed implements Runnable {

    public void run() {
      // create the object
      int iStart = Integer.MAX_VALUE;
      double dStart = Math.random();
      SimpleDelta deltaObj = new SimpleDelta(iStart, dStart);
      currRegion.put(KEY_STRING, deltaObj);
      
      // update the object
      for (int i = 1; i < 25; i++) {
        int remainder = i % 3;
        switch(remainder) {
          // change both fields
          case 0: {
            deltaObj.setIntVal(i);
            deltaObj.setDoubleVal(i);
            System.out.println(Thread.currentThread().getName() + "  putting "
                + KEY_STRING + ", value=" + deltaObj);
            
            break;
          }
          
          // only change the int field
          case 1: {
            deltaObj.setIntVal(i);
            System.out.println(Thread.currentThread().getName() + " putting "
                + KEY_STRING + ", value=" + deltaObj);
            
            break;
          }
          
          // only change the double field
          case 2: {
            deltaObj.setDoubleVal(i);
            System.out.println(Thread.currentThread().getName() + " putting "
                + KEY_STRING + ", value=" + deltaObj);
            break;
          }
          
          default:
            break;
        }
        
        currRegion.put(KEY_STRING, deltaObj);
     
      }
    }
  }
  
  private class CollectionDeltaFeed implements Runnable {

    public void run() {
      // Initialize with 25 keys in the collection
      DeltaCollection deltaCollection = new DeltaCollection(25);
      currRegion.put(KEY_STRING, deltaCollection);

      // update,add,remove from map
      for (int i = 0; i < 25; i++) {
        int remainder = i % 3;
        switch (remainder) {
          // remove map entry
          case 0: {
            deltaCollection.removeFromMap(KEY_STRING + i);
            break;
          }
          
          // change map entry
          case 1: {
            SimpleDelta deltaObj = new SimpleDelta();
            deltaObj.setDoubleVal(i);
            deltaCollection.addToMap(KEY_STRING + i, deltaObj);
        
            break;
          }

          // Add map entry
          case 2: {
            SimpleDelta deltaObj = new SimpleDelta(i, i);
            deltaCollection.addToMap(KEY_STRING + i + "_added", deltaObj);

            break;
          }
          
          default:
            break;
        }
      }

      currRegion.put(KEY_STRING, deltaCollection);
    }
  }
}
