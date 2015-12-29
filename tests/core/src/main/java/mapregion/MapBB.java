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
/*
 * MapBB.java
 *
 * Created on September 19, 2005, 4:29 PM
 */

package mapregion;

/**
 *
 * @author  prafulla
 */

//import util.*;
import hydra.*;
import hydra.blackboard.Blackboard;
//import com.gemstone.gemfire.cache.*;


public class MapBB extends Blackboard{
    
    // Blackboard creation variables
    static String MAP_BB_NAME = "Map_Blackboard";
    static String MAP_BB_TYPE = "RMI";

    // singleton instance of blackboard
    private static MapBB bbInstance = null;
    
    // Key in the blackboard sharedMap for keeping track of region names currently in use
    static String CURRENT_REGION_NAMES = "CurrentRegionNames";
    
    // Counters for number of times test did certain operations
    public static int NUM_PUT;
    //public static int NUM_UPDATE;
    public static int NUM_PUTALL;
    public static int NUM_DESTROY;
    public static int NUM_REMOVE;
    public static int NUM_CLEAR;
    public static int NUM_INVALIDATE;
    public static int NUM_REGION_DESTROY;
    public static int NUM_REGION_INVALIDATE;
    public static int NUM_LOCAL_DESTROY;
    public static int NUM_LOCAL_INVALIDATE;
    public static int NUM_LOCAL_REGION_DESTROY;
    public static int NUM_LOCAL_REGION_INVALIDATE;
    public static int NUM_LOCAL_CLEAR;
    public static int NUM_CLOSE;
    public static int NUM_SIZE;
    public static int NUM_ISEMPTY;
    public static int NUM_CONTAINSVALUE;
    public static int NUM_KEYSET;
    
    /** Zero-arg constructor for remote method invocations. */
    
    public MapBB() {
    }
    
    /**
     *  Creates a sample blackboard using the specified name and transport type.
     */
    public MapBB(String name, String type) {
        super(name, type, MapBB.class);
    }
    
    /**
     *  Get the EventBB
     */
    public static MapBB getBB() {
        if (bbInstance == null) {
           synchronized ( MapBB.class ) {
              if (bbInstance == null)
                 bbInstance = new MapBB(MAP_BB_NAME, MAP_BB_TYPE);
           }
        }
        return bbInstance;
    }
    
    /**
     * Sets the value count with the given name to zero.
     */
    public static void zero(String counterName, int counter) {
        MapBB.getBB().getSharedCounters().zero(counter);
        Log.getLogWriter().info(counterName + " has been zeroed");
    }
    
    /**
     * Increments the counter with the given name
     */
    public static long incrementCounter(String counterName, int counter) {
        long counterValue = MapBB.getBB().getSharedCounters().incrementAndRead(counter);
        //Log.getLogWriter().info("After incrementing, " + counterName + " is " + counterValue);
        return counterValue;
    }
    
    /**
     * Decrements the counter with the given name
     */
    public static long decrementCounter(String counterName, int counter) {
      long counterValue = MapBB.getBB().getSharedCounters().decrementAndRead(counter);
      return counterValue;
    }
    
    /**
     * Adds a given amount to the value of a given counter
     */
    public static long add(String counterName, int counter, int amountToAdd) {
        long counterValue = MapBB.getBB().getSharedCounters().add(counter, amountToAdd);
        //Log.getLogWriter().info("After adding " + amountToAdd + ", " + counterName + " is " + counterValue);
        return counterValue;
    }
    
}//end of MapBB
