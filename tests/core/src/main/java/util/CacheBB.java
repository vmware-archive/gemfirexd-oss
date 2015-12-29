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
package util;

//import util.*;
import hydra.*;
//import java.io.*;
import hydra.blackboard.Blackboard;
import com.gemstone.gemfire.cache.*;

public class CacheBB extends Blackboard {
   
// Blackboard variables
static String CACHE_BB_NAME = "Cache_Blackboard";
static String CACHE_BB_TYPE = "RMI";

// singleton instance of blackboard
private static CacheBB bbInstance = null;

// Blackboard shared map keys
static String SCOPE = "ScopeAttribute";
static String MIRROR = "MirrorAttribute";
static String DATA_POLICY = "DataPolicyAttribute";

// Counters for timings
public static int MAX_GET_TIME_NANOS;

/**
 *  Get the CacheBB
 */
public static CacheBB getBB() {
   if (bbInstance == null) {
     synchronized ( CacheBB.class ) {
        if (bbInstance == null) 
          bbInstance = new CacheBB(CACHE_BB_NAME, CACHE_BB_TYPE);
     }
   }
   return bbInstance;
}
   
   
/**
 *  Initialize the CacheBB
 *  This saves caching attributes in the blackboard that must only be
 *  read once per test run.
 */
public static boolean HydraTask_initialize() {
   getBB().initialize();
   return true;
}

/**
 *  Initialize the bb
 *  This saves caching attributes in the blackboard that must only be
 *  read once per test run.
 */
public void initialize() {
   hydra.blackboard.SharedMap aMap = this.getSharedMap();

   // set hydra params in map which should only be read once
   String scope = TestConfig.tab().stringAt(CachePrms.scopeAttribute, null); 
   String mirror = TestConfig.tab().stringAt(CachePrms.mirrorAttribute, null); 
   String dataPolicy = TestConfig.tab().stringAt(CachePrms.dataPolicyAttribute, null);
   if (scope != null) {
      aMap.put(SCOPE, TestConfig.tab().stringAt(CachePrms.scopeAttribute, null)); 
      Log.getLogWriter().info("Scope attribute is " + aMap.get(SCOPE));
   }
   if (mirror != null) {
      aMap.put(MIRROR, TestConfig.tab().stringAt(CachePrms.mirrorAttribute, null)); 
      Log.getLogWriter().info("Mirroring attribute is " + aMap.get(MIRROR));
   }
   if (dataPolicy != null) {
      aMap.put(DATA_POLICY, dataPolicy);
      Log.getLogWriter().info("DataPolicy attribute is " + aMap.get(DATA_POLICY));
   }

}

/**
 *  Return the scope attribute for this test run.
 */
public Scope getScopeAttribute() {
   hydra.blackboard.SharedMap aMap = this.getSharedMap();
   String attribute = (String)aMap.get(SCOPE);
   return TestHelper.getScope(attribute);
}

/**
 *  Return the mirroring attribute for this test run.
 */
public MirrorType getMirrorAttribute() {
   hydra.blackboard.SharedMap aMap = this.getSharedMap();
   String attribute = (String)aMap.get(MIRROR);
   return TestHelper.getMirrorType(attribute);
}

/**
 *  Return the dataPolicy attribute for this test run.
 */
public DataPolicy getDataPolicyAttribute() {
   hydra.blackboard.SharedMap aMap = this.getSharedMap();
   String attribute = (String)aMap.get(DATA_POLICY);
   return TestHelper.getDataPolicy(attribute);
}

/**
 *  Zero-arg constructor for remote method invocations.
 */
public CacheBB() {
}
   
/**
 *  Creates a sample blackboard using the specified name and transport type.
 */
public CacheBB(String name, String type) {
   super(name, type, CacheBB.class);
}
   
}
