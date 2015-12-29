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
package parReg.query;

//import util.*;
//import hydra.*;
//import util.CacheUtil;
//import util.StopStartBB;
//import hydra.TestConfig;
import com.gemstone.gemfire.pdx.PdxSerializable;

import java.util.Map;

import objects.Portfolio;
import pdx.PdxTest;
import util.TestException;
import hydra.blackboard.Blackboard;

//import java.util.*;

//import query.QueryPrms;
//import query.QueryTest;
//import com.gemstone.gemfire.cache.*;

public class ParRegQueryBB extends Blackboard {
   
// Blackboard creation variables
static String PARREG_QUERY_BB_NAME = "ParReg_Query_Blackboard";
static String PARREG_QUERY_BB_TYPE = "RMI";

public static ParRegQueryBB bbInstance = null;

public static int operationCount;
public static int stopStartVmsCounter;
public static int stopStartSignal;
public static int queryCount;

/**
 *  Get the QueryBB.
 */
public static ParRegQueryBB getBB() {
  if (bbInstance == null) {
    synchronized ( ParRegQueryBB.class ) {
       if (bbInstance == null) 
          bbInstance = new ParRegQueryBB(PARREG_QUERY_BB_NAME, PARREG_QUERY_BB_TYPE);
    }
  }
  return bbInstance;  
}
   
/**
 *  Initialize the QueryBB
 *  This saves caching attributes in the blackboard that must only be
 *  read once per test run.
 */
public static void HydraTask_initialize() {
  //do nothing
}

public void initialize(String key) {
  hydra.blackboard.SharedMap aMap = getBB().getSharedMap();
  
  aMap.put(key, new NewPortfolio());
}

/**
 * To write the portofolio onto the shared map.
 * @param key
 * @param portfolio
 */
public static void putPortfolio(String key, NewPortfolio portfolio) {
   hydra.blackboard.SharedMap aMap = getBB().getSharedMap();
   aMap.put(key, portfolio);
}

/**
 * To get the portfolio from the shared map.
 * @param key
 * @return
 */
public static NewPortfolio getPortfolio(String key) {
   return ((NewPortfolio)(ParRegQueryBB.getBB().getSharedMap().get(key)));
}

//  Zero-arg constructor for remote method invocations.
public ParRegQueryBB() {
}
   
/**
 *  Creates a sample blackboard using the specified name and transport type.
 */
public ParRegQueryBB(String name, String type) {
   super(name, type, ParRegQueryBB.class);
}

/** Method to put a value that might be PdxSerializable.
 *  PdxSerializable objects cannot be put into the blackboard because
 *  MasterController does not have these domain classes on its classpath.
 *  So we create a map containing a mapping of instance fields to instance
 *  field values and put that in the shared map. 
 * @param key
 * @param value
 */
public static void putToSharedMap(Object key, Object value) {
  String className = value.getClass().getName();
  if (className.equals("parReg.query.PdxVersionedNewPortfolio") ||
      className.equals("parReg.query.VersionedNewPortfolio")) {
    Map fieldMap = ((NewPortfolio)value).createPdxHelperMap();
    getBB().getSharedMap().put(key, fieldMap);
  } else { // not a PdxSerializable
    getBB().getSharedMap().put(key, value);
  }
}

/** Method to get a value from the shared map. The value might be a 
 *  Map generated with putToSharedMap(key, value) that represents a
 *  PdxSerializable. If so, convert the Map to the proper instance of
 *  PdxSerializable and return it. 
 * @param key
 * @return
 */
public static Object getFromSharedMap(Object key) {
  Object value = getBB().getSharedMap().get(key);
  if (value instanceof Map) { // this was a PdxSerializable field map created by putToSharedMap
    Map fieldMap = (Map)value;
    String className = (String)fieldMap.get("className");
    NewPortfolio npf = PdxTest.getVersionedNewPortfolio(className, (String)(fieldMap.get("name")), (Integer)(fieldMap.get("id")));
    npf.restoreFromPdxHelperMap(fieldMap);
    return npf;
  } else {
    return value;
  }
}

}
