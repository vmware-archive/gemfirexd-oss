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
package query.common;

//import util.*;
//import hydra.*;
import com.gemstone.gemfire.pdx.PdxSerializable;
import com.gemstone.gemfire.pdx.PdxInstance;

import java.util.Map;

import objects.Portfolio;
import parReg.query.NewPortfolio;
import pdx.PdxTestVersionHelper;

import hydra.Log;
import hydra.blackboard.Blackboard;
//import com.gemstone.gemfire.cache.*;

public class QueryBB extends Blackboard {
   
// Blackboard creation variables
static String QUERY_BB_NAME = "Query_Blackboard";
static String QUERY_BB_TYPE = "RMI";

// singleton instance of blackboard
static public QueryBB bbInstance = null;

static String QUERY_VALIDATOR = "QueryValidator";

static String COUNT_STAR_RESULTS = "counStartResults";
/**
 *  Get the QueryBB
 */
public static QueryBB getBB() {
   if (bbInstance ==  null) {
      synchronized ( QueryBB.class ) {  
         if (bbInstance == null) 
            bbInstance = new QueryBB(QUERY_BB_NAME, QUERY_BB_TYPE);
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
   hydra.blackboard.SharedMap aMap = getBB().getSharedMap();
   aMap.put(QUERY_VALIDATOR, new QueryValidator());
}

public static void putQueryValidator(QueryValidator qv) {
   hydra.blackboard.SharedMap aMap = getBB().getSharedMap();
   if (qv.getValue() != null) {
     // cannot put a versioned test object into the bb because MasterController
     // will try to deserialize it and MasterController doesn't have this
     // object's class on its classpath
     Object anObj = qv.getValue();
     if (anObj instanceof PdxInstance) {
       anObj = PdxTestVersionHelper.toBaseObject(anObj);
     }
     String className = anObj.getClass().getName();
     if (className.equals("objects.PdxVersionedPortfolio") ||
         className.equals("objects.VersionedPortfolio")) {
       Portfolio portfolio = (Portfolio)anObj;
       Map infoMap = portfolio.createPdxHelperMap();
       qv.setValue(infoMap);
     } else if (className.equals("parReg.query.PdxVersionedNewPortfolio") ||
                className.equals("parReg.query.VersionedNewPortfolio")) {
       NewPortfolio portfolio = (NewPortfolio)anObj;
       Map infoMap = portfolio.createPdxHelperMap();
       qv.setValue(infoMap);
     }
   }
   Log.getLogWriter().info("Putting into bb at key " + QUERY_VALIDATOR + " value of QueryValidator: " + qv.getValue());
   aMap.put(QUERY_VALIDATOR, qv);
}

public static QueryValidator getQueryValidatorObject() {
   return ((QueryValidator)(QueryBB.getBB().getSharedMap().get(QueryBB.QUERY_VALIDATOR)));
}

//  Zero-arg constructor for remote method invocations.
public QueryBB() {
}
   
/**
 *  Creates a sample blackboard using the specified name and transport type.
 */
public QueryBB(String name, String type) {
   super(name, type, QueryBB.class);
}

public static void putCountStarResults(int[][] results) {
  hydra.blackboard.SharedMap aMap = getBB().getSharedMap();
  Log.getLogWriter().info("Putting into bb at key " + COUNT_STAR_RESULTS + " value of count-star results: " + results);
  aMap.put(COUNT_STAR_RESULTS, results);
}

public static int[][] getCountResultsObject() {
  return ((int[][])(QueryBB.getBB().getSharedMap().get(QueryBB.COUNT_STAR_RESULTS)));
}
}
