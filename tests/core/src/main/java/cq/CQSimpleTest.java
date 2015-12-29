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
package cq;

import java.util.*;

import objects.Portfolio;
import hydra.*;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.util.*;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.query.*;

import util.*;

/** Class to test CQ issues that simple and need unit test like coverage.
 */
public class CQSimpleTest {
    
/* The singleton instance of CQSimpleTest in this VM */
static protected CQSimpleTest testInstance;
    
// static fields
protected static final String VmIDStr = "VmId_";

// instance fields
protected boolean isBridgeConfiguration;    // true if this test is being run in a bridge configuration, false otherwise
protected boolean isBridgeClient;           // true if this vm is a bridge client, false otherwise
protected QueryService qService;            // the QueryService in this VM
protected Region aRegion;                   // the region in this VM

// used to save errors in blackboard; then they are thrown in the close task
// this allows all tests to run, saving up errors as we go
protected static String KnownErrorsKey = "KnownErrors";
protected static String UnknownErrorsKey = "UnknownErrorsKey";

/** Creates and initializes the singleton instance of CQTest in a client.
 */
public synchronized static void HydraTask_initializeClient() {
   if (testInstance == null) {
      testInstance = new CQSimpleTest();
      testInstance.initializeRegion("clientRegion");
      testInstance.initializeInstance();
      if (testInstance.isBridgeConfiguration) {
         testInstance.isBridgeClient = true;
         testInstance.initializeQueryService();
         CQUtilBB.getBB().getSharedMap().put(KnownErrorsKey, "");
         CQUtilBB.getBB().getSharedMap().put(UnknownErrorsKey, "");
      }
   }
}
    
/** Creates and initializes a data store PR in a bridge server.
 */
public synchronized static void HydraTask_initializeBridgeServer() {
   if (testInstance == null) {
      testInstance = new CQSimpleTest();
      testInstance.initializeRegion("serverRegion");
      testInstance.initializeInstance();
      BridgeHelper.startBridgeServer("bridge");
      testInstance.isBridgeClient = false;
   }
}

/**
 *  Create a region with the given region description name.
 *
 *  @param regDescriptName The name of a region description.
 */
protected void initializeRegion(String regDescriptName) {
   CacheHelper.createCache("cache1");
   String key = VmIDStr + RemoteTestModule.getMyVmid();
   String xmlFile = key + ".xml";
   CacheHelper.generateCacheXmlFile("cache1", regDescriptName, xmlFile);
   aRegion = RegionHelper.createRegion(regDescriptName);
}

protected void initializeQueryService() {
   try {
      Log.getLogWriter().info("Creating QueryService.");
      String usingPool = TestConfig.tab().stringAt(CQUtilPrms.QueryServiceUsingPool, "false");
      boolean queryServiceUsingPool = Boolean.valueOf(usingPool).booleanValue();
      if (queryServiceUsingPool){
        Pool pool = PoolHelper.createPool(CQUtilPrms.getQueryServicePoolName());
        qService = pool.getQueryService();
        Log.getLogWriter().info("Initializing QueryService using Pool. PoolName: " + pool.getName());
      } else {
        qService = CacheHelper.getCache().getQueryService();
        Log.getLogWriter().info("Initializing QueryService using Cache.");
      }
      Log.getLogWriter().info("Done creating QueryService");
   } catch (Exception e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
}
    
/** Initialize instance fields of this test class */
protected void initializeInstance() {
   Vector bridgeNames = TestConfig.tab().vecAt(BridgePrms.names, null);
   isBridgeConfiguration = bridgeNames != null;
}

// ========================================================================
// hydra task methods
    
/** Hydra task for test min/max values of long/Longs.
 *  ref. bug 37119, Long.MIN_VALUE as long literal in queries
 */
public static void HydraTask_testMinMaxLongs() {
   StringBuffer errStr = new StringBuffer();
   String[] fieldNames = new String[] {"aPrimitiveLong", "aLong"};
   Object[] values = new Object[] {new Long(Long.MIN_VALUE), new Long(0), new Long(Long.MAX_VALUE)};
   ArrayList valuesToPutList = new ArrayList();
   for (int i = 0; i < values.length; i++) {
      QueryObject qo = new QueryObject(10, QueryObject.EQUAL_VALUES, -1, 1);
      qo.aLong = (Long)(values[i]);
      valuesToPutList.add(qo);
      qo = new QueryObject(10, QueryObject.EQUAL_VALUES, -1, 1);
      qo.aPrimitiveLong = ((Long)values[i]).longValue();
      valuesToPutList.add(qo);
   }
   Object[] valuesToPut = valuesToPutList.toArray();
   for (int i = 0; i < fieldNames.length; i++) {
      String fieldName = fieldNames[i];
      testInstance.testQuery(fieldName, ">",  "L", valuesToPut, values, new int[] {5, 4, 0});
      testInstance.testQuery(fieldName, "<",  "L", valuesToPut, values, new int[] {0, 1, 5});
      testInstance.testQuery(fieldName, ">=", "L", valuesToPut, values, new int[] {6, 5, 1});
      testInstance.testQuery(fieldName, "<=", "L", valuesToPut, values, new int[] {1, 2, 6});
      testInstance.testQuery(fieldName, "=",  "L", valuesToPut, values, new int[] {1, 1, 1});
      testInstance.testQuery(fieldName, "<>", "L", valuesToPut, values, new int[] {5, 5, 5});
      testInstance.testQuery(fieldName, "!=", "L", valuesToPut, values, new int[] {5, 5, 5});
   }
}

/** Hydra task for test min/max values of int/Integers.
 *  ref. bug 37119, Integer.MIN_VALUE as integer literal in queries
 */
public static void HydraTask_testMinMaxIntegers() {
   StringBuffer errStr = new StringBuffer();
   String[] fieldNames = new String[] {"aPrimitiveInt", "anInteger"};
   Object[] values = new Object[] {new Integer(Integer.MIN_VALUE), new Integer(0), new Integer(Integer.MAX_VALUE)};
   ArrayList valuesToPutList = new ArrayList();
   for (int i = 0; i < values.length; i++) {
      QueryObject qo = new QueryObject(10, QueryObject.EQUAL_VALUES, -1, 1);
      qo.anInteger = (Integer)(values[i]);
      valuesToPutList.add(qo);
      qo = new QueryObject(10, QueryObject.EQUAL_VALUES, -1, 1);
      qo.aPrimitiveInt = ((Integer)values[i]).intValue();
      valuesToPutList.add(qo);
   }
   Object[] valuesToPut = valuesToPutList.toArray();
   for (int i = 0; i < fieldNames.length; i++) {
      String fieldName = fieldNames[i];
      testInstance.testQuery(fieldName, ">",  "", valuesToPut, values, new int[] {5, 4, 0});
      testInstance.testQuery(fieldName, "<",  "", valuesToPut, values, new int[] {0, 1, 5});
      testInstance.testQuery(fieldName, ">=", "", valuesToPut, values, new int[] {6, 5, 1});
      testInstance.testQuery(fieldName, "<=", "", valuesToPut, values, new int[] {1, 2, 6});
      testInstance.testQuery(fieldName, "=",  "", valuesToPut, values, new int[] {1, 1, 1});
      testInstance.testQuery(fieldName, "<>", "", valuesToPut, values, new int[] {5, 5, 5});
      testInstance.testQuery(fieldName, "!=", "", valuesToPut, values, new int[] {5, 5, 5});
   }
}

/** Hydra task for test min/max values of short/Shorts.
 */
public static void HydraTask_testMinMaxShorts() {
   StringBuffer errStr = new StringBuffer();
   String[] fieldNames = new String[] {"aPrimitiveShort", "aShort"};
   Object[] values = new Object[] {new Short(Short.MIN_VALUE), new Short((short)0), new Short(Short.MAX_VALUE)};
   ArrayList valuesToPutList = new ArrayList();
   for (int i = 0; i < values.length; i++) {
      QueryObject qo = new QueryObject(10, QueryObject.EQUAL_VALUES, -1, 1);
      qo.aShort = (Short)(values[i]);
      valuesToPutList.add(qo);
      qo = new QueryObject(10, QueryObject.EQUAL_VALUES, -1, 1);
      qo.aPrimitiveShort = ((Short)values[i]).shortValue();
      valuesToPutList.add(qo);
   }
   Object[] valuesToPut = valuesToPutList.toArray();
   for (int i = 0; i < fieldNames.length; i++) {
      String fieldName = fieldNames[i];
      testInstance.testQuery(fieldName, ">",  "", valuesToPut, values, new int[] {5, 4, 0});
      testInstance.testQuery(fieldName, "<",  "", valuesToPut, values, new int[] {0, 1, 5});
      testInstance.testQuery(fieldName, ">=", "", valuesToPut, values, new int[] {6, 5, 1});
      testInstance.testQuery(fieldName, "<=", "", valuesToPut, values, new int[] {1, 2, 6});
      testInstance.testQuery(fieldName, "=",  "", valuesToPut, values, new int[] {1, 1, 1});
      testInstance.testQuery(fieldName, "<>", "", valuesToPut, values, new int[] {5, 5, 5});
      testInstance.testQuery(fieldName, "!=", "", valuesToPut, values, new int[] {5, 5, 5});
   }
}

/** Hydra task for test min/max values of byte/Bytes.
 */
public static void HydraTask_testMinMaxBytes() {
   StringBuffer errStr = new StringBuffer();
   String[] fieldNames = new String[] {"aPrimitiveByte", "aByte"};
   Object[] values = new Object[] {new Byte(Byte.MIN_VALUE), new Byte((byte)0), new Byte(Byte.MAX_VALUE)};
   ArrayList valuesToPutList = new ArrayList();
   for (int i = 0; i < values.length; i++) {
      QueryObject qo = new QueryObject(10, QueryObject.EQUAL_VALUES, -1, 1);
      qo.aByte = (Byte)(values[i]);
      valuesToPutList.add(qo);
      qo = new QueryObject(10, QueryObject.EQUAL_VALUES, -1, 1);
      qo.aPrimitiveByte = ((Byte)values[i]).byteValue();
      valuesToPutList.add(qo);
   }
   Object[] valuesToPut = valuesToPutList.toArray();
   for (int i = 0; i < fieldNames.length; i++) {
      String fieldName = fieldNames[i];
      testInstance.testQuery(fieldName, ">",  "", valuesToPut, values, new int[] {5, 4, 0});
      testInstance.testQuery(fieldName, "<",  "", valuesToPut, values, new int[] {0, 1, 5});
      testInstance.testQuery(fieldName, ">=", "", valuesToPut, values, new int[] {6, 5, 1});
      testInstance.testQuery(fieldName, "<=", "", valuesToPut, values, new int[] {1, 2, 6});
      testInstance.testQuery(fieldName, "=",  "", valuesToPut, values, new int[] {1, 1, 1});
      testInstance.testQuery(fieldName, "<>", "", valuesToPut, values, new int[] {5, 5, 5});
      testInstance.testQuery(fieldName, "!=", "", valuesToPut, values, new int[] {5, 5, 5});
   }
}

/** Hydra task for test min/max values of float/Floats
\ *  ref. bug 37716, NaN is not behaving correctly - It has been agreed upon that
 *  it is ok for NaN to behave the way Float and Double Objects behave, where
 *  NaN is considered the largest number possible, larger than Infinity when doing
 *  comparisons.  We will not be adhering to the way the primitive float and double behave. 
 *  More details in 37716 as for possible patch to fix behavior to match primitives in future
 */
public static void HydraTask_testMinMaxFloats() {
   StringBuffer errStr = new StringBuffer();
   String[] fieldNames = new String[] {"aPrimitiveFloat", "aFloat"};
   // note that the smallest noninfinite (negative) float is -Float.MAX_VALUE, not Float.MIN_VALUE
   Object[] values = new Object[] {new Float(-Float.MAX_VALUE), new Float(0.0), new Float(Float.MAX_VALUE),
                                   new Float(Float.NEGATIVE_INFINITY), new Float(Float.POSITIVE_INFINITY),
                                   new Float(Float.NaN)};
   ArrayList valuesToPutList = new ArrayList();
   for (int i = 0; i < values.length; i++) {
      QueryObject qo = new QueryObject(10, QueryObject.EQUAL_VALUES, -1, 1);
      qo.aFloat = (Float)(values[i]);
      qo.aPrimitiveFloat = ((Float)values[i]).floatValue();
      valuesToPutList.add(qo);
   }
   Object[] valuesToPut = valuesToPutList.toArray();
   // use a string representation of the extreme floats to make sure they get converted properly to the right float value
   // (instead of relying on toString)
   // i.e. use "-3.4028234663852886E38" instead of new Float(-Float.MAX_VALUE)
   Object[] valuesToQueryWith = new Object[] {"-3.4028234663852886E38", new Float(0.0), "3.4028234663852886E38"};
   for (int i = 0; i < fieldNames.length; i++) {
      String fieldName = fieldNames[i];
      testInstance.testQuery(fieldName, ">",  "F", valuesToPut, valuesToQueryWith, new int[] {4, 3, 2});
      testInstance.testQuery(fieldName, "<",  "F", valuesToPut, valuesToQueryWith, new int[] {1, 2, 3});
      testInstance.testQuery(fieldName, ">=", "F", valuesToPut, valuesToQueryWith, new int[] {5, 4, 3});
      testInstance.testQuery(fieldName, "<=", "F", valuesToPut, valuesToQueryWith, new int[] {2, 3, 4});
      testInstance.testQuery(fieldName, "=",  "F", valuesToPut, valuesToQueryWith, new int[] {1, 1, 1});
      testInstance.testQuery(fieldName, "<>", "F", valuesToPut, valuesToQueryWith, new int[] {5, 5, 5});
      testInstance.testQuery(fieldName, "!=", "F", valuesToPut, valuesToQueryWith, new int[] {5, 5, 5});
   }
}

/** Hydra task for test min/max values of double/Doubles
 *  ref. bug 37716, NaN is not behaving correctly - It has been agreed upon that
 *  it is ok for NaN to behave the way Float and Double Objects behave, where
 *  NaN is considered the largest number possible, larger than Infinity when doing
 *  comparisons.  We will not be adhering to the way the primitive float and double behave. 
 *  More details in 37716 as for possible patch to fix behavior to match primitives in future
 */
public static void HydraTask_testMinMaxDoubles() {
   StringBuffer errStr = new StringBuffer();
   String[] fieldNames = new String[] {"aPrimitiveDouble", "aDouble"};
   // note that the smallest noninfinite (negative) double is -Double.MAX_VALUE, not Double.MIN_VALUE
   Object[] values = new Object[] {new Double(-Double.MAX_VALUE), new Double(0.0), new Double(Double.MAX_VALUE),
                                   new Double(Double.NEGATIVE_INFINITY), new Double(Double.POSITIVE_INFINITY),
                                   new Double(Double.NaN) };
   ArrayList valuesToPutList = new ArrayList();
   for (int i = 0; i < values.length; i++) {
      QueryObject qo = new QueryObject(10, QueryObject.EQUAL_VALUES, -1, 1);
      qo.aDouble = (Double)(values[i]);
      qo.aPrimitiveDouble = ((Double)values[i]).doubleValue();
      valuesToPutList.add(qo);
   }
   Object[] valuesToPut = valuesToPutList.toArray();
   // use a string representation of the extreme doubles to make sure they get converted properly to the right double value
   // (instead of relying on toString)
   // i.e. use "-1.7976931348623157E308" instead of new Double(-Double.MAX_VALUE)
   Object[] valuesToQueryWith = new Object[] {"-1.7976931348623157E308", new Double(0.0), "1.7976931348623157E308"};
   for (int i = 0; i < fieldNames.length; i++) {
      String fieldName = fieldNames[i];
      testInstance.testQuery(fieldName, ">",  "D", valuesToPut, valuesToQueryWith, new int[] {4, 3, 2});
      testInstance.testQuery(fieldName, "<",  "D", valuesToPut, valuesToQueryWith, new int[] {1, 2, 3});
      testInstance.testQuery(fieldName, ">=", "D", valuesToPut, valuesToQueryWith, new int[] {5, 4, 3});
      testInstance.testQuery(fieldName, "<=", "D", valuesToPut, valuesToQueryWith, new int[] {2, 3, 4});
      testInstance.testQuery(fieldName, "=",  "D", valuesToPut, valuesToQueryWith, new int[] {1, 1, 1});
      testInstance.testQuery(fieldName, "<>", "D", valuesToPut, valuesToQueryWith, new int[] {5, 5, 5});
      testInstance.testQuery(fieldName, "!=", "D", valuesToPut, valuesToQueryWith, new int[] {5, 5, 5});
   }
}

/** Hydra task for test min/max values of char/Character
 */
public static void HydraTask_testMinMaxCharacters() {
   StringBuffer errStr = new StringBuffer();
   String[] fieldNames = new String[] {"aPrimitiveChar", "aCharacter"};
   // use the MAX_VALUE - 1 instead of MAX_VALUE (which is EOF and not legal)
   // note that new Character((char)0) is the SAME VALUE as new Character(Character.MIN_VALUE)
   Object[] values = new Object[] {new Character(Character.MIN_VALUE), 
                                   new Character((char)0), 
                                   new Character((char)(Character.MAX_VALUE - 1))};
   ArrayList valuesToPutList = new ArrayList();
   for (int i = 0; i < values.length; i++) {
      QueryObject qo = new QueryObject(10, QueryObject.EQUAL_VALUES, -1, 1);
      qo.aCharacter = (Character)(values[i]);
      qo.aPrimitiveChar = ((Character)values[i]).charValue();
      valuesToPutList.add(qo);
   }
   Object[] valuesToPut = valuesToPutList.toArray();
   Object[] valuesToQueryWith = new Object[] {
      "CHAR '" + new Character(Character.MIN_VALUE) + "'", 
      "CHAR '" + new Character((char)0) + "'", 
      "CHAR '" + new Character((char)(Character.MAX_VALUE - 1)) + "'"};
   for (int i = 0; i < fieldNames.length; i++) {
      String fieldName = fieldNames[i];
      testInstance.testQuery(fieldName, ">",  "", valuesToPut, valuesToQueryWith, new int[] {1, 1, 0});
      testInstance.testQuery(fieldName, "<",  "", valuesToPut, valuesToQueryWith, new int[] {0, 0, 2});
      testInstance.testQuery(fieldName, ">=", "", valuesToPut, valuesToQueryWith, new int[] {3, 3, 1});
      testInstance.testQuery(fieldName, "<=", "", valuesToPut, valuesToQueryWith, new int[] {2, 2, 3});
      testInstance.testQuery(fieldName, "=",  "", valuesToPut, valuesToQueryWith, new int[] {2, 2, 1});
      testInstance.testQuery(fieldName, "<>", "", valuesToPut, valuesToQueryWith, new int[] {1, 1, 2});
      testInstance.testQuery(fieldName, "!=", "", valuesToPut, valuesToQueryWith, new int[] {1, 1, 2});
   }
}

/** Hydra task for test min/max values of Strings
 */
public static void HydraTask_testMinMaxStrings() {
   StringBuffer errStr = new StringBuffer();
   String[] fieldNames = new String[] {"aString"};
   // the empty string is actually the minimum in collation order
   String minStr = "";
   for (int i = 1; i <= 10; i++) {
      minStr = minStr + new Character(Character.MIN_VALUE);
   }
   String maxStr = "";
   // use the MAX_VALUE - 1 instead of MAX_VALUE (which is EOF and not legal)
   for (int i = 1; i <= 10; i++) {
      maxStr = maxStr + new Character((char)(Character.MAX_VALUE - 1));
   }
   Object[] values = new Object[] {minStr, "", maxStr};
   ArrayList valuesToPutList = new ArrayList();
   for (int i = 0; i < values.length; i++) {
      QueryObject qo = new QueryObject(10, QueryObject.EQUAL_VALUES, -1, 1);
      qo.aString = (String)(values[i]);
      valuesToPutList.add(qo);
   }
   Object[] valuesToPut = valuesToPutList.toArray();
   Object[] valuesToQueryWith = new Object[] {
     // the empty string is actually the minimum in collation order
      "''", 
      "'" + minStr + "'", 
      "'" + maxStr + "'"};
   for (int i = 0; i < fieldNames.length; i++) {
      String fieldName = fieldNames[i];
      testInstance.testQuery(fieldName, ">",  "", valuesToPut, valuesToQueryWith, new int[] {2, 1, 0});
      testInstance.testQuery(fieldName, "<",  "", valuesToPut, valuesToQueryWith, new int[] {0, 1, 2});
      testInstance.testQuery(fieldName, ">=", "", valuesToPut, valuesToQueryWith, new int[] {3, 2, 1});
      testInstance.testQuery(fieldName, "<=", "", valuesToPut, valuesToQueryWith, new int[] {1, 2, 3});
      testInstance.testQuery(fieldName, "=",  "", valuesToPut, valuesToQueryWith, new int[] {1, 1, 1});
      testInstance.testQuery(fieldName, "<>", "", valuesToPut, valuesToQueryWith, new int[] {2, 2, 2});
      testInstance.testQuery(fieldName, "!=", "", valuesToPut, valuesToQueryWith, new int[] {2, 2, 2});
   }
}

/** Hydra task to check for any errors put in the blackboard.
 */
public static void HydraTask_checkForErrors() {
   String errStr = "";
   String unknownErrors = (String)(CQUtilBB.getBB().getSharedMap().get(UnknownErrorsKey));
   if (unknownErrors.length() > 0) {
      errStr = "Unknown errors:\n:" + unknownErrors;
   }
   String knownErrors = (String)(CQUtilBB.getBB().getSharedMap().get(KnownErrorsKey));
   if (knownErrors.length() > 0) {
      if (errStr.length() != 0) {
         errStr = errStr + "\n";
      }
      errStr = errStr + "Known errors with bug numbers: " + knownErrors;
   }
   if (errStr.length() > 0) {
      throw new TestException(errStr);
   }
}

public static void HydraTask_testDistinctQuery() {
  testInstance.testDistinctQuery();
}

protected void testDistinctQuery() {
  aRegion.clear();
  Log.getLogWriter().info("Putting 5 portfolios in region");
  int NUMBER_OF_KEYS = 5;
  for (int i=0; i<NUMBER_OF_KEYS; i++) {
    aRegion.put("Key"+i, new Portfolio(i));
  }
  Query q = qService.newQuery("SELECT DISTINCT ID from "+aRegion.getFullPath());
  try {
    SelectResults results = (SelectResults)q.execute();
    int i=0;
    for (Iterator it = results.iterator(); it.hasNext();) {
      int id = (Integer)it.next();
      Log.getLogWriter().info("SWAP:p:"+id);
      i++;
    }
    if (i != NUMBER_OF_KEYS) throw new TestException("Size mismatch: expected "+NUMBER_OF_KEYS+" found "+i);
    
  } catch (FunctionDomainException e) {
    e.printStackTrace();
  } catch (TypeMismatchException e) {
    e.printStackTrace();
  } catch (NameResolutionException e) {
    e.printStackTrace();
  } catch (QueryInvocationTargetException e) {
    e.printStackTrace();
  }
}

/** Run a series of queries on min/max points. A query will be run
 *  comparing fieldName to each value in valuesToQueryWith using
 *  comparator.
 *
 *  @param fieldName The name of the field to query.
 *  @param comparator The compare operator ("<", etc)
 *  @param suffix A suffix for query values, such as "L" for longs as
 *         in "select * from /testRegion where aLong > 45L", or ""
 *         if no suffix. 
 *  @param valuesToPut The values to put in the region before the queries
 *         are executed.
 *  @param valuesToQueryWith The values to query against.
 *  @param resultsSizes Parallel to valuesToQueryWith, this the the expected
 *         size of the results for querying against the corresponding
 *         valuesToQueryWith value using the comparator argument.
 *
 *  Any errors are stored in the blackboard with KnownErrorsKey and 
 *  UnknownErrorsKey.
 */
protected void testQuery(String fieldName, 
                         String comparator, 
                         String suffix,
                         Object[] valuesToPut, 
                         Object[] valuesToQueryWith, 
                         int[] resultSizes) {
   if (resultSizes.length != valuesToQueryWith.length) {
      throw new TestException("Test problem, resultSizes is length " + resultSizes.length + 
                              " but valuesToQueryWith is length " + valuesToQueryWith.length);
   }
   CqAttributesFactory cqFac = new CqAttributesFactory();
   CqAttributes cqAttr = cqFac.create();
   aRegion.clear();
   for (int i = 0; i < valuesToPut.length; i++) {
      aRegion.put("key" + (i+1), valuesToPut[i]);
   }

   for (int i = 0; i < valuesToQueryWith.length; i++) {
      String queryStr = "select * from /testRegion where " + fieldName + " " + comparator + " " + valuesToQueryWith[i] + suffix;
      CqQuery cq = null;
      try {
         Log.getLogWriter().info("Creating cq for query: " + queryStr);
         cq = qService.newCq(queryStr, cqAttr);
      } catch (Exception e) {
         String aStr;
         Throwable cause = TestHelper.findCause(e, NumberFormatException.class);
         if (((e instanceof QueryInvalidException)
              && (e.getMessage().indexOf("Unable to parse float  9223372036854775808") >= 0))
             || (cause != null && cause.getMessage().indexOf("For input string: \"2147483648\"") >= 0)) {
           aStr = "Bug 37119 detected: For query " + queryStr + " got " + e + "\n";
           String bbStr = (String)(CQUtilBB.getBB().getSharedMap().get(KnownErrorsKey));
           bbStr = bbStr + aStr;
           CQUtilBB.getBB().getSharedMap().put(KnownErrorsKey, bbStr);
         }
         else {
           aStr = "For query " + queryStr + " got " + e + "\n";
           String bbStr = (String)(CQUtilBB.getBB().getSharedMap().get(UnknownErrorsKey));
           bbStr = bbStr + aStr;
           CQUtilBB.getBB().getSharedMap().put(UnknownErrorsKey, bbStr);
         }
         Log.getLogWriter().info(aStr + " " + TestHelper.getStackTrace(e));
         continue;
      }
      Log.getLogWriter().info("Calling executeWithInitialResults on " + cq + " with query: " + queryStr);
      SelectResults sr = null;
      try {
         CqResults rs = cq.executeWithInitialResults();
         sr = CQUtil.getSelectResults(rs);
      } catch (Exception e) {
         String aStr = "For query " + queryStr + " got " + e + "\n";
         Log.getLogWriter().info(aStr + " " + TestHelper.getStackTrace(e));
         String bbStr = (String)(CQUtilBB.getBB().getSharedMap().get(UnknownErrorsKey));
         bbStr = bbStr + aStr;
         CQUtilBB.getBB().getSharedMap().put(UnknownErrorsKey, bbStr);
         continue;
      }
      if (sr == null) {
         String aStr = "Bug 37060 detected: For cq " + cq + ", query string: " + queryStr + 
                   ", executeWithInitialResults returned " + sr + "\n";
         Log.getLogWriter().info(aStr);
         String bbStr = (String)(CQUtilBB.getBB().getSharedMap().get(KnownErrorsKey));
         bbStr = bbStr + aStr;
         CQUtilBB.getBB().getSharedMap().put(KnownErrorsKey, bbStr);
         continue;
      }
      String srAsString = QueryObject.toStringFull(sr.asList());
      Log.getLogWriter().info("Executed cq for query: \"" + queryStr + "\", result size is " + sr.size() + ": " + srAsString);
      if (sr.size() != resultSizes[i]) {
         String aStr;
        
         // if the actual result is as expected plus NaN, then it's bug 37716
         if (sr.size() == resultSizes[i] + 1 &&
                ((srAsString.indexOf("aPrimitiveFloat: NaN, aFloat: NaN") > 0) ||
                 (srAsString.indexOf("aPrimitiveDouble: NaN, aDouble: NaN") > 0))) {
           aStr = "Bug 37716 detected: Expected query \"" + queryStr + "\" to return " + resultSizes[i] + 
                  " elements from executeWithInitialResults, but it returned " + 
                  sr.size() + " " + sr + "\n";
           String bbStr = (String)(CQUtilBB.getBB().getSharedMap().get(KnownErrorsKey));
           bbStr = bbStr + aStr;
           CQUtilBB.getBB().getSharedMap().put(KnownErrorsKey, bbStr);
         }
         else {
           aStr = "Expected query \"" + queryStr + "\" to return " + resultSizes[i] + 
                  " elements from executeWithInitialResults, but it returned " + 
                  sr.size() + " " + sr + "\n";
           String bbStr = (String)(CQUtilBB.getBB().getSharedMap().get(UnknownErrorsKey));
           bbStr = bbStr + aStr;
           CQUtilBB.getBB().getSharedMap().put(UnknownErrorsKey, bbStr);
         }
         Log.getLogWriter().info(aStr);
      }
   }
}

}
