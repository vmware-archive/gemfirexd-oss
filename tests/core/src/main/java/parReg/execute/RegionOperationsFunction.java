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
package parReg.execute;

import hydra.Log;
import hydra.TestConfig;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import pdx.PdxInstanceCreator;
import pdx.PdxTest;
import pdx.PdxTestVersionHelper;
import util.BaseValueHolder;
import util.NameFactory;
import util.RandomValues;
import util.TestException;
import util.TestHelper;
import util.ValueHolder;
import util.ValueHolderPrms;

import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.pdx.PdxInstance;
import com.gemstone.gemfire.pdx.WritablePdxInstance;

import delta.DeltaValueHolder;

public class RegionOperationsFunction extends FunctionAdapter implements
    Declarable {
  
  final static String objectType = TestConfig.tab().stringAt(
      ValueHolderPrms.objectType, "util.ValueHolder");

  public void execute(FunctionContext context) {
    ArrayList arguments = (ArrayList)(context.getArguments());
    String operation = (String)arguments.get(0);
    Object initiatingThreadID = arguments.get(1);
    String aStr = "In execute with context " + context + " with operation " + operation +
      " initiated in hydra thread thr_" + initiatingThreadID + "_";
    for (int i = 2; i < arguments.size(); i++) {
      aStr = aStr + " additional arg: " + arguments.get(i);
    }
    Log.getLogWriter().info(aStr);
    final boolean isRegionContext = context instanceof RegionFunctionContext;
    boolean isPartitionedRegionContext = false;
    RegionFunctionContext regionContext = null;
    if (isRegionContext) {
      regionContext = (RegionFunctionContext)context;
      isPartitionedRegionContext = 
        PartitionRegionHelper.isPartitionedRegion(regionContext.getDataSet());
    }
    Log.getLogWriter().info("isPartitionedRegionContext: " + isPartitionedRegionContext);

    if (isPartitionedRegionContext) {
      if (operation.equalsIgnoreCase("addKey")) {
        Log.getLogWriter().info("Inside addKey execute");
        Set keySet = regionContext.getFilter();
        Log.getLogWriter().info("got the filter set " + keySet.toString());
        PartitionedRegion pr = (PartitionedRegion)regionContext.getDataSet();

        Iterator iterator = keySet.iterator();
        Object value = null;
        while (iterator.hasNext()) {
          Object key = iterator.next();
          RandomValues randomValues = new RandomValues();
          value = createObject(key, randomValues);
          pr.put(key, value);
          Log.getLogWriter().info("Did put using execute..");
        }
        if (value instanceof BaseValueHolder) {
          context.getResultSender().lastResult((BaseValueHolder)value);
        } else {
          context.getResultSender().lastResult(new Boolean(true));
        }
      }
      else if (operation.equalsIgnoreCase("putIfAbsent")) {
        Log.getLogWriter().info("Inside putIfAbsent execute");
        Set keySet = regionContext.getFilter();
        Log.getLogWriter().info("got the filter set " + keySet.toString());
        PartitionedRegion pr = (PartitionedRegion)regionContext.getDataSet();

        Iterator iterator = keySet.iterator();
        Object value = null;
        while (iterator.hasNext()) {
          Object key = iterator.next();
          RandomValues randomValues = new RandomValues();
          value = createObject(key, randomValues);
          pr.putIfAbsent(key, value);
          Log.getLogWriter().info("Did putIfAbsent using execute..");
        }
        if (value instanceof BaseValueHolder) {
          context.getResultSender().lastResult((BaseValueHolder)value);
        } else {
          context.getResultSender().lastResult(Boolean.TRUE);
        }
      }
      else if (operation.equalsIgnoreCase("invalidate")) {
        Log.getLogWriter().info("Inside invalidate execute");
        Set keySet = regionContext.getFilter();
        Log.getLogWriter().info("got the filter set " + keySet.toString());
        PartitionedRegion pr = (PartitionedRegion)regionContext.getDataSet();

        Iterator iterator = keySet.iterator();
        while (iterator.hasNext()) {
          Object key = iterator.next();
          pr.invalidate(key);
          Log.getLogWriter().info("Did invalidate using execute..");
        }
        context.getResultSender().lastResult( Boolean.TRUE);
      }
      else if (operation.equalsIgnoreCase("destroy")) {

        Log.getLogWriter().info("Inside destroy execute");
        Set keySet = regionContext.getFilter();
        Log.getLogWriter().info("got the filter set " + keySet.toString());
        PartitionedRegion pr = (PartitionedRegion)regionContext.getDataSet();

        Iterator iterator = keySet.iterator();
        Object value = null;
        while (iterator.hasNext()) {
          Object key = iterator.next();
          try{
            value = pr.destroy(key);
            Log.getLogWriter().info("Did destroy using execute..");
          }
          catch (EntryNotFoundException e) {
            if (regionContext.isPossibleDuplicate()) {
              Log.getLogWriter().info(
              "EntryNotFoundException expected in re-execute scenario");
            }
            else {
              throw e;
            }
          }
        }
        if (value instanceof BaseValueHolder) {
          context.getResultSender().lastResult((BaseValueHolder)value);
        } else {
          context.getResultSender().lastResult(Boolean.TRUE);
        }
      }
      else if (operation.equalsIgnoreCase("remove")) {

        Log.getLogWriter().info("Inside remove execute");
        Set keySet = regionContext.getFilter();
        Log.getLogWriter().info("got the filter set " + keySet.toString());
        PartitionedRegion pr = (PartitionedRegion)regionContext.getDataSet();

        Iterator iterator = keySet.iterator();
        while (iterator.hasNext()) {
          Object key = iterator.next();
          try {
            boolean removed = pr.remove(key, PdxTestVersionHelper.toBaseObject(pr.get(key)));
            Log.getLogWriter().info("remove returned " + removed + " for key " + key);
          }
          catch (EntryNotFoundException e) {
            if (regionContext.isPossibleDuplicate()) {
              Log.getLogWriter().info(
              "EntryNotFoundException expected in re-execute scenario");
            }
            else {
              throw e;
            }
          }
        }
        context.getResultSender().lastResult( Boolean.TRUE);
      }
      else if (operation.equalsIgnoreCase("update")) {
        Log.getLogWriter().info("Inside update execute");
        Set keySet = regionContext.getFilter();
        Log.getLogWriter().info("got the filter set " + keySet.toString());
        PartitionedRegion pr = (PartitionedRegion)regionContext.getDataSet();

        Iterator iterator = keySet.iterator();
        Object value = null; 
        while (iterator.hasNext()) {
          Object key = iterator.next();
          RandomValues randomValues = new RandomValues();
          BaseValueHolder existingValue = PdxTest.toValueHolder(pr.get(key));
          Object newValue = createObject(key, randomValues);
          if (existingValue == null)
            throw new TestException("Get of key " + key
                + " returned unexpected " + existingValue);
          if (existingValue.myValue instanceof String) {
            if (regionContext.isPossibleDuplicate()) {
              Log.getLogWriter().info(
              "Entry already updated - expected in re-execute scenario");
            }
            else {
              throw new TestException(
                  "Trying to update a key which was already updated: "
                  + TestHelper.toString(existingValue.myValue));
            }
          }
          Object updatedValue = "updated_" + NameFactory.getCounterForName(key);
          if (newValue instanceof BaseValueHolder) {
            Log.getLogWriter().info("updating " + key + " with newValue " + updatedValue);
            ((BaseValueHolder)newValue).myValue = updatedValue;
          } else if (newValue instanceof PdxInstance) {
            Log.getLogWriter().info("updating " + key + " with WritablePdxInstance to " + updatedValue);
            PdxInstance pdxInst = (PdxInstance)newValue;
            WritablePdxInstance writable = pdxInst.createWriter();
            writable.setField("myValue", updatedValue);
            newValue = writable;
          }
  
          value = pr.put(key, newValue);
          Log.getLogWriter().info("Did update using execute..");
        }
        if (value instanceof BaseValueHolder) {
          context.getResultSender().lastResult((BaseValueHolder)value);
        } else {
          context.getResultSender().lastResult(Boolean.TRUE);
        }
      }
      else if (operation.equalsIgnoreCase("replace")) {
        Log.getLogWriter().info("Inside replace execute");
        Set keySet = regionContext.getFilter();
        Log.getLogWriter().info("got the filter set " + keySet.toString());
        PartitionedRegion pr = (PartitionedRegion)regionContext.getDataSet();

        Iterator iterator = keySet.iterator();
        Object value = null;
        while (iterator.hasNext()) {
          Object key = iterator.next();
          RandomValues randomValues = new RandomValues();
          BaseValueHolder existingValue = PdxTest.toValueHolder(pr.get(key));
          Object newValue = createObject(key, randomValues);
          if (existingValue == null)
            throw new TestException("Get of key " + key
                + " returned unexpected " + existingValue);
          if (existingValue.myValue instanceof String) {
            if (regionContext.isPossibleDuplicate()) {
              Log.getLogWriter().info(
              "Entry already replaced - expected in re-execute scenario");
            }
            else {
              throw new TestException(
                  "Trying to replace a key which was already replaced: "
                  + TestHelper.toString(existingValue.myValue));
            }
          }
          Object updatedValue = "updated_" + NameFactory.getCounterForName(key);
          if (newValue instanceof BaseValueHolder) {
            Log.getLogWriter().info("replacing " + key + " with newValue " + updatedValue);
            ((BaseValueHolder)newValue).myValue = updatedValue;
          } else if (newValue instanceof PdxInstance) {
            Log.getLogWriter().info("replacing " + key + " with WritablePdxInstance to " + updatedValue);
            PdxInstance pdxInst = (PdxInstance)newValue;
            WritablePdxInstance writable = pdxInst.createWriter();
            writable.setField("myValue", updatedValue);
            newValue = writable;
          }
          value = pr.replace(key, newValue);
          Log.getLogWriter().info("Did replace using execute..");
        }
        if (value instanceof BaseValueHolder) {
          context.getResultSender().lastResult((BaseValueHolder)value);
        } else {
          context.getResultSender().lastResult(Boolean.TRUE);
        }
      }
      else if (operation.equalsIgnoreCase("get")) {
        Log.getLogWriter().info("Inside get execute");
        Set keySet = regionContext.getFilter();
        Log.getLogWriter().info("got the filter set " + keySet.toString());
        Region region = PartitionRegionHelper.getLocalDataForContext(regionContext);

        Iterator iterator = keySet.iterator();
        Object existingValue = null;
        while (iterator.hasNext()) {
          Object key = iterator.next();
          existingValue = PdxTestVersionHelper.toBaseObject(region.get(key));
          if (existingValue == null) {
            throw new TestException("get for the object " + key
                + " cannot be null");
          }
          Log.getLogWriter().info("Did get using execute..");
        }
        if (existingValue instanceof BaseValueHolder) {
          context.getResultSender().lastResult((BaseValueHolder)existingValue);
        } else {
          context.getResultSender().lastResult(Boolean.TRUE);
        }
      }
      else if (operation.equalsIgnoreCase("query")) {
        Log.getLogWriter().info("Inside query execute");
        Region region = PartitionRegionHelper
        .getLocalDataForContext(regionContext);
        Set keySet = regionContext.getFilter();
        Iterator iterator = keySet.iterator();
        SelectResults result = null;
        while (iterator.hasNext()) {
          Object key = iterator.next();
          Object existingValue = null;
          existingValue = PdxTestVersionHelper.toBaseObject(region.get(key));
          String[] keySplit = key.toString().split("_");
          String queryCriterion = keySplit[1];
          String queries = " myValue = " + queryCriterion;
          try {
            result = region.query(queries);
            if ((result instanceof Collection)) {
              Log.getLogWriter().info(
                  "Size of query result for region " + region.getName()
                  + " is :" + ((Collection)result).size());
            }
            if (!(result instanceof Collection)
                || ((Collection)result).size() == 0) {
              throw new TestException(
                  "Size of the query result to be 1 but list is "
                  + result.toString());
            }
          }
          catch (Exception e) {
            throw new TestException("Caught exception during query execution"
                + TestHelper.getStackTrace(e));
          }

        }

        Log.getLogWriter().info("Did query using execute..");
        context.getResultSender().lastResult(new Boolean(true));
      }
      else if (operation.equalsIgnoreCase("localinvalidate")) {
        Log.getLogWriter().info("Inside local invalidate execute");
        Set keySet = regionContext.getFilter();
        Log.getLogWriter().info("got the filter set " + keySet.toString());
        PartitionedRegion pr = (PartitionedRegion)regionContext.getDataSet();

        Iterator iterator = keySet.iterator();
        while (iterator.hasNext()) {
          Object key = iterator.next();
          pr.localInvalidate(key);
          Log.getLogWriter().info("Did local invalidate using execute..");
        }
        context.getResultSender().lastResult( Boolean.TRUE);
      }
      else if (operation.equalsIgnoreCase("localdestroy")) {
        Log.getLogWriter().info("Inside local destroy execute");
        Set keySet = regionContext.getFilter();
        Log.getLogWriter().info("got the filter set " + keySet.toString());
        PartitionedRegion pr = (PartitionedRegion)regionContext.getDataSet();

        Iterator iterator = keySet.iterator();
        while (iterator.hasNext()) {
          Object key = iterator.next();
          try{
            pr.localDestroy(key);
            Log.getLogWriter().info("Did local destroy using execute..");
          }
          catch (EntryNotFoundException e) {
            if (regionContext.isPossibleDuplicate()) {
              Log.getLogWriter().info(
              "EntryNotFoundException expected in re-execute scenario");
            }
            else {
              throw e;
            }
          }
        }
        context.getResultSender().lastResult( Boolean.TRUE);
      } else if (operation.equalsIgnoreCase("putAll")) {
        Log.getLogWriter().info("Inside putAll execute");
        HashMap map = (HashMap)arguments.get(2);
        Log.getLogWriter().info("got the hash set " + map.toString());
        PartitionedRegion pr = (PartitionedRegion)regionContext.getDataSet();
        pr.putAll(map);
        Log.getLogWriter().info("Did putAll using execute..");

        context.getResultSender().lastResult( Boolean.TRUE);
      }
      else {
        throw new TestException("Test problem: did not execute a server side function");
        //context.getResultSender().lastResult( Boolean.FALSE);
      }
    } else if (context instanceof RegionFunctionContext) {
      if( arguments.size() > 2) { // we have extra args 
        if (operation.equalsIgnoreCase("addKey")) {
          Log.getLogWriter().info("Inside addKey execute");
          Region region = regionContext.getDataSet();
          Object retValue = null;
          for (int i = 2; i < arguments.size(); i++) {
            Object key = arguments.get(i);
            RandomValues randomValues = new RandomValues();
            Object value = createObject(key, randomValues);
            retValue = region.put(key, value);
            Log.getLogWriter().info("Did put using execute..");
          }
          if (retValue instanceof BaseValueHolder) {
            context.getResultSender().lastResult((BaseValueHolder)retValue);
          } else {
            context.getResultSender().lastResult(Boolean.TRUE);
          }
        }
        else if (operation.equalsIgnoreCase("putIfAbsent")) {
          Log.getLogWriter().info("Inside putIfAbsent execute");
          Region region = regionContext.getDataSet();
          Object retValue = null;
          for (int i = 2; i < arguments.size(); i++) {
            Object key = arguments.get(i);
            RandomValues randomValues = new RandomValues();
            Object value = createObject(key, randomValues);
            retValue = region.putIfAbsent(key, value);
            Log.getLogWriter().info("Did putIfAbsent using execute..");
          }
          if (retValue instanceof BaseValueHolder) {
            context.getResultSender().lastResult((BaseValueHolder)retValue);
          } else {
            context.getResultSender().lastResult(Boolean.TRUE);
          }
        }
        else if (operation.equalsIgnoreCase("invalidate")) {
          Log.getLogWriter().info("Inside invalidate execute");
          Region region = regionContext.getDataSet();
          for (int i = 2; i < arguments.size(); i++) {
            Object key = arguments.get(i);
            region.invalidate(key);
            Log.getLogWriter().info("Did invalidate using execute..");
          }
          context.getResultSender().lastResult( Boolean.TRUE);
        }
        else if (operation.equalsIgnoreCase("destroy")) {

          Log.getLogWriter().info("Inside destroy execute");
          Region region = regionContext.getDataSet();

          Object value = null;
          for (int i = 2; i < arguments.size(); i++) {
            Object key = arguments.get(i);
            try{
              value = region.destroy(key);
              Log.getLogWriter().info("Did destroy using execute..");
            }
            catch (EntryNotFoundException e) {
              if (regionContext.isPossibleDuplicate()) {
                Log.getLogWriter().info(
                "EntryNotFoundException expected in re-execute scenario");
              }
              else {
                throw e;
              }
            }
          }
          if (value instanceof BaseValueHolder) {
            context.getResultSender().lastResult((BaseValueHolder)value);
          } else {
            context.getResultSender().lastResult(Boolean.TRUE);
          }
        }
        else if (operation.equalsIgnoreCase("remove")) {

          Log.getLogWriter().info("Inside remove execute");
          Region region = regionContext.getDataSet();
          for (int i = 2; i < arguments.size(); i++) {
            Object key = arguments.get(i);
            try{
              boolean removed = region.remove(key, PdxTestVersionHelper.toBaseObject(region.get(key)));
              Log.getLogWriter().info("remove returned " + removed + " for key " + key);
            }
            catch (EntryNotFoundException e) {
              if (regionContext.isPossibleDuplicate()) {
                Log.getLogWriter().info(
                "EntryNotFoundException expected in re-execute scenario");
              }
              else {
                throw e;
              }
            }
          }
          context.getResultSender().lastResult( Boolean.TRUE);
        }
        else if (operation.equalsIgnoreCase("update")) {
          Log.getLogWriter().info("Inside update execute");
          Region region = regionContext.getDataSet();

          Object value = null;
          for (int i = 2; i < arguments.size(); i++) {
            Object key = arguments.get(i);
            RandomValues randomValues = new RandomValues();
            BaseValueHolder existingValue = PdxTest.toValueHolder(region.get(key));
            Object newValue = createObject(key, randomValues);
            if (existingValue == null)
              throw new TestException("Get of key " + key
                  + " returned unexpected " + existingValue);
            if (existingValue.myValue instanceof String){
              if(regionContext.isPossibleDuplicate()){
                Log.getLogWriter().info("Entry already updated - expected in re-execute scenario");
              }else{
                throw new TestException(
                    "Trying to update a key which was already updated: "
                    + TestHelper.toString(existingValue.myValue));
              }
            }
            Object updatedValue = "updated_" + NameFactory.getCounterForName(key);
            if (newValue instanceof BaseValueHolder) {
              Log.getLogWriter().info("updating " + key + " with newValue " + updatedValue);
              ((BaseValueHolder)newValue).myValue = updatedValue;
            } else if (newValue instanceof PdxInstance) {
              Log.getLogWriter().info("updating " + key + " with WritablePdxInstance to " + updatedValue);
              PdxInstance pdxInst = (PdxInstance)newValue;
              WritablePdxInstance writable = pdxInst.createWriter();
              writable.setField("myValue", updatedValue);
              newValue = writable;
            }

            value = region.put(key, newValue);
            Log.getLogWriter().info("Did update using execute..");
          }
          if (value instanceof BaseValueHolder) {
            context.getResultSender().lastResult((BaseValueHolder)value);
          } else {
            context.getResultSender().lastResult(Boolean.TRUE);
          }
        }
        else if (operation.equalsIgnoreCase("replace")) {
          Log.getLogWriter().info("Inside replace execute");
          Region region = regionContext.getDataSet();

          Object value = null;
          for (int i = 2; i < arguments.size(); i++) {
            Object key = arguments.get(i);
            RandomValues randomValues = new RandomValues();
            BaseValueHolder existingValue = PdxTest.toValueHolder(region.get(key));
            Object newValue = createObject(key, randomValues);
            if (existingValue == null)
              throw new TestException("Get of key " + key
                  + " returned unexpected " + existingValue);
            if (existingValue.myValue instanceof String){
              if(regionContext.isPossibleDuplicate()){
                Log.getLogWriter().info("Entry already replaced - expected in re-execute scenario");
              }else{
                throw new TestException(
                    "Trying to replace a key which was already replaced: "
                    + TestHelper.toString(existingValue.myValue));
              }
            }
            Object updatedValue = "updated_" + NameFactory.getCounterForName(key);
            if (newValue instanceof BaseValueHolder) {
              Log.getLogWriter().info("replacing " + key + " with newValue " + updatedValue);
              ((BaseValueHolder)newValue).myValue = updatedValue;
            } else if (newValue instanceof PdxInstance) {
              Log.getLogWriter().info("replacing " + key + " with WritablePdxInstance to " + updatedValue);
              PdxInstance pdxInst = (PdxInstance)newValue;
              WritablePdxInstance writable = pdxInst.createWriter();
              writable.setField("myValue", updatedValue);
              newValue = writable;
            }

            value = region.replace(key, newValue);
            Log.getLogWriter().info("Did replace using execute..");
          }
          if (value instanceof BaseValueHolder) {
            context.getResultSender().lastResult((BaseValueHolder)value);
          } else {
            context.getResultSender().lastResult(Boolean.TRUE);
          }

        }
        else if (operation.equalsIgnoreCase("get")) {
          Log.getLogWriter().info("Inside get execute");
          Region region = regionContext.getDataSet();

          Object existingValue = null;
          for (int i = 2; i < arguments.size(); i++) {
            Object key = arguments.get(i);
            existingValue = PdxTestVersionHelper.toBaseObject(region.get(key));
            if (existingValue == null) {
              throw new TestException("get for the object " + key
                  + " cannot be null");
            }
            Log.getLogWriter().info("Did get using execute..");
          }
          if (existingValue instanceof BaseValueHolder) {
            context.getResultSender().lastResult((BaseValueHolder)existingValue);
          } else {
            context.getResultSender().lastResult(Boolean.TRUE);
          }

        }
        else if (operation.equalsIgnoreCase("localinvalidate")) {
          Log.getLogWriter().info("Inside local invalidate execute");
          Region region = regionContext.getDataSet();
          for (int i = 2; i < arguments.size(); i++) {
            Object key = arguments.get(i);
            region.localInvalidate(key);
            Log.getLogWriter().info("Did local invalidate using execute..");
          }
          context.getResultSender().lastResult( Boolean.TRUE);
        }
        else if (operation.equalsIgnoreCase("localdestroy")) {
          Log.getLogWriter().info("Inside local destroy execute");
          Region region = regionContext.getDataSet();
          for (int i = 2; i < arguments.size(); i++) {
            Object key = arguments.get(i);
            region.localDestroy(key);
            Log.getLogWriter().info("Did local destroy using execute..");
          }
          context.getResultSender().lastResult(Boolean.TRUE);
        }
        else if (operation.equalsIgnoreCase("putAll")) {
          Log.getLogWriter().info("Inside putAll execute");
          HashMap map = (HashMap)arguments.get(2);
          Log.getLogWriter().info("got the hash set " + map.toString());
          Region region = regionContext.getDataSet();
          region.putAll(map);
          Log.getLogWriter().info("Did putAll using execute..");

          context.getResultSender().lastResult(Boolean.TRUE);
        }
        else{
          context.getResultSender().lastResult(Boolean.FALSE);
        }
      } else {
        throw new TestException("Did not find function to execute");
      }
    }    

  }
  
  

  public String getId() {
    return this.getClass().getName();
  }

  public boolean hasResult() {
    return true;
  }
  
  public boolean optimizeForWrite() {
    return true;
  }

  public void init(Properties props) {
  }
  
  public boolean isHA() {
    return true;
  }
  
  protected Object createObject (Object key, RandomValues randomValues) {
    if (objectType.equals("delta.DeltaValueHolder")) {
      return new DeltaValueHolder((String)key, randomValues);
    } else if ((objectType.equals("util.PdxVersionedValueHolder") ||
               (objectType.equals("util.VersionedValueHolder")))) {
      // in a function we don't have a classpath for pdx objects, to use the PdxInstanceFactory
      return PdxInstanceCreator.getVersionedValueHolder(objectType, (String)key, randomValues);
    }
    else {
      return new ValueHolder((String)key, randomValues);
    }
  }


}
