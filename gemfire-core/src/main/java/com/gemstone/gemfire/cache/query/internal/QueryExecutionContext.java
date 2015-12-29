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
package com.gemstone.gemfire.cache.query.internal;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.pdx.internal.PdxString;
import com.gemstone.gnu.trove.TIntHashSet;

/**
 * This ExecutionContext will be used ONLY for querying because this
 * is a bit heavt-weight context whose life is longer in JVM than
 * {@link ExecutionContext} which will be used ONLY for index updates.
 *
 * @author shobhit
 * @since 7.0
 */
public class QueryExecutionContext extends ExecutionContext {

  private int nextFieldNum = 0;
  private Query query;
  private TIntHashSet successfulBuckets;

  private boolean cqQueryContext = false;
  
  private SelectResults results;
  
  private List bucketList;
  
  /**
   * stack used to determine which execCache to currently be using
   */
  private final Stack execCacheStack = new Stack();
  
  /**
   * a map that stores general purpose maps for caching data that is valid 
   * for one query execution only
   */
  private final Map execCaches = new HashMap();

  /**
   * This map stores PdxString corresponding to the bind argument
   */
  private Map<Integer, PdxString> bindArgumentToPdxStringMap;

  
  /**
   * @param bindArguments
   * @param cache
   */
  public QueryExecutionContext(Object[] bindArguments, Cache cache) {
    super(bindArguments, cache);
  }

  /**
   * @param bindArguments
   * @param cache
   * @param results
   */
  public QueryExecutionContext(Object[] bindArguments, Cache cache,
      SelectResults results) {
    super(bindArguments, cache);
    this.results = results;
  }

  /**
   * @param bindArguments
   * @param cache
   * @param query
   */
  public QueryExecutionContext(Object[] bindArguments, Cache cache, Query query) {
    super(bindArguments, cache);
    this.query = query;
  }


  // General purpose caching methods for data that is only valid for one
  // query execution
  void cachePut(Object key, Object value) {
    //execCache can be empty in cases where we are doing adds to indexes
    //in that case, we use a default execCache
    int scopeId = -1;
    if (!execCacheStack.isEmpty()) {
      scopeId = (Integer) execCacheStack.peek();
    }
    Map execCache = (Map)execCaches.get(scopeId);
    if (execCache == null) {
      execCache = new HashMap();
      execCaches.put(scopeId, execCache);
    }
    execCache.put(key, value);
  }

  public Object cacheGet(Object key) {
    //execCache can be empty in cases where we are doing adds to indexes
    //in that case, we use a default execCache
    int scopeId = -1;
    if (!execCacheStack.isEmpty()) {
      scopeId = (Integer) execCacheStack.peek();
    }
    Map execCache = (Map)execCaches.get(scopeId);
    return execCache != null? execCache.get(key): null;
  }

  public void pushExecCache(int scopeNum) {
    execCacheStack.push(scopeNum);
  }
  
  public void popExecCache() {
    execCacheStack.pop();
  }

  /**
   * Added to reset the state from the last execution. This is added for CQs only.
   */
  public void reset(){
    super.reset();
    this.execCacheStack.clear();
  }

  int nextFieldNum() {
    return this.nextFieldNum++;
  }
  
  public void setCqQueryContext(boolean cqQuery){
    this.cqQueryContext = cqQuery;
  }

  public boolean isCqQueryContext(){
    return this.cqQueryContext;
  }

  public SelectResults getResults() {
    return this.results;
  }

  public Query getQuery() {
    return query;
  }
  
  public void setBucketList(List list) {
    this.bucketList = list;
    this.successfulBuckets = new TIntHashSet();
  }

  public List getBucketList() {
    return this.bucketList;
  }
  
  public void addToSuccessfulBuckets(int bId) {
    this.successfulBuckets.add(bId);
  }
  
  public int[] getSuccessfulBuckets() {
    return this.successfulBuckets.toArray();
  }
  
  /**
   * creates new PdxString from String and caches it
   */
  public PdxString getSavedPdxString(int index){
    if(bindArgumentToPdxStringMap == null){
      bindArgumentToPdxStringMap = new HashMap<Integer, PdxString>();
    } 
    
    PdxString pdxString = bindArgumentToPdxStringMap.get(index-1);
    if(pdxString == null){
      pdxString = new PdxString((String)bindArguments[index-1]);
      bindArgumentToPdxStringMap.put(index-1, pdxString);
    }
    return pdxString;
    
  }
}