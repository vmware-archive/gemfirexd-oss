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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.LowMemoryException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.internal.ExecutablePool;
import com.gemstone.gemfire.cache.client.internal.GetDurableCQsOp;
import com.gemstone.gemfire.cache.client.internal.InternalPool;
import com.gemstone.gemfire.cache.client.internal.ProxyCache;
import com.gemstone.gemfire.cache.client.internal.ServerCQProxy;
import com.gemstone.gemfire.cache.client.internal.UserAttributes;
import com.gemstone.gemfire.cache.query.AmbiguousNameException;
import com.gemstone.gemfire.cache.query.CqAttributes;
import com.gemstone.gemfire.cache.query.CqException;
import com.gemstone.gemfire.cache.query.CqExistsException;
import com.gemstone.gemfire.cache.query.CqQuery;
import com.gemstone.gemfire.cache.query.CqServiceStatistics;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.IndexCreationException;
import com.gemstone.gemfire.cache.query.IndexExistsException;
import com.gemstone.gemfire.cache.query.IndexNameConflictException;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.NameResolutionException;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryExecutionLowMemoryException;
import com.gemstone.gemfire.cache.query.QueryInvalidException;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.RegionNotFoundException;
import com.gemstone.gemfire.cache.query.TypeMismatchException;
import com.gemstone.gemfire.cache.query.internal.index.IndexData;
import com.gemstone.gemfire.cache.query.internal.index.IndexManager;
import com.gemstone.gemfire.cache.query.internal.index.IndexUtils;
import com.gemstone.gemfire.cache.query.internal.index.PartitionedIndex;
import com.gemstone.gemfire.cache.query.internal.parse.OQLLexerTokenTypes;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.control.MemoryThresholds;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * @author Eric Zoerner
 * @author Asif
 * @version $Revision: 1.2 $
 */
public class DefaultQueryService implements QueryService {

  /** 
   * System property to allow query on region with heterogeneous objects. 
   * By default its set to false.
   */
  public static final boolean QUERY_HETEROGENEOUS_OBJECTS = 
    Boolean.valueOf(System.getProperty("gemfire.QueryService.QueryHeterogeneousObjects", "true")).booleanValue(); 

  /** Test purpose only */
  public static boolean TEST_QUERY_HETEROGENEOUS_OBJECTS = false;

  private final Cache cache;

  private Pool pool;
  
  private ServerCQProxy serverProxy;

  private LogWriterI18n logger;
  
  public DefaultQueryService(Cache cache) {
    if (cache == null)
        throw new IllegalArgumentException(LocalizedStrings.DefaultQueryService_CACHE_MUST_NOT_BE_NULL.toLocalizedString());
    this.cache = cache;
    logger = cache.getLoggerI18n();
  }

  /**
   * Constructs a new <code>Query</code> object. Uses the default namespace,
   * which is the Objects Context of the current application.
   * 
   * @return The new <code>Query</code> object.
   * @throws IllegalArgumentException if the query syntax is invalid.
   * @see com.gemstone.gemfire.cache.query.Query
   */
  public Query newQuery(String queryString) {
    if (QueryMonitor.isLowMemory()) {
      String reason = LocalizedStrings.QueryMonitor_LOW_MEMORY_CANCELED_QUERY.toLocalizedString(QueryMonitor.getMemoryUsedDuringLowMemory());
      throw new QueryExecutionLowMemoryException(reason);
    }
    if (queryString == null)
        throw new QueryInvalidException(LocalizedStrings.DefaultQueryService_THE_QUERY_STRING_MUST_NOT_BE_NULL.toLocalizedString());
    if (queryString.length() == 0)
        throw new QueryInvalidException(LocalizedStrings.DefaultQueryService_THE_QUERY_STRING_MUST_NOT_BE_EMPTY.toLocalizedString());
    DefaultQuery query = new DefaultQuery(queryString, this.cache);
    query.setServerProxy(this.serverProxy);
    return query;
  }
  
  public Query newQuery(String queryString,ProxyCache proxyCache){
    Query query = newQuery(queryString);
    ((DefaultQuery) query).setProxyCache(proxyCache);
    return query;
  }

  public Index createHashIndex(String indexName,
      String indexedExpression, String fromClause)
      throws IndexNameConflictException, IndexExistsException, 
      RegionNotFoundException {
    return createHashIndex(indexName, indexedExpression, fromClause,
        null);
    }
  
  public Index createHashIndex(String indexName,
      String indexedExpression, String fromClause, String imports)
      throws IndexNameConflictException, IndexExistsException, 
      RegionNotFoundException {
    return createIndex(indexName, IndexType.HASH, indexedExpression, fromClause,
        imports);
  }
  
  public Index createIndex(String indexName,
      String indexedExpression, String fromClause)
      throws IndexNameConflictException, IndexExistsException, 
      RegionNotFoundException {
    return createIndex(indexName, IndexType.FUNCTIONAL, indexedExpression, fromClause,
        null);
  }

  public Index createIndex(String indexName,
      String indexedExpression, String fromClause, String imports)
      throws IndexNameConflictException, IndexExistsException, 
      RegionNotFoundException {
    return createIndex(indexName, IndexType.FUNCTIONAL, indexedExpression, fromClause,
        imports);
  }

  public Index createKeyIndex(String indexName,
      String indexedExpression, String fromClause)
      throws IndexNameConflictException, IndexExistsException, 
      RegionNotFoundException {
    return createIndex(indexName, IndexType.PRIMARY_KEY, indexedExpression, fromClause,
        null);
  }
  
  public Index createIndex(String indexName, IndexType indexType,
      String indexedExpression, String fromClause)
      throws IndexNameConflictException, IndexExistsException, 
      RegionNotFoundException {
    return createIndex(indexName, indexType, indexedExpression, fromClause,
        null);
  }

  public Index createIndex(String indexName, IndexType indexType,
      String indexedExpression, String fromClause, String imports)
      throws IndexNameConflictException, IndexExistsException, 
      RegionNotFoundException {
    
    if (pool != null){
      throw new UnsupportedOperationException("Index creation on the server is not supported from the client.");  
    }
    
    QCompiler compiler = new QCompiler(this.cache.getLoggerI18n());
    PartitionedIndex parIndex = null;
    if (imports != null) {
      compiler.compileImports(imports);
    }
    List list = compiler.compileFromClause(fromClause);
    CompiledValue cv = QueryUtils
        .obtainTheBottomMostCompiledValue(((CompiledIteratorDef) list.get(0))
            .getCollectionExpr());
    String regionPath = null;
    if (cv.getType() == OQLLexerTokenTypes.RegionPath) {
      regionPath = ((CompiledRegion) cv).getRegionPath();
    }
    else {
      throw new RegionNotFoundException(LocalizedStrings.DefaultQueryService_DEFAULTQUERYSERVICECREATEINDEXFIRST_ITERATOR_OF_INDEX_FROM_CLAUSE_DOES_NOT_EVALUATE_TO_A_REGION_PATH_THE_FROM_CLAUSE_USED_FOR_INDEX_CREATION_IS_0.toLocalizedString(fromClause));
    }
    Region region = cache.getRegion(regionPath);
    if (region == null) { throw new RegionNotFoundException(LocalizedStrings.DefaultQueryService_REGION_0_NOT_FOUND_FROM_1.toLocalizedString(new Object[] {regionPath, fromClause})); }
    RegionAttributes ra = region.getAttributes();
    
//    LogWriterI18n l = region.getCache().getLogger();
    /*
    l.finer("defaultqueruservice list from clause : "
        + ((CompiledIteratorDef)list.get(0)).getName()
        + " and compiled value : "
        + ((CompiledIteratorDef)list.get(0)).getCollectionExpr()
        + " the from clasue : " + fromClause + " indexedExpression : "+indexedExpression+" and the length of list : "+list.size());
    if (list.size() > 1) {
    l.info(LocalizedStrings.DefaultQueryService_PRINTING_LIST_CONTENTS_);
    for (int k = 0 ; k < list.size(); k++)
      l.info(LocalizedStrings.DefaultQueryService_LIST_ELEMENT___0___AND_ITS_NAME___1, new Object[] {k, ((CompiledIteratorDef)list.get(k)).getName()});
    l.info(LocalizedStrings.DefaultQueryService_ENDING_LIST_CONTENTS_);
    }*/
    //Asif: If the evistion action is Overflow to disk then do not allow index creation
    //It is Ok to have index creation if it is persist only mode as data will always
    //exist in memory
    //if(ra.getEvictionAttributes().getAction().isOverflowToDisk() ) {
    //  throw new UnsupportedOperationException(LocalizedStrings.DefaultQueryService_INDEX_CREATION_IS_NOT_SUPPORTED_FOR_REGIONS_WHICH_OVERFLOW_TO_DISK_THE_REGION_INVOLVED_IS_0.toLocalizedString(regionPath));
    //}
    // if its a pr the create index on all of the local buckets.
    if (((LocalRegion)region).memoryThresholdReached.get() &&
        !MemoryThresholds.isLowMemoryExceptionDisabled()) {
      LocalRegion lr = (LocalRegion)region;
      throw new LowMemoryException(LocalizedStrings.ResourceManager_LOW_MEMORY_FOR_INDEX
          .toLocalizedString(region.getName()), lr.getMemoryThresholdReachedMembers());
    }
    if (region instanceof PartitionedRegion) {
      try {
        parIndex = (PartitionedIndex)((PartitionedRegion)region).createIndex(
            false, indexType, indexName, indexedExpression, fromClause,  imports);
      }
      catch (ForceReattemptException ex) {
        region.getCache().getLoggerI18n().info(
          LocalizedStrings.DefaultQueryService_EXCEPTION_WHILE_CREATING_INDEX_ON_PR_DEFAULT_QUERY_PROCESSOR,
          ex);
      }
      catch (IndexCreationException exx) {
        region.getCache().getLoggerI18n().info(
          LocalizedStrings.DefaultQueryService_EXCEPTION_WHILE_CREATING_INDEX_ON_PR_DEFAULT_QUERY_PROCESSOR,
          exx);
      }
      return parIndex;

    }
    else {
    	
      IndexManager indexManager = IndexUtils.getIndexManager(region, true);
      Index index = indexManager.createIndex(indexName, indexType,
        indexedExpression, fromClause , imports, null, null);
      
      return index;
    }
  }

  /**
   * Asif : Gets an exact match index ( match level 0)
   * 
   * @param regionPath String containing the region name
   * @param definitions An array of String objects containing canonicalized
   *          definitions of RuntimeIterators. A Canonicalized definition of a
   *          RuntimeIterator is the canonicalized expression obtainded from its
   *          underlying collection expression.
   * @param indexType IndexType object which can be either of type RangeIndex or
   *          PrimaryKey Index
   * @param indexedExpression CompiledValue containing the path expression on which
   *          index needs to be created
   * @param  context ExecutionContext         
   * @return IndexData object
   * @throws NameResolutionException 
   * @throws TypeMismatchException 
   * @throws AmbiguousNameException 
   */
  public IndexData getIndex(String regionPath, String[] definitions,
      IndexType indexType, CompiledValue indexedExpression, ExecutionContext context) 
  throws AmbiguousNameException, TypeMismatchException, NameResolutionException {
    Region region = cache.getRegion(regionPath);
    if (region == null) { return null; }
    IndexManager indexManager = IndexUtils.getIndexManager(region, true);
    IndexData indexData = indexManager.getIndex(indexType, definitions,
        indexedExpression, context);
    return indexData;
  }

  public Index getIndex(Region region, String indexName) {
    
    if (pool != null){
      throw new UnsupportedOperationException("Index Operation is not supported on the Server Region.");  
    }

    // A Partition Region does not have an IndexManager, but it's buckets have.
    if (region instanceof PartitionedRegion) {
      return (Index) ((PartitionedRegion) region).getIndex().get(indexName);
    } else {
      IndexManager indexManager = IndexUtils.getIndexManager(region, false);
      if (indexManager == null)
        return null;
      return indexManager.getIndex(indexName);
    }
  }

  /**
   * Asif: Gets a best match index which is available. An index with match level
   * equal to 0 is the best index to use as it implies that the query from
   * clause iterators belonging to the region exactly match the index from
   * clause iterators ( the difference in the relative positions of the
   * iterators do not matter). A match level less than 0 means that number of
   * iteratots in the index resultset is more than that present in the query
   * from clause and hence index resultset will need a cutdown. A match level
   * greater than 0 means that there definitely is atleast one iterator in the
   * query from clause which is more than the index from clause iterators &
   * hence definitely expansion of index results will be needed. Pls note that a
   * match level greater than 0 does not imply that index from clause does not
   * have an extra iterator in it , too. Hence a match level greater than 0 will
   * definitely mean expansion of index results but may also require a cut down
   * of results . The order of preference is match level 0 , less than 0 and
   * lastly greater than 0
   * 
   * @param regionPath String containing the region name
   * @param definitions An array of String objects containing canonicalized
   *          definitions of RuntimeIterators. A Canonicalized definition of a
   *          RuntimeIterator is the canonicalized expression obtainded from its
   *          underlying collection expression.
   * @param indexType IndexType object which can be either of type RangeIndex or
   *          PrimaryKey Index
   * @param indexedExpression CompiledValue representing  the path expression on which
   *          index needs to be created
   * @param  context ExecutionContext object         
   * @return IndexData object
   * @throws NameResolutionException 
   * @throws TypeMismatchException 
   * @throws AmbiguousNameException 
   */
  public IndexData getBestMatchIndex(String regionPath, String definitions[],
      IndexType indexType, CompiledValue indexedExpression, ExecutionContext context) throws AmbiguousNameException, TypeMismatchException, NameResolutionException {
    Region region = cache.getRegion(regionPath);
//    LogWriterI18n logger = cache.getLogger();
    if (region == null){ 
      return null; 
    }
    // return getBestMatchIndex(region, indexType, definitions,
    // indexedExpression);
    IndexManager indexManager = IndexUtils.getIndexManager(region, false);
    if (indexManager == null) { 
      return null;
    }
    return indexManager.getBestMatchIndex(indexType, definitions,
        indexedExpression, context);
  }

  public Collection getIndexes() {
    ArrayList allIndexes = new ArrayList();
    Iterator rootRegions = cache.rootRegions().iterator();
    while (rootRegions.hasNext()) {
      Region region = (Region) rootRegions.next();
      Collection indexes = getIndexes(region);
      if (indexes != null) allIndexes.addAll(indexes);
      Iterator subRegions = region.subregions(true).iterator();
      while (subRegions.hasNext()) {
        indexes = getIndexes((Region) subRegions.next());
        if (indexes != null) allIndexes.addAll(indexes);
      }
    }
    return allIndexes;
  }

  public Collection getIndexes(Region region) {

    if (pool != null){
      throw new UnsupportedOperationException("Index Operation is not supported on the Server Region.");  
    }

    if (region instanceof PartitionedRegion) {
      return ((PartitionedRegion)region).getIndexes();
    }
    IndexManager indexManager = IndexUtils.getIndexManager(region, false);
    if (indexManager == null) return null;
    return indexManager.getIndexes();
  }

  public Collection getIndexes(Region region, IndexType indexType) {

    if (pool != null){
      throw new UnsupportedOperationException("Index Operation is not supported on the Server Region.");  
    }

    IndexManager indexManager = IndexUtils.getIndexManager(region, false);
    if (indexManager == null) return null;
    return indexManager.getIndexes(indexType);
  }

  public void removeIndex(Index index) {

    if (pool != null){
      throw new UnsupportedOperationException("Index Operation is not supported on the Server Region.");  
    }

    Region region = index.getRegion();
    if (region instanceof PartitionedRegion) {
      try {
       ((PartitionedRegion)region).removeIndex(index, false);
      }
      catch (ForceReattemptException ex) {
        region.getCache().getLoggerI18n().info(LocalizedStrings.DefaultQueryService_EXCEPTION_REMOVING_INDEX___0, ex);
      }
      return;
    }
    IndexManager indexManager = ((LocalRegion) index.getRegion())
        .getIndexManager();
    indexManager.removeIndex(index);
  }

  public void removeIndexes() {
    if (pool != null){
      throw new UnsupportedOperationException("Index Operation is not supported on the Server Region.");  
    }

    Iterator rootRegions = cache.rootRegions().iterator();
    while (rootRegions.hasNext()) {
      Region region = (Region) rootRegions.next();
      Iterator subRegions = region.subregions(true).iterator();
      while (subRegions.hasNext()) {
        removeIndexes((Region) subRegions.next());
      }
      removeIndexes(region);
    }
  }

  public void removeIndexes(Region region) {

    if (pool != null){
      throw new UnsupportedOperationException("Index Operation is not supported on the Server Region.");  
    }
    
    // removing indexes on paritioned region will reguire sending message and
    // remvoing all the local indexes on the local bucket regions.
    if (region instanceof PartitionedRegion) {
      try {
        //not remotely orignated
        ((PartitionedRegion)region).removeIndexes(false);
      }
      catch(ForceReattemptException ex ) {
           // will have to throw a proper exception relating to remove index.
           region.getCache().getLoggerI18n().info(LocalizedStrings.DefaultQueryService_EXCEPTION_REMOVING_INDEX___0, ex);
      }
    }
    IndexManager indexManager = IndexUtils.getIndexManager(region, false);
    if (indexManager == null) return;
    
    indexManager.removeIndexes();
  }

  
  // CqService Related API implementation.
  
  /**
   * Constructs a new continuous query, represented by an instance of
   * CqQuery. The CqQuery is not executed until the execute method
   * is invoked on the CqQuery.
   * @param queryString the OQL query
   * @param cqAttributes the CqAttributes
   * @return the newly created  CqQuery object
   * @throws IllegalArgumentException if queryString or cqAttr is null
   * @throws IllegalStateException if this method is called from a cache
   *         server
   * @throws QueryInvalidException if there is a syntax error in the query
   * @throws CqException if failed to create cq, failure during creating  
   *         managing cq metadata info.
   *   E.g.: Query string should refer only one region, join not supported.
   *         The query must be a SELECT statement.
   *         DISTINCT queries are not supported.
   *         Projections are not supported.
   *         Only one iterator in the FROM clause is supported, and it must be a region path. 
   *         Bind parameters in the query are not yet supported.
   */
  public CqQuery newCq(String queryString, CqAttributes  cqAttributes) 
  throws QueryInvalidException, CqException {
    CqQueryImpl cq = null;
    try {
      cq = (CqQueryImpl)getCqService().newCq(null, queryString, cqAttributes, this.serverProxy, false);
    } catch (CqExistsException cqe) {
      // Should not throw in here.
      if (logger.fineEnabled()){
        logger.fine("Unable to createCq. Error :" + cqe.getMessage(), cqe);
      }
    }
    return cq;
  }
  
  /**
   * Constructs a new continuous query, represented by an instance of
   * CqQuery. The CqQuery is not executed until the execute method
   * is invoked on the CqQuery.
   * @param queryString the OQL query
   * @param cqAttributes the CqAttributes
   * @param isDurable true if the CQ is durable
   * @return the newly created  CqQuery object
   * @throws IllegalArgumentException if queryString or cqAttr is null
   * @throws IllegalStateException if this method is called from a cache
   *         server
   * @throws QueryInvalidException if there is a syntax error in the query
   * @throws CqException if failed to create cq, failure during creating  
   *         managing cq metadata info.
   *   E.g.: Query string should refer only one region, join not supported.
   *         The query must be a SELECT statement.
   *         DISTINCT queries are not supported.
   *         Projections are not supported.
   *         Only one iterator in the FROM clause is supported, and it must be a region path. 
   *         Bind parameters in the query are not yet supported.
   */
  public CqQuery newCq(String queryString, CqAttributes  cqAttributes, boolean isDurable) 
  throws QueryInvalidException, CqException {
    CqQueryImpl cq = null;
    try {
      cq = (CqQueryImpl)getCqService().newCq(null, queryString, cqAttributes, this.serverProxy, isDurable);
    } catch (CqExistsException cqe) {
      // Should not throw in here.
      if (logger.fineEnabled()){
        logger.fine("Unable to createCq. Error :" + cqe.getMessage(), cqe);
      }
    }
    return cq;
  }
  
  
  /**
   * Constructs a new named continuous query, represented by an instance of
   * CqQuery. The CqQuery is not executed, however, until the execute method
   * is invoked on the CqQuery. The name of the query will be used
   * to identify this query in statistics archival.
   *
   * @param cqName the String name for this query
   * @param queryString the OQL query
   * @param cqAttributes the CqAttributes
   * @return the newly created  CqQuery object
   * @throws CqExistsException if a CQ by this name already exists on this
   *         client
   * @throws IllegalArgumentException if queryString or cqAttr is null
   * @throws IllegalStateException if this method is called from a cache
   *         server
   * @throws QueryInvalidException if there is a syntax error in the query
   * @throws CqException if failed to create cq, failure during creating  
   *         managing cq metadata info.
   *   E.g.: Query string should refer only one region, join not supported.
   *         The query must be a SELECT statement.
   *         DISTINCT queries are not supported.
   *         Projections are not supported.
   *         Only one iterator in the FROM clause is supported, and it must be a region path. 
   *         Bind parameters in the query are not yet supported.
   */
  public CqQuery newCq(String cqName, String queryString, CqAttributes cqAttributes)
  throws QueryInvalidException, CqExistsException, CqException {
    if (cqName == null) {
      throw new IllegalArgumentException(LocalizedStrings.DefaultQueryService_CQNAME_MUST_NOT_BE_NULL.toLocalizedString());
    }
    CqQueryImpl cq = (CqQueryImpl)getCqService().newCq(cqName, queryString, cqAttributes, this.serverProxy, false);
    return cq;
  }
  
  /**
   * Constructs a new named continuous query, represented by an instance of
   * CqQuery. The CqQuery is not executed, however, until the execute method
   * is invoked on the CqQuery. The name of the query will be used
   * to identify this query in statistics archival.
   *
   * @param cqName the String name for this query
   * @param queryString the OQL query
   * @param cqAttributes the CqAttributes
   * @param isDurable true if the CQ is durable
   * @return the newly created  CqQuery object
   * @throws CqExistsException if a CQ by this name already exists on this
   *         client
   * @throws IllegalArgumentException if queryString or cqAttr is null
   * @throws IllegalStateException if this method is called from a cache
   *         server
   * @throws QueryInvalidException if there is a syntax error in the query
   * @throws CqException if failed to create cq, failure during creating  
   *         managing cq metadata info.
   *   E.g.: Query string should refer only one region, join not supported.
   *         The query must be a SELECT statement.
   *         DISTINCT queries are not supported.
   *         Projections are not supported.
   *         Only one iterator in the FROM clause is supported, and it must be a region path. 
   *         Bind parameters in the query are not yet supported.
   */
  public CqQuery newCq(String cqName, String queryString, CqAttributes cqAttributes, boolean isDurable)
  throws QueryInvalidException, CqExistsException, CqException {
    if (cqName == null) {
      throw new IllegalArgumentException(LocalizedStrings.DefaultQueryService_CQNAME_MUST_NOT_BE_NULL.toLocalizedString());
    }
    CqQueryImpl cq = (CqQueryImpl)getCqService().newCq(cqName, queryString, cqAttributes, this.serverProxy, isDurable);
    return cq;
  }
  
  /** 
   * Close all CQs executing in this VM, and release resources
   * associated with executing CQs.
   * CqQuerys created by other VMs are unaffected.
   *
   */
  public void closeCqs() {
    try {
      getCqService().closeAllCqs(true);
    } catch (CqException cqe) {
      logger.fine("Unable to closeAll Cqs. Error :" + cqe.getMessage(), cqe);  
    }
  }
  
  /**
   * Retrieve a CqQuery by name.
   * @return the CqQuery or null if not found
   */
  public CqQuery getCq(String cqName) {
    CqQuery cq = null;
    try {
      cq = getCqService().getCq(cqName);
    } catch (CqException cqe) {
      logger.fine("Unable to getCq. Error :" + cqe.getMessage(), cqe);  
    }
    return cq;
  }
  
  /**
   * Retrieve  all CqQuerys created by this VM.
   * @return null if there are no cqs.   
   */ 
  public CqQuery[] getCqs() {
    CqQuery[] cqs = null;
    try {
      cqs = getCqService().getAllCqs();
    } catch (CqException cqe) {
      logger.fine("Unable to getAllCqs. Error :" + cqe.getMessage(), cqe);  
    }
    return cqs;
  }
  
  /**
   * Returns all the cq on a given region.
   */
  public CqQuery[] getCqs(final String regionName) throws CqException
  {
    return getCqService().getAllCqs(regionName);
  }

  /**
   * Starts execution of all the registered continuous queries for this client.
   * This is complementary to stopCqs.
   * @see QueryService#stopCqs()
   * 
   * @throws CqException if failure to execute CQ.
   */
  public void executeCqs() throws CqException {
    try {
      getCqService().executeAllClientCqs();
    } catch (CqException cqe) {
      logger.fine("Unable to execute all cqs. Error :" + cqe.getMessage(), cqe);  
    }
  }
  
  
  /**
   * Stops execution of all the continuous queries for this client to become inactive.
   * This is useful when client needs to control the incoming cq messages during
   * bulk region operations.
   * @see QueryService#executeCqs()
   * 
   * @throws CqException if failure to execute CQ.
   */
  public void stopCqs() throws CqException {
    try {
      getCqService().stopAllClientCqs();
    } catch (CqException cqe) {
      logger.fine("Unable to stop all CQs. Error :" + cqe.getMessage(), cqe);  
    }
  }
  
  /**
   * Starts execution of all the continuous queries registered on the specified 
   * region for this client. 
   * This is complementary method to stopCQs().  
   * @see QueryService#stopCqs()
   * 
   * @throws CqException if failure to stop CQs.
   */
  public void executeCqs(String regionName) throws CqException {
    try {
      getCqService().executeAllRegionCqs(regionName);
    } catch (CqException cqe) {
      logger.fine("Unable to execute cqs on the specified region. Error :" + cqe.getMessage(), cqe);  
    }
  }
  
  /**
   * Stops execution of all the continuous queries registered on the specified 
   * region for this client. 
   * This is useful when client needs to control the incoming cq messages during
   * bulk region operations.
   * @see QueryService#executeCqs()
   * 
   * @throws CqException if failure to execute CQs.
   */  
  public void stopCqs(String regionName) throws CqException {
    try {
      getCqService().stopAllRegionCqs(regionName);
    } catch (CqException cqe) {
      logger.fine("Unable to stop cqs on the specified region. Error :" + cqe.getMessage(), cqe);  
    }
  }
  
  /**
   * Get statistics information for this query.
   * @return CQ statistics
   *         null if the continuous query object not found for the given cqName.
   */
  public CqServiceStatistics getCqStatistics() {
    CqServiceStatistics stats = null;
    try {
      stats = getCqService().getCqStatistics();
    } catch (CqException cqe) {
      logger.fine("Unable get CQ Statistics. Error :" + cqe.getMessage(), cqe);  
    }
    return stats;
  }
  
  /**
   * Is the CQ service in a cache server environment
   * @return true if cache server, false otherwise
   */
  public boolean isServer() {
    boolean isServer = false;
    try {
      isServer = getCqService().isServer();
    } catch (CqException cqe) {
      logger.fine("Unable get Server Status. Error :" + cqe.getMessage(), cqe);  
    }
    return isServer;
  }
  
  /**
   * Close the CQ Service after clean up if any.
   *
   */
  public void closeCqService() {
    CqService.closeCqService();
  }
  
  /**
   * @return CqService
   */
  public CqService getCqService() throws CqException {    
    return CqService.getCqService(cache);
  }
  
  public void setPool(Pool pool) {
    this.pool = pool;
    this.serverProxy = new ServerCQProxy((InternalPool)this.pool); 
    if (logger.fineEnabled()){
      logger.fine("Setting ServerProxy with the Query Service using the pool :" + pool.getName() +
        " And ServerProxy is: " + this.serverProxy);
    }
  }
  
  public Pool getPool() {
    return this.pool;   
  }

  ServerCQProxy getServerProxy() {
    return this.serverProxy;
  }
  
  public List<String> getAllDurableCqsFromServer() throws CqException {
    if (!isServer()) {
      if (serverProxy != null) {
        return serverProxy.getAllDurableCqsFromServer();
      }
      if (pool != null) {
        return GetDurableCQsOp.execute((ExecutablePool)pool);
      }
      else {
        throw new UnsupportedOperationException("GetAllDurableCQsFromServer requires a pool to be configured.");  
      }
    }
    else {
      //we are a server
      return Collections.EMPTY_LIST;
    }
  }

  public UserAttributes getUserAttributes(String cqName) {
    try {
      return getCqService().getUserAttributes(cqName);
    } catch (CqException ce) {
      return null;
    }
  }
}
