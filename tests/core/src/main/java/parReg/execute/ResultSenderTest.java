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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import parReg.ParRegBB;
import parReg.colocation.KeyResolver;
import parReg.colocation.Month;
import util.TestException;
import getInitialImage.InitImageBB;
import hydra.BridgeHelper;
import hydra.CacheHelper;
import hydra.Log;
import hydra.PoolHelper;
import hydra.RegionHelper;
import hydra.TestConfig;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

public class ResultSenderTest {
  
  protected static ResultSenderTest testInstance;

  protected static Cache theCache;

  protected static Region aRegion;
  
  public static final int NUM_KEYS = 50;
  public static final String BUCKETS_ON_NODE = "Buckets on node";
  public static final String ALL_BUCKET_IDS = "All Bucket Ids";
  
  public synchronized static void HydraTask_initialize() {
    if (testInstance == null) {
      testInstance = new ResultSenderTest();
      testInstance.initialize("region");
    }
  }
  
  public synchronized static void HydraTask_initializeServer() {
    if (testInstance == null) {
      testInstance = new ResultSenderTest();
      testInstance.initialize("bridge");
       BridgeHelper.startBridgeServer("bridge");
    }
 }
  
  public synchronized static void HydraTask_initializeClient() {
    if (testInstance == null) {
      testInstance = new ResultSenderTest();
      testInstance.initialize("edge");
    }
 }
  
  public synchronized static void HydraTask_populateRegion() {
    if (testInstance == null) {
      testInstance = new ResultSenderTest();
    }
    testInstance.populateRegion(aRegion);
  }
  
  public synchronized static void HydraTask_populateRegionWithCustomPartition() {
    if (testInstance == null) {
      testInstance = new ResultSenderTest();
    }
    testInstance.populateRegionWithCustomPartition(aRegion);
  }
  
  public synchronized static void HydraTask_registerFunction() {
    if (testInstance == null) {
      testInstance = new ResultSenderTest();
    }
    FunctionService.registerFunction(new ResultSenderFunction());
    FunctionService.registerFunction(new NodePruningFunction());
  }
  
  public synchronized static void HydraTask_updateBBWithNodes() {
    if (testInstance == null) {
      testInstance = new ResultSenderTest();
    }
    testInstance.updateBBWithNodes(aRegion);
  }
  
  public synchronized static void HydraTask_doRegionFunction() {
    if (testInstance == null) {
      testInstance = new ResultSenderTest();
    }
    testInstance.doRegionFunction(aRegion);
  }

  public synchronized static void HydraTask_doNodePruningRegionFunction() {
    if (testInstance == null) {
      testInstance = new ResultSenderTest();
    }
    testInstance.doNodePruningRegionFunction(aRegion);
  }
  
  
  /**
   * Task to initialize the cache and the region
   */
  protected void initialize(String regionDescriptName) {
    theCache = CacheHelper.createCache("cache1");
    String regionName = RegionHelper.getRegionDescription(regionDescriptName)
        .getRegionName();
    Log.getLogWriter().info("Creating region " + regionName);
    RegionAttributes attributes = RegionHelper
        .getRegionAttributes(regionDescriptName);
    String poolName = attributes.getPoolName();
    if (poolName != null) {
      PoolHelper.createPool(poolName);
    }
    aRegion = theCache.createRegion(regionName, attributes);
    Log.getLogWriter().info("Completed creating region " + aRegion.getName());
  }
  
  /**
   * Task to populate the region
   */
  protected void populateRegion(Region region) {
    for (int i = 0; i < NUM_KEYS; i++) {
      String keyName = "Key "
          + ParRegBB.getBB().getSharedCounters().incrementAndRead(
              ParRegBB.numOfPutOperations);
      Integer value = new Integer(i);
      region.put(keyName, value);
    }
    Log.getLogWriter().info(
        "Completed put for " + NUM_KEYS + " keys and region size is "
            + region.size());
  }
  
  /**
   * Task to populate the region with custom partitioning (node pruning test)
   */
  protected void populateRegionWithCustomPartition(Region region) {
    for (int i = 0; i < NUM_KEYS; i++) {
      String keyName = "Key "
          + ParRegBB.getBB().getSharedCounters().incrementAndRead(
              ParRegBB.numOfPutOperations);
      Month routingObjectHolder = Month.months[TestConfig.tab().getRandGen()
          .nextInt(11)];
      KeyResolver key = new KeyResolver(keyName, routingObjectHolder);
      Integer value = new Integer(i);
      region.put(key, value);
    }
    Log.getLogWriter().info(
        "Completed put for " + NUM_KEYS + " keys and region size is "
            + region.size());
  }
  
  
  /**
   * Task to update the BB with the buckets on each nodes and total buckets in
   * the PR
   */
  protected void updateBBWithNodes(Region region) {
    if (!(region instanceof PartitionedRegion)) {
      throw new TestException("This test should be using partitioned region");
    }
    DistributedSystem ds = InternalDistributedSystem.getAnyInstance();
    String localVM = ds.getDistributedMember().getId();

    List bucketListOnNode = ((PartitionedRegion)region)
        .getLocalBucketsListTestOnly();
    ParRegBB.getBB().getSharedMap().put(localVM, bucketListOnNode);

    HashSet allBucketIds;
    if (ParRegBB.getBB().getSharedMap().get(ALL_BUCKET_IDS) == null) {
      allBucketIds = new HashSet();
      allBucketIds.addAll(bucketListOnNode);
    }
    else {
      allBucketIds = (HashSet)ParRegBB.getBB().getSharedMap().get(
          ALL_BUCKET_IDS);
      allBucketIds.addAll(bucketListOnNode);
    }
    ParRegBB.getBB().getSharedMap().put(ALL_BUCKET_IDS, allBucketIds);
  } 
  
  /**
   * Task to do function execution on region
   */
  protected void doRegionFunction(Region region) {
    ResultSenderFunction function = new ResultSenderFunction();
    Set keySet = region.keySet();

    Execution dataSet = FunctionService.onRegion(region).withFilter(keySet)
        .withCollector(new BBResultCollector());

    ArrayList list;
    try {
      list = (ArrayList)dataSet.execute(new ResultSenderFunction()).getResult();
    }
    catch (Exception e) {
      throw new TestException("Function execution failed with exception ", e);
    }

    if (list.size() - 1 != keySet.size()) {
      throw new TestException(
          "The getResult should have returned the list of size "
              + keySet.size() + " size of filter but returned "
              + (list.size() - 1));
    }

    ParRegBB.getBB().getSharedCounters().zero(ParRegBB.resultSenderCounter);

  }
  
  /**
   * Task to do node pruning tests
   */
  protected void doNodePruningRegionFunction(Region region) {
    verifyExecuteLocally(region);
    verifyExecuteSingleRemoteNode(region);
    //verifyRandomFilter(region); //BUG 40226 : Node pruning is for without filter case only
    verifyExecuteOnAllBuckets(region);
  }
  
  
  /**
   * This task does function execution on nodes with filter. It gets from the BB
   * the buckets on a remote node and get keys in that bucket. Those keys are
   * passed as filter and expect this function execution happen only on one node
   * (either that node for which keys were taken or any other node which has the
   * same number of bucket - but should be only one node)
   * 
   */
  protected void verifyExecuteSingleRemoteNode(Region region) {
    Log.getLogWriter().info("verifyExecuteSingleRemoteNode");
    DistributedSystem ds = InternalDistributedSystem.getAnyInstance();
    Set allMemberSet = new HashSet(((InternalDistributedSystem)ds)
        .getDistributionManager().getNormalDistributionManagerIds());
    InternalDistributedMember localVM = ((InternalDistributedSystem)ds)
        .getDistributionManager().getDistributionManagerId();
    allMemberSet.remove(localVM);

    Iterator iterator = allMemberSet.iterator();

    while (iterator.hasNext()) {
      String node = ((InternalDistributedMember)iterator.next()).toString();
      List bucketsOnNode = (ArrayList)ParRegBB.getBB().getSharedMap().get(node);
      Log.getLogWriter().info(
          "Filter set is for the buckets on node " + node
              + " who have buckets " + bucketsOnNode);

      Set keySet = new HashSet();

      Iterator iterator1 = bucketsOnNode.iterator();
      while (iterator1.hasNext()) {
        int bucketId = (Integer)iterator1.next();
        try {
          keySet.addAll(((PartitionedRegion)region).getBucketKeys(bucketId));
        }
        catch (Exception e) {
          throw new TestException("Test issue: Caught the exception ", e);
        }
      }

      HashSet keySetHashCodes = new HashSet();
      Iterator keySetItr = keySet.iterator();
      while (keySetItr.hasNext()) {
        KeyResolver key = (KeyResolver)keySetItr.next();
        keySetHashCodes.add(key.getRoutingHint().toString());
      }
      Log.getLogWriter().info(
          "Partition resolvers for the keySet : " + keySetHashCodes.toString());

      Execution dataSet = FunctionService.onRegion(region).withFilter(keySet)
          .withCollector(new ArrayListResultCollector());
      ArrayList list;
      try {
        list = (ArrayList)dataSet.execute(new NodePruningFunction())
            .getResult();
      }
      catch (Exception e) {
        throw new TestException("Function execution failed with exception ", e);
      }

      ParRegBB.getBB().printSharedMap();
      if (list.size() != 1) {
        throw new TestException(
            "This function Should have executed only on 1 node " + node
                + " but executed on " + list);
      }
    }
  }
  
  
  /**
   * This task does function execution on nodes with no filter passed. It gets
   * from the BB the total number of buckets of the PR. Test finds out the
   * optimal number of nodes on which the function has to be executed and
   * expects the function to executed on those many nodes (or lesser than those
   * nodes, if product has a better way of doing it)
   * 
   */
  protected void verifyExecuteOnAllBuckets(Region region) {
    Log.getLogWriter().info("verifyExecuteOnAllBuckets");

    Log.getLogWriter().info("Function to be executed on buckets all buckets");

    ArrayList allBucketIds = new ArrayList();
    HashSet allBuckets = (HashSet)ParRegBB.getBB().getSharedMap().get(
        ALL_BUCKET_IDS);
    allBucketIds.addAll(allBuckets);

    if (allBucketIds.size() != 12) {
      throw new TestException(
          "With custom partitioning on, the total number of buckets should be 12 but the buckets are "
              + allBucketIds);
    }

    Execution dataSet = FunctionService.onRegion(region).withCollector(
        new ArrayListResultCollector());
    ArrayList list;
    try {
      list = (ArrayList)dataSet.execute(new NodePruningFunction()).getResult();
    }
    catch (Exception e) {
      throw new TestException("Function execution failed with exception ", e);
    }

    hydra.Log.getLogWriter().info("The list is " + list);

    List optimumNodes = getOptimumNodeList(allBucketIds);

    ParRegBB.getBB().printSharedMap();
    if (list.size() > optimumNodes.size()) {
      throw new TestException(
          "This function (execute on all buckets - without filter) should have executed on "
              + optimumNodes.size()
              + " nodes ("
              + optimumNodes
              + ") but executed on " + list.size() + " nodes (" + list + ")");
    }
    else {
      Log.getLogWriter().info(
          "Execution on number of nodes " + list.size() + " (" + list
              + ") and the test calculated " + optimumNodes.size() + " nodes ("
              + optimumNodes + ")");
    }
  }
  
  
  
  /**
   * This task does function execution on nodes with filter. Test finds out the
   * optimal number of nodes on which the function has to be executed and
   * expects the function to executed on those many nodes (or lesser than those
   * nodes, if product has a better way of doing it)
   * 
   */
  protected void verifyRandomFilter(Region region) {
    Log.getLogWriter().info("verifyRandomFilter");
    Set keySet = new HashSet();
    List bucketIds;
    do {
      bucketIds = getRandomBucketIds(12);
    } while (bucketIds.size() == 0);

    Iterator iterator1 = bucketIds.iterator();
    while (iterator1.hasNext()) {
      int bucketId = (Integer)iterator1.next();
      try {
        keySet.addAll(((PartitionedRegion)region).getBucketKeys(bucketId));
      }
      catch (Exception e) {
        throw new TestException("Test issue: Caught the exception ", e);
      }
    }

    Log.getLogWriter().info("Function to be executed on buckets " + bucketIds);

    HashSet keySetHashCodes = new HashSet();
    Iterator keySetItr = keySet.iterator();
    while (keySetItr.hasNext()) {
      KeyResolver key = (KeyResolver)keySetItr.next();
      keySetHashCodes.add(key.getRoutingHint().toString());
    }
    Log.getLogWriter().info(
        "Partition resolvers for the keySet : " + keySetHashCodes.toString());

    Execution dataSet = FunctionService.onRegion(region).withFilter(keySet)
        .withCollector(new ArrayListResultCollector());
    ArrayList list;
    try {
      list = (ArrayList)dataSet.execute(new NodePruningFunction()).getResult();
    }
    catch (Exception e) {
      throw new TestException("Function execution failed with exception " , e);
    }

    hydra.Log.getLogWriter().info("The list is " + list);

    List optimumNodes = getOptimumNodeList(bucketIds);

    ParRegBB.getBB().printSharedMap();
    if (list.size() > optimumNodes.size()) {
      throw new TestException("This function with routing Objects "
          + keySetHashCodes + " could have executed on " + optimumNodes
          + " but executed on " + list);
    }
    else {
      Log.getLogWriter().info(
          "Execution on number of nodes " + list.size() + " (" + list
              + ") and the test calculated " + optimumNodes.size() + " nodes ("
              + optimumNodes + ")");
    }
  }
  
  
  
  /**
   * Method to find out the optimal number of nodes for the given bucketIds
   * 
   */
  protected List getOptimumNodeList(List bucketIds) {
    DistributedSystem ds = InternalDistributedSystem.getAnyInstance();
    Set<Object> allMemberSet = new HashSet<Object>(
        ((InternalDistributedSystem)ds).getDistributionManager()
            .getNormalDistributionManagerIds());

    ArrayList optimumNodeList = new ArrayList();

    while (bucketIds.size() != 0) {
      Iterator memberSetIterator = allMemberSet.iterator();
      String bestNode = "";
      int numOfBucketsOnBestNode = 0;
      HashMap bucketMap = new HashMap();
      while (memberSetIterator.hasNext()) {
        String node = ((InternalDistributedMember)memberSetIterator.next())
            .toString();
        List bucketsOnNode = (ArrayList)ParRegBB.getBB().getSharedMap().get(
            node);
        bucketsOnNode.retainAll(bucketIds);
        bucketMap.put(node, bucketsOnNode);
        if (bucketsOnNode.size() > numOfBucketsOnBestNode) {
          numOfBucketsOnBestNode = bucketsOnNode.size();
          bestNode = node;
        }
      }

      optimumNodeList.add(bestNode);
      bucketIds.removeAll((List)bucketMap.get(bestNode));
    }

    return optimumNodeList;
  }
  
  
  /**
   * This task does function execution on nodes with filter. It gets from the BB
   * the buckets on a local node and get keys in that bucket. Those keys are
   * passed as filter and expect this function execution happen only on that
   * node. (Even if other node has those buckets, preference should be for the
   * local vm)
   * 
   */
  protected void verifyExecuteLocally(Region region) {
    Log.getLogWriter().info("verifyExecuteLocally");
    List bucketListOnLocalNode = ((PartitionedRegion)region)
        .getLocalBucketsListTestOnly();
    Set keySet = new HashSet();
    DistributedSystem ds = InternalDistributedSystem.getAnyInstance();
    String localVM = ds.getDistributedMember().getId();

    Iterator iterator = bucketListOnLocalNode.iterator();
    while (iterator.hasNext()) {
      int bucketId = (Integer)iterator.next();
      try {
        keySet.addAll(((PartitionedRegion)region).getBucketKeys(bucketId));
      }
      catch (Exception e) {
        throw new TestException("Test issue: Caught the exception ", e);
      }
    }

    Execution dataSet = FunctionService.onRegion(region).withFilter(keySet)
        .withArgs(localVM).withCollector(new ArrayListResultCollector());
    ArrayList list;
    try {
      list = (ArrayList)dataSet.execute(new NodePruningFunction()).getResult();
    }
    catch (Exception e) {
      throw new TestException("Function execution failed with exception " , e);
    }

    if (list.size() != 1) {
      throw new TestException(
          "This function Should have executed only on 1 node " + localVM
              + " but executed on " + list);
    }
  }
  
  
  /**
   * Task to get random bucket IDs
   */
  protected List getRandomBucketIds(int maxBucketId) {
    ArrayList list = new ArrayList();
    long now = System.currentTimeMillis();
    Random rand = new Random(now);

    for (int i = 1; i <= 12; i++) {

      if (rand.nextBoolean()) {
        list.add(i);
      }
    }
    return list;
  }
  
  
}