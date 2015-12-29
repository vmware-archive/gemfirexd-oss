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
/**
 * 
 */
package com.gemstone.gemfire.cache30;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.wan.GatewayEventFilter;
import com.gemstone.gemfire.cache.wan.GatewayReceiver;
import com.gemstone.gemfire.cache.wan.GatewayReceiverFactory;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.cache.wan.GatewaySenderFactory;
import com.gemstone.gemfire.cache.wan.GatewayTransportFilter;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEvent;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventListener;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueue;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueueFactory;
import com.gemstone.gemfire.cache.asyncqueue.internal.AsyncEventQueueImpl;
import com.gemstone.gemfire.cache.util.GatewayConflictHelper;
import com.gemstone.gemfire.cache.util.GatewayConflictResolver;
import com.gemstone.gemfire.cache.util.TimestampedEntryEvent;
import com.gemstone.gemfire.cache.util.Gateway.OrderPolicy;
import com.gemstone.gemfire.internal.cache.wan.parallel.ParallelGatewaySenderImpl;
import com.gemstone.gemfire.internal.cache.wan.serial.SerialGatewaySenderImpl;
import com.gemstone.gemfire.internal.cache.xmlcache.AsyncEventQueueCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXml;
import com.gemstone.gemfire.internal.cache.xmlcache.ParallelGatewaySenderCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.RegionAttributesCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.SerialGatewaySenderCreation;


/**
 * @author bruce
 *
 */
public class CacheXml70Test extends CacheXml66Test {
  private static final long serialVersionUID = 225193925777688541L;

  public CacheXml70Test(String name) {
    super(name);
  }

  
  // ////// Helper methods

  @Override
  protected String getGemFireVersion()
  {
    return CacheXml.VERSION_7_0;
  }

  /** make sure we can create regions with concurrencyChecksEnabled=true */
  public void testConcurrencyChecksEnabled() throws CacheException {
    CacheCreation cache = new CacheCreation();
    RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    attrs.setScope(Scope.DISTRIBUTED_ACK);
    attrs.setDataPolicy(DataPolicy.REPLICATE);
    attrs.setConcurrencyChecksEnabled(true);
    cache.createRegion("replicated", attrs);
    
    attrs = new RegionAttributesCreation(cache);
    attrs.setDataPolicy(DataPolicy.PARTITION);
    attrs.setConcurrencyChecksEnabled(true);
    cache.createRegion("partitioned", attrs);
    
    attrs = new RegionAttributesCreation(cache);
    attrs.setScope(Scope.DISTRIBUTED_ACK);
    attrs.setDataPolicy(DataPolicy.EMPTY);
    attrs.setConcurrencyChecksEnabled(true);
    cache.createRegion("empty", attrs);
    
    attrs = new RegionAttributesCreation(cache);
    attrs.setScope(Scope.DISTRIBUTED_ACK);
    attrs.setConcurrencyChecksEnabled(true);
    cache.createRegion("normal", attrs);
    
    testXml(cache);
    
    Cache c = getCache();
    assertNotNull(c);

    Region region = c.getRegion("replicated");
    assertNotNull(region);
    assertTrue("expected concurrency checks to be enabled", region.getAttributes().getConcurrencyChecksEnabled());
    region.localDestroyRegion();

    region = c.getRegion("partitioned");
    assertNotNull(region);
    assertTrue("expected concurrency checks to be enabled", region.getAttributes().getConcurrencyChecksEnabled());
    region.localDestroyRegion();

    region = c.getRegion("empty");
    assertNotNull(region);
    assertTrue("expected concurrency checks to be enabled", region.getAttributes().getConcurrencyChecksEnabled());
    region.localDestroyRegion();

    region = c.getRegion("normal");
    assertNotNull(region);
    assertTrue("expected concurrency checks to be enabled", region.getAttributes().getConcurrencyChecksEnabled());
    region.localDestroyRegion();
  }

  public void testSerialGatewaySender() throws CacheException{
    getSystem();
    CacheCreation cache = new CacheCreation();
    GatewaySenderFactory gatewaySenderFactory = cache.createGatewaySenderFactory();
    gatewaySenderFactory.setParallel(false);
    gatewaySenderFactory.setManualStart(true);
    gatewaySenderFactory.setSocketBufferSize(124);
    gatewaySenderFactory.setSocketReadTimeout(1000);          
    gatewaySenderFactory.setBatchConflationEnabled(false);
    gatewaySenderFactory.setBatchSize(100);
    gatewaySenderFactory.setBatchTimeInterval(10);           
    gatewaySenderFactory.setPersistenceEnabled(true);          
    gatewaySenderFactory.setDiskStoreName("LNSender"); 
    gatewaySenderFactory.setDiskSynchronous(true);
    gatewaySenderFactory.setMaximumQueueMemory(200);           
    gatewaySenderFactory.setAlertThreshold(30);
    
    GatewayEventFilter myeventfilter1 = new MyGatewayEventFilter1();
    gatewaySenderFactory.addGatewayEventFilter(myeventfilter1);
    GatewayTransportFilter myStreamfilter1 = new MyGatewayTransportFilter1();
    gatewaySenderFactory.addGatewayTransportFilter(myStreamfilter1);
    GatewayTransportFilter myStreamfilter2 = new MyGatewayTransportFilter2();
    gatewaySenderFactory.addGatewayTransportFilter(myStreamfilter2);
    GatewaySender serialGatewaySender = gatewaySenderFactory.create("LN", 2);
    
    RegionAttributesCreation attrs = new RegionAttributesCreation();
    attrs.addGatewaySenderId(serialGatewaySender.getId());
    cache.createRegion("UserRegion", attrs);
    
    testXml(cache);
    Cache c = getCache();
    assertNotNull(c);
    Set<GatewaySender> sendersOnCache = c.getGatewaySenders();
    for(GatewaySender sender : sendersOnCache){
      assertEquals(false, sender.isParallel());
      validateGatewaySender(serialGatewaySender, sender);
    }
  }
  
  public void testAsyncEventQueue() {
    getSystem();
    CacheCreation cache = new CacheCreation();
    
    String id = "WBCLChannel";
    AsyncEventQueueFactory factory = cache.createAsyncEventQueueFactory();
    factory.setBatchSize(100);
    factory.setBatchTimeInterval(500);
    factory.setBatchConflationEnabled(true);
    factory.setMaximumQueueMemory(200);
    factory.setPersistent(true);
    factory.setDiskStoreName("WBCLStore");
    factory.setDiskSynchronous(true);
    factory.setParallel(true);
    
    AsyncEventListener eventListener = new MyAsyncEventListener();
    AsyncEventQueue asyncEventQueue = factory.create(id, eventListener);
    
    RegionAttributesCreation attrs = new RegionAttributesCreation();
    attrs.addAsyncEventQueueId(asyncEventQueue.getId());
    cache.createRegion("UserRegion", attrs);
    
    testXml(cache);
    Cache c = getCache();
    assertNotNull(c);
    
    Set<AsyncEventQueue> asyncEventQueuesOnCache = c.getAsyncEventQueues();
    assertTrue("Size of asyncEventQueues should be greater than 0", asyncEventQueuesOnCache.size() > 0);
    
    for (AsyncEventQueue asyncEventQueueOnCache : asyncEventQueuesOnCache) {
      validateAsyncEventQueue(asyncEventQueue, asyncEventQueueOnCache);
    }
  }
  
  public void testConcurrentAsyncEventQueue() {
    getSystem();
    CacheCreation cache = new CacheCreation();
    
    String id = "WBCLChannel";
    AsyncEventQueueFactory factory = cache.createAsyncEventQueueFactory();
    factory.setBatchSize(100);
    factory.setBatchTimeInterval(500);
    factory.setBatchConflationEnabled(true);
    factory.setMaximumQueueMemory(200);
    factory.setPersistent(true);
    factory.setDiskStoreName("WBCLStore");
    factory.setDiskSynchronous(true);
    factory.setDispatcherThreads(5);
    factory.setOrderPolicy(OrderPolicy.THREAD);
    
    AsyncEventListener eventListener = new MyAsyncEventListener();
    AsyncEventQueue asyncEventQueue = factory.create(id, eventListener);
    
    RegionAttributesCreation attrs = new RegionAttributesCreation();
    attrs.addAsyncEventQueueId(asyncEventQueue.getId());
    cache.createRegion("UserRegion", attrs);
    
    testXml(cache);
    Cache c = getCache();
    assertNotNull(c);
    
    Set<AsyncEventQueue> asyncEventQueuesOnCache = c.getAsyncEventQueues();
    assertTrue("Size of asyncEventQueues should be greater than 0", asyncEventQueuesOnCache.size() > 0);
    
    for (AsyncEventQueue asyncEventQueueOnCache : asyncEventQueuesOnCache) {
      validateConcurrentAsyncEventQueue(asyncEventQueue, asyncEventQueueOnCache);
    }
  }
  
  public void testParallelGatewaySender() throws CacheException{
    getSystem();
    CacheCreation cache = new CacheCreation();
    
    GatewaySenderFactory gatewaySenderFactory = cache.createGatewaySenderFactory();
    gatewaySenderFactory.setParallel(true);
    gatewaySenderFactory.setManualStart(true);
    gatewaySenderFactory.setSocketBufferSize(1234);
    gatewaySenderFactory.setSocketReadTimeout(1050);          
    gatewaySenderFactory.setBatchConflationEnabled(false);
    gatewaySenderFactory.setBatchSize(88);
    gatewaySenderFactory.setBatchTimeInterval(9);           
    gatewaySenderFactory.setPersistenceEnabled(true);          
    gatewaySenderFactory.setDiskStoreName("LNSender");  
    gatewaySenderFactory.setDiskSynchronous(true);
    gatewaySenderFactory.setMaximumQueueMemory(211);           
    gatewaySenderFactory.setAlertThreshold(35);
    
    GatewayEventFilter myeventfilter1 = new MyGatewayEventFilter1();
    gatewaySenderFactory.addGatewayEventFilter(myeventfilter1);
    GatewayTransportFilter myStreamfilter1 = new MyGatewayTransportFilter1();
    gatewaySenderFactory.addGatewayTransportFilter(myStreamfilter1);
    GatewayTransportFilter myStreamfilter2 = new MyGatewayTransportFilter2();
    gatewaySenderFactory.addGatewayTransportFilter(myStreamfilter2);
    GatewaySender parallelGatewaySender = gatewaySenderFactory.create("LN", 2);
    
    testXml(cache);
    Cache c = getCache();
    assertNotNull(c);
    Set<GatewaySender> sendersOnCache = c.getGatewaySenders();
    for(GatewaySender sender : sendersOnCache){
      assertEquals(true, sender.isParallel());
      validateGatewaySender(parallelGatewaySender, sender);
    }
  }
  
  public void testGatewayReceiver() throws CacheException{
    getSystem();
    CacheCreation cache = new CacheCreation();
    
    GatewayReceiverFactory gatewayReceiverFactory = cache.createGatewayReceiverFactory();
    gatewayReceiverFactory.setBindAddress("");
    gatewayReceiverFactory.setStartPort(54321);
    gatewayReceiverFactory.setEndPort(54331);
    gatewayReceiverFactory.setMaximumTimeBetweenPings(2000);
    gatewayReceiverFactory.setSocketBufferSize(1500);
    GatewayTransportFilter myStreamfilter1 = new MyGatewayTransportFilter1();
    gatewayReceiverFactory.addGatewayTransportFilter(myStreamfilter1);
    GatewayTransportFilter myStreamfilter2 = new MyGatewayTransportFilter2();
    gatewayReceiverFactory.addGatewayTransportFilter(myStreamfilter2);
    GatewayReceiver receiver1 = gatewayReceiverFactory.create();
    try {
      receiver1.start();
    }
    catch (IOException e) {
      fail("Could not start GatewayReceiver");
    }
    testXml(cache);
    Cache c = getCache();
    assertNotNull(c);
    Set<GatewayReceiver> receivers = c.getGatewayReceivers();
    for(GatewayReceiver receiver : receivers){
      validateGatewayReceiver(receiver1, receiver);
    }
  }
  
  /** make sure we can create regions with concurrencyChecksEnabled=true */
  public void testGatewayConflictResolver() throws CacheException {
    CacheCreation cache = new CacheCreation();
    cache.setGatewayConflictResolver(new ConflictResolver());
    testXml(cache);
    Cache c = getCache();
    assertNotNull(c);
    GatewayConflictResolver resolver = c.getGatewayConflictResolver();
    assertNotNull(resolver);
    assertTrue(resolver instanceof ConflictResolver);
    assertTrue(((ConflictResolver)resolver).initialized);
    c.close();
  }

  public static class MyAsyncEventListener implements
      AsyncEventListener<Object, Object>, Declarable {

    public boolean processEvents(List<AsyncEvent<Object, Object>> events) {
      return true;
    }

    public void close() {
    }

    public void init(Properties properties) {
    }
  }

  private void validateGatewaySender(GatewaySender sender1, GatewaySender gatewaySender) {
    assertEquals(sender1.getId(), gatewaySender.getId());
    assertEquals(sender1.getRemoteDSId(), gatewaySender.getRemoteDSId());
    assertEquals(sender1.isParallel(), gatewaySender.isParallel());
    assertEquals(sender1.isBatchConflationEnabled(), gatewaySender.isBatchConflationEnabled());
    assertEquals(sender1.getBatchSize(), gatewaySender.getBatchSize());
    assertEquals(sender1.getBatchTimeInterval(), gatewaySender.getBatchTimeInterval());
    assertEquals(sender1.isPersistenceEnabled(), gatewaySender.isPersistenceEnabled());
    assertEquals(sender1.getDiskStoreName(),gatewaySender.getDiskStoreName());
    assertEquals(sender1.isDiskSynchronous(),gatewaySender.isDiskSynchronous());
    assertEquals(sender1.getMaximumQueueMemory(), gatewaySender.getMaximumQueueMemory());
    assertEquals(sender1.getAlertThreshold(), gatewaySender.getAlertThreshold());
    assertEquals(sender1.getGatewayEventFilters().size(), gatewaySender.getGatewayEventFilters().size());
    assertEquals(sender1.getGatewayTransportFilters().size(), gatewaySender.getGatewayTransportFilters().size());
    
    boolean isParallel = sender1.isParallel();
    if (isParallel) {
      assertTrue("sender should be instanceof Creation", sender1 instanceof ParallelGatewaySenderCreation);
      assertTrue("sender should be instanceof Impl", gatewaySender instanceof ParallelGatewaySenderImpl);
    } else {
      assertTrue("sender should be instanceof Creation", sender1 instanceof SerialGatewaySenderCreation);
      assertTrue("sender should be instanceof Impl", gatewaySender instanceof SerialGatewaySenderImpl);
    }
  }
  
  private void validateGatewayReceiver(GatewayReceiver receiver1,
      GatewayReceiver gatewayReceiver) {
    assertEquals(receiver1.getHost(), gatewayReceiver.getHost());
    assertEquals(receiver1.getStartPort(), gatewayReceiver.getStartPort());
    assertEquals(receiver1.getEndPort(), gatewayReceiver.getEndPort());
    assertEquals(receiver1.getMaximumTimeBetweenPings(), gatewayReceiver
        .getMaximumTimeBetweenPings());
    assertEquals(receiver1.getSocketBufferSize(), gatewayReceiver
        .getSocketBufferSize());
    assertEquals(receiver1.getGatewayTransportFilters().size(), gatewayReceiver
        .getGatewayTransportFilters().size());
  } 
  
  private void validateAsyncEventQueue(AsyncEventQueue eventChannelFromXml, AsyncEventQueue channel) {
    assertEquals("AsyncEventQueue id doesn't match", eventChannelFromXml.getId(), channel.getId());
    assertEquals("AsyncEventQueue batchSize doesn't match", eventChannelFromXml.getBatchSize(), channel.getBatchSize());
    assertEquals("AsyncEventQueue batchTimeInterval doesn't match", eventChannelFromXml.getBatchTimeInterval(), channel.getBatchTimeInterval());
    assertEquals("AsyncEventQueue batchConflationEnabled doesn't match", eventChannelFromXml.isBatchConflationEnabled(), channel.isBatchConflationEnabled());
    assertEquals("AsyncEventQueue persistent doesn't match", eventChannelFromXml.isPersistent(), channel.isPersistent());
    assertEquals("AsyncEventQueue diskStoreName doesn't match", eventChannelFromXml.getDiskStoreName(), channel.getDiskStoreName());
    assertEquals("AsyncEventQueue isDiskSynchronous doesn't match", eventChannelFromXml.isDiskSynchronous(), channel.isDiskSynchronous());
    assertEquals("AsyncEventQueue maximumQueueMemory doesn't match", eventChannelFromXml.getMaximumQueueMemory(), channel.getMaximumQueueMemory());
    assertEquals("AsyncEventQueue Parallel doesn't match", eventChannelFromXml.isParallel(), channel.isParallel());
    assertTrue("AsyncEventQueue should be instanceof Creation", eventChannelFromXml instanceof AsyncEventQueueCreation);
    assertTrue("AsyncEventQueue should be instanceof Impl", channel instanceof AsyncEventQueueImpl);
  }
  
  private void validateConcurrentAsyncEventQueue(AsyncEventQueue eventChannelFromXml, AsyncEventQueue channel) {
    assertEquals("AsyncEventQueue id doesn't match", eventChannelFromXml.getId(), channel.getId());
    assertEquals("AsyncEventQueue batchSize doesn't match", eventChannelFromXml.getBatchSize(), channel.getBatchSize());
    assertEquals("AsyncEventQueue batchTimeInterval doesn't match", eventChannelFromXml.getBatchTimeInterval(), channel.getBatchTimeInterval());
    assertEquals("AsyncEventQueue batchConflationEnabled doesn't match", eventChannelFromXml.isBatchConflationEnabled(), channel.isBatchConflationEnabled());
    assertEquals("AsyncEventQueue persistent doesn't match", eventChannelFromXml.isPersistent(), channel.isPersistent());
    assertEquals("AsyncEventQueue diskStoreName doesn't match", eventChannelFromXml.getDiskStoreName(), channel.getDiskStoreName());
    assertEquals("AsyncEventQueue isDiskSynchronous doesn't match", eventChannelFromXml.isDiskSynchronous(), channel.isDiskSynchronous());
    assertEquals("AsyncEventQueue maximumQueueMemory doesn't match", eventChannelFromXml.getMaximumQueueMemory(), channel.getMaximumQueueMemory());
    assertEquals("AsyncEventQueue Parallel doesn't match", eventChannelFromXml.isParallel(), channel.isParallel());
    assertEquals("AsyncEventQueue dispatcherThreads doesn't match", eventChannelFromXml.getDispatcherThreads(), channel.getDispatcherThreads());
    assertEquals("AsyncEventQueue orderPolicy doesn't match", eventChannelFromXml.getOrderPolicy(), channel.getOrderPolicy());
    assertTrue("AsyncEventQueue should be instanceof Creation", eventChannelFromXml instanceof AsyncEventQueueCreation);
    assertTrue("AsyncEventQueue should be instanceof Impl", channel instanceof AsyncEventQueueImpl);
  }

  public static class ConflictResolver implements GatewayConflictResolver, Declarable {
    public boolean initialized;
    
    public ConflictResolver() {
    }

    /* (non-Javadoc)
     * @see com.gemstone.gemfire.cache.Declarable#init(java.util.Properties)
     */
    @Override
    public void init(Properties props) {
      this.initialized = true;
    }

    /* (non-Javadoc)
     * @see com.gemstone.gemfire.cache.util.GatewayConflictResolver#onEvent(com.gemstone.gemfire.cache.util.TimestampedEntryEvent, com.gemstone.gemfire.cache.util.GatewayConflictHelper)
     */
    @Override
    public void onEvent(TimestampedEntryEvent event,
        GatewayConflictHelper helper) {
    }

  }

  // test bug 47197
  public void testPartitionedRegionAttributesForCoLocation3(){
    closeCache();
    setXmlFile(findFile("coLocation3.xml"));    
    Cache c = getCache();
    assertNotNull(c);
    Region cust = c.getRegion(Region.SEPARATOR+"Customer");
    assertNotNull(cust);
    Region order = c.getRegion(Region.SEPARATOR+"Order");
    assertNotNull(order);
    
    assertTrue(cust.getAttributes().getPartitionAttributes().getColocatedWith()==null);
    assertTrue(order.getAttributes().getPartitionAttributes().getColocatedWith().equals("Customer"));
  }
}
