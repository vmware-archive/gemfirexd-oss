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
 * Created on Feb 3, 2006
 *
 */
package com.gemstone.gemfire.internal.cache.tier.sockets;

import java.util.Map;
import java.util.Properties;

import junit.framework.TestCase;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.internal.BridgePoolImpl;
import com.gemstone.gemfire.cache.client.internal.Connection;
import com.gemstone.gemfire.cache.client.internal.PutOp;
import com.gemstone.gemfire.cache.client.internal.QueueStateImpl.SequenceIdAndExpirationObject;
import com.gemstone.gemfire.cache.util.BridgeServer;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.TXManagerImpl;
import com.gemstone.gemfire.internal.cache.ha.ThreadIdentifier;

import io.snappydata.test.dunit.DistributedTestBase;
import io.snappydata.test.dunit.DistributedTestBase.WaitCriterion;

/**
 * @author asif
 *
 * Tests the functionality of operations of AbstractConnectionProxy & its
 * derived classes.
 */
public class ConnectionProxyJUnitTest extends TestCase
{
  DistributedSystem system;

  Cache cache;

  BridgePoolImpl proxy = null;
  
  SequenceIdAndExpirationObject seo = null;
  
  protected void setUp() throws Exception
  {

    super.setUp();
    Properties p = new Properties();
    p.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    p.setProperty(DistributionConfig.LOCATORS_NAME, "");
    this.system = DistributedSystem.connect(p);
    this.cache = CacheFactory.create(system);
  }

  protected void tearDown() throws Exception
  {
  System.out.println("Tearing down + " + getName());
    this.cache.close();
    this.system.disconnect();
    if (proxy != null)
      proxy.close();
    super.tearDown();

  }

  /**
   * This test verifies the behaviour of client request when the listener on the
   * server sits forever. This is done in following steps:<br>
   * 1)create server<br>
   * 2)initialize proxy object and create region for client having a
   * CacheListener and make afterCreate in the listener to wait infinitely<br>
   * 3)perform a PUT on client by acquiring Connection through proxy<br>
   * 4)Verify that exception occurs due to infinite wait in the listener<br>
   * 5)Verify that above exception occurs sometime after the readTimeout
   * configured for the client <br>
   *
   * @author Dinesh
   */
  public void DISABLE_testListenerOnServerSitForever()
  {
    int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    Region testRegion = null ;
    try {
      BridgeServer server = this.cache.addBridgeServer();
      server.setMaximumTimeBetweenPings(10000);
      server.setPort(port3);
      server.start();
    }
    catch (Exception e) {
      e.printStackTrace();
      fail("Failed to create server");
    }
    try {
      Properties props = new Properties();
      props.setProperty("retryAttempts", "1");
      props.setProperty("endpoints", "ep1=localhost:"+port3);
      props.setProperty("establishCallbackConnection", "false");
      props.setProperty("LBPolicy", "Sticky");
      props.setProperty("readTimeout", "2000");
      props.setProperty("socketBufferSize", "32768");
      props.setProperty("retryInterval", "10000");
      props.setProperty("connectionsPerServer", "2");
      props.setProperty("redundancyLevel", "-1");
      AttributesFactory factory = new AttributesFactory();
      factory.setScope(Scope.DISTRIBUTED_ACK);
      factory.setCacheListener(new CacheListenerAdapter() {
        public void afterCreate(EntryEvent event)
        {
          synchronized (ConnectionProxyJUnitTest.this) {
            try {
              ConnectionProxyJUnitTest.this.wait();
            }
            catch (InterruptedException e) {
              fail("interrupted");
            }
          }
        }
      });
      proxy = BridgePoolImpl.create(props, true/*useByBridgeWriter*/);

      RegionAttributes attrs = factory.create();
      testRegion = cache.createRegion("testregion", attrs);

    }
    catch (Exception ex) {
      ex.printStackTrace();
      fail("Failed to initialize client");
    }
    Connection conn = (proxy).acquireConnection();
    long t1 = 0;
    try {
      t1 = System.currentTimeMillis();
      EntryEventImpl event = new EntryEventImpl((Object)null);
      try {
      event.setEventId(new EventID(new byte[] { 1 },1,1));
      PutOp.execute(conn, proxy, testRegion.getFullPath(), "key1", "val1", event, null, false);
      } finally {
        event.release();
      }
      fail("Test failed as exception was expected");
    }
    catch (Exception e) {
      long t2 = System.currentTimeMillis();
      long net = (t2 - t1);
      assertTrue(net / 1000 < 5);
    }
    synchronized (ConnectionProxyJUnitTest.this) {
      ConnectionProxyJUnitTest.this.notify();
    }
  }
  /**
   * Tests the DeadServerMonitor when identifying an 
   * Endpoint as alive , does not create a persistent Ping connection
   * ( i.e sends a CLOSE protocol , if the number of connections is zero.
   * @author Asif
   */
  public void testDeadServerMonitorPingNature1() {
    int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    
//    final int maxWaitTime = 10000;    
    try {
      Properties props = new Properties();
      props.setProperty("retryAttempts", "1");
      props.setProperty("endpoints", "ep1=localhost:"+port3);
      props.setProperty("establishCallbackConnection", "false");
      props.setProperty("LBPolicy", "Sticky");
      props.setProperty("readTimeout", "2000");
      props.setProperty("socketBufferSize", "32768");
      props.setProperty("retryInterval", "500");
      props.setProperty("connectionsPerServer", "1");
      
      proxy = BridgePoolImpl.create(props, true/*useByBridgeWriter*/);
    }catch (Exception ex) {
      ex.printStackTrace();
      fail("Failed to initialize client");
    }
    try {
      (proxy).acquireConnection();
    }catch(Exception ok) {
     ok.printStackTrace(); 
    }
    
    try {
      (proxy).acquireConnection();
    }catch(Exception ok) {
     ok.printStackTrace(); 
    }
    
//    long start = System.currentTimeMillis();
    assertEquals(0, proxy.getConnectedServerCount());
    //start the server
   BridgeServer server = null;
   try {
   try {
     server = this.cache.addBridgeServer();
      server.setMaximumTimeBetweenPings(15000);
      server.setPort(port3);
      server.start();
    }
    catch (Exception e) {
      e.printStackTrace();
      fail("Failed to create server");
    }
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return proxy.getConnectedServerCount() == 1;
      }
      public String description() {
        return null;
      }
    };
    DistributedTestBase.waitForCriterion(ev, 90 * 1000, 200, true);
   }
   finally {
     if (server != null) {
       server.stop();
     }
   }
  }
  
  /**
   * Tests the DeadServerMonitor when identifying an 
   * Endpoint as alive , does creates a persistent Ping connection
   * ( i.e sends a PING protocol , if the number of connections is more than 
   * zero.
   * @author Asif
   */
  public void testDeadServerMonitorPingNature2() {
    int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    
//    final int maxWaitTime = 10000;    
    try {
      Properties props = new Properties();
      props.setProperty("retryAttempts", "1");
      props.setProperty("endpoints", "ep1=localhost:"+port3);
      props.setProperty("establishCallbackConnection", "false");
      props.setProperty("LBPolicy", "Sticky");
      props.setProperty("readTimeout", "2000");
      props.setProperty("socketBufferSize", "32768");
      props.setProperty("retryInterval", "500");
      props.setProperty("connectionsPerServer", "1");
      proxy = BridgePoolImpl.create(props, true/*useByBridgeWriter*/);      
    }catch (Exception ex) {
      ex.printStackTrace();
      fail("Failed to initialize client");
    }
    //let LiveServerMonitor detect it as alive as the numConnection is more than zero
    
//    long start = System.currentTimeMillis();
    assertEquals(0, proxy.getConnectedServerCount());
    //start the server
   BridgeServer server = null;
   try {
   try {
     server = this.cache.addBridgeServer();
      server.setMaximumTimeBetweenPings(15000);
      server.setPort(port3);
      server.start();
    }
    catch (Exception e) {
      e.printStackTrace();
      fail("Failed to create server");
    }
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return proxy.getConnectedServerCount() == 1;
      }
      public String description() {
        return null;
      }
    };
    DistributedTestBase.waitForCriterion(ev, 90 * 1000, 200, true);
   }
   finally {
     if (server != null) {
       server.stop();
     }
   }
  }

 public void testThreadIdToSequenceIdMapCreation()
  {
    int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
   BridgeServer server = null;
   try {
   try {
     server = this.cache.addBridgeServer();
      server.setMaximumTimeBetweenPings(10000);
      server.setPort(port3);
      server.start();
    }
    catch (Exception e) {
      e.printStackTrace();
      fail("Failed to create server");
    }
    try {
      Properties props = new Properties();
      props.setProperty("endpoints", "ep1=localhost:" + port3);
      props.setProperty("establishCallbackConnection", "true");
      props.setProperty("redundancyLevel", "-1");
      proxy = BridgePoolImpl.create(props, true/*useByBridgeWriter*/);
      if (proxy.getThreadIdToSequenceIdMap() == null) {
        fail(" ThreadIdToSequenceIdMap is null. ");
      }
    }
    catch (Exception ex) {
      ex.printStackTrace();
      fail("Failed to initialize client");
    }
   }
   finally {
     if (server != null) {
       try {
         Thread.sleep(500);
       }
       catch (InterruptedException ie) {
         fail("interrupted");
       }
       server.stop();
     }
   }
  }

 public void testThreadIdToSequenceIdMapExpiryPositive()
 {
   int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
   BridgeServer server = null;
   try {
   try {
     server = this.cache.addBridgeServer();
     server.setMaximumTimeBetweenPings(10000);
     server.setPort(port3);
     server.start();
   }
   catch (Exception e) {
     e.printStackTrace();
     fail("Failed to create server");
   }
   try {
     Properties props = new Properties();
     props.setProperty("endpoints", "ep1=localhost:"+port3);
     props.setProperty("redundancyLevel", "-1");
     props.setProperty("clientAckInterval", "2000");
     props.setProperty("messageTrackingTimeout", "4000");
     props.setProperty("establishCallbackConnection", "true");
     
     proxy = BridgePoolImpl.create(props, true/*useByBridgeWriter*/);

     EventID eid = new EventID(new byte[0],1,1);
     if(proxy.verifyIfDuplicate(eid)){
       fail(" eid should not be duplicate as it is a new entry");
     }

     verifyExpiry(60 * 1000);

     if(proxy.verifyIfDuplicate(eid)){
       fail(" eid should not be duplicate as the previous entry should have expired ");
     }

   }
   catch (Exception ex) {
     ex.printStackTrace();
     fail("Failed to initialize client");
   }
   }
   finally {
     if (server != null) {
       server.stop();
     }
   }
 }


 public void testThreadIdToSequenceIdMapExpiryNegative()
 {
   int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
   BridgeServer server = null;
   try {
   try {
     server = this.cache.addBridgeServer();
     server.setMaximumTimeBetweenPings(10000);     
     server.setPort(port3);
     server.start();
   }
   catch (Exception e) {
     e.printStackTrace();
     fail("Failed to create server");
   }
   try {
     Properties props = new Properties();
     props.setProperty("endpoints", "ep1=localhost:"+port3);
     props.setProperty("establishCallbackConnection", "true");
     props.setProperty("redundancyLevel", "-1");
     props.setProperty("messageTrackingTimeout", "10000");
     proxy = BridgePoolImpl.create(props, true/*useByBridgeWriter*/);

     final EventID eid = new EventID(new byte[0],1,1);
     if(proxy.verifyIfDuplicate(eid)){
       fail(" eid should not be duplicate as it is a new entry");
     }

     WaitCriterion ev = new WaitCriterion() {
       public boolean done() {
         return proxy.verifyIfDuplicate(eid);
       }
       public String description() {
         return null;
       }
     };
     DistributedTestBase.waitForCriterion(ev, 20 * 1000, 200, true);
   }
   catch (Exception ex) {
     ex.printStackTrace();
     fail("Failed to initialize client");
   }
   }
   finally {
     if (server != null) {
       server.stop();
     }
   }
 }

 public void testThreadIdToSequenceIdMapConcurrency()
 {
   int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
   BridgeServer server = null;
   try {
   try {
     server = this.cache.addBridgeServer();
     server.setMaximumTimeBetweenPings(10000);
     server.setPort(port3);
     server.start();
   }
   catch (Exception e) {
     e.printStackTrace();
     fail("Failed to create server");
   }
   try {
     Properties props = new Properties();
     props.setProperty("endpoints", "ep1=localhost:"+port3);
     props.setProperty("redundancyLevel", "-1");
     props.setProperty("clientAckInterval", "2000");
     props.setProperty("messageTrackingTimeout", "5000");
     props.setProperty("establishCallbackConnection", "true");
     
     proxy = BridgePoolImpl.create(props, true/*useByBridgeWriter*/);
     final int EVENT_ID_COUNT = 10000; // why 10,000?
     EventID[] eid = new EventID[EVENT_ID_COUNT];
     for (int i = 0; i < EVENT_ID_COUNT; i++) {
       eid[i] = new EventID(new byte[0],i,i);
       if(proxy.verifyIfDuplicate(eid[i])){
         fail(" eid can never be duplicate, it is being created for the first time! ");
       }
       }
     verifyExpiry(30 * 1000);

     for (int i = 0; i < EVENT_ID_COUNT; i++) {
       if(proxy.verifyIfDuplicate(eid[i])){
         fail(" eid can not be found to be  duplicate since the entry should have expired! "+i);
       }
      }
   }
   catch (Exception ex) {
     ex.printStackTrace();
     fail("Failed to initialize client");
   }
   }
   finally {
     if (server != null) {
       server.stop();
     }
   }
 }



 public void testDuplicateSeqIdLesserThanCurrentSeqIdBeingIgnored()
 {
   int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
   BridgeServer server = null;
   try {
   try {
     server = this.cache.addBridgeServer();
     server.setMaximumTimeBetweenPings(10000);
     server.setPort(port3);
     server.start();
   }
   catch (Exception e) {
     e.printStackTrace();
     fail("Failed to create server");
   }
   try {
     Properties props = new Properties();
     props.setProperty("endpoints", "ep1=localhost:"+port3);
     props.setProperty("establishCallbackConnection", "true");
     props.setProperty("redundancyLevel", "-1");
     props.setProperty("messageTrackingTimeout", "100000");
     proxy = BridgePoolImpl.create(props, true/*useByBridgeWriter*/);
     EventID eid1 = new EventID(new byte[0],1,5);
     if(proxy.verifyIfDuplicate(eid1)){
       fail(" eid1 can never be duplicate, it is being created for the first time! ");
     }

     EventID eid2 = new EventID(new byte[0],1,2);

     if(!proxy.verifyIfDuplicate(eid2)){
       fail(" eid2 should be duplicate, seqId is less than highest (5)");
     }

     EventID eid3 = new EventID(new byte[0],1,3);

     if(!proxy.verifyIfDuplicate(eid3)){
       fail(" eid3 should be duplicate, seqId is less than highest (5)");
     }

     assertTrue(!proxy.getThreadIdToSequenceIdMap().isEmpty());
     proxy.close();
   }
   catch (Exception ex) {
     ex.printStackTrace();
     fail("Failed to initialize client");
   }
   }
   finally {
     if (server != null) {
       server.stop();
     }
   }
 }




 public void testCleanCloseOfThreadIdToSeqId()
 {
   int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
   BridgeServer server = null;
   try {
   try {
     server = this.cache.addBridgeServer();
     server.setMaximumTimeBetweenPings(10000);
     server.setPort(port3);
     server.start();
   }
   catch (Exception e) {
     e.printStackTrace();
     fail("Failed to create server");
   }
   try {
     Properties props = new Properties();
     props.setProperty("endpoints", "ep1=localhost:"+port3);
     props.setProperty("establishCallbackConnection", "true");
     props.setProperty("redundancyLevel", "-1");
     props.setProperty("messageTrackingTimeout", "100000");
     proxy = BridgePoolImpl.create(props, true/*useByBridgeWriter*/);
     EventID eid1 = new EventID(new byte[0],1,2);
     if(proxy.verifyIfDuplicate(eid1)){
         fail(" eid can never be duplicate, it is being created for the first time! ");
       }
     EventID eid2 = new EventID(new byte[0],1,3);
     if(proxy.verifyIfDuplicate(eid2)){
         fail(" eid can never be duplicate, since sequenceId is greater ");
       }

     if(!proxy.verifyIfDuplicate(eid2)){
       fail(" eid had to be a duplicate, since sequenceId is equal ");
     }
     EventID eid3 = new EventID(new byte[0],1,1);
     if(!proxy.verifyIfDuplicate(eid3)){
         fail(" eid had to be a duplicate, since sequenceId is lesser ");
       }   }
   catch (Exception ex) {
     ex.printStackTrace();
     fail("Failed to initialize client");
   }
   }
   finally {
     if (server != null) {
       try {
          Thread.sleep(500);
        }
        catch (InterruptedException ie) {
          fail("interrupted");
        }
       server.stop();
     }
   }
 }

 public void testTwoClientsHavingDifferentThreadIdMaps()
 {
   int port3 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
   BridgeServer server = null;
   try {
   try {
     server = this.cache.addBridgeServer();
     server.setMaximumTimeBetweenPings(10000);
     server.setPort(port3);
     server.start();
   }
   catch (Exception e) {
     e.printStackTrace();
     fail("Failed to create server");
   }
   try {
     Properties props = new Properties();
     props.setProperty("endpoints", "ep1=localhost:"+port3);
     props.setProperty("establishCallbackConnection", "true");
     props.setProperty("redundancyLevel", "-1");
     props.setProperty("messageTrackingTimeout", "100000");
     BridgePoolImpl proxy1 = BridgePoolImpl.create(props, true/*useByBridgeWriter*/);
     BridgePoolImpl proxy2 = BridgePoolImpl.create(props, true/*useByBridgeWriter*/);

     Map map1 = proxy1.getThreadIdToSequenceIdMap();
     Map map2 = proxy2.getThreadIdToSequenceIdMap();

     assertTrue(!(map1==map2));

      // Close the proxies to remove them from the set of all proxies
     proxy1.close();
     proxy2.close();
    }
   catch (Exception ex) {
     ex.printStackTrace();
     fail("Failed to initialize client");
   }
   }
   finally {
     if (server != null) {
       server.stop();
     }
   }
 }

 public void testPeriodicAckSendByClient()
 {
   int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
   BridgeServer server = null;
   try {
   try {
     server = this.cache.addBridgeServer();
     server.setPort(port);
     server.start();
   }
   catch (Exception e) {
     e.printStackTrace();
     fail("Failed to create server");
   }
   try {
     Properties props = new Properties();
     props.setProperty("endpoints", "ep1=localhost:"+port);
     props.setProperty("establishCallbackConnection", "true");
     props.setProperty("redundancyLevel", "1");
     props.setProperty("readTimeout", "20000");
     props.setProperty("messageTrackingTimeout", "15000");
     props.setProperty("clientAckInterval", "5000");
     
     proxy = BridgePoolImpl.create(props, true/*useByBridgeWriter*/);   
     
     EventID eid = new EventID(new byte[0],1,1);
     
     if(proxy.verifyIfDuplicate(eid)){
       fail(" eid should not be duplicate as it is a new entry");
     }        
     
     seo = (SequenceIdAndExpirationObject)proxy.
     getThreadIdToSequenceIdMap().get(new ThreadIdentifier(new byte[0],1));    
     assertFalse(seo.getAckSend());
     
//   should send the ack to server     
      seo = (SequenceIdAndExpirationObject)proxy.
         getThreadIdToSequenceIdMap().get(new ThreadIdentifier(new byte[0],1));
      verifyAckSend(60 * 1000,true);
      
//   New update on same threadId   
      eid = new EventID(new byte[0],1,2);     
     if(proxy.verifyIfDuplicate(eid)){
       fail(" eid should not be duplicate as it is a new entry");
     }     
     seo = (SequenceIdAndExpirationObject)proxy.
     getThreadIdToSequenceIdMap().get(new ThreadIdentifier(new byte[0],1));
     assertFalse(seo.getAckSend());

//   should send another ack to server    
     seo = (SequenceIdAndExpirationObject)proxy.
     getThreadIdToSequenceIdMap().get(new ThreadIdentifier(new byte[0],1));
     verifyAckSend(6000,true);    
     
   // should expire with the this mentioned. 
     verifyExpiry(15 * 1000);
   }
   catch (Exception ex) {
     ex.printStackTrace();
     fail("Test testPeriodicAckSendByClient Failed");
   }
   }
   finally {
     if (server != null) {
       server.stop();
     }
   }
 }

 //No ack will be send if Redundancy level = 0
 public void testNoAckSendByClient()
 {
   int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
   BridgeServer server = null;
   try {
   try {
     server = this.cache.addBridgeServer();
     server.setPort(port);
     server.start();
   }
   catch (Exception e) {
     e.printStackTrace();
     fail("Failed to create server");
   }
   try {
     Properties props = new Properties();
     props.setProperty("endpoints", "ep1=localhost:"+port);     
     props.setProperty("readTimeout", "20000");
     props.setProperty("messageTrackingTimeout", "8000");
     props.setProperty("clientAckInterval", "2000");
     props.setProperty("establishCallbackConnection", "true");
     
     proxy = BridgePoolImpl.create(props, true/*useByBridgeWriter*/);     
     
     EventID eid = new EventID(new byte[0],1,1);
     
     if(proxy.verifyIfDuplicate(eid)){
       fail(" eid should not be duplicate as it is a new entry");
     }
     
     seo = (SequenceIdAndExpirationObject)proxy.
     getThreadIdToSequenceIdMap().get(new ThreadIdentifier(new byte[0],1));    
     assertFalse(seo.getAckSend());
    
    //  should not send an ack as redundancy level = 0;
     seo = (SequenceIdAndExpirationObject)proxy.
     getThreadIdToSequenceIdMap().get(new ThreadIdentifier(new byte[0],1));     
     verifyAckSend(30 * 1000, false);
     
     // should expire without sending an ack as redundancy level = 0.       
     verifyExpiry(90 * 1000);      
     }
     
     catch (Exception ex) {
       ex.printStackTrace();
       fail("Test testPeriodicAckSendByClient Failed");
     }
   }
   finally {
     if (server != null) {
       server.stop();
     }
   }
  }
 
  private void verifyAckSend(long timeToWait, final boolean expectedAckSend )
  { 
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        return expectedAckSend == seo.getAckSend();
      }
      public String description() {
        return "ack flag never became " + expectedAckSend;
      }
    };
    DistributedTestBase.waitForCriterion(wc, timeToWait, 1000, true);
  }  
  
  private void verifyExpiry(long timeToWait)
  {
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        return 0 == proxy.getThreadIdToSequenceIdMap().size();
      }
      public String description() {
        return "Entry never expired";
      }
    };
    DistributedTestBase.waitForCriterion(wc, timeToWait * 2, 200, true);
  }
 
}
