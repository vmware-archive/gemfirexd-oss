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
 * Test various distributed aspects of transactions,
 * e.g. locking/reservation symantics that do not need multiple Region
 * configurations. For those tests see
 * <code>MultiVMRegionTestCase</code>.
 * 
 *
 * @author Mitch Thomas
 * @since 4.0
 * @see MultiVMRegionTestCase
 *
 */

package com.gemstone.gemfire.cache30;


//import com.gemstone.gemfire.*;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.internal.membership.jgroup.MembershipManagerHelper;
import com.gemstone.gemfire.internal.cache.*;

import dunit.*;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;

public class TXDistributedTest extends CacheTestCase {
  public TXDistributedTest(String name) {
    super(name);
  }

  protected RegionAttributes getRegionAttributes() {
    return this.getRegionAttributes(Scope.DISTRIBUTED_ACK);
  }

  protected RegionAttributes getRegionAttributes(Scope scope) {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(scope);
    if (scope.isDistributedAck()) {
      factory.setEarlyAck(false);
    }
    return factory.create();
  }

  /*
  /**
   * Test a remote grantor
   * [sumedh] No longer valid in the new TX model.
   *
  public void testRemoteGrantor() throws Exception {
    final CacheTransactionManager txMgr = this.getCache().getCacheTransactionManager();
    final String rgnName = getUniqueName();
    Region rgn = getCache().createRegion(rgnName, getRegionAttributes());
    rgn.create("key", null);

    invokeInEveryVM(new SerializableRunnable("testRemoteGrantor: initial configuration") {
        public void run() {
          try {
            Region rgn1 = getCache().createRegion(rgnName, getRegionAttributes());
            rgn1.put("key", "val0");
          } catch (CacheException e) {
            fail("While creating region", e);
          }
        }
      });
    
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
//    VM vm1 = host.getVM(1);
//    VM vm2 = host.getVM(2);

    vm0.invoke(new SerializableRunnable("testRemoteGrantor: remote grantor init") {
        public void run() {
          try {
            Region rgn1 = getCache().getRegion(rgnName);
            final CacheTransactionManager txMgr2 = getCache().getCacheTransactionManager();
            txMgr2.begin();
            rgn1.put("key", "val1");
            txMgr2.commit();
            assertNotNull(TXLockService.getDTLS());
            assertTrue(TXLockService.getDTLS().isLockGrantor());
          } catch (CacheException e) {
            fail("While performing first transaction");
          }
        }
      });

    // fix for bug 38843 causes the DTLS to be created in every TX participant
    assertNotNull(TXLockService.getDTLS());
    assertFalse(TXLockService.getDTLS().isLockGrantor());
    assertEquals("val1", rgn.getEntry("key").getValue());

    vm0.invoke(new SerializableRunnable("Disconnect from DS, remote grantor death") {
        public void run() {
            try {
              MembershipManagerHelper.crashDistributedSystem(getSystem());
            } finally {
                // Allow getCache() to re-establish a ds connection
                closeCache();
            }
        }
      });

    // Make this VM the remote Grantor
    txMgr.begin();
    rgn.put("key", "val2");
    txMgr.commit();
    assertNotNull(TXLockService.getDTLS());
    assertTrue(TXLockService.getDTLS().isLockGrantor());

    SerializableRunnable remoteComm = 
      new SerializableRunnable("testRemoteGrantor: remote grantor commit") {
        public void run() {
          try {
            Cache c = getCache();
            CacheTransactionManager txMgr2 = c.getCacheTransactionManager();
            Region rgn1 = c.getRegion(rgnName);
            if (rgn1 == null) {
                // This block should only execute on VM0
                rgn1 = c.createRegion(rgnName, getRegionAttributes());
            }

            txMgr2.begin();
            rgn1.put("key", "val3");
            txMgr2.commit();

            if (TXLockService.getDTLS() != null) {
              assertTrue(!TXLockService.getDTLS().isLockGrantor());
            }
          } catch (CacheException e) {
            fail("While creating region", e);
          }
        }
      };
    invokeInEveryVM(remoteComm);
    // vm1.invoke(remoteComm);
    // vm2.invoke(remoteComm);

    assertNotNull(TXLockService.getDTLS());
    assertTrue(TXLockService.getDTLS().isLockGrantor());
    assertEquals("val3", rgn.getEntry("key").getValue());
    rgn.destroyRegion();
  }
  */

  /**
   * Test the internal callbacks used for what else... testing
   */
  @SuppressWarnings({ "serial", "unchecked" })
  public void testInternalCallbacks() throws Exception {
    final CacheTransactionManager txMgr = this.getCache()
        .getCacheTransactionManager();
    final String rgnName1 = getUniqueName() + "_1";
    final String rgnName2 = getUniqueName() + "_2";
    final String rgnName3 = getUniqueName() + "_3";
    Region<Object, Object> rgn1 = getCache().createRegionFactory(
        getRegionAttributes()).create(rgnName1);

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    SerializableRunnable createRgn = new SerializableRunnable(
        "testInternalCallbacks: initial configuration") {
        public void run() {
          try {
            Region<Object, Object> rgn1a = getCache().createRegionFactory(
                getRegionAttributes()).create(rgnName1);
            Region<Object, Object> rgn2 = getCache().createRegionFactory(
                getRegionAttributes()).create(rgnName2);
            Region<Object, Object> rgn3 = getCache().createRegionFactory(
                getRegionAttributes(Scope.DISTRIBUTED_NO_ACK)).create(rgnName3);
            rgn1a.create("key", null);
            rgn2.create("key", null);
            rgn3.create("key", null);
          } catch (CacheException e) {
            fail("While creating region", e);
          }
        }
      };
    vm0.invoke(createRgn);
    vm1.invoke(createRgn);
   
    // Standard commit check
    txMgr.begin();
    rgn1.put("key", "value0");
    txMgr.commit();
    SerializableRunnable checkRgn1 = 
      new SerializableRunnable("testInternalCallbacks: check rgn1 valus") {
        public void run() {
          Region<Object, Object> rgn1a = getCache().getRegion(rgnName1);
          assertNotNull(rgn1a);
          assertEquals("value0", rgn1a.getEntry("key").getValue());
        }
      };
    // wait for commit to complete before checking on other VMs
    TXManagerImpl.waitForPendingCommitForTest();
    vm0.invoke(checkRgn1);
    vm1.invoke(checkRgn1);

    {
      final byte cbSensors[] = {0,0,0,0,0,0,0,0,0};
      txMgr.begin();
      setInternalCallbacks(((TXManagerImpl)txMgr).getTXState(), cbSensors);
      rgn1.put("key", "value1");
      txMgr.commit();
      for (int i = 5; i >= 0; --i) {
        assertEquals("Internal callback " + i + " was not called the expected number of times!", 
                     (byte) 1, cbSensors[i]);
      }
      for (int i = 7; i > 5; --i) {
        assertEquals("Internal \"during\" callback " + i + " invoked an unexpected number of times!", 
                     (byte)0, cbSensors[i]);
      }
    }
    SerializableRunnable checkRgn1Again = 
      new SerializableRunnable("testInternalCallbacks: validate remote values") {
        public void run() {
          Region rgn1a = getCache().getRegion(rgnName1);
          assertNotNull(rgn1a);
          assertEquals("value1", rgn1a.getEntry("key").getValue());
        }
      };
    // wait for commit to complete before checking on other VMs
    TXManagerImpl.waitForPendingCommitForTest();
    vm0.invoke(checkRgn1Again);
    vm1.invoke(checkRgn1Again);
    
    // Try 2 regions
    Region rgn2 = getCache().createRegion(rgnName2, getRegionAttributes());
    txMgr.begin();
    rgn1.put("key", "value2");
    rgn2.put("key", "value2");
    txMgr.commit();
    // wait for commit to complete before checking on other VMs
    TXManagerImpl.waitForPendingCommitForTest();
    SerializableRunnable checkRgn12 = 
      new SerializableRunnable("testInternalCallbacks: check rgn1 valus") {
        public void run() {
          Region rgn1a = getCache().getRegion(rgnName1);
          assertNotNull(rgn1a);
          assertEquals("value2", rgn1a.getEntry("key").getValue());
          Region rgn2a = getCache().getRegion(rgnName2);
          assertNotNull(rgn2a);
          assertEquals("value2", rgn2a.getEntry("key").getValue());
        }
      };
    // wait for commit to complete before checking on other VMs
    TXManagerImpl.waitForPendingCommitForTest();
    vm0.invoke(checkRgn12);
    vm1.invoke(checkRgn12);

    {
      final byte cbSensors[] = {0,0,0,0,0,0,0,0,0};
      txMgr.begin();
      setInternalCallbacks(((TXManagerImpl)txMgr).getTXState(), cbSensors);
      rgn1.put("key", "value3");
      rgn2.put("key", "value3");
      txMgr.commit();

      for (int i = 5; i >= 0; i--) {
        assertEquals("Internal callback " + i + " was not called the expected number of times!", 
                     (byte) 1, cbSensors[i]);
      }
      for (int i = 7; i > 5; --i) {
        assertEquals("Internal \"during\" callback " + i + " invoked an unexpected number of times!", 
                     (byte) 0, cbSensors[i]);
      }
    }
    SerializableRunnable checkRgn12Again = 
      new SerializableRunnable("testInternalCallbacks: validate both regions remote values") {
        public void run() {
          Region rgn1a = getCache().getRegion(rgnName1);
          assertNotNull(rgn1a);
          assertEquals("value3", rgn1a.getEntry("key").getValue());
          Region rgn2a = getCache().getRegion(rgnName2);
          assertNotNull(rgn2a);
          assertEquals("value3", rgn2a.getEntry("key").getValue());
        }
      };
    // wait for commit to complete before checking on other VMs
    TXManagerImpl.waitForPendingCommitForTest();
    vm0.invoke(checkRgn12Again);
    vm1.invoke(checkRgn12Again);

    // Try a third region (D_NO_ACK)
    Region rgn3 = getCache().createRegion(rgnName3, getRegionAttributes(Scope.DISTRIBUTED_NO_ACK));
    txMgr.begin();
    rgn1.put("key", "value4");
    rgn2.put("key", "value4");
    rgn3.put("key", "value4");
    txMgr.commit();
    SerializableRunnable checkRgn123 = 
      new SerializableRunnable("testInternalCallbacks: check rgn1 valus") {
        public void run() {
          Region rgn1a = getCache().getRegion(rgnName1);
          assertNotNull(rgn1a);
          assertEquals("value4", rgn1a.getEntry("key").getValue());
          Region rgn2a = getCache().getRegion(rgnName2);
          assertNotNull(rgn2a);
          assertEquals("value4", rgn2a.getEntry("key").getValue());
          Region rgn3a = getCache().getRegion(rgnName3);
          assertNotNull(rgn3a);
          assertEquals("value4", rgn3a.getEntry("key").getValue());
        }
      };
    // wait for commit to complete before checking on other VMs
    TXManagerImpl.waitForPendingCommitForTest();
    vm0.invoke(checkRgn123);
    vm1.invoke(checkRgn123);

    {
      final byte cbSensors[] = {0,0,0,0,0,0,0,0,0};
      txMgr.begin();
      setInternalCallbacks(((TXManagerImpl)txMgr).getTXState(), cbSensors);

      rgn1.put("key", "value5");
      rgn2.put("key", "value5");
      rgn3.put("key", "value5");
      txMgr.commit();

      for (int i = 5; i >= 0; i--) {
        assertEquals("Internal callback " + i + " was not called the expected number of times!", 
                     (byte) 1, cbSensors[i]);
      }
      for (int i = 7; i > 5; --i) {
        assertEquals("Internal \"during\" callback " + i + " invoked an unexpected number of times!", 
                     (byte) 0, cbSensors[i]);
      }
    }
    SerializableRunnable checkRgn123Again = 
      new SerializableRunnable("testInternalCallbacks: validate both regions remote values") {
        public void run() {
          Region rgn1a = getCache().getRegion(rgnName1);
          assertNotNull(rgn1a);
          assertEquals("value5", rgn1a.getEntry("key").getValue());
          Region rgn2a = getCache().getRegion(rgnName2);
          assertNotNull(rgn2a);
          assertEquals("value5", rgn2a.getEntry("key").getValue());
          Region rgn3a = getCache().getRegion(rgnName3);
          assertNotNull(rgn3a);
          assertEquals("value5", rgn3a.getEntry("key").getValue());
        }
      };
    // wait for commit to complete before checking on other VMs
    TXManagerImpl.waitForPendingCommitForTest();
    vm0.invoke(checkRgn123Again);
    vm1.invoke(checkRgn123Again);

    rgn1.destroyRegion();
    rgn2.destroyRegion();
  }

  static final void setInternalCallbacks(TXStateInterface txp,
      final byte[] cbSensors) {
    assertEquals(9, cbSensors.length);
    txp.setObserver(new TransactionObserverAdapter() {

      @Override
      public void afterApplyChanges(TXStateProxy tx) {
        ++cbSensors[0];
      }

      @Override
      public void afterReleaseLocalLocks(TXStateProxy tx) {
        ++cbSensors[1];
      }

      @Override
      public void beforeSend(TXStateProxy tx, boolean rollback) {
        if (rollback) {
          ++cbSensors[6];
        }
        else {
          ++cbSensors[2];
        }
      }

      @Override
      public void afterSend(TXStateProxy tx, boolean rollback) {
        if (rollback) {
          ++cbSensors[7];
        }
        else {
          ++cbSensors[3];
        }
      }

      @Override
      public void duringIndividualCommit(TXStateProxy tx, Object callbackArg) {
        ++cbSensors[4];
      }

      @Override
      public void afterIndividualCommit(TXStateProxy tx, Object callbackArg) {
        ++cbSensors[5];
      }
    });
  }

  /** 
   * Test distributed ack transactions that consist only of 
   * data from loaded values
   */
  public void testDACKLoadedMessage() throws Exception {
    final TXManagerImpl txMgr = (TXManagerImpl)getCache()
        .getCacheTransactionManager();
    final String rgnName = getUniqueName();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setEarlyAck(false);
    factory.setCacheLoader(new CacheLoader() {
        public Object load(LoaderHelper helper) {
          return "val" + helper.getArgument();
        }
        public void close() {}
      });
    Region rgn = getCache().createRegion(rgnName, factory.create());
    
    invokeInEveryVM(new SerializableRunnable("testDACKLoadedMessage: intial configuration") {
        public void run() {
          try {
            AttributesFactory factory2 = new AttributesFactory();
            factory2.setScope(Scope.DISTRIBUTED_ACK);
            factory2.setEarlyAck(false);
            // factory.setDataPolicy(DataPolicy.REPLICATE);
            factory2.setMirrorType(MirrorType.KEYS);
            getCache().createRegion(rgnName, factory2.create());
          } catch (CacheException e) {
            fail("While creating region", e);
          }
        }
      });

    // Confirm the standard case
    txMgr.begin();
    rgn.put("key1", "val1");
    txMgr.commit();
    assertEquals("val1", rgn.getEntry("key1").getValue());

    // ensure commit complete before checking on other nodes
    TXManagerImpl.waitForPendingCommitForTest();
    invokeInEveryVM(new SerializableRunnable("testDACKLoadedMessage: confirm standard case") {
        public void run() {
          Region rgn1 = getCache().getRegion(rgnName);
          assertEquals("val1", rgn1.getEntry("key1").getValue());
        }
      });

    // Confirm loaded value case
    txMgr.begin();
    rgn.get("key2", new Integer(2));
    txMgr.commit();
    assertEquals("val2", rgn.getEntry("key2").getValue());
    TXManagerImpl.waitForPendingCommitForTest();

    invokeInEveryVM(new SerializableRunnable("testDACKLoadedMessage: confirm standard case") {
        public void run() {
          Region rgn1 = getCache().getRegion(rgnName);
          assertEquals("val2", rgn1.getEntry("key2").getValue());
        }
      });

    // This should use the ack w/ the lockid
    txMgr.begin();
    rgn.put("key3", "val3");
    rgn.get("key4", new Integer(4));
    txMgr.commit();
    TXManagerImpl.waitForPendingCommitForTest();

    invokeInEveryVM(new SerializableRunnable("testDACKLoadedMessage: confirm standard case") {
        public void run() {
          Region rgn1 = getCache().getRegion(rgnName);
          assertEquals("val3", rgn1.getEntry("key3").getValue());
          assertEquals("val4", rgn1.getEntry("key4").getValue());
        }
      });

  }

  /**
   * HA support not yet complete in the new TX model.
   */
  public void DISABLED_TILL_NEW_TX_IMPL_COMPLETE_testHighAvailabilityFeatures() throws Exception {
//    final CacheTransactionManager txMgr = this.getCache().getCacheTransactionManager();
//    final TXManagerImpl txMgrImpl = (TXManagerImpl) txMgr;
    final String rgnName = getUniqueName();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setEarlyAck(false);
    Region rgn = getCache().createRegion(rgnName, factory.create());
    invokeInEveryVM(new SerializableRunnable("testHighAvailabilityFeatures: intial region configuration") {
        public void run() {
          try {
            AttributesFactory factory2 = new AttributesFactory();
            factory2.setScope(Scope.DISTRIBUTED_ACK);
            factory2.setEarlyAck(false);
            factory2.setDataPolicy(DataPolicy.REPLICATE);
            getCache().createRegion(rgnName, factory2.create());
          } catch (CacheException e) {
            fail("While creating region", e);
          }
        }
      });

    // create entries
    rgn.put("key0", "val0_0");
    rgn.put("key1", "val1_0");

    Host host = Host.getHost(0);
    // This test assumes that there are at least three VMs; the origin and two recipients
    assertTrue(host.getVMCount() >= 3);
    final VM originVM = host.getVM(0);

    // Test that there is no commit after a partial commit message
    // send (only sent to a minority of the recipients)
    originVM.invoke(new SerializableRunnable("Flakey DuringIndividualSend Transaction") {
        public void run() {
          final Region rgn1 = getCache().getRegion(rgnName);
          assertNotNull(rgn1);
          try {
            final CacheTransactionManager txMgr2 = getCache().getCacheTransactionManager();
            final CacheTransactionManager txMgrImpl = txMgr2;
            
            txMgr2.begin();
            // 1. setup an internal callback on originVM that will call
            // disconnectFromDS() on the 2nd duringIndividualSend
            // call.
            TXStateInterface txState = ((TXManagerImpl)txMgrImpl).getTXState();
            txState.setObserver(new TransactionObserverAdapter() {
              private int numCalled = 0;
              @Override
              public synchronized void beforeSend(TXStateProxy tx,
                  boolean rollback) {
                ++numCalled;
                rgn1.getCache().getLogger().info("beforeSend called "
                    + numCalled + " times");
                if (numCalled > 1) {
                  disconnectFromDS();
                }
              }
            });
            rgn1.put("key0", "val0_1");
            rgn1.put("key1", "val1_1");
            // 2. commit a transaction in originVM, it will disconnect from the DS
            txMgr2.commit();
          } 
          catch (VirtualMachineError e) {
            SystemFailure.initiateFailure(e);
            throw e;
          }
          catch (Throwable e) {
            rgn1.getCache().getLogger().warning("Ignoring Exception", e);
          } 
          finally {
            // Allow this VM to re-connect to the DS upon getCache() call
            closeCache();
          }
        }
      });
    // 3. verify on all VMs that the transaction was not committed
    final SerializableRunnable noChangeValidator = 
      new SerializableRunnable("testHighAvailabilityFeatures: validate no change in Region") {
        public void run() {
          Region rgn1 = getCache().getRegion(rgnName);
          if (rgn1 == null) {
            // Expect a null region from originVM
            try {
              AttributesFactory factory2 = new AttributesFactory();
              factory2.setScope(Scope.DISTRIBUTED_ACK);
              factory2.setDataPolicy(DataPolicy.REPLICATE);
              rgn1 = getCache().createRegion(rgnName, factory2.create());
            } catch (CacheException e) {
              fail("While creating region", e);
            }
          }
          Region.Entry re = rgn1.getEntry("key0");
          assertNotNull(re);
          assertEquals("val0_0", re.getValue());
          re = rgn1.getEntry("key1");
          assertNotNull(re);
          assertEquals("val1_0", re.getValue());
        }
      };
    invokeInEveryVM(noChangeValidator);

    // Test that there is no commit after sending to all recipients
    // but prior to sending the "commit process" message
    originVM.invoke(new SerializableRunnable("Flakey AfterIndividualSend Transaction") {
        public void run() {
          final Region rgn1 = getCache().getRegion(rgnName);
          assertNotNull(rgn1);
          try {
            final CacheTransactionManager txMgr2 = getCache().getCacheTransactionManager();
            final CacheTransactionManager txMgrImpl = txMgr2;
            
            txMgr2.begin();
            // 1. setup an internal callback on originVM that will call
            // disconnectFromDS() on AfterIndividualSend
            TXStateInterface txState = ((TXManagerImpl)txMgrImpl).getTXState();
            txState.setObserver(new TransactionObserverAdapter() {
                @Override
                public synchronized void afterSend(TXStateProxy tx,
                    boolean rollback) {
                  MembershipManagerHelper.crashDistributedSystem(getSystem());
                }
              });
            rgn1.put("key0", "val0_2");
            rgn1.put("key1", "val1_2");
            // 2. commit a transaction in originVM, it will disconnect from the DS
            txMgr2.commit();
          } 
          catch (VirtualMachineError e) {
            SystemFailure.initiateFailure(e);
            throw e;
          }
          catch (Throwable e) {
            rgn1.getCache().getLogger().warning("Ignoring Exception", e);
          } 
          finally {
            // Allow this VM to re-connect to the DS upon getCache() call
            closeCache();
          }
        }
      });
    // 3. verify on all VMs, including the origin, that the transaction was not committed
    invokeInEveryVM(noChangeValidator);

    // Test commit success upon a single commit process message received.
    originVM.invoke(new SerializableRunnable("Flakey DuringIndividualCommitProcess Transaction") {
        public void run() {
          final Region rgn1 = getCache().getRegion(rgnName);
          assertNotNull(rgn1);
          try {
            final CacheTransactionManager txMgr2 = getCache().getCacheTransactionManager();
            final CacheTransactionManager txMgrImpl = txMgr2;
            
            txMgr2.begin();

            TXStateInterface txState = ((TXManagerImpl)txMgrImpl).getTXState();
            // 1. setup an internal callback on originVM that will call
            // disconnectFromDS() on the 2nd internalDuringIndividualCommitProcess
            // call.
            txState.setObserver(new TransactionObserverAdapter() {
                private int numCalled = 0;
                public synchronized void duringInvidividualCommitProcess(TXStateProxy tx) {
                  ++numCalled;
                  rgn1.getCache().getLogger().info("duringIndividual"
                      + "CommitProcess called " + numCalled + " times");
                  if (numCalled > 1) {
                    MembershipManagerHelper.crashDistributedSystem(getSystem());
                  }
                }
              });
            rgn1.put("key0", "val0_3");
            rgn1.put("key1", "val1_3");
            // 2. commit a transaction in originVM, it will disconnect from the DS
            txMgr2.commit();
          } 
          catch (VirtualMachineError e) {
            SystemFailure.initiateFailure(e);
            throw e;
          }
          catch (Throwable e) {
            rgn1.getCache().getLogger().warning("Ignoring Exception", e);
          } 
          finally {
            // Allow this VM to re-connect to the DS upon getCache() call
            closeCache();
          }
        }
      });
    // 3. verify on all VMs that the transaction was committed (including the orgin, due to GII)
    SerializableRunnable nonSoloChangeValidator1 = 
      new SerializableRunnable("testHighAvailabilityFeatures: validate v1 non-solo Region changes") {
        public void run() {
          Region rgn1 = getCache().getRegion(rgnName);
          if (rgn1 == null) {
            // Expect a null region from originVM
            try {
              AttributesFactory factory2 = new AttributesFactory();
              factory2.setScope(Scope.DISTRIBUTED_ACK);
              factory2.setEarlyAck(false);
              factory2.setDataPolicy(DataPolicy.REPLICATE);
              rgn1 = getCache().createRegion(rgnName, factory2.create());
            } catch (CacheException e) {
              fail("While creating region", e);
            }
          }
          Region.Entry re = rgn1.getEntry("key0");
          assertNotNull(re);
          assertEquals("val0_3", re.getValue());
          re = rgn1.getEntry("key1");
          assertNotNull(re);
          assertEquals("val1_3", re.getValue());
        }
      };
    invokeInEveryVM(nonSoloChangeValidator1);

    // Verify successful solo region commit after duringIndividualSend
    // (same as afterIndividualSend).
    // Create a region that only exists on the origin and another VM
    final String soloRegionName = getUniqueName() + "_solo";
    SerializableRunnable createSoloRegion = 
      new SerializableRunnable("testHighAvailabilityFeatures: solo region configuration") {
        public void run() {
          try {
            AttributesFactory factory2 = new AttributesFactory();
            factory2.setScope(Scope.DISTRIBUTED_ACK);
            factory2.setEarlyAck(false);
            factory2.setDataPolicy(DataPolicy.REPLICATE);
            Region rgn1 = getCache().createRegion(soloRegionName, factory2.create());
            rgn1.put("soloKey0", "soloVal0_0");
            rgn1.put("soloKey1", "soloVal1_0");
          } catch (CacheException e) {
            fail("While creating region", e);
          }
        }
      };
    final VM soloRegionVM = host.getVM(1);
    originVM.invoke(createSoloRegion);
    soloRegionVM.invoke(createSoloRegion);
    originVM.invoke(new SerializableRunnable("Flakey solo region DuringIndividualSend Transaction") {
        public void run() {
          final Region soloRgn = getCache().getRegion(soloRegionName);
          assertNotNull(soloRgn);
          try {
            final CacheTransactionManager txMgr2 = getCache().getCacheTransactionManager();
            final CacheTransactionManager txMgrImpl = txMgr2;
            
            txMgr2.begin();
            // 1. setup an internal callback on originVM that will call
            // disconnectFromDS() on the 2nd duringIndividualSend
            // call.
            TXStateInterface txState = ((TXManagerImpl)txMgrImpl).getTXState();
            txState.setObserver(new TransactionObserverAdapter() {
                private int numCalled = 0;
                public synchronized void beforeSend() {
                  ++numCalled;
                  soloRgn.getCache().getLogger().info("beforeSend called "
                      + numCalled + " times");
                  if (numCalled > 1) {
                    MembershipManagerHelper.crashDistributedSystem(getSystem());
                  }
                }
              });
            soloRgn.put("soloKey0", "soloVal0_1");
            soloRgn.put("soloKey1", "soloVal1_1");
            // 2. commit a transaction in originVM, it will disconnect from the DS
            txMgr2.commit();
          } 
          catch (VirtualMachineError e) {
            SystemFailure.initiateFailure(e);
            throw e;
          }
          catch (Throwable e) {
            soloRgn.getCache().getLogger().warning("Ignoring Exception", e);
          } 
          finally {
            // Allow this VM to re-connect to the DS upon getCache() call
            closeCache();
          }
        }
      });
    // 3. verify on the soloRegionVM that the transaction was committed
    final SerializableRunnable soloRegionCommitValidator1 = 
      new SerializableRunnable("testHighAvailabilityFeatures: validate successful v1 commit in solo Region") {
        public void run() {
          Region soloRgn = getCache().getRegion(soloRegionName);
          if (soloRgn == null) {
            // Expect a null region from originVM
            try {
              AttributesFactory factory2 = new AttributesFactory();         
              factory2.setScope(Scope.DISTRIBUTED_ACK);
              factory2.setEarlyAck(false);
              factory2.setDataPolicy(DataPolicy.REPLICATE);
              soloRgn = getCache().createRegion(soloRegionName, factory2.create());
            } catch (CacheException e) {
              fail("While creating region ", e);
            }
          }
          Region.Entry re = soloRgn.getEntry("soloKey0");
          assertNotNull(re);
          assertEquals("soloVal0_1", re.getValue());
          re = soloRgn.getEntry("soloKey1");
          assertNotNull(re);
          assertEquals("soloVal1_1", re.getValue());
        }
      };
    originVM.invoke(soloRegionCommitValidator1);
    soloRegionVM.invoke(soloRegionCommitValidator1);
    // verify no change in nonSolo region, re-establish region in originVM
    invokeInEveryVM(nonSoloChangeValidator1);

    // Verify no commit for failed send (afterIndividualSend) for solo
    // Region combined with non-solo Region
    originVM.invoke(new SerializableRunnable("Flakey mixed (solo+non-solo) region DuringIndividualSend Transaction") {
        public void run() {
          final Region rgn1 = getCache().getRegion(rgnName);
          assertNotNull(rgn1);
          final Region soloRgn = getCache().getRegion(soloRegionName);
          assertNotNull(soloRgn);
          try {
            final CacheTransactionManager txMgr2 = getCache().getCacheTransactionManager();
            final CacheTransactionManager txMgrImpl = txMgr2;
            
            txMgr2.begin();
            // 1. setup an internal callback on originVM that will call
            // disconnectFromDS() on the afterIndividualSend
            TXStateInterface txState = ((TXManagerImpl)txMgrImpl).getTXState();
            txState.setObserver(new TransactionObserverAdapter() {
                @Override
                public synchronized void afterSend(TXStateProxy tx,
                    boolean rollback) {
                  MembershipManagerHelper.crashDistributedSystem(getSystem());
                }
              });

            rgn1.put("key0", "val0_4");
            rgn1.put("key1", "val1_4");
            soloRgn.put("soloKey0", "soloVal0_2");
            soloRgn.put("soloKey1", "soloVal1_2");

            // 2. commit a transaction in originVM, it will disconnect from the DS
            txMgr2.commit();
          } 
          catch (VirtualMachineError e) {
            SystemFailure.initiateFailure(e);
            throw e;
          }
          catch (Throwable e) {
            rgn1.getCache().getLogger().warning("Ignoring Exception", e);
          } 
          finally {
            // Allow this VM to re-connect to the DS upon getCache() call
            closeCache();
          }
        }
      });
    // Origin and Solo Region VM should be the same as last validation
    originVM.invoke(soloRegionCommitValidator1);
    soloRegionVM.invoke(soloRegionCommitValidator1);
    invokeInEveryVM(nonSoloChangeValidator1);

    // Verify commit after sending a single
    // (duringIndividualCommitProcess) commit process for solo Region
    // combined with non-solo Region
    originVM.invoke(new SerializableRunnable("Flakey mixed (solo+non-solo) region DuringIndividualCommitProcess Transaction") {
        public void run() {
          final Region rgn1 = getCache().getRegion(rgnName);
          assertNotNull(rgn1);
          final Region soloRgn = getCache().getRegion(soloRegionName);
          assertNotNull(soloRgn);
          try {
            final CacheTransactionManager txMgr2 = getCache().getCacheTransactionManager();
            final CacheTransactionManager txMgrImpl = txMgr2;
            
            txMgr2.begin();
            // 1. setup an internal callback on originVM that will call
            // disconnectFromDS() on the afterIndividualSend
            // call.
            // TODO the below line commented to compile. See the test later: Neeraj
            //((TXStateProxyImpl)((TXManagerImpl)txMgrImpl).getTXState()).forceLocalBootstrap();
            // TODO the below line modified also. 
            //TXState txState = (TXState)((TXStateProxyImpl)((TXManagerImpl)txMgrImpl).getTXState()).getRealDeal(null,null);
            TXState txState = (TXState)((TXManagerImpl)txMgrImpl).getTXState();
            txState.setObserver(new TransactionObserverAdapter() {
                private int numCalled = 0;
                @Override
                public synchronized void afterSend(TXStateProxy tx,
                    boolean rollback) {
                  ++numCalled;
                  rgn1.getCache().getLogger().info("afterSend called "
                      + numCalled + " times");
                  if (numCalled > 1) {
                    MembershipManagerHelper.crashDistributedSystem(getSystem());
                  }
                }
              });

            rgn1.put("key0", "val0_5");
            rgn1.put("key1", "val1_5");
            soloRgn.put("soloKey0", "soloVal0_3");
            soloRgn.put("soloKey1", "soloVal1_3");

            // 2. commit a transaction in originVM, it will disconnect from the DS
            txMgr2.commit();
          } 
          catch (VirtualMachineError e) {
            SystemFailure.initiateFailure(e);
            throw e;
          }
          catch (Throwable e) {
            rgn1.getCache().getLogger().warning("Ignoring Exception", e);
          } 
          finally {
            // Allow this VM to re-connect to the DS upon getCache() call
            closeCache();
          }
        }
      });
    final SerializableRunnable soloRegionCommitValidator2 = 
      new SerializableRunnable("testHighAvailabilityFeatures: validate successful v2 commit in solo Region") {
        public void run() {
          Region soloRgn = getCache().getRegion(soloRegionName);
          if (soloRgn == null) {
            // Expect a null region from originVM
            try {
              AttributesFactory factory2 = new AttributesFactory();         
              factory2.setScope(Scope.DISTRIBUTED_ACK);
              factory2.setEarlyAck(false);
              factory2.setDataPolicy(DataPolicy.REPLICATE);
              soloRgn = getCache().createRegion(soloRegionName, factory2.create());
            } catch (CacheException e) {
              fail("While creating region ", e);
            }
          }
          Region.Entry re = soloRgn.getEntry("soloKey0");
          assertNotNull(re);
          assertEquals("soloVal0_3", re.getValue());
          re = soloRgn.getEntry("soloKey1");
          assertNotNull(re);
          assertEquals("soloVal1_3", re.getValue());
        }
      };
    originVM.invoke(soloRegionCommitValidator2);
    soloRegionVM.invoke(soloRegionCommitValidator2);
    SerializableRunnable nonSoloChangeValidator2 = 
      new SerializableRunnable("testHighAvailabilityFeatures: validate v2 non-solo Region changes") {
        public void run() {
          Region rgn1 = getCache().getRegion(rgnName);
          if (rgn1 == null) {
            // Expect a null region from originVM
            try {
              AttributesFactory factory2 = new AttributesFactory();
              factory2.setScope(Scope.DISTRIBUTED_ACK);
              factory2.setEarlyAck(false);
              factory2.setDataPolicy(DataPolicy.REPLICATE);
              rgn1 = getCache().createRegion(rgnName, factory2.create());
            } catch (CacheException e) {
              fail("While creating region", e);
            }
          }
          Region.Entry re = rgn1.getEntry("key0");
          assertNotNull(re);
          assertEquals("val0_5", re.getValue());
          re = rgn1.getEntry("key1");
          assertNotNull(re);
          assertEquals("val1_5", re.getValue());
        }
      };
    invokeInEveryVM(nonSoloChangeValidator2);
  }

  /*
  /** 
   * A class used in testLockBatchParticipantsUpdate to pause a transaction in the
   * afterResrvation and afterSend states.
   *
  static public class PausibleTX implements Runnable {
    public boolean isRunning = false;
    public String rgnName = null;
    public Cache myCache = null;
    public Object key = null;
    public Object value = null;

    public boolean getIsRunning() {
      return this.isRunning;
    }

    public void run() {
      Region rgn = this.myCache.getRegion(this.rgnName);
      final CacheTransactionManager txMgr = this.myCache.getCacheTransactionManager();
      txMgr.begin();
      // TODO the below line commented to compile. See the test later: Neeraj
      //((TXStateProxyImpl)((TXManagerImpl)txMgrImpl).getTXState()).forceLocalBootstrap();
      // TODO the below line modified also. 
      //TXState txState = (TXState)((TXStateProxyImpl)((TXManagerImpl)txMgrImpl).getTXState()).getRealDeal(null,null);
      TXState txState = (TXState)((TXManagerImpl)txMgr).getTXState();
      txState.setAfterReservation(new Runnable() {
        public void run() {
          try {
            synchronized (PausibleTX.class) {
              PausibleTX.this.isRunning = true;
              // Notify the thread that created this, that we are ready
              PausibleTX.class.notifyAll();
              // Wait for the controller to start a GII and let us proceed
              PausibleTX.class.wait();
            }
          }
          catch (InterruptedException ie) {
//            PausibleTX.this.myCache.getLogger().info("Why was I interrupted? " + ie);
            fail("interrupted");
          }
        }
      });
      txState.setAfterSend(new Runnable() {
        public void run() {
          try {
            synchronized (PausibleTX.class) {
              // Notify the controller that we have sent the TX data (and the
              // update)
              PausibleTX.class.notifyAll();
              // Wait until the controller has determined in fact the update
              // took place
              PausibleTX.class.wait();
            }
          }
          catch (InterruptedException ie) {
//            PausibleTX.this.myCache.getLogger().info("Why was I interrupted? " + ie);
            fail("interrupted");
          }
        }
      });
      try { 
        rgn.put(key, value);
        txMgr.commit();
      } catch (ConflictException cce) {
        fail("Did not expect conflict exception when sending updates to new members in PausibleTX" + cce);
//      } catch (CacheException ce) {
//        fail("Did not expect cache exception when sending updates to new members in PausibleTX" + ce);
      }
    }
  }
  */

  /**
   * Returns the GemFire system ID of the VM on which this method is run
   */
  public static Serializable getSystemId() {
    Serializable ret = null;
    if (DistributedTestCase.system != null) {
      ret = DistributedTestCase.system.getDistributionManager().getId();
    }
    return ret;
  }
  static HashSet preTXSystemIds;
  public static void setPreTXSystemIds(HashSet ids) {
    TXDistributedTest.preTXSystemIds = ids;
  }
  static HashSet postTXSystemIds;
  public static void setPostTXSystemIds(HashSet ids) {
    TXDistributedTest.postTXSystemIds = ids;
  }
  static Serializable txHostId;
  public static void setTXHostSystemId(Serializable id) {
    TXDistributedTest.txHostId = id;
  }

  /*
  /**
   * Test update of lock batch participants (needed when new members are
   * discovered between a commit's locking phase and the applicatoin of the
   * Region's data. See bug 32999
   * [sumedh] No longer valid in the new TX model.
   *
  public void testLockBatchParticipantsUpdate() throws Exception {
//    final CacheTransactionManager txMgr = this.getCache().getCacheTransactionManager();
    final String rgnName = getUniqueName();
    Region rgn = getCache().createRegion(rgnName, getRegionAttributes());
    rgn.create("key", null);

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    SerializableRunnable initRegions = 
      new SerializableRunnable("testLockBatchParticipantsUpdate: initial configuration") {
      public void run() {
        try {
          Region rgn1 = getCache().createRegion(rgnName, getRegionAttributes());
          rgn1.create("key", null);
        } catch (CacheException e) {
          fail("While creating region", e);
        }
      }
    };
    vm0.invoke(initRegions);
    vm1.invoke(initRegions);
    rgn.put("key", "val1");

    // Connect vm2 also since it may have been shutdown when logPerTest
    // is turned on
    vm2.invoke(new SerializableRunnable("connect vm2 if not connected") {
      public void run() {
        getCache();
      }
    });

    // Make VM0 the Grantor 
    vm0.invoke(new SerializableRunnable("testLockBatchParticipantsUpdate: remote grantor init") {
      public void run() {
        try {
          Region rgn1 = getCache().getRegion(rgnName);
          final CacheTransactionManager txMgr2 = getCache().getCacheTransactionManager();
          assertEquals("val1", rgn1.getEntry("key").getValue());
          txMgr2.begin();
          rgn1.put("key", "val2");
          txMgr2.commit();
          assertNotNull(TXLockService.getDTLS());
          assertTrue(TXLockService.getDTLS().isLockGrantor());
        } catch (CacheException e) {
          fail("While performing first transaction");
        }
      }
    });
    
    // fix for bug 38843 causes the DTLS to be created in every TX participant
    assertNotNull(TXLockService.getDTLS());
    assertFalse(TXLockService.getDTLS().isLockGrantor());
    assertEquals("val2", rgn.getEntry("key").getValue());

    // Build sets of System Ids and set them up on VM0 for future batch member checks
    HashSet txMembers = new HashSet(4);
    txMembers.add(getSystemId());
    txMembers.add(vm0.invoke(TXDistributedTest.class, "getSystemId"));
    vm0.invoke(TXDistributedTest.class, "setPreTXSystemIds", new Object[] {txMembers});
    txMembers.add(vm2.invoke(TXDistributedTest.class, "getSystemId"));
    vm0.invoke(TXDistributedTest.class, "setPostTXSystemIds", new Object[] {txMembers});

    // Don't include the tx host in the batch member set(s)
    Serializable vm1HostId = (Serializable) vm1.invoke(TXDistributedTest.class, "getSystemId");
    vm0.invoke(TXDistributedTest.class, "setTXHostSystemId", new Object[] {vm1HostId});

    // Create a TX on VM1 (such that it will ask for locks on VM0) that uses the callbacks
    // to pause and give us time to start a GII process on another VM
    vm1.invoke(new SerializableRunnable("testLockBatchParticipantsUpdate: slow tx (one that detects new member)") {
      public void run() {
        // fix for bug 38843 causes the DTLS to be created in every TX participant
        assertNotNull(TXLockService.getDTLS());
        assertFalse(TXLockService.getDTLS().isLockGrantor());
        
        PausibleTX pauseTXRunnable = new PausibleTX();
        pauseTXRunnable.rgnName = rgnName;
        pauseTXRunnable.myCache = getCache();
        pauseTXRunnable.key = "key";
        pauseTXRunnable.value = "val3";
        new Thread(pauseTXRunnable, "PausibleTX Thread").start();
        synchronized(PausibleTX.class) {
          while(!pauseTXRunnable.getIsRunning()) {
            try {
              PausibleTX.class.wait();
            }
            catch (InterruptedException ie) {
              fail("Did not expect " + ie);
            }
          }
        }
      }
    });

    // Verify that the lock batch exists VM0 and has the size we expect
    vm0.invoke(new SerializableRunnable("testLockBatchParticipantsUpdate: Verify lock batch exists on VM0 with expected size") {
      public void run() {
        getCache().getRegion(rgnName);
        TXLockServiceImpl dtls = (TXLockServiceImpl) TXLockService.getDTLS();
        assertNotNull(dtls);
        assertTrue(dtls.isLockGrantor());
        DLockService dLockSvc = dtls.getInternalDistributedLockService();
        assertNotNull(TXDistributedTest.txHostId);
        DLockBatch[] batches = dLockSvc.getGrantor().getLockBatches((InternalDistributedMember)TXDistributedTest.txHostId);
        assertEquals(batches.length, 1);
        TXLockBatch txLockBatch = (TXLockBatch) batches[0];
        assertNotNull(txLockBatch);
        assertNotNull(TXDistributedTest.preTXSystemIds);
        assertTrue("Members in lock batch " + txLockBatch.getParticipants() + " not the same as " + TXDistributedTest.preTXSystemIds, 
                   txLockBatch.getParticipants().equals(TXDistributedTest.preTXSystemIds));
      }
    });
    
    // Start a GII process on VM2
    vm2.invoke(new SerializableRunnable("testLockBatchParticipantsUpdate: start GII") {
      public void run() {
        try {
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setDataPolicy(DataPolicy.REPLICATE);
          getCache().createRegion(rgnName, factory.create());
        } catch (CacheException e) {
          fail("While creating region", e);
        }
      }
    });

    // Notify TX on VM1 so that it can continue
    vm1.invoke(new SerializableRunnable("testLockBatchParticipantsUpdate: Notfiy VM1 TX to continue") {
      public void run() {
        synchronized(PausibleTX.class) {
          // Notify VM1 that it should proceed to the TX send
          PausibleTX.class.notifyAll();
          // Wait until VM1 has sent the TX
          try {
            PausibleTX.class.wait();
          }
          catch (InterruptedException ie) {
            fail("Did not expect " + ie);
          }
        }
      }
    });
    
    // Verify that the batch on VM0 has added VM2 into the set
    vm0.invoke(new SerializableRunnable("testLockBatchParticipantsUpdate: Verify lock batch contains VM2") {
      public void run() {
        getCache().getRegion(rgnName);
        TXLockServiceImpl dtls = (TXLockServiceImpl) TXLockService.getDTLS();
        assertNotNull(dtls);
        assertTrue(dtls.isLockGrantor());
        DLockService dLockSvc = dtls.getInternalDistributedLockService();
        assertNotNull(TXDistributedTest.txHostId);
        DLockBatch[] batches = dLockSvc.getGrantor().getLockBatches((InternalDistributedMember)TXDistributedTest.txHostId);
        assertEquals(batches.length, 1);
        TXLockBatch txLockBatch = (TXLockBatch) batches[0];
        assertNotNull(txLockBatch);
        assertNotNull(TXDistributedTest.preTXSystemIds);
        assertTrue("Members in lock batch " + txLockBatch.getParticipants() + " not the same as " + TXDistributedTest.postTXSystemIds, 
                   txLockBatch.getParticipants().equals(TXDistributedTest.postTXSystemIds));
      }
    });
    // fix for bug 38843 causes the DTLS to be created in every TX participant
    assertNotNull(TXLockService.getDTLS());
    assertFalse(TXLockService.getDTLS().isLockGrantor());
    assertEquals("val3", rgn.getEntry("key").getValue());
    
    
    // Notify TX on VM1 that it can go ahead and complete the TX
    vm1.invoke(new SerializableRunnable("testLockBatchParticipantsUpdate: Notfiy VM1 TX to finish") {
      public void run() {
        synchronized(PausibleTX.class) {
          // Notify VM1 that it should finish the TX
          PausibleTX.class.notifyAll();
        }
      }
    });
    
    
    rgn.destroyRegion();
  }
  */

  /**
   * Hitachi bug 38809:  Applying an exception to a remote VM fails due to
   * an IOException on a Region configured for LRU Overflow
   */
  public static final String TROUBLE_KEY = "GoBoom";
  public static class TXTroubleMaker implements LocalRegion.TestCallable {
    // private final Region r;
    public void call(LocalRegion r, Operation op, RegionEntry re) {
      if (TROUBLE_KEY.equals(re.getKey())) {
        throw new DiskAccessException(TROUBLE_KEY, r);
      }
    }
  }

  public void testRemoteCommitFailure() {
    final String rgnName1= getUniqueName()  + "_1";
    final String rgnName2 = getUniqueName() + "_2";
    Host host = Host.getHost(0);
    VM origin = host.getVM(0);
    VM trouble1 = host.getVM(1);
    VM trouble2 = host.getVM(2);
    VM noTrouble = host.getVM(3);
    CacheSerializableRunnable initRegions =
      new CacheSerializableRunnable("Initialize no trouble regions") {
      @Override
      public void run2() {
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.REPLICATE);
        af.setScope(Scope.DISTRIBUTED_ACK);
        getCache().createRegion(rgnName1, af.create());
        getCache().createRegion(rgnName2, af.create());
      }
    };
    origin.invoke(initRegions);
    noTrouble.invoke(initRegions);
    SerializableRunnable initTroulbeRegions =
      new CacheSerializableRunnable("Initialize regions that cause trouble") {
      @Override
      public void run2() {
        GemFireCacheImpl gfc = (GemFireCacheImpl) getCache();
        InternalRegionArguments ira = new InternalRegionArguments().setTestCallable(new TXTroubleMaker());
        try {
          AttributesFactory af = new AttributesFactory();
          af.setDataPolicy(DataPolicy.REPLICATE);
          af.setScope(Scope.DISTRIBUTED_ACK);
          gfc.createVMRegion(rgnName1, af.create(), ira);
          gfc.createVMRegion(rgnName2, af.create(), ira);
        } catch (IOException ioe) {
          fail(ioe.toString());
        }
        catch (TimeoutException e) {
          fail(e.toString());
        }
        catch (ClassNotFoundException e) {
          fail(e.toString());
        }
      }
    };
    trouble1.invoke(initTroulbeRegions);
    trouble2.invoke(initTroulbeRegions);

    SerializableRunnable doTransaction =
      new CacheSerializableRunnable("Run failing transaction") {
      @Override
      public void run2() {
        Cache c = getCache();
        Region r1 = c.getRegion(rgnName1);
        assertNotNull(r1);
        Region r2 = c.getRegion(rgnName2);
        assertNotNull(r2);
        CacheTransactionManager txmgr = c.getCacheTransactionManager();
        txmgr.begin();
        r1.put("k1", "k1");
        r1.put("k2", "k2");
        r1.put(TROUBLE_KEY, TROUBLE_KEY);
        r2.put("k1", "k1");
        r2.put("k2", "k2");
        r2.put(TROUBLE_KEY, TROUBLE_KEY);
        try {
          txmgr.commit();
          // commit phase2 now happens in background assuming success,
          // so no exception is thrown
          //fail("Expected a tx incomplete exception");
        } catch (TransactionInDoubtException yay) {
          String msg = yay.getMessage();
          // Each region on a trouble VM should be mentioned (two regions per trouble VM)
          int ind=0, match=0;
          while((ind = msg.indexOf(rgnName1, ind)) >= 0) {
            ind++; match++;
          }
          assertEquals(1, match);
          // DiskAccessExcpetion should be mentioned
          ind=match=0;
          while((ind = msg.indexOf(DiskAccessException.class.getName(), ind)) >= 0) {
            ind++; match++;
          }
          assertEquals(1, match);
          // we do not expect exceptions in commit phase 2 to be sent back
          fail("do not expect to receive exceptions from commit phase 2");
        }
        TXManagerImpl.waitForPendingCommitForTest();
      }
    };
    ExpectedException ee = null;
    try {
      ee = addExpectedException(DiskAccessException.class.getName() + "|" +
          TransactionInDoubtException.class.getName());
      origin.invoke(doTransaction);
    } finally {
      if (ee!=null) ee.remove();
    }

    // Assert proper content on failing VMs
    SerializableRunnable assertTroubledContent =
      new CacheSerializableRunnable("Assert partial commit data") {
      @Override
      public void run2() {
        Cache c = getCache();
        {
          Region r1 = c.getRegion(rgnName1);
          assertNull(r1);
//          assertEquals("k1", r1.getEntry("k1").getValue());
//          assertEquals("k2", r1.getEntry("k2").getValue());
//          assertNull(r1.getEntry(TROUBLE_KEY));
        }
        {
          // [sumedh] due to eager failure the second region never fails now
//          Region r2 = c.getRegion(rgnName2);
//          assertNull(r2);
//          assertEquals("k1", r2.getEntry("k1").getValue());
//          assertEquals("k2", r2.getEntry("k2").getValue());
//          assertNull(r2.getEntry(TROUBLE_KEY));
        }
      }
    };
    trouble1.invoke(assertTroubledContent);
    trouble2.invoke(assertTroubledContent);

    // Assert proper content on successful VMs
    SerializableRunnable assertSuccessfulContent =
      new CacheSerializableRunnable("Assert complete commit of data on successful VMs") {
      @Override
      public void run2() {
        Cache c = getCache();
        {
          Region r1 = c.getRegion(rgnName1);
          assertNotNull(r1);
          assertEquals("k1", r1.getEntry("k1").getValue());
          assertEquals("k2", r1.getEntry("k2").getValue());
          assertEquals(TROUBLE_KEY, r1.getEntry(TROUBLE_KEY).getValue());
        }
        {
          Region r2 = c.getRegion(rgnName2);
          assertNotNull(r2);
          assertEquals("k1", r2.getEntry("k1").getValue());
          assertEquals("k2", r2.getEntry("k2").getValue());
          assertEquals(TROUBLE_KEY, r2.getEntry(TROUBLE_KEY).getValue());
        }
      }
    };
    noTrouble.invoke(assertSuccessfulContent);
    
    // Assert no content on originating VM
    SerializableRunnable assertNoContent =
      new CacheSerializableRunnable("Assert data survives on origin VM") {
      @Override
      public void run2() {
        Cache c = getCache();
        {
          Region r1 = c.getRegion(rgnName1);
          assertNotNull(r1);
          assertNotNull(r1.getEntry("k1"));
          assertNotNull(r1.getEntry("k2"));
          assertNotNull(r1.getEntry(TROUBLE_KEY));
        }
        {
          Region r2 = c.getRegion(rgnName2);
          assertNotNull(r2);
          assertNotNull(r2.getEntry("k1"));
          assertNotNull(r2.getEntry("k2"));
          assertNotNull(r2.getEntry(TROUBLE_KEY));
        }
      }
    };
    origin.invoke(assertNoContent);
  }
}
