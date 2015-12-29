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
package com.gemstone.gemfire.internal.offheap;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.gemstone.gemfire.OutOfOffHeapMemoryException;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.DistributedSystemDisconnectedException;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.cache.DistributedRegion;
import com.gemstone.gemfire.internal.util.StopWatch;

import dunit.Host;
import dunit.SerializableRunnable;

/**
 * Test behavior of region when running out of off-heap memory.
 * 
 * @author Kirk Lund
 */
@SuppressWarnings("serial")
public class OutOfOffHeapMemoryDUnitTest extends CacheTestCase {

  private static final String EXPECTED_EXCEPTIONS = "com.gemstone.gemfire.OutOfOffHeapMemoryException";
  private static final String ADD_EXPECTED_EXCEPTIONS = "<ExpectedException action=add>" + EXPECTED_EXCEPTIONS + "</ExpectedException>";
  private static final String REMOVE_EXPECTED_EXCEPTIONS = "<ExpectedException action=remove>" + EXPECTED_EXCEPTIONS + "</ExpectedException>";
  
  public OutOfOffHeapMemoryDUnitTest(String name) {
    super(name);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    long begin = System.currentTimeMillis();
    Cache gfc = null;
    while (gfc == null) {
      try {
        gfc = getCache();
        break;
      } catch (IllegalStateException e) {
        if (System.currentTimeMillis() > begin+60*1000) {
          fail("OutOfOffHeapMemoryDUnitTest waited too long to getCache", e);
        } else if (e.getMessage().contains("A connection to a distributed system already exists in this VM.  It has the following configuration")) {
          InternalDistributedSystem ids = InternalDistributedSystem.getAnyInstance();
          if (ids != null && ids.isConnected()) {
            ids.getLogWriter().warning("OutOfOffHeapMemoryDUnitTest found DistributedSystem connection from previous test", e);
            ids.disconnect();
          }
        } else {
          throw e;
        }
      }
    }
  }

  @Override
  public void tearDown2() throws Exception {
    invokeInEveryVM(getClass(), "cleanup");
//    invokeInEveryVM(new SerializableRunnable() {
//      public void run() {
//        cleanup();
//      }
//    });
  }

//  public static void caseSetUp() {
//    for (int i = 0; i < Host.getHost(0).getVMCount(); i++) {
//      Host.getHost(0).getVM(i).invoke(new SerializableRunnable() {
//        public void run() {
//          InternalDistributedSystem ids = InternalDistributedSystem.getAnyInstance();
//          if (ids != null && ids.isConnected()) {
//            ids.getLogWriter().warning("OutOfOffHeapMemoryDUnitTest: Found DistributedSystem connection from previous test " + ids);
//            ids.disconnect();
//          }
//        }
//      });
//    }
//  }
  
  @SuppressWarnings("unused") // invoked by reflection from tearDown2()
  private static void cleanup() {
    try {
      //cache.get().getLogger().info(REMOVE_EXPECTED_EXCEPTIONS);
      //getCache().getLogger().info(REMOVE_EXPECTED_EXCEPTIONS);
      getLogWriter().info(REMOVE_EXPECTED_EXCEPTIONS);
    } finally {
      disconnectFromDS();
      SimpleMemoryAllocatorImpl.freeOffHeapMemory();
      cache.set(null);
      system.set(null);
      isSmallerVM.set(false);
    }
  }
  
  protected String getOffHeapMemorySize() {
    return "2m";
  }
  
  protected String getSmallerOffHeapMemorySize() {
    return "1m";
  }
  
  protected RegionShortcut getRegionShortcut() {
    return RegionShortcut.REPLICATE;
  }
  
  protected String getRegionName() {
    return "region1";
  }
  
  @Override
  public Properties getDistributedSystemProperties() {
    Properties props = new Properties();
    props.put(DistributionConfig.STATISTIC_SAMPLING_ENABLED_NAME, "true");
    if (isSmallerVM.get()) {
      props.setProperty(DistributionConfig.OFF_HEAP_MEMORY_SIZE_NAME, getSmallerOffHeapMemorySize());
    } else {
      props.setProperty(DistributionConfig.OFF_HEAP_MEMORY_SIZE_NAME, getOffHeapMemorySize());
    }
    return props;
  }
  
  public void testSimpleOutOfOffHeapMemoryMemberDisconnects() {
    final Cache cache = getCache();
    final DistributedSystem system = getSystem();
    Region<Object, Object> region = cache.createRegionFactory(getRegionShortcut()).setEnableOffHeapMemory(true).create(getRegionName());
    OutOfOffHeapMemoryException ooohme;
    try {
      for (int i = 0; true; i++) {
        region.put("key-"+i, new Byte[1024]);
      }
    } catch (OutOfOffHeapMemoryException e) {
      ooohme = e;
    }
    assertNotNull(ooohme);

    final WaitCriterion waitForDisconnect = new WaitCriterion() {
      public boolean done() {
        return cache.isClosed() && !system.isConnected();
      }
      public String description() {
        return "Waiting for disconnect to complete";
      }
    };
    waitForCriterion(waitForDisconnect, 10*1000, 100, true);
    
    assertTrue(cache.isClosed());
    assertFalse(system.isConnected());

//    final WaitCriterion waitForNull = new WaitCriterion() {
//      public boolean done() {
//        return GemFireCacheImpl.getInstance() == null;
//      }
//      public String description() {
//        return "Waiting for GemFireCacheImpl to null its instance";
//      }
//    };
//    waitForCriterion(waitForNull, 10*1000, 100, true);
//    
//    assertNull(GemFireCacheImpl.getInstance());
    try {
      CacheFactory.getAnyInstance();
      fail("CacheFactory.getAnyInstance() should throw CacheClosedException");
    } catch (CacheClosedException e) {
      // pass
    } catch (DistributedSystemDisconnectedException e) {
      boolean passed = false;
      for (Throwable cause = e.getCause(); cause != null;) {
        if (cause instanceof CacheClosedException) {
          passed = true;
          break;
        }
      }
      if (!passed) {
        throw e;
      }
    }
    //assertNull(InternalDistributedSystem.getAnyInstance());
    assertFalse(InternalDistributedSystem.getAnyInstance().isConnected());
  }
  
  @SuppressWarnings("rawtypes")
  protected static Region createRegion(Cache cache, RegionShortcut shortcut, String name) {
    return cache.createRegionFactory(shortcut).setEnableOffHeapMemory(true).create(name);
  }
  
  protected static final AtomicReference<Cache> cache = new AtomicReference<Cache>();
  protected static final AtomicReference<DistributedSystem> system = new AtomicReference<DistributedSystem>();
  protected static final AtomicBoolean isSmallerVM = new AtomicBoolean();
  
  public void testOtherMembersSeeOutOfOffHeapMemoryMemberDisconnects() {
    assertEquals(4, Host.getHost(0).getVMCount());

    final String name = getRegionName();
    final RegionShortcut shortcut = getRegionShortcut();
    final int smallerVM = 1;
    final int count = Host.getHost(0).getVMCount();
    
    Host.getHost(0).getVM(smallerVM).invoke(new SerializableRunnable() {
      public void run() {
        OutOfOffHeapMemoryDUnitTest.isSmallerVM.set(true);
      }
    });
    
    for (int i = 0; i < Host.getHost(0).getVMCount(); i++) {
      Host.getHost(0).getVM(i).invoke(new SerializableRunnable() {
        public void run() {
          OutOfOffHeapMemoryDUnitTest.cache.set(getCache());
          OutOfOffHeapMemoryDUnitTest.system.set(getSystem());
          Region<Object, Object> region = OutOfOffHeapMemoryDUnitTest.cache.get().
              createRegionFactory(shortcut).setEnableOffHeapMemory(true).create(name);
          assertNotNull(region);
        }
      });
    }
    
    for (int i = 0; i < count; i++) {
      Host.getHost(0).getVM(i).invoke(new SerializableRunnable() {
        public void run() {
          assertFalse(OutOfOffHeapMemoryDUnitTest.cache.get().isClosed());
          assertTrue(OutOfOffHeapMemoryDUnitTest.system.get().isConnected());

          int countMembersPlusLocator = count+2; // add one for locator
          int countOtherMembers = count-1; // subtract one for self
          assertEquals(countMembersPlusLocator, ((InternalDistributedSystem)OutOfOffHeapMemoryDUnitTest.
              system.get()).getDistributionManager().getDistributionManagerIds().size());
          assertEquals(countOtherMembers, ((DistributedRegion)OutOfOffHeapMemoryDUnitTest.
              cache.get().getRegion(name)).getDistributionAdvisor().getNumProfiles());
        }
      });
    }
    
    Host.getHost(0).getVM(smallerVM).invoke(new SerializableRunnable() {
      public void run() {
        //OutOfOffHeapMemoryDUnitTest.cache.get().getLogger().info(ADD_EXPECTED_EXCEPTIONS);
        getLogWriter().info(ADD_EXPECTED_EXCEPTIONS);
      }
    });
    
    // perform puts in bigger member until smaller member goes OOOHME
    Host.getHost(0).getVM(0).invoke(new SerializableRunnable() {
      public void run() {
        final long TIME_LIMIT = 30 * 1000;
        final StopWatch stopWatch = new StopWatch(true);
        
        int i = 0;
        int countOtherMembers = count-1;
        Region<Object, Object> region = OutOfOffHeapMemoryDUnitTest.cache.get().getRegion(name);
        for (i = 0; countOtherMembers > count-2; i++) {
          region.put("key-"+i, new byte[1024]);
          countOtherMembers = ((DistributedRegion)OutOfOffHeapMemoryDUnitTest.
              cache.get().getRegion(name)).getDistributionAdvisor().getNumProfiles();
          assertTrue("puts failed to push member out of off-heap memory within time limit", 
              stopWatch.elapsedTimeMillis() < TIME_LIMIT);
        }
        assertEquals("Member did not depart from OutOfOffHeapMemory", count-2, countOtherMembers);
      }
    });
    
    // verify that member with OOOHME closed
    Host.getHost(0).getVM(smallerVM).invoke(new SerializableRunnable() {
      public void run() {
//        assertTrue(OutOfOffHeapMemoryDUnitTest.cache.get().isClosed());
//        assertFalse(OutOfOffHeapMemoryDUnitTest.system.get().isConnected());
        final WaitCriterion waitForDisconnect = new WaitCriterion() {
          public boolean done() {
            return OutOfOffHeapMemoryDUnitTest.cache.get().isClosed() && !OutOfOffHeapMemoryDUnitTest.system.get().isConnected();
          }
          public String description() {
            return "Waiting for disconnect to complete";
          }
        };
        waitForCriterion(waitForDisconnect, 10*1000, 100, true);
      }
    });
    
    // verify that closed member is closed according to all members
    for (int i = 0; i < count; i++) {
      if (i == smallerVM) {
        continue;
      }
      Host.getHost(0).getVM(i).invoke(new SerializableRunnable() {
        public void run() {
          final int countMembersPlusLocator = count+2-1; // add one for locator (minus one for OOOHME member)
          final int countOtherMembers = count-1-1; // subtract one for self (minus one for OOOHME member)
          
          assertEquals(countMembersPlusLocator, ((InternalDistributedSystem)OutOfOffHeapMemoryDUnitTest.
              system.get()).getDistributionManager().getDistributionManagerIds().size());
//          final WaitCriterion waitForView = new WaitCriterion() {
//            public boolean done() {
//              return ((InternalDistributedSystem)OutOfOffHeapMemoryDUnitTest.system.get()).getDistributionManager().getDistributionManagerIds().size() == countMembersPlusLocator;
//            }
//            public String description() {
//              return "Waiting for OOOHM to depart view";
//            }
//          };
//          waitForCriterion(waitForView, 10*1000, 100, true);
          
          assertEquals(countOtherMembers, ((DistributedRegion)OutOfOffHeapMemoryDUnitTest.
              cache.get().getRegion(name)).getDistributionAdvisor().getNumProfiles());
//          final WaitCriterion waitForProfiles = new WaitCriterion() {
//            public boolean done() {
//              return ((DistributedRegion)OutOfOffHeapMemoryDUnitTest.cache.get().getRegion(name)).getDistributionAdvisor().getNumProfiles() == countOtherMembers;
//            }
//            public String description() {
//              return "Waiting for OOOHM to depart profiles";
//            }
//          };
//          waitForCriterion(waitForProfiles, 10*1000, 100, true);
        }
      });
    }
    
    
  }

}
