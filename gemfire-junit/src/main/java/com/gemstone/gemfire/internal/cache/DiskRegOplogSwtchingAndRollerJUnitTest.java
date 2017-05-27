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
package com.gemstone.gemfire.internal.cache;

import java.io.File;
import java.util.*;
import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.DiskAccessException;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.internal.cache.lru.LRUStatistics;

/**
 * Miscellaneous disk tests
 * 
 * 
 *  
 */
public class DiskRegOplogSwtchingAndRollerJUnitTest extends
    DiskRegionTestingBase
{

  DiskRegionProperties diskProps = new DiskRegionProperties();

  protected boolean encounteredException = false;

  protected volatile boolean hasBeenNotified = false;

  protected static File[] dirs1 = null;

  protected static int[] diskDirSize1 = null;

  public DiskRegOplogSwtchingAndRollerJUnitTest(String name) {
    super(name);

  }

  protected void setUp() throws Exception
  {
    super.setUp();
  }

  protected void tearDown() throws Exception
  {
    super.tearDown();
  }

  /**
   * tests non occurence of DiskAccessException
   *  
   */
  public void testSyncPersistRegionDAExp()
  {
    File testingDirectory1 = new File("testingDirectory1");
    testingDirectory1.mkdir();
    testingDirectory1.deleteOnExit();
    File file1 = new File("testingDirectory1/" + "testSyncPersistRegionDAExp"
        + "1");
    file1.mkdir();
    file1.deleteOnExit();
    dirs1 = new File[1];
    dirs1[0] = file1;
    diskDirSize1 = new int[1];
    diskDirSize1[0] = 2048;

    diskProps.setDiskDirsAndSizes(dirs1, diskDirSize1);
    diskProps.setPersistBackup(true);
    diskProps.setRolling(true);
    diskProps.setMaxOplogSize(100000000);
    diskProps.setRegionName("region_SyncPersistRegionDAExp");
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    DiskStore ds = cache.findDiskStore(((LocalRegion)region).getDiskStoreName());
//    int[] diskSizes1 = ((LocalRegion)region).getDiskDirSizes();
    assertTrue(ds != null);
    int[] diskSizes1 = null;
    diskSizes1 = ds.getDiskDirSizes();
    assertEquals(1, diskDirSize1.length);
    assertTrue("diskSizes != 2048 ", diskSizes1[0] == 2048);

    this.diskAccessExpHelpermethod(region);

    //region.close(); // closes disk file which will flush all buffers
    closeDown();
    
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;

  }// end of testSyncPersistRegionDAExp

  public void testAsyncPersistRegionDAExp()
  {
    File testingDirectory1 = new File("testingDirectory1");
    testingDirectory1.mkdir();
    testingDirectory1.deleteOnExit();
    File file1 = new File("testingDirectory1/" + "testAsyncPersistRegionDAExp"
        + "1");
    file1.mkdir();
    file1.deleteOnExit();
    dirs1 = new File[1];
    dirs1[0] = file1;
    diskDirSize1 = new int[1];
    diskDirSize1[0] = 2048;

    diskProps.setDiskDirsAndSizes(dirs1, diskDirSize1);
    diskProps.setPersistBackup(true);
    diskProps.setRolling(true);
    diskProps.setMaxOplogSize(100000000);
    diskProps.setRegionName("region_AsyncPersistRegionDAExp");
    region = DiskRegionHelperFactory
        .getAsyncPersistOnlyRegion(cache, diskProps);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    DiskStore ds = cache.findDiskStore(((LocalRegion)region).getDiskStoreName());
    //  int[] diskSizes1 = ((LocalRegion)region).getDiskDirSizes();
    assertTrue(ds != null);
    int[] diskSizes1 = null;
    diskSizes1 = ds.getDiskDirSizes();
    assertEquals(1, diskDirSize1.length);
    assertTrue("diskSizes != 2048 ", diskSizes1[0] == 2048);

    this.diskAccessExpHelpermethod(region);

   // region.close(); // closes disk file which will flush all buffers
    closeDown();
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
  }// end of testAsyncPersistRegionDAExp

  private void diskAccessExpHelpermethod(final Region region)
  {
    final byte[] value = new byte[990];
    Arrays.fill(value, (byte)77);
    try {
      for (int i = 0; i < 2; i++) {
        region.put("" + i, value);
      }
    }
    catch (DiskAccessException e) {
      logWriter.error("exception not expected", e);
      fail("FAILED::" + e.toString());
    }
    try {
      region.put("" + 2, value);
    }
    catch (DiskAccessException e) {
      fail("FAILED::DiskAccessException is NOT expected here !!");
    }
  }

  protected Object forWaitNotify = new Object();

  protected boolean gotNotification = false;

  /**
   * DiskRegionRollingJUnitTest :
   * 
   *  
   */
  public void testSyncRollingHappening()
  {
    try {
      DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
      diskRegionProperties.setDiskDirs(dirs);
      diskRegionProperties.setMaxOplogSize(512);
      diskRegionProperties.setRolling(true);
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
      Region region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,
          diskRegionProperties, Scope.LOCAL);

      CacheObserverHolder.setInstance(new CacheObserverAdapter() {
        public void beforeGoingToCompact()
        {
          synchronized (forWaitNotify) {
            forWaitNotify.notify();
            gotNotification = true;
          }
        }
      });

      //This will end up 50% garbage
      //1 live create
      //2 live tombstones
      //2 garbage creates and 1 garbage modify
      region.put("k1", "v1");
      region.put("k2", "v2");
      region.put("k2", "v3");
      region.put("k3", "v3");
      region.remove("k1");
      region.remove("k2");
      

      region.forceRolling();

      synchronized (forWaitNotify) {
        if (!gotNotification) {
          forWaitNotify.wait(10000);
        }
      }

      if (!gotNotification) {
        fail("Expected rolling to have happened but did not happen");
      }

    }
    catch (Exception e) {
      fail(" tests failed due to " + e);
    }
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
  }

  public void testSyncRollingNotHappening()
  {
    try {
      DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
      diskRegionProperties.setDiskDirs(dirs);
      diskRegionProperties.setMaxOplogSize(512);
      diskRegionProperties.setRolling(false);
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
      Region region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,
          diskRegionProperties, Scope.LOCAL);

      CacheObserverHolder.setInstance(new CacheObserverAdapter() {
        public void beforeGoingToCompact()
        {
          synchronized (forWaitNotify) {
            forWaitNotify.notify();
            gotNotification = true;
          }
        }
      });

      region.forceRolling();

      synchronized (forWaitNotify) {
        if (!gotNotification) {
          forWaitNotify.wait(3000);
        }
      }

      if (gotNotification) {
        fail("Expected rolling not to have happened but it did happen");
      }

    }
    catch (Exception e) {
      fail(" tests failed due to " + e);
    }

    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
  }

  public void testAsyncRollingHappening()
  {
    try {
      DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
      diskRegionProperties.setDiskDirs(dirs);
      diskRegionProperties.setMaxOplogSize(512);
      diskRegionProperties.setRolling(true);
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
      Region region = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache,
          diskRegionProperties);

      CacheObserverHolder.setInstance(new CacheObserverAdapter() {
        public void beforeGoingToCompact()
        {
          synchronized (forWaitNotify) {
            forWaitNotify.notify();
            gotNotification = true;
          }
        }
      });

      
      //This will end up 50% garbage
      //1 live create
      //2 live tombstones
      //2 garbage creates and 1 garbage modify
      region.put("k1", "v1");
      region.put("k2", "v2");
      region.put("k3", "v3");
      ((LocalRegion)region).forceFlush(); // so that the next modify doesn't conflate
      region.put("k2", "v3");
      ((LocalRegion)region).forceFlush(); // so that the next 2 removes don't conflate
      region.remove("k1");
      region.remove("k2");
      ((LocalRegion)region).forceFlush();

      region.forceRolling();

      synchronized (forWaitNotify) {
        if (!gotNotification) {
          forWaitNotify.wait(10000);
        }
      }

      if (!gotNotification) {
        fail("Expected rolling to have happened but did not happen");
      }

    }
    catch (Exception e) {
      fail(" tests failed due to " + e);
    }

    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
  }

  public void testAsyncRollingNotHappening()
  {
    try {
      DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
      diskRegionProperties.setDiskDirs(dirs);
      diskRegionProperties.setMaxOplogSize(512);
      diskRegionProperties.setRolling(false);
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
      Region region = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache,
          diskRegionProperties);

      CacheObserverHolder.setInstance(new CacheObserverAdapter() {
        public void beforeGoingToCompact()
        {
          synchronized (forWaitNotify) {
            forWaitNotify.notify();
            gotNotification = true;
          }
        }
      });

      region.forceRolling();

      synchronized (forWaitNotify) {
        if (!gotNotification) {
          forWaitNotify.wait(3000);
        }
      }

      if (gotNotification) {
        fail("Expected rolling not to have happened but it did happen");
      }

    }
    catch (Exception e) {
      fail(" tests failed due to " + e);
    }
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
  }

  protected Object getValOnDsk = null;

  /**
   * DiskRegOplog1OverridingOplog2JUnitTest: Disk Region test : oplog1 flush
   * overriding oplog2 flush
   * 
   * This test will hold the flush of oplog1 and flush oplog2 before it. After
   * that oplog1 is allowed to flush. A get of an entry which was first put in
   * oplog1 and then in oplog2 should result in the get being done from oplog2.
   *  
   */
  public void testOplog1FlushOverridingOplog2Flush()
  {
    hasBeenNotified = false;
    diskProps.setDiskDirs(dirs);
    diskProps.setPersistBackup(true);
    diskProps.setRolling(false);
    diskProps.setBytesThreshold(100000000);
    diskProps.setTimeInterval(4000);
    diskProps.setRegionName("region_Oplog1FlushOverridingOplog2Flush");
    region = DiskRegionHelperFactory
        .getAsyncPersistOnlyRegion(cache, diskProps);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;

    CacheObserverHolder.setInstance(new CacheObserverAdapter() {
      boolean callOnce = false;

      public void goingToFlush()
      {
        synchronized (this) {

          if (!callOnce) {
            try {
              region.put("1", "2");
            }
            catch (Exception e) {
              logWriter.error("exception not expected", e);
              failureCause = "FAILED::" + e.toString();
              testFailed = true;
              fail("FAILED::" + e.toString());
            }

            Thread th = new Thread(new DoesFlush(region));
            th.setName("TestingThread");
            th.start();
            Thread.yield();
          }
          callOnce = true;
        }

      }
    });

    try {
      region.put("1", "1");
    }
    catch (Exception e) {
      logWriter.error("exception not expected", e);
      fail("FAILED::" + e.toString());
    }

    region.forceRolling();

    synchronized (region) {
      if (!hasBeenNotified) {
        try {
          region.wait();
        }
        catch (InterruptedException e) {
          logWriter.error("exception not expected", e);
          fail("Failed:" + e.toString());
        }
      }
    }

    ((LocalRegion)region).getDiskRegion().flushForTesting();

    try {

      getValOnDsk = ((LocalRegion)region).getValueOnDisk("1");
    }
    catch (EntryNotFoundException e1) {
      logWriter.error("exception not expected", e1);
      fail("Failed:" + e1.toString());
    }

    assertTrue(getValOnDsk.equals("2"));
    assertFalse(failureCause, testFailed);
   // region.close(); // closes disk file which will flush all buffers
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;

  }// end of testOplog1FlushOverridingOplog2Flush

  class DoesFlush implements Runnable
  {

    private Region region;

    public DoesFlush(Region region) {
      this.region = region;
    }

    public void run()
    {
      ((LocalRegion)region).getDiskRegion().flushForTesting();
      synchronized (region) {
        region.notify();
        hasBeenNotified = true;
      }
    }

  }

  /**
   * OplogRoller should correctly add those entries created in an Oplog , but by
   * the time rolling has started , the entry exists in the current oplog
   */
  public void testEntryExistsinCurrentOplog()
  {
    hasBeenNotified = false;
    diskProps.setDiskDirs(dirs);
    diskProps.setPersistBackup(true);
    diskProps.setRolling(true);
    diskProps.setCompactionThreshold(100);
    diskProps.setRegionName("testEntryExistsinCurrentOplog");
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;

    CacheObserverHolder.setInstance(new CacheObserverAdapter() {
      boolean callOnce = false;

      public void beforeGoingToCompact()
      {
        synchronized (this) {
          if (!callOnce) {
            try {
              for (int i = 0; i < 100; i++) {
                region.put(new Integer(i), "newVal" + i);
              }
            }
            catch (Exception e) {
              logWriter.error("exception not expected", e);
              failureCause = "FAILED::" + e.toString();
              testFailed = true;
              fail("FAILED::" + e.toString());
            }
          }
          callOnce = true;
        }

      }

      public void afterHavingCompacted()
      {
        synchronized (region) {
          region.notify();
          hasBeenNotified = true;
        }
      }
    });

    try {
      for (int i = 0; i < 100; i++) {
        region.put(new Integer(i), new Integer(i));
      }
    }
    catch (Exception e) {
      logWriter.error("exception not expected", e);
      fail("FAILED::" + e.toString());
    }

    for (int i = 0; i < 100; i++) {
      assertTrue(region.get(new Integer(i)).equals(new Integer(i)));
    }
    region.forceRolling();

    synchronized (region) {
      if (!hasBeenNotified) {
        try {
          region.wait(10000);
          assertTrue(hasBeenNotified);
        }
        catch (InterruptedException e) {
          logWriter.error("exception not expected", e);
          fail("interrupted");
        }
      }
    }

    for (int i = 0; i < 100; i++) {
      assertEquals("newVal" + i, region.get(new Integer(i)));
    }

    region.close();

    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);

    for (int i = 0; i < 100; i++) {
      assertTrue(region.containsKey(new Integer(i)));
      assertEquals("newVal" + i, region.get(new Integer(i)));
    }
    assertFalse(failureCause, testFailed);
   // region.close();
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
  }// end of testEntryExistsinCurrentOplog

  /**
   * Entries deleted in current Oplog are recorded correctly during the rolling
   * of that oplog
   *  
   */
  public void testEntryDeletedinCurrentOplog()
  {
    hasBeenNotified = false;
    diskProps.setDiskDirs(dirs);
    diskProps.setPersistBackup(true);
    diskProps.setRolling(true);
    diskProps.setCompactionThreshold(100);
    diskProps.setMaxOplogSize(100000);
    diskProps.setRegionName("testEntryDeletedinCurrentOplog1");
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;

    CacheObserverHolder.setInstance(new CacheObserverAdapter() {
      boolean callOnce = false;

      public void beforeGoingToCompact()
      {
        synchronized (this) {

          if (!callOnce) {
            try {

              region.destroy(new Integer(10));
              region.destroy(new Integer(20));
              region.destroy(new Integer(30));
              region.destroy(new Integer(40));
              region.destroy(new Integer(50));

            }
            catch (Exception e) {
              logWriter.error("exception not expected", e);
              failureCause = "FAILED::" + e.toString();
              testFailed = true;
              fail("FAILED::" + e.toString());
            }
          }
          callOnce = true;
        }

      }

      public void afterHavingCompacted()
      {
        synchronized (region) {
          region.notify();
          hasBeenNotified = true;
        }
      }

    });

    try {
      for (int i = 0; i < 100; i++) {
        region.put(new Integer(i), new Integer(i));
      }
    }
    catch (Exception e) {
      logWriter.error("exception not expected", e);
      fail("FAILED::" + e.toString());
    }

    for (int i = 0; i < 100; i++) {
      assertTrue(region.get(new Integer(i)).equals(new Integer(i)));
    }
    region.forceRolling();

    synchronized (region) {
      if (!hasBeenNotified) {
        try {
          region.wait(10000);
          assertTrue(hasBeenNotified);
        }
        catch (InterruptedException e) {
          logWriter.error("exception not expected", e);
          fail("interrupted");
        }
      }
    }

    DiskRegion dr = ((LocalRegion)region).getDiskRegion();
    Oplog oplog = dr.testHook_getChild();

//     Set set = oplog.getEntriesDeletedInThisOplog();

//     assertTrue(set.size() == 5);

    region.close();

    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);

    for (int i = 0; i < 100; i++) {
      if (i == 10 || i == 20 || i == 30 || i == 40 || i == 50) {
        assertTrue(" failed on key " + i, !region.containsKey(new Integer(i)));
      }
      else {
        assertTrue(region.get(new Integer(i)).equals(new Integer(i)));
      }
    }
    assertFalse(failureCause, testFailed);
   // region.close();
  }// end of testEntryDeletedinCurrentOplog

  // deadcoding since overflow only no longer compacts/rolls
//   /**
//    * Oplog Roller not rolling the entries which have OplogKeyID as negative ,
//    * for over flow mode,even if oplogID attribute matches
//    *  
//    */
//   public void testEvictedEntryRolling1() {
//     hasBeenNotified = false;
//     diskProps.setDiskDirs(dirs);
//     diskProps.setPersistBackup(false);
//     diskProps.setRolling(true);
//     diskProps.setCompactionThreshold(100);
//     diskProps.setOverFlowCapacity(50);
//     diskProps.setRegionName("testEntriesInMemoryNotBeingRolled");
//     region = DiskRegionHelperFactory
//         .getSyncOverFlowOnlyRegion(cache, diskProps);
//     LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;

//     CacheObserver old = CacheObserverHolder
//         .setInstance(new CacheObserverAdapter() {

//           public void afterHavingCompacted()
//           {
//             synchronized (region) {
//               region.notify();
//               hasBeenNotified = true;
//             }
//           }
//         });

//     try {
//       for (int i = 1; i < 101; i++) {
//         region.put(new Integer(i), new Integer(i));
//       }
//     }
//     catch (Exception e) {
//       logWriter.error("exception not expected", e);
//       fail("FAILED::" + e.toString());
//     }
//     region.forceRolling();

//     synchronized (region) {
//       if (!hasBeenNotified) {
//         try {
//           region.wait(10000);
//           assertTrue(hasBeenNotified);
//         }
//         catch (InterruptedException e) {
//           logWriter.error("exception not expected", e);
//           fail("interrupted");
//         }
//       }
//     }

//     for (int i = 1; i < 51; i++) {
//       try {
//         Object val = ((LocalRegion)region).getValueOnDisk(new Integer(i));
//         assertNotNull(val);
//       }
//       catch (Exception e1) {
//         e1.printStackTrace();
//         fail("Entries from 1 to 51 should have been on the disk. However, exception occured while getting the entry from disk.Exception= "
//             + e1);
//       }
//     }
//     boolean exceptionOccured[] = new boolean[50];
//     for (int i = 51; i < 101; i++) {

//       try {
//         Object val = ((LocalRegion)region).getValueOnDisk(new Integer(i));
//         exceptionOccured[i - 51] = val == null;
//       }
//       catch (EntryNotFoundException e) {
//         logWriter.error("exception not expected", e);
//         fail("FAILED::" + e.toString());
//       }

//     }
//     for (int j = 0; j < 50; ++j) {
//       assertTrue(
//           "All the entries betwen 51 to 100 shoudl not have  been present on disk",
//           exceptionOccured[j]);
//     }

//     //region.close();
//     CacheObserverHolder.setInstance(old);
//     LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
//   } // end of testEntriesInMemoryNotBeingRolled

  // deadcoding since overflow only no longer compacts/rolls
//   /**
//    * Oplog Roller not rolling the entries which have OplogKeyID as negative ,
//    * for over flow mode,even if oplogID attribute matches
//    *  
//    */
//   public void testEvictedEntryRolling2()
//   {
//     hasBeenNotified = false;
//     diskProps.setDiskDirs(dirs);
//     diskProps.setPersistBackup(false);
//     diskProps.setRolling(true);
//     diskProps.setCompactionThreshold(100);
//     diskProps.setOverFlowCapacity(50);
//     diskProps.setRegionName("testEntriesInMemoryNotBeingRolled");
//     region = DiskRegionHelperFactory
//         .getSyncOverFlowOnlyRegion(cache, diskProps);
//     LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;

//     CacheObserver old = CacheObserverHolder
//         .setInstance(new CacheObserverAdapter() {

//           public void afterHavingCompacted()
//           {
//             synchronized (region) {
//               region.notify();
//               hasBeenNotified = true;
//             }
//           }
//         });

//     try {
//       for (int i = 1; i < 101; i++) {
//         region.put(new Integer(i), new Integer(i));
//       }
//       for (int i = 1; i < 51; i++) {
//         region.get(new Integer(i));
//       }
//     }
//     catch (Exception e) {
//       logWriter.error("exception not expected", e);
//       fail("FAILED::" + e.toString());
//     }
//     region.forceRolling();

//     synchronized (region) {
//       if (!hasBeenNotified) {
//         try {
//           region.wait(10000);
//           assertTrue(hasBeenNotified);
//         }
//         catch (InterruptedException e) {
//           logWriter.error("exception not expected", e);
//           fail("interrupted");
//         }
//       }
//     }

//     for (int i = 1; i < 101; i++) {
//       try {
//         Object val = ((LocalRegion)region).getValueOnDisk(new Integer(i));
//         assertNotNull(val);
//       }
//       catch (Exception e1) {
//         e1.printStackTrace();
//         fail("Entries from 1 to 51 should have been on the disk. However, exception occured while getting the entry from disk.Exception= "
//             + e1);
//       }
//     }
//     //region.close();
//     CacheObserverHolder.setInstance(old);
//     LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
//   } // end of testEntriesInMemoryNotBeingRolled

//   public void testPotentialBug_Probably35046()
//   {
//     diskProps.setDiskDirs(dirs);
//     diskProps.setPersistBackup(false);
//     //diskProps.setRolling(true);
//     diskProps.setOverFlowCapacity(1);
//     diskProps.setSynchronous(false);
//     diskProps.setBytesThreshold(1000000000L);
//     diskProps.setTimeInterval(1000000000L);

//     diskProps.setRegionName("testug35046");
//     region = DiskRegionHelperFactory.getAsyncOverFlowOnlyRegion(cache,
//         diskProps);
//     byte[] val = new byte[100];

//     hasBeenNotified = false;
//     LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;

//     CacheObserver old = CacheObserverHolder
//         .setInstance(new CacheObserverAdapter() {

//           public void goingToFlush()
//           {

//             try {
//               encounteredException = region.get("1") == null;
//             }
//             catch (Exception e) {
//               encounteredException = true;
//               logWriter.error("Exception occured", e);

//             }
//             synchronized (region) {
//               region.notify();
//               hasBeenNotified = true;
//             }
//           }

//         });
//     region.put("1", val);
//     region.put("2", val);
//     region.forceRolling();
//     // @todo: why do we expect goingToFlush to be called? With the byteThreshold
//     // and the timeInterval set so high I don't understand why this test would pass.
//     synchronized (region) {
//       if (!hasBeenNotified) {
//         try {
//           region.wait(50000);
//           assertTrue(hasBeenNotified);
//         }
//         catch (Exception e) {
//           logWriter.error("Exception occured", e);
//           encounteredException = true;
//         }
//       }
//     }
//     assertTrue("Test failed as it encounterd exception", !encounteredException);
//     CacheObserverHolder.setInstance(old);
//     LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
//   }

  // deadcoding since overflow only no longer compacts/rolls
//   /**
//    * DiskRegOplogRollerRetrvingEntryEvctdJUnitTest: Disk Region Test: 1] Oplog
//    * roller retrieving an entry evicted from memory , from the disk and rolling
//    * it correctly. 2]Oplog roller retrieving an entry which is evicted and then
//    * invalidated rolling it correclty. 3]Oplog roller retrieving an entry with
//    * value as byte array evicted to disk and rolling it correctly.
//    */
//   public void testDiskRegOlgRlrRetrvingEntryEvctd()
//   {
//     Object valInVM0 = null;

//     Object valInVM1 = null;

//     Object valInVM2 = null;

//     Object valOnDsk0 = null;

// //    Object valOnDsk1 = null;

//     Object valOnDsk2 = null;

//     LogWriter log = null;
//     log = ds.getLogWriter();

//     diskProps.setDiskDirs(dirs);
//     diskProps.setOverflow(true);
//     diskProps.setCompactionThreshold(100);
//     diskProps.setOverFlowCapacity(1);
//     diskProps.setRolling(true);
//     diskProps.setMaxOplogSize(1000000000);
//     region = DiskRegionHelperFactory
//         .getSyncOverFlowOnlyRegion(cache, diskProps);

//     LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;

//     LRUStatistics lruStats = getLRUStats(region);
//     // Put in data until we start evicting
//     int total;
//     for (total = 0; lruStats.getEvictions() <= 0; ++total) {
//       log.info("DEBUG: total " + total + ", evictions "
//           + lruStats.getEvictions());
//       region.put(new Long(total), new String("firstVal"));
//     }

//     assertEquals("The no. of entries evicted is not " + "equal to 1", 1,
//         lruStats.getEvictions());

//     region.put(new Long(1), new String("secondVal"));
//     //Put byte []
//     final byte[] value = new byte[1024];
//     Arrays.fill(value, (byte)77);
//     region.put(new Long(2), value);

//     region.put(new Long(3), new String("fourthVal"));

//     assertEquals("The no. of entries evicted are not " + "equal to 3", 3,
//         lruStats.getEvictions());
//     //invalidate the entry with key new Long(1)
//     try {
//       region.invalidate(new Long(1));
//     }
//     catch (Exception e) {
//       fail("Failed while doing invalidate Operation" + e.toString());
//     }

//     try {
//       valInVM0 = ((LocalRegion)region).getValueInVM(new Long(0));
//       valInVM1 = ((LocalRegion)region).getValueInVM(new Long(1));
//       valInVM2 = ((LocalRegion)region).getValueInVM(new Long(2));
//     }
//     catch (Exception e) {
//       fail("Failed: while getting value in VM" + e.toString());
//     }

//     assertTrue("The valInVM0 is not null ", valInVM0 == null);
//     assertTrue("The valInVM1 is not INVALID ", valInVM1 == Token.INVALID);
//     assertTrue("The valInVM2 is not null ", valInVM2 == null);

//     CacheObserverHolder.setInstance(new CacheObserverAdapter() {
//       public void beforeGoingToCompact()
//       {
//         try {
//           Thread.sleep(1000);
//         }
//         catch (InterruptedException ignore) {
//           fail("interrupted");
//         }
//       }

//       public void afterHavingCompacted()
//       {
//         synchronized (region) {
//           region.notify();
//           hasBeenNotified = true;
//         }
//       }
//     });
//     region.forceRolling();
//     synchronized (region) {
//       if (!hasBeenNotified) {
//         try {
//           region.wait(10000);
//           assertTrue(hasBeenNotified);
//         }
//         catch (InterruptedException e) {
//           logWriter.error("exception not expected", e);
//           fail("interrupted");
//         }
//       }
//     }
//     //Get value on disk

//     if (((DiskEntry)(((LocalRegion)region).basicGetEntry(new Long(1))))
//         .getDiskId().getOplogId() == -1) {
//       fail(" Entry was not supposed to be roll since it has been invalidated but it has been rolled !");
//     }

//     try {
//       valOnDsk0 = ((LocalRegion)region).getValueOnDisk(new Long(0));
//       valOnDsk2 = ((LocalRegion)region).getValueOnDisk(new Long(2));
//     }
//     catch (Exception e) {
//       logWriter.error("exception not expected", e);
//       fail("Failed while geting value on disk: " + e.toString());
//     }

//     assertTrue("Failed to retrieve the value[valOnDsk0] " + "on disk",
//         (valOnDsk0.equals(new String("firstVal"))));

//     assertTrue("valOnDsk2 is not an isnstance of byte[]",
//         valOnDsk2 instanceof byte[]);
//     // verify the getValueOnDisk operation performed on byte []
//     this.getByteArrVal(new Long(2), region);

//     //region.close(); // closes disk file which will flush all buffers
//   }

  /**
   * 
   * @param region
   *          get LRU statistics
   */
  protected LRUStatistics getLRUStats(Region region)
  {
    return ((LocalRegion)region).getEvictionController().getLRUHelper()
        .getStats();
  }

  /**
   * to validate the get operation performed on a byte array.
   * 
   * @param key
   * @param region
   * @return
   */

  private boolean getByteArrVal(Long key, Region region)
  {
    Object val = null;
    byte[] val2 = new byte[1024];
    Arrays.fill(val2, (byte)77);
    try {
      //val = region.get(key);
      val = ((LocalRegion)region).getValueOnDisk(key);
    }
    catch (Exception ex) {
      ex.printStackTrace();
      fail("Failed to get the value on disk");
    }
    //verify that the retrieved byte[] equals to the value put initially.
    boolean result = false;
    byte[] x = null;
    x = (byte[])val;
    Arrays.fill(x, (byte)77);
    for (int i = 0; i < x.length; i++) {
      result = (x[i] == val2[i]);
    }
    if (!result) {
      fail("The val of byte[] put at 100th key obtained from disk is not euqal to the value put initially");
    }
    return result;
  }

  /**
   * DiskRegOplogRollerWaitingForAsyncWriterJUnitTest: Disk Region test : Oplog
   * Roller should wait for asynch writer to terminate if asynch flush is going
   * on , before deleting the oplog
   */
  protected boolean afterWritingBytes = false;

  public void testOplogRollerWaitingForAsyncWriter()
  {

    hasBeenNotified = false;
    diskProps.setDiskDirs(dirs);
    diskProps.setPersistBackup(true);
    diskProps.setRolling(false);
    diskProps.setBytesThreshold(100000000);
    diskProps.setTimeInterval(4000);
    diskProps.setRegionName("region_OplogRollerWaitingForAsyncWriter");
    region = DiskRegionHelperFactory
        .getAsyncPersistOnlyRegion(cache, diskProps);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;

    CacheObserverHolder.setInstance(new CacheObserverAdapter() {
      boolean callOnce = false;

      public void afterWritingBytes()
      {

        synchronized (this) {

          if (!callOnce) {
            try {
              region.put("1", "2");
            }
            catch (Exception e) {
              logWriter.error("exception not expected", e);
              fail("FAILED::" + e.toString());
              failureCause = "FAILED::" + e.toString();
              testFailed = true;
            }

            Thread th = new Thread(new DoesFlush1(region));
            th.setName("TestingThread");
            th.start();
            Thread.yield();

          }
          callOnce = true;
          afterWritingBytes = true;
        }

      }

      public void afterHavingCompacted()
      {
        if (!afterWritingBytes) {
          fail("Roller didnt wait for Async writer to terminate!");
          failureCause = "Roller didnt wait for Async writer to terminate!";
          testFailed = true;
        }
      }

    });

    try {
      region.put("1", "1");
    }
    catch (Exception e) {
      logWriter.error("exception not expected", e);
      fail("FAILED::" + e.toString());
    }

    region.forceRolling();

    synchronized (region) {
      if (!hasBeenNotified) {
        try {
          region.wait(10000);
          assertTrue(hasBeenNotified);
        }
        catch (InterruptedException e) {
          logWriter.error("exception not expected", e);
          fail("interrupted");
        }
      }
    }

    ((LocalRegion)region).getDiskRegion().flushForTesting();

    try {
      assertTrue((((LocalRegion)region).getValueOnDisk("1")).equals("2"));
    }
    catch (EntryNotFoundException e1) {
      e1.printStackTrace();
      fail("Failed:" + e1.toString());
    }
    assertFalse(failureCause, testFailed);
    //region.close(); // closes disk file which will flush all buffers

  }// end of testOplogRollerWaitingForAsyncWriter

  class DoesFlush1 implements Runnable
  {

    private Region region;

    public DoesFlush1(Region region) {
      this.region = region;
    }

    public void run()
    {
      ((LocalRegion)region).getDiskRegion().flushForTesting();
      synchronized (region) {
        region.notify();
        hasBeenNotified = true;
      }
    }

  }

  /**
   * Task 125: Ensuring that retrieval of evicted entry data for rolling
   * purposes is correct & does not cause any eviction sort of things
   * 
   * @throws EntryNotFoundException
   *  
   */
  public void testGetEvictedEntry() throws EntryNotFoundException
  {
    hasBeenNotified = false;
    diskProps.setDiskDirs(dirs);
    diskProps.setPersistBackup(false);
    diskProps.setRolling(false);
    diskProps.setCompactionThreshold(100);
    diskProps.setAllowForceCompaction(true);
    diskProps.setMaxOplogSize(1000000000);
    diskProps.setOverflow(true);
    diskProps.setOverFlowCapacity(1);
    diskProps.setRegionName("region_testGetEvictedEntry");
    region = DiskRegionHelperFactory
        .getSyncOverFlowOnlyRegion(cache, diskProps);
    DiskRegionStats stats = ((LocalRegion)region).getDiskRegion().getStats();
//     LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
//     CacheObserverHolder.setInstance(new CacheObserverAdapter() {
//       boolean callOnce = false;

//       public void afterHavingCompacted()
//       {
//         synchronized (this) {

//           if (!callOnce) {
//             synchronized (region) {
//               region.notify();
//               hasBeenNotified = true;
//             }
//           }
//         }
//         callOnce = true;
//         afterWritingBytes = true;
//       }

//     });

    try {
      for (int i = 0; i < 100; i++) {
        region.put("key" + i, "val" + i);
      }
    }
    catch (Exception e) {
      logWriter.error("exception not expected", e);
      fail("FAILED::" + e.toString());
    }

    //  Check region stats
    assertTrue("before ForcRolling getNumKeys != 100", region.size() == 100);
    assertTrue("before ForcRolling getNumEntriesInVM != 1", stats
        .getNumEntriesInVM() == 1);
    assertTrue("before ForcRolling getNumOverflowOnDisk != 99", stats
        .getNumOverflowOnDisk() == 99);

//     region.forceRolling();
//     region.forceCompaction();

//     synchronized (region) {
//       if (!hasBeenNotified) {
//         try {
//           region.wait(10000);
//           assertTrue(hasBeenNotified);
//         }
//         catch (InterruptedException e) {
//           logWriter.error("exception not expected", e);
//           fail("interrupted");
//         }
//       }
//     }

//     //  Check region stats
//     assertTrue("after ForcRolling getNumKeys != 100", region.size() == 100);
//     assertTrue("after ForcRolling getNumEntriesInVM != 1", stats
//         .getNumEntriesInVM() == 1);
//     assertTrue("after ForcRolling getNumOverflowOnDisk != 99", stats
//         .getNumOverflowOnDisk() == 99);

    for (int i = 0; i < 99; i++) {
      assertTrue(((LocalRegion)region).getValueOnDisk("key" + i).equals(
          "val" + i));
    }

    ((LocalRegion)region).getDiskRegion().flushForTesting();

    // Values in VM for entries 0 to 98 must be null
    for (int i = 0; i < 99; i++) {
      assertTrue(((LocalRegion)region).getValueInVM("key" + i) == null);
    }
    assertTrue(((LocalRegion)region).getValueInVM("key99").equals("val99"));
    // Values on disk for entries 0 to 98 must have their values
    for (int i = 0; i < 99; i++) {
      assertTrue(((LocalRegion)region).getValueOnDisk("key" + i).equals(
          "val" + i));
    }
    assertTrue(((LocalRegion)region).getValueOnDisk("key99") == null);

    //region.close(); // closes disk file which will flush all buffers
  }// end of testGetEvictedEntry

  /**
   * DiskRegNoDiskAccessExceptionTest: tests that while rolling is set to true
   * DiskAccessException doesn't occur even when amount of put data exceeds the
   * max dir sizes.
   */

  public void testDiskFullExcep()
  {
    boolean exceptionOccured = false;
    int[] diskDirSize1 = new int[4];
    diskDirSize1[0] = 1048576;
    diskDirSize1[1] = 1048576;
    diskDirSize1[2] = 1048576;
    diskDirSize1[3] = 1048576;

    diskProps.setDiskDirsAndSizes(dirs, diskDirSize1);
    diskProps.setPersistBackup(true);
    diskProps.setRolling(true);
    diskProps.setMaxOplogSize(1000000000);
    diskProps.setBytesThreshold(1000000);
    diskProps.setTimeInterval(1500000);
    region = DiskRegionHelperFactory
        .getAsyncPersistOnlyRegion(cache, diskProps);
    DiskStore ds = cache.findDiskStore(((LocalRegion)region).getDiskStoreName());
    //  int[] diskSizes1 = ((LocalRegion)region).getDiskDirSizes();
    assertTrue(ds != null);
    int[] diskSizes1 = null;
    diskSizes1 = ds.getDiskDirSizes();

    assertTrue("diskSizes != 1048576 ", diskSizes1[0] == 1048576);
    assertTrue("diskSizes != 1048576 ", diskSizes1[1] == 1048576);
    assertTrue("diskSizes != 1048576 ", diskSizes1[2] == 1048576);
    assertTrue("diskSizes != 1048576 ", diskSizes1[3] == 1048576);

    final byte[] value = new byte[1024];
    Arrays.fill(value, (byte)77);
    try {
      for (int i = 0; i < 7000; i++) {
        region.put("" + i, value);
      }
    }
    catch (DiskAccessException e) {
      logWriter.error("exception not expected", e);
      exceptionOccured = true;
    }
    if (exceptionOccured) {
      fail("FAILED::DiskAccessException is Not expected here !!");
    }

    //region.close(); // closes disk file which will flush all buffers

  }//end of testDiskFullExcep

}// end of test class
