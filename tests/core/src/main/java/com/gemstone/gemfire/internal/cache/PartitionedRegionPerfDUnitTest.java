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


import java.io.Serializable;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.MirrorType;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.internal.LogWriterImpl;

import dunit.Host;
import dunit.VM;

/**
 * @author tapshank, Created on Jan 19, 2006
 *  
 */
public class PartitionedRegionPerfDUnitTest extends PartitionedRegionDUnitTestCase
{

  //////constructor //////////
	public PartitionedRegionPerfDUnitTest(String name) {
		super(name);
	}//end of constructor
	
	public final static String PR_NAME_PREFIX = "p_";
	public final static String DR_NAME_PREFIX = "d_";
	public final static Scope scope = Scope.DISTRIBUTED_ACK;
	public final static MirrorType mirror = MirrorType.KEYS_VALUES;
	public final static long RUN_LENGTH = 1 * 60 * 1000;
	public final static Integer SMALL_OBJECT_SIZE = new Integer(100);
	public final static Integer MEDIUM_OBJECT_SIZE = new Integer(1000);
	public final static Integer LARGE_OBJECT_SIZE = new Integer(10000);
	
	public static int TARGET_TOTAL_BYTES = 1000000;
	
	public VM vm0;
	public VM vm1;
	public VM vm2;
	public VM vm3;
	

  /** This one is just a single VM test -- preferably also add a multi VM one */
  public void testCompareIteratorGetsWithCHM_42553() {
    // disable stats; latter especially really screws numbers due to time calls
    String statsDisabled = System.setProperty("gemfire.statsDisabled", "true");
    String timeStats = System.setProperty("gemfire.enable-time-statistics", "false");

    final Map<Object, Object> map1 = new ConcurrentHashMap<Object, Object>();
    Properties dsProps = getAllDistributedSystemProperties(new Properties());
    dsProps.put("mcast-port", "0");
    final LocalRegion lmap2 = (LocalRegion)new CacheFactory(dsProps).create()
        .createRegionFactory(RegionShortcut.LOCAL).create("map2");
    final AbstractRegionMap amap2 = (AbstractRegionMap)lmap2.entries;

    final int numOps = 10000;
    // puts in map1, map2
    for (int key = 1; key <= numOps; ++key) {
      map1.put("key-" + key, "value-" + key);
    }
    for (int key = 1; key <= numOps; ++key) {
      lmap2.put("key-" + key, "value-" + key);
    }

    long currentTime;
    int count;
    // warmup map1 for gets
    for (int index = 0; index < 2000; ++index) {
      for (Object key : map1.keySet()) {
        map1.get(key);
      }
    }
    // timed run for map1 gets
    currentTime = System.currentTimeMillis();
    count = 0;
    for (int index = 0; index < numOps; ++index) {
      for (Object key : map1.keySet()) {
        map1.get(key);
        ++count;
      }
    }
    long duration1 = System.currentTimeMillis() - currentTime;
    getLogWriter().info(
        "ConcurrentHashMap gets time " + duration1 + " count=" + count);

    map1.clear();

    System.gc();

    // warmup map2 for gets
    for (int index = 0; index < 2000; ++index) {
      for (Object key : lmap2.keySet()) {
        // lmap2.get(key);
        amap2.getEntry(key);
      }
    }
    // timed run for map2 gets
    currentTime = System.currentTimeMillis();
    count = 0;
    for (int index = 0; index < numOps; ++index) {
      for (Object key : lmap2.keySet()) {
        // lmap2.get(key);
        amap2.getEntry(key);
        ++count;
      }
    }
    long duration2 = System.currentTimeMillis() - currentTime;
    getLogWriter().info(
        "AbstractRegionMap#getEntry time " + duration2 + " count=" + count);

    // log an error if the difference is more than 50%
    final long diff = duration2 - duration1;
    if ((diff * 2) <= duration1) {
      getLogWriter().info("Yay! diff between CHM iteration and LocalRegion "
          + "iteration (" + ((diff * 100) / duration1) + "%) is less than 50%");
    }
    else {
      getLogWriter().error("Difference between CHM iteration and LocalRegion "
          + "iteration (" + ((diff * 100) / duration1) + "%) is more than 50%");
    }

    // restore the system props
    if (statsDisabled != null) {
      System.setProperty("gemfire.statsDisabled", statsDisabled);
    }
    else {
      System.clearProperty("gemfire.statsDisabled");
    }
    if (timeStats != null) {
      System.setProperty("gemfire.enable-time-statistics", timeStats);
    }
    else {
      System.clearProperty("gemfire.enable-time-statistics");
    }
  }

	//////////test methods ////////////////
//	public void testSmallPuts() throws Exception {
//		final String pRegionName = PR_NAME_PREFIX + getUniqueName();
//		final String dRegionName = DR_NAME_PREFIX + getUniqueName();
//		doCreateRegions(pRegionName, dRegionName);
//
//		Object[] args = {pRegionName, SMALL_OBJECT_SIZE};
//		PerfResults res1 = (PerfResults) vm0.invoke(this, "doPuts", args);
//		res1.mode = "Partitioned Regions";
//				
//		args[0] = dRegionName;
//		PerfResults res2 = (PerfResults) vm0.invoke(this, "doPuts", args);
//		res2.mode = "Mirrored Keys-Values";
//		getLogWriter().info(displayComparedResults(res1, res2));
//	}
	
// 	public void testMediumPuts() throws Exception {
// 		final String pRegionName = PR_NAME_PREFIX + getUniqueName();
// 		final String dRegionName = DR_NAME_PREFIX + getUniqueName();
// 		doCreateRegions(pRegionName, dRegionName);
//
// 		Object[] args = {pRegionName, MEDIUM_OBJECT_SIZE};
// 		PerfResults res1 = (PerfResults) vm0.invoke(this, "doPuts", args);
// 		res1.mode = "Partitioned Regions";
//				
// 		args[0] = dRegionName;
// 		PerfResults res2 = (PerfResults) vm0.invoke(this, "doPuts", args);
// 		res2.mode = "Mirrored Keys-Values";
// 		getLogWriter().info(displayComparedResults(res1, res2));
// 	}
	
//	public void testLargePuts() throws Exception {
//		final String pRegionName = PR_NAME_PREFIX + getUniqueName();
//		final String dRegionName = DR_NAME_PREFIX + getUniqueName();
//		doCreateRegions(pRegionName, dRegionName);
//
//		Object[] args = {pRegionName, LARGE_OBJECT_SIZE};
//		PerfResults res1 = (PerfResults) vm0.invoke(this, "doPuts", args);
//		res1.mode = "Partitioned Regions";
//				
//		args[0] = dRegionName;
//		PerfResults res2 = (PerfResults) vm0.invoke(this, "doPuts", args);
//		res2.mode = "Mirrored Keys-Values";
//		getLogWriter().info(displayComparedResults(res1, res2));
//	}
//
//	public void testSmallGets() throws Exception {
//		final String pRegionName = PR_NAME_PREFIX + getUniqueName();
//		final String dRegionName = DR_NAME_PREFIX + getUniqueName();
//		doCreateRegions(pRegionName, dRegionName);
//
//		Object[] args = {pRegionName, SMALL_OBJECT_SIZE};
//		PerfResults res1 = (PerfResults) vm0.invoke(this, "doGets", args);
//		res1.mode = "Partitioned Regions";
//				
//		args[0] = dRegionName;
//		PerfResults res2 = (PerfResults) vm0.invoke(this, "doGets", args);
//		res2.mode = "Mirrored Keys-Values";
//		getLogWriter().info(displayComparedResults(res1, res2));
//	}
	
// 	public void testMediumGets() throws Exception {
// 		final String pRegionName = PR_NAME_PREFIX + getUniqueName();
// 		final String dRegionName = DR_NAME_PREFIX + getUniqueName();
// 		doCreateRegions(pRegionName, dRegionName);
//
// 		Object[] args = {pRegionName, MEDIUM_OBJECT_SIZE};
// 		PerfResults res1 = (PerfResults) vm0.invoke(this, "doGets", args);
// 		res1.mode = "Partitioned Regions";
//				
// 		args[0] = dRegionName;
// 		PerfResults res2 = (PerfResults) vm0.invoke(this, "doGets", args);
// 		res2.mode = "Mirrored Keys-Values";
// 		getLogWriter().info(displayComparedResults(res1, res2));
// 	}
	
//	public void testLargeGets() throws Exception {
//		final String pRegionName = PR_NAME_PREFIX + getUniqueName();
//		final String dRegionName = DR_NAME_PREFIX + getUniqueName();
//		doCreateRegions(pRegionName, dRegionName);
//
//		Object[] args = {pRegionName, LARGE_OBJECT_SIZE};
//		PerfResults res1 = (PerfResults) vm0.invoke(this, "doGets", args);
//		res1.mode = "Partitioned Regions";
//				
//		args[0] = dRegionName;
//		PerfResults res2 = (PerfResults) vm0.invoke(this, "doGets", args);
//		res2.mode = "Mirrored Keys-Values";
//		getLogWriter().info(displayComparedResults(res1, res2));
//	}

// 	public void testMisses() throws Exception {
// 		final String pRegionName = PR_NAME_PREFIX + getUniqueName();
// 		final String dRegionName = DR_NAME_PREFIX + getUniqueName();
// 		doCreateRegions(pRegionName, dRegionName);
		
// 		Object[] args = {pRegionName};
// 		PerfResults res1 = (PerfResults) vm0.invoke(this, "doMisses", args);
// 		res1.mode = "Partitioned Regions";
				
// 		args[0] = dRegionName;
// 		PerfResults res2 = (PerfResults) vm0.invoke(this, "doMisses", args);
// 		res2.mode = "Mirrored Keys-Values";
// 		getLogWriter().info(displayComparedResults(res1, res2));
// 	}
	


	
	
	
	//helper methods
	public void doCreateRegions(final String pRegionName, final String dRegionName) throws Exception {
		Host host = Host.getHost(0);
		
		vm0 = host.getVM(0);
		vm1 = host.getVM(1);
		vm2 = host.getVM(2);
		vm3 = host.getVM(3);
		
		
		CacheSerializableRunnable createRegions = new CacheSerializableRunnable("createRegions") {
			@Override
			public void run2() throws CacheException
			{
				Cache cache = getCache();
				cache.createRegion(pRegionName,
                                    createRegionAttributesForPR(0, 50));
				cache.createRegion(dRegionName,
                                    createRegionAttributesForDR(scope, mirror));
			}
		};
		
		CacheSerializableRunnable createAccessor = new CacheSerializableRunnable("createAccessor") {
			@Override
			public void run2() throws CacheException
			{
				Cache cache = getCache();
				cache.createRegion(pRegionName,
                                    createRegionAttributesForPR(0, 0));
				cache.createRegion(dRegionName,
                                    createRegionAttributesForDR(scope, MirrorType.NONE));
				
			}
		};
		
		// Create PRs on all 4 VMs
		vm0.invoke(createAccessor);
		vm1.invoke(createRegions);
		vm2.invoke(createRegions);
		vm3.invoke(createRegions);

	}
		
	
	
	public PerfResults doPuts(final String regionName, final Integer objectSize_) throws Exception {
		int objectSize = objectSize_.intValue();
		Cache cache = getCache();
		Region r = cache.getRegion(Region.SEPARATOR + regionName);
                
                final int origLogLevel = setLogLevel(cache.getLogger(), LogWriterImpl.WARNING_LEVEL);                           
		long startTime = System.currentTimeMillis();
		long endTime = startTime + RUN_LENGTH;
		int putCount = 0;
		int maxKey = TARGET_TOTAL_BYTES / objectSize;
		byte[] value = new byte[objectSize];
		
		while(System.currentTimeMillis() < endTime) {
                  for (int i=0; i<100; i++) {
                    r.put(String.valueOf(putCount % maxKey), value);
                    putCount++;
                  }
		}
		endTime = System.currentTimeMillis();
                setLogLevel(cache.getLogger(), origLogLevel);
		
		PerfResults results = new PerfResults();
		results.putCount = putCount;
		results.objectSize = objectSize;
		results.operationType = "PUT";
		results.time = endTime - startTime;
		
		return results;
	}

	
	

	public PerfResults doGets(final String regionName, final Integer objectSize_) throws Exception {
		int objectSize = objectSize_.intValue();
		Cache cache = getCache();

		Region r = cache.getRegion(Region.SEPARATOR + regionName);
		
		boolean partitioned = false;
		if(r instanceof PartitionedRegion) {
			partitioned = true;
		}
		
                final int origLogLevel = setLogLevel(cache.getLogger(), LogWriterImpl.WARNING_LEVEL);                
		long startTime = System.currentTimeMillis();
		long endTime = startTime + RUN_LENGTH;
		int getCount = 0;
		int missCount = 0;
		int maxKey = TARGET_TOTAL_BYTES / objectSize;
		Object value = new byte[objectSize];;
		
		//load up the cache
		for(int i=0; i<maxKey; i++) {
			r.put(String.valueOf(i), value);
		}
		
		//localInvalidate doesn't work on PRs, and it's Mitch's fault.
		if(!partitioned) {
			for(int i=0; i<maxKey; i++) {
				r.localInvalidate(String.valueOf(i));
			}
			
		}
		
		value = null;
		while(System.currentTimeMillis() < endTime) {
                  for (int i=0; i<500; i++) {
			value = r.get(String.valueOf(getCount % maxKey));
			getCount++;
			if(value == null) {
				missCount++;
			}
			if(!partitioned) {
				r.localInvalidate(String.valueOf(getCount % maxKey));
			}
                  }
		}
		endTime = System.currentTimeMillis();
                setLogLevel(cache.getLogger(), origLogLevel);
                

		PerfResults results = new PerfResults();
		results.getCount = getCount;
		results.missCount = missCount;
		results.objectSize = objectSize;
		results.operationType = "GET";
		results.time = endTime - startTime;
		
		return results;
		
	}

	
	public PerfResults doMisses(final String regionName) throws Exception {
		Cache cache = getCache();
		Region r = cache.getRegion(Region.SEPARATOR + regionName);
		
                final int origLogLevel = setLogLevel(cache.getLogger(), LogWriterImpl.WARNING_LEVEL);              
		long startTime = System.currentTimeMillis();
		long endTime = startTime + RUN_LENGTH;
		int missCount = 0;
		Object value = null;
		
		while(System.currentTimeMillis() < endTime) {
                  for (int i=0; i<500; i++) {
			value = r.get(String.valueOf(missCount));
			missCount++;
			if(value != null) {
				fail("Got back non-null value for a key that should have no value.");
			}
                  }
		}
		endTime = System.currentTimeMillis();
                setLogLevel(cache.getLogger(), origLogLevel);
		
		PerfResults results = new PerfResults();
		results.missCount = missCount;
		results.objectSize = 0;
		results.operationType = "MISS";
		results.time = endTime - startTime;
		
		return results;
		

	}

	
	
	public String displayComparedResults(PerfResults res1, PerfResults res2) {
		
		StringBuffer sb = new StringBuffer();
		sb.append("\n######################################\n");
		sb.append("### ");
		sb.append(res1.operationType);
		sb.append(" ##############################\n");
		sb.append("######################################\n");
		sb.append("Object Size = ");
		sb.append(res1.objectSize);
		sb.append(" bytes\n");
		sb.append("Test Length = ");
		sb.append(res1.time);
		sb.append(" ms\n");

		sb.append("## ");
		sb.append(res1.mode);
		sb.append(" ##");
		if(res1.putCount != 0) {
			sb.append("\nPut count = ");
			sb.append(res1.putCount);
			sb.append("\nPuts/sec = ");
			sb.append(1000.0 * res1.putCount/res1.time);
		}
		if(res1.getCount != 0) {
			sb.append("\nGet count = ");
			sb.append(res1.getCount);
			sb.append("\nGets/sec = ");
			sb.append(1000.0 * res1.getCount/res1.time);
		}
		if(res1.missCount != 0) {
			sb.append("\nMiss count = ");
			sb.append(res1.missCount);
			sb.append("\nMisses/sec = ");
			sb.append(1000.0 * res1.missCount/res1.time);
		}
		sb.append("\n");

		sb.append("## ");
		sb.append(res2.mode);
		sb.append(" ##");
		if(res2.putCount != 0) {
			sb.append("\nPut count = ");
			sb.append(res2.putCount);
			sb.append("\nPuts/sec = ");
			sb.append(1000.0 * res2.putCount/res2.time);
		}
		if(res2.getCount != 0) {
			sb.append("\nGet count = ");
			sb.append(res2.getCount);
			sb.append("\nGets/sec = ");
			sb.append(1000.0 * res2.getCount/res2.time);
		}
		if(res2.missCount != 0) {
			sb.append("\nMiss count = ");
			sb.append(res2.missCount);
			sb.append("\nMisses/sec = ");
			sb.append(1000.0 * res2.missCount/res2.time);
		}
		sb.append("\n");
		
		sb.append("######################################\n");
		sb.append("######################################\n");
		sb.append("######################################\n");
		
		return sb.toString();
	}
	
	
	
	
	
	public String displayResults(String operationName, int objectSize, long totalTime, int operationCount) {
		
		StringBuffer sb = new StringBuffer();
		sb.append("######################################\n");
		sb.append("### ");
		sb.append(operationName);
		sb.append(" #############################\n");
		sb.append("######################################\n");
		sb.append("Object Size = ");
		sb.append(objectSize);
		sb.append(" bytes\n");
		sb.append("Test Length = ");
		sb.append(totalTime);
		sb.append(" ms\n");
		sb.append("Operation count = ");
		sb.append(operationCount);
		sb.append("\n");
		sb.append("Operations/sec = ");
		if(totalTime == 0) {
			sb.append("unbounded");
		} else {
			sb.append(1000.0 * operationCount/totalTime);
		}
		sb.append("\n");
		sb.append("######################################\n");
		sb.append("######################################\n");
		sb.append("######################################\n");
		
		return sb.toString();
		
	}
	
	protected RegionAttributes createRegionAttributesForPR(int redundancy,
            int localMaxMem) {
		AttributesFactory attr = new AttributesFactory();
		PartitionAttributesFactory paf = new PartitionAttributesFactory();
		PartitionAttributes prAttr = paf.setRedundantCopies(redundancy)
		.setLocalMaxMemory(localMaxMem).create();
		attr.setPartitionAttributes(prAttr);
		return attr.create();
	}
	
	protected RegionAttributes createRegionAttributesForDR(Scope scope,
            MirrorType mirror) {
		AttributesFactory attr = new AttributesFactory();
		attr.setScope(scope);
		attr.setMirrorType(mirror);
		return attr.create();
	}

	
	class PerfResults implements Serializable {
		String operationType;
		String mode;
		long time;
		long putCount = 0;
		long getCount = 0;
		long missCount = 0;
		long objectSize;
	}
	
	
}
