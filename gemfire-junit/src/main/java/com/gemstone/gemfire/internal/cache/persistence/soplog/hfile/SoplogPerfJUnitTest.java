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
//package com.gemstone.gemfire.internal.cache.persistence.soplog.hfile;
//
//import java.io.File;
//import java.io.FilenameFilter;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.Random;
//import java.util.SortedMap;
//import java.util.Timer;
//import java.util.TreeMap;
//import java.util.concurrent.Callable;
//import java.util.concurrent.ConcurrentLinkedQueue;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.util.concurrent.Future;
//import java.util.concurrent.atomic.AtomicInteger;
//import java.util.concurrent.atomic.AtomicLong;
//
//import junit.framework.TestCase;
//
//import com.gemstone.gemfire.cache.Cache;
//import com.gemstone.gemfire.cache.CacheFactory;
//import com.gemstone.gemfire.internal.cache.persistence.soplog.Compactor.CompactionTracker;
//import com.gemstone.gemfire.internal.cache.persistence.soplog.Compactor.Fileset;
//import com.gemstone.gemfire.internal.cache.persistence.soplog.RecoverableSortedOplogSet;
//import com.gemstone.gemfire.internal.cache.persistence.soplog.SizeTieredCompactor;
//import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedOplogFactory;
//import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedOplogSet;
//import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedOplogSet.FlushHandler;
//import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedOplogSetImpl;
//import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedOplogStatistics;
//import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedReaderTestCase;
//import com.gemstone.gemfire.internal.util.Bytes;
//
//public class SoplogPerfJUnitTest extends TestCase {
//  private static final int threads = 32;
//  private static final int count = 100000000;
//  private static final int size = 1024;
//  private static final int flushSize = 50 * 1024 * 1024;
//  private static final float maxUnflushed = 0.25f;
//  
//  private Cache cache;
//  private SortedOplogSet sos;
//  private SortedOplogStatistics stats;
//  private Timer flush;
//  
//  private AtomicLong maxKey;
//  
//  private enum Operation { READ, WRITE, CREATE }
//  
//  public void testPerf() throws Exception {
//    final ConcurrentLinkedQueue<Operation> ops = new ConcurrentLinkedQueue<Operation>();
//    for (int i = 0; i < threads/2; i++) {
//      ops.offer(Operation.READ);
//      ops.offer(Operation.WRITE);
//    }
//    
//    // initial values
//    doWrite(1000, size);
//    
//    ExecutorService exec = Executors.newFixedThreadPool(threads);
//    ArrayList<Callable<Boolean>> tasks = new ArrayList<Callable<Boolean>>();
//    for (int i = 0; i < threads; i++) {
//      tasks.add(new Callable<Boolean>() {
//        @Override
//        public Boolean call() {
//          try {
//            Operation op = ops.remove();
//            switch (op) {
//            case READ: 
//              doRead(count, size);
//              break;
//            case WRITE:
//              doWrite(count, size);
//              break;
//            case CREATE:
//              doCreate(count, size);
//              break;
//            }
//            return true;
//            
//          } catch (Exception e) {
//            return false;
//          }
//        }
//      });
//    }
//    
//    for (Future<Boolean> f : exec.invokeAll(tasks)) {
//      if (!f.get()) {
//        fail();
//      }
//    }
//    
//    doFlush();
//  }
//  
//  private void doCreate(int count, int size) throws IOException {
//    for (int i = 0; i < count; i++) {
//      byte[] key = new byte[8];
//      Bytes.putLong(maxKey.getAndIncrement(), key, 0);
//      if (sos.read(key) == null) {
//        byte[] val = new byte[size];
//        Arrays.fill(val, (byte) i);
//        
//        sos.put(key, val);
//      }
//    }
//  }
//
//  private void doRead(int count, int size) throws IOException {
//    Random r = new Random();
//    for (int i = 0; i < count; i++) {
//      byte[] key = new byte[8];
//      Bytes.putLong(Math.round(r.nextDouble() * maxKey.get()), key, 0);
//
//      sos.read(key);
//    }
//  }
//  
//  private void doWrite(int count, int size) throws Exception {
//    int i = 0;
//    while (i++ < count) {
//      byte[] key = new byte[8];
//      Bytes.putLong(maxKey.getAndIncrement(), key, 0);
//      
//      byte[] val = new byte[size];
//      Arrays.fill(val, (byte) i);
//      
//      sos.put(key, val);
//    }
//  }
//
//  private void doFlush() throws IOException {
//    sos.flush(null, new FlushHandler() {
//      @Override
//      public void complete() {
//      }
//
//      @Override
//      public void error(Throwable t) {
//      }
//    });
//  }
//  
//  public void setUp() throws Exception {
//    CacheFactory cf = new CacheFactory()
//      .set("mcast-port", "0")
//      .set("log-level", getGemFireLogLevel())
//      .set("log-file", "gemfire.log")
//      .set("statistic-archive-file","statArchive.gfs");
//    cache = cf.create();
//  
//    stats = new SortedOplogStatistics(cache.getDistributedSystem(), "Soplog", "perf");
//    SortedOplogFactory factory = new HFileSortedOplogFactory("perf", stats);
//    
//    ExecutorService exec = Executors.newFixedThreadPool(4);
//    SizeTieredCompactor comp = new SizeTieredCompactor(
//        factory, new MockFileset(), new MockTracker(), exec, 4, 10);
//    
//    maxKey = new AtomicLong(0);
//    sos = new RecoverableSortedOplogSet(new SortedOplogSetImpl(factory, exec, comp), flushSize, maxUnflushed);
//    flush = new Timer();
//  }
//
//  public void tearDown() throws Exception {
//    flush.cancel();
//    sos.close();
//    cache.close();
//    for (File f : SortedReaderTestCase.getSoplogsToDelete()) {
//      f.delete();
//    }
//    
//    for (File f : getAppendLogsToDelete()) {
//      f.delete();
//    }
//  }
//  
//  private static File[] getAppendLogsToDelete() {
//    return new File(".").listFiles(new FilenameFilter() {
//      @Override
//      public boolean accept(File dir, String name) {
//        return name.endsWith("aolog");
//      }
//    });
//  }
//  
//  private static class MockFileset implements Fileset<Integer> {
//    private final AtomicInteger id = new AtomicInteger(0);
//    
//    @Override
//    public SortedMap<Integer, ? extends Iterable<File>> recover() {
//      return new TreeMap<Integer, Iterable<File>>();
//    }
//
//    @Override
//    public File getNextFilename() {
//      return new File("perf-" + id.getAndIncrement() + ".soplog");
//    }
//  }
//  
//  private static class MockTracker implements CompactionTracker<Integer> {
//    @Override
//    public void fileAdded(File f, Integer attach) {
//    }
//
//    @Override
//    public void fileRemoved(File f, Integer attach) {
//    }
//
//    @Override
//    public void fileDeleted(File f) {
//    }
//  }
//}
