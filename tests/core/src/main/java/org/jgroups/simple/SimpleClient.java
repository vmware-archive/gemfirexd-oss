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

package org.jgroups.simple;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Vector;
import java.util.logging.Handler;

import org.jgroups.simple.cache.*;

import com.gemstone.gemfire.*;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.org.jgroups.util.StringId;

import hydra.*;
import hydra.blackboard.*;

/**
 * 
 * A sample client that does some stuff with distributed cache.
 * 
 */

public class SimpleClient
{
  static boolean useHydra = true;
  
  static CacheAttributes cacheAttr = null;

  static CacheAccess ca = null;

  static int duration;
  static int sltime;

  static String ORDERS = "MyVec";

  static final String REGION_NAME = System.getProperty("regionName",
          "OrderTest");

  static SharedCounters counters;
  
  static private final int TIMEOUT_MS = 2000;
  
  static private final String jgNames[] = new String[] {
    "jg1", "jg2",    "jg3",    "jg4",    "jg5",    "jg6",
    "jg7",     "jg8",     "jg9",    "jg10",    "jg11"  };
  static private final Long jgNamesKeys[] = new Long[] {
    SimpleParms.jg1, SimpleParms.jg2,    SimpleParms.jg3,   
    SimpleParms.jg4,    SimpleParms.jg5,    SimpleParms.jg6,
    SimpleParms.jg7,     SimpleParms.jg8,     
    SimpleParms.jg9,    SimpleParms.jg10,    SimpleParms.jg11  };
  
//  static private final String pkg = "com.gemstone.org.javagroups.protocols.";
  
//  static private String INITIAL_HOSTS = "[" + "]";
  static private final String MULTICAST_HOST ="224.0.0.0";
  static private final String MULTICAST_PORT = "16385";
  static private final String MULTICAST_TTL = "32";
  
  static private final String jgDefaults[] = new String[] {
//    "UDP(ip_mcast=false;use_packet_handler=false)",
//    "TCPGOSSIP(initial_hosts=" + INITIAL_HOSTS + ";down_thread=false;" +
//        "num_initial_members=2;up_thread=false;gossip_refresh_rate=57123)",
      "UDP(ip_mcast=true;mcast_port=" + MULTICAST_PORT + ";" +
          "use_packet_handler=false;mcast_addr=" + MULTICAST_HOST + ";" +
          "ip_ttl=" + MULTICAST_TTL + ")",
      "PING(timeout=1000;num_initial_members=2)",
    "FD_SOCK(start_port=0)",
    "VERIFY_SUSPECT(timeout=1500;down_thread=false;up_thread=false)",
    "pbcast.NAKACK(max_xmit_size=4096;down_thread=false;gc_lag=50;up_thread=false;" +
        "retransmit_timeout=300,600,1200,2400,4800)",
    "UNICAST(timeout=5000;down_thread=false)",
    "pbcast.STABLE(desired_avg_gossip=20000;down_thread=false;up_thread=false)",
    "FRAG(frag_size=4096;down_thread=false;up_thread=false)",
    "pbcast.GMS(print_local_addr=false;join_timeout=5000;" +
        "join_retry_timeout=2000;shun=false)",
    "pbcast.STATE_TRANSFER(down_thread=false;up_thread=false)"
  };
  

  static class StandaloneLogger implements LogWriter {

    public boolean severeEnabled()
    {
      return true;
    }

    public void severe(String msg, Throwable ex)
    {
      System.out.println("[severe] " + msg + " " + ex.toString());
      ex.printStackTrace();
    }

    public void severe(String msg)
    {
      System.out.println("[severe] " + msg);
    }

    public void severe(Throwable ex)
    {
      System.out.println("[severe] " + ex.toString());
      ex.printStackTrace();
    }

    public boolean errorEnabled()
    {
      return true;
    }

    public void error(String msg, Throwable ex)
    {
      System.out.println("[error] " + msg + " " + ex.toString());
      ex.printStackTrace();
    }

    public void error(String msg)
    {
      System.out.println("[error] " + msg);
    }

    public void error(Throwable ex)
    {
      System.out.println("[error] " + ex.toString());
      ex.printStackTrace();
    }

    public boolean warningEnabled()
    {
      return true;
    }

    public void warning(String msg, Throwable ex)
    {
      System.out.println("[warning] " + msg + " " + ex.toString());
      ex.printStackTrace();
    }

    public void warning(String msg)
    {
      System.out.println("[warning] " + msg);
    }

    public void warning(Throwable ex)
    {
      System.out.println("[warning] " + ex.toString());
      ex.printStackTrace();
    }

    public boolean infoEnabled()
    {
      return true;
    }

    public void info(String msg, Throwable ex)
    {
      System.out.println("[info] " + msg + " " + ex.toString());
      ex.printStackTrace();
    }

    public void info(String msg)
    {
      System.out.println("[info] " + msg);
    }

    public void info(Throwable ex)
    {
      System.out.println("[info] " + ex.toString());
      ex.printStackTrace();
    }

    public boolean configEnabled()
    {
      return true;
    }

    public void config(String msg, Throwable ex)
    {
      System.out.println("[config] " + msg + " " + ex.toString());
      ex.printStackTrace();
    }

    public void config(String msg)
    {
      System.out.println("[config] " + msg);
    }

    public void config(Throwable ex)
    {
      System.out.println("[config] " + ex.toString());
      ex.printStackTrace();
    }

    public boolean fineEnabled()
    {
      return true;
    }

    public void fine(String msg, Throwable ex)
    {
      System.out.println("[fine] " + msg + " " + ex.toString());
      ex.printStackTrace();
    }

    public void fine(String msg)
    {
      System.out.println("[fine] " + msg);
    }

    public void fine(Throwable ex)
    {
      System.out.println("[fine] " + ex.toString());
      ex.printStackTrace();
    }

    public boolean finerEnabled()
    {
      return true;
    }

    public void finer(String msg, Throwable ex)
    {
      System.out.println("[finer] " + msg + " " + ex.toString());
      ex.printStackTrace();
    }

    public void finer(String msg)
    {
      System.out.println("[finer] " + msg);
    }

    public void finer(Throwable ex)
    {
      System.out.println("[finer] " + ex.toString());
      ex.printStackTrace();
    }

    public void entering(String sourceClass, String sourceMethod)
    {
      // TODO Auto-generated method stub
    }

    public void exiting(String sourceClass, String sourceMethod)
    {
      // TODO Auto-generated method stub
    }

    public void throwing(String sourceClass, String sourceMethod, Throwable thrown)
    {
      // TODO Auto-generated method stub
    }

    public boolean finestEnabled()
    {
      return true;
    }

    public void finest(String msg, Throwable ex)
    {
      System.out.println("[finest] " + msg + " " + ex.toString());
      ex.printStackTrace();
    }

    public void finest(String msg)
    {
      System.out.println("[finest] " + msg);
    }

    public void finest(Throwable ex)
    {
      System.out.println("[finest] " + ex.toString());
      ex.printStackTrace();
    }

    public Handler getHandler()
    {
      // TODO Auto-generated method stub
      return null;
    }

    public void config(StringId msgId, Object param, Throwable ex) {
      // TODO Auto-generated method stub
      
    }

    public void config(StringId msgId, Object param) {
      // TODO Auto-generated method stub
      
    }

    public void config(StringId msgId, int i) {
      // TODO Auto-generated method stub
      
    }
    
    public void config(StringId msgId, Object[] params, Throwable ex) {
      // TODO Auto-generated method stub
      
    }

    public void config(StringId msgId, Object[] params) {
      // TODO Auto-generated method stub
      
    }

    public void config(StringId msgId, Throwable ex) {
      // TODO Auto-generated method stub
      
    }

    public void config(StringId msgId) {
      // TODO Auto-generated method stub
      
    }

    public void error(StringId msgId, Object param, Throwable ex) {
      // TODO Auto-generated method stub
      
    }

    public void error(StringId msgId, Object param) {
      // TODO Auto-generated method stub
      
    }

    public void error(StringId msgId, Object[] params, Throwable ex) {
      // TODO Auto-generated method stub
      
    }

    public void error(StringId msgId, Object[] params) {
      // TODO Auto-generated method stub
      
    }

    public void error(StringId msgId, Throwable ex) {
      // TODO Auto-generated method stub
      
    }

    public void error(StringId msgId) {
      // TODO Auto-generated method stub
      
    }
    
    public void error(StringId msgId, int i) {
      // TODO Auto-generated method stub
      
    }
    
    public void info(StringId msgId, Object param, Throwable ex) {
      // TODO Auto-generated method stub
      
    }

    public void info(StringId msgId, Object param) {
      // TODO Auto-generated method stub
      
    }

    public void info(StringId msgId, Object[] params, Throwable ex) {
      // TODO Auto-generated method stub
      
    }

    public void info(StringId msgId, Object[] params) {
      // TODO Auto-generated method stub
      
    }

    public void info(StringId msgId, Throwable ex) {
      // TODO Auto-generated method stub
      
    }

    public void info(StringId msgId, int i) {
      // TODO Auto-generated method stub
      
    }
    
    public void info(StringId msgId) {
      // TODO Auto-generated method stub
      
    }

    public void severe(StringId msgId, Object param, Throwable ex) {
      // TODO Auto-generated method stub
      
    }

    public void severe(StringId msgId, Object param) {
      // TODO Auto-generated method stub
      
    }

    public void severe(StringId msgId, Object[] params, Throwable ex) {
      // TODO Auto-generated method stub
      
    }

    public void severe(StringId msgId, Object[] params) {
      // TODO Auto-generated method stub
      
    }

    public void severe(StringId msgId, Throwable ex) {
      // TODO Auto-generated method stub
      
    }

    public void severe(StringId msgId, int i) {
      // TODO Auto-generated method stub
      
    }
    
    public void severe(StringId msgId) {
      // TODO Auto-generated method stub
      
    }

    public void warning(StringId msgId, Object param, Throwable ex) {
      // TODO Auto-generated method stub
      
    }

    public void warning(StringId msgId, Object param) {
      // TODO Auto-generated method stub
      
    }

    public void warning(StringId msgId, Object[] params, Throwable ex) {
      // TODO Auto-generated method stub
      
    }

    public void warning(StringId msgId, Object[] params) {
      // TODO Auto-generated method stub
      
    }

    public void warning(StringId msgId, Throwable ex) {
      // TODO Auto-generated method stub
      
    }

    public void warning(StringId msgId, int i, Throwable ex) {
      // TODO Auto-generated method stub
      
    }
    
    public void warning(StringId msgId, int i) {
      warning(msgId, i, null);
    }
    
    public void warning(StringId msgId) {
      // TODO Auto-generated method stub
      
    }
    
    public LogWriter convertToLegacyLogWriter() {
      return this;
    }
    
    public LogWriterI18n convertToLogWriterI18n() {
      throw new UnsupportedOperationException("Cannot convert this LogWriter to LogWriterI18n");
    }
  }

  static StandaloneLogger myLogger;


  static public void initialize()
  {
    if (useHydra) {
      counters = SimpleBlackboard.getInstance()
      .getSharedCounters();
      duration = TestConfig.tab().intAt(SimpleParms.duration);
      sltime = TestConfig.tab().intAt(SimpleParms.sleepTime);
    }
    else {
      myLogger = new StandaloneLogger();
    }
    StringBuffer sb = new StringBuffer();
    boolean foundJg = false;
    for (int i = 0; i < jgNames.length; i ++) {
      String p;
      if (useHydra)
        p = TestConfig.tab().stringAt(jgNamesKeys[i]);
      else
        p = System.getProperty(jgNames[i]);
      if (p == null || p.length() == 0)
        continue;
      foundJg = true;
      sb.append(p);
      if (i < jgNames.length - 1)
        sb.append(":");
    } // for
    if (!foundJg) {
      for (int i = 0; i < jgDefaults.length; i ++) {
        sb.append(jgDefaults[i]);
        if (i < jgDefaults.length - 1)
          sb.append(":");
      }
    }
    String props = sb.toString();
    if (!useHydra) {
      log().config("Jgroups configuration = " + props);
    }

    String regionModifier;

    try {
      cacheAttr = new CacheAttributes();
      cacheAttr.setDistribute(true);
      regionModifier = System.getProperty("regionModifier", "");

      JCache.init(cacheAttr);

      Attributes regionAttr = new Attributes();
      long flags = Attributes.DISTRIBUTE;
      regionAttr.setFlags(flags);

      CacheAccess.defineRegion(REGION_NAME + regionModifier, regionAttr);

      // get a CacheAccess
      ca = CacheAccess.getAccess(REGION_NAME + regionModifier, props);

      // weirdness here...
      Assert.assertTrue(CacheAccess.isOpen());
      
      // make sure at least one entry with given name
      Vector vec = createVector();
      getOwnershipIfSync(ORDERS);
      ca.put(ORDERS, regionAttr, vec);
    }
    catch (Exception ex) {
      ex.printStackTrace();
      System.exit(1);
    }

  }

  static private Vector createVector()
  {
    Vector vec = new Vector();
    for (int j = 0; j < 10; j++) {
      vec.addElement(new Order(String.valueOf(j), 34, new Date(), 34.5));
    }
    return vec;
  }

  // Shuts down the CacheAccess and Cache.
  static public void shutdown()
  {

    try {
      if (ca != null) {
        ca.close();
        ca = null;
      }
    }
    catch (Exception ex) {
      ex.printStackTrace();
//      System.exit(1);
    }
  }

  static boolean replyRequested(String entryName) throws JCacheException
  {
    Attributes attrs = null;
    try {
      attrs = ca.getAttributes(entryName);
    }
    catch (JCacheException ex1) {
      attrs = ca.getAttributes(); // get Region attributes
    }
    return ((attrs.getFlags() & Attributes.REPLY) > 0);
  }

  static public void putOrders()
  {
    int sleepTime = sltime;
    int i;
    long t1, t2, t3 = 0;
    Vector vec;
    int timeOuts = 0;

    long start, end;
    start = System.currentTimeMillis();
    end = start + (1000L * duration);

    Attributes objAttr = new Attributes();
    long flags = Attributes.DISTRIBUTE;
    objAttr.setFlags(flags);

    try {
      for (i = 0; System.currentTimeMillis() < end; i++) {
        try {
          vec = createVector();
          getOwnershipIfSync(ORDERS);
          if (ca.isPresent(ORDERS)) {
            ca.destroy(ORDERS);
          }

          t1 = System.currentTimeMillis();
          ca.put(ORDERS, objAttr, vec);
          t2 = System.currentTimeMillis();
          t3 += (t2 - t1);
          // out.print((t2 - t1) + " ");
          if (sleepTime > 0) {
            try {
              Thread.sleep(sleepTime);
            }
            catch (InterruptedException ex) {
            }
          }

        }
        catch (Exception ex) {
          ex.printStackTrace();
          throw new SimpleTestException("Failure during operation", ex);
        }
        finally {
          try {
            if (replyRequested(ORDERS)) {
              ca.waitForResponse(TIMEOUT_MS);
              // out.println("completed waitForResponse");
            }
          }
          catch (JTimeoutException ex1) {
            timeOuts ++;
            ca.cancelResponse();
          }
          releaseOwnershipIfSync(ORDERS);
        }
      } // for
      if (useHydra) {
        counters.add(SimpleBlackboard.NumEvents, i);
        counters.add(SimpleBlackboard.EventsElapsed, t3);
        counters.add(SimpleBlackboard.NumTimeOuts, timeOuts);
      }
      else {
        System.out.println("" + i + "\t" + t3 + "\t" + timeOuts);
        }
    }
    catch (Exception ex) {
      ex.printStackTrace();
      throw new SimpleTestException("failure during operation", ex);
    }
  }

  static public void replaceUnchangedOrders()
  {
    int sleepTime = sltime;
    long t1, t2, t3 = 0;
    int i;
    Vector vec;
    int timeOuts = 0;

    long start, end;
    start = System.currentTimeMillis();
    end = start + (1000L * duration);

    Attributes objAttr = new Attributes();
    long flags = Attributes.DISTRIBUTE;
    objAttr.setFlags(flags);

    vec = createVector();
    try {
      ca.put(ORDERS, objAttr, vec);
    }
    catch (JCacheException ce) {
    }

    try {
      for (i = 0; System.currentTimeMillis() < end; i++) {
        try {
          getOwnershipIfSync(ORDERS);
          vec = (Vector)ca.get(ORDERS);

          t1 = System.currentTimeMillis();
          ca.replace(ORDERS, vec);
          t2 = System.currentTimeMillis();
          t3 += (t2 - t1);
          // out.print((t2 - t1) + " ");
          if (sleepTime > 0) {
            try {
              Thread.sleep(sleepTime);
            }
            catch (InterruptedException ex) {
            }
          }

        }
        catch (Exception ex) {
          ex.printStackTrace();
          throw new SimpleTestException("Failure during operation", ex);
        }
        finally {
          try {
            if (replyRequested(ORDERS)) {
              ca.waitForResponse(TIMEOUT_MS);
              // out.println("completed waitForResponse");
            }
          }
          catch (JTimeoutException ex1) {
            timeOuts ++;
            ca.cancelResponse();
          }
          releaseOwnershipIfSync(ORDERS);
        }
      }
      if (useHydra) {
        counters.add(SimpleBlackboard.NumEvents, i);
        counters.add(SimpleBlackboard.EventsElapsed, t3);
        counters.add(SimpleBlackboard.NumTimeOuts, timeOuts);
      }
      else {
        System.out.println("" + i + "\t" + t3 + "\t" + timeOuts);
      }
    }
    catch (Exception ex) {
      ex.printStackTrace();
      return;
    }
  }

  static public void replaceChangedOrders()
  {
    int sleepTime = sltime;
    long t1, t2, t3 = 0;
    int i;
    Vector vec;
    int timeOuts = 0;

    long start, end;
    start = System.currentTimeMillis();
    end = start + (1000L * duration);

    Attributes objAttr = new Attributes();
    long flags = Attributes.DISTRIBUTE;
    objAttr.setFlags(flags);

    vec = createVector();
    try {
      ca.put(ORDERS, objAttr, vec);
    }
    catch (JCacheException ce) {
    }

    try {
      for (i = 0; System.currentTimeMillis() < end; i++) {
        try {
          vec = createVector();
          getOwnershipIfSync(ORDERS);

          t1 = System.currentTimeMillis();
          ca.replace(ORDERS, vec);
          t2 = System.currentTimeMillis();
          t3 += (t2 - t1);
          // out.print((t2 - t1) + " ");
          if (sleepTime > 0) {
            try {
              Thread.sleep(sleepTime);
            }
            catch (InterruptedException ex) {
            }
          }

        }
        catch (Exception ex) {
          ex.printStackTrace();
          throw new SimpleTestException("failure during operation", ex);
        }
        finally {
          try {
            if (replyRequested(ORDERS)) {
              ca.waitForResponse(TIMEOUT_MS);
              // out.println("completed waitForResponse");
            }
          }
          catch (JTimeoutException ex1) {
            ca.cancelResponse();
            timeOuts ++;
          }
          releaseOwnershipIfSync(ORDERS);
        }
      }
      if (useHydra) {
        counters.add(SimpleBlackboard.NumEvents, i);
        counters.add(SimpleBlackboard.EventsElapsed, t3);
        counters.add(SimpleBlackboard.NumTimeOuts, timeOuts);
      }
      else {
        System.out.println("" + i + "\t" + t3 + "\t" + timeOuts);
      }
    }
    catch (Exception ex) {
      ex.printStackTrace();
      throw new SimpleTestException("failure during operation", ex);
    }
  }

  static public void getOrders()
  {
    int sleepTime = sltime;
    long t1, t2, t3 = 0;
    int i;

    // out.println("sleeping for 10 seconds");
    // try { Thread.sleep(10000);} catch (InterruptedException ie) { };
    // out.println("continuing");

    long start, end;
    start = System.currentTimeMillis();
    end = start + (1000L * duration);

    try {
      for (i = 0; System.currentTimeMillis() < end; i++) {
        try {

          t1 = System.currentTimeMillis();
          ca.get(ORDERS);
          t2 = System.currentTimeMillis();
          t3 += (t2 - t1);
          // out.print((t2 - t1) + " ");
          if (sleepTime > 0) {
            try {
              Thread.sleep(sleepTime);
            }
            catch (InterruptedException ex) {
            }
          }

        }
        catch (Exception ex) {
          ex.printStackTrace();
          throw new SimpleTestException("failure during operation", ex);
        }
      }
      if (useHydra) {
        counters.add(SimpleBlackboard.NumEvents, i);
        counters.add(SimpleBlackboard.EventsElapsed, t3);
      }
      else {
        System.out.println("" + i + "\t" + t3);
      }
    }
    catch (Exception ex) {
      ex.printStackTrace();
      throw new SimpleTestException("failure during operation", ex);
    }
  }

  // public void ownOrders(CacheAccess ca) {
  // try {
  // getOwnership(ORDERS,ca);
  // } catch (Exception ex) {
  // ex.printStackTrace();
  // }
  // }

  // public void releaseOrders(CacheAccess ca) {
  // try {
  // releaseOwnership(ORDERS,ca);
  // } catch (Exception ex) {
  // ex.printStackTrace(err);
  // }
  // }

  static boolean isSynchronized(String entryName) throws JCacheException
  {
    Attributes attrs = null;
    try {
      attrs = ca.getAttributes(entryName);
    }
    catch (JCacheException ex1) {
      attrs = ca.getAttributes(); // get Region attributes
    }
    return ((attrs.getFlags() & Attributes.SYNCHRONIZE) > 0);
  }

  static void getOwnership(String entryName) throws JCacheException
  {
    try {
      ca.getOwnership(entryName, TIMEOUT_MS);
    }
    catch (JCacheException ex) {
      String s = "Could not get ownership of entry " + entryName + " within "
              + TIMEOUT_MS + " milliseconds.";
      throw new SimpleTestException(s, ex);
    }
  }

  static void getOwnershipIfSync(String entryName) throws JCacheException
  {
    if (isSynchronized(entryName)) {
      getOwnership(entryName);
    }
  }

  static void releaseOwnership(String entryName) throws JCacheException
  {
    try {
      ca.releaseOwnership(TIMEOUT_MS);
    }
    catch (JCacheException ex) {
      String s = "Could not release ownership of entry " + entryName
              + " within " + TIMEOUT_MS + " milliseconds.";
      throw new SimpleTestException(s, ex);
    }
  }

  static void releaseOwnershipIfSync(String entryName) throws JCacheException
  {
    if (isSynchronized(entryName)) {
      releaseOwnership(entryName);
    }
  }

  /**
   * createCacheTask: general task to hook a vm to the distributed cache.
   */
  public static synchronized void createCacheTask()
  {
    Assert.assertTrue(useHydra);
    log().info("initializing the cache...");
    initialize();
    log().info("Initialized the cache");
  }

  /**
   * closeCacheTask: general task to unhook a vm from the distributed cache.
   */
  public static synchronized void closeCacheTask()
  {
    Assert.assertTrue(useHydra);
    log().info("Closing the cache");
    shutdown();
    log().info("Closed the cache");
  }

  private static void runBenchmark(String task) {
    if (task == null || task.length() == 0)
      task = "get";
    if (task.equals("get"))
      getOrders();
    else
      if (task.equals("put"))
        putOrders();
      else
        if (task.equals("replaceChanged"))
          replaceChangedOrders();
        else
          if (task.equals("replaceUnchanged"))
          replaceUnchangedOrders();
          else
            throw new SimpleTestException("Unknown task type = " + task);
  }
  
  public static void runBenchmarkTask() {
    Assert.assertTrue(useHydra);
    String task = TestConfig.tab().stringAt(SimpleParms.task);
    runBenchmark(task);
  }

 public static void printBlackboardsTask()
  {
   Assert.assertTrue(useHydra);
    SimpleBlackboard bb = SimpleBlackboard.getInstance();
       bb.printSharedCounters();
       
     // note that names are always returned in same order.
    String names[] = bb.getCounterNames();
    long[] counterValues = bb.getSharedCounters().getCounterValues();

    try {
      // One must refetch these values, because it's a different
      // instance of the class that's reporting this!
      duration = TestConfig.tab().intAt(SimpleParms.duration);
      sltime = TestConfig.tab().intAt(SimpleParms.sleepTime);
      String summary = TestConfig.tab().stringAt(SimpleParms.summary);
      if (summary == null || summary.length() == 0)
        summary = "results.tsv"; // fault tolerance
      
      File f = new File(summary);
      boolean needHeader = !f.exists();
      
      FileOutputStream os = new FileOutputStream(f, true);
      PrintStream ps = new PrintStream(os);
      
      if (needHeader) {
        ps.print("Date\t");
        ps.print("duration\tsleepTime\tnumVM\tnumThreads\t");

        for (int i = 0; i < names.length; i++) {
          ps.print(names[i]);
          ps.print("\t");
        }
        for (int i = 0; i < jgNames.length; i++) {
          ps.print(jgNames[i]);
          if (i < jgNames.length - 1)
            ps.print("\t");
        }
        ps.println();
      }
      
      long numVM = TestConfig.tab().longAt( ClientPrms.vmQuantities);
      long numThreads = TestConfig.tab().longAt(ClientPrms.vmThreads);

      SimpleDateFormat df = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss.SSS");
      String now = df.format(new Date());
      
      ps.print(now + "\t" 
              + duration + "\t" 
              + sltime + "\t" 
              + numVM + "\t" 
              + numThreads + "\t");

      for (int i = 0; i < names.length; i ++) {
        ps.print(counterValues[i]);
        ps.print("\t");
      }

      for (int i = 0; i < jgNames.length; i ++) {
        String val = TestConfig.tab().stringAt(jgNamesKeys[i]);
        ps.print(val);
        if (i < jgNames.length - 1)
          ps.print("\t");
      }

      ps.println();
      ps.close();
    }
    catch (Exception e) {
      throw new SimpleTestException("Error writing stats: ", e);
    }
  }

  private static LogWriter log()
  {
    if (useHydra)
      return Log.getLogWriter();
    else
      return myLogger;
  }
  
  public static void main(String args[]) {
   String task = args[0];
   
   duration = Integer.parseInt(args[1]);
   sltime = Integer.parseInt(args[2]);
   
   useHydra = false;
   initialize();
   runBenchmark(task);
   shutdown();
  }
}
