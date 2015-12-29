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
package regions.validate;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXmlGenerator;
import distcache.*;
import distcache.gemfire.GemFireCacheTestImpl;
import hydra.*;
import java.io.*;
import java.util.*;
import objects.ObjectHelper;
import util.NameFactory;
import util.TestException;

/**
 * Contains Hydra tasks that use a {@link ValidateBlackboard} to
 * validate the contents of a GemFire cache {@link Region}.  The
 * region is configured and created using a {@link DistCacheFactory}.
 * The keys of the region are generated using a {@link NameFactory}.
 * The values are generated using an {@link ObjectHelper}.
 *
 * @see ValidatePrms
 *
 * @author David Whitlock
 * @since 3.5
 */
public class ValidateTasks {

  /** The IN_USE marker */
  private static final InUse IN_USE = InUse.singleton;

  /** The region used by this test */
  private static Region region;

  /** The ValidateBlackboard used by this test */
  private static ValidateBlackboard bb;

  /** Should we debug this test run? */
  private static boolean DEBUG;

  /** Used for logging */
  private static LogWriter logger;

  /** The random number generator used for this test */
  private static GsRandom random;

  /** The latency (in milliseconds) to expect in distribution */
  private static long latency;

  ///////////////////////  Hydra Tasks  ///////////////////////

  /**
   * An INIT task that creates the Region used by this test
   */
  public static void initializeVM() throws Exception {
    synchronized (ValidateTasks.class) {
      if (region != null) {
        return;
      }

      DistCache dc = DistCacheFactory.createInstance();
      dc.open();
      region = ((GemFireCacheTestImpl) dc).getRegion();
      Assert.assertTrue(region != null);
      bb = ValidateBlackboard.getInstance();
      DEBUG = ValidatePrms.isDebug();
      logger = Log.getLogWriter();
      random = TestConfig.tab().getRandGen();
      latency = ValidatePrms.getDistributionLatency();

      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw, true);
      pw.println("Current cache configuration:");
      CacheXmlGenerator.generate(region.getCache(), pw);
      Log.getLogWriter().info(sw.toString());
    }
  }

  /**
   * An INIT task that places 100 entries into the region.  Note that
   * because this is an INIT task, it is executed by each VM.  So, the
   * total initial entry count will be numVMs*numTaskThreads*100.
   */
  public static void populateRegion() throws Exception {
    // Before populating check if interest registration needs to
    // be done.
    boolean receiveValuesAsInvalidates = ValidatePrms.getReceiveValuesAsInvalidates();
    if (receiveValuesAsInvalidates) {
      region.registerInterestRegex(".*", false, false);
    }

    for (int i = 0 ; i < 100; i++) {
      create();
    }
  }

  /**
   * A TASK that chooses a random operation to perform on the cache.
   * The {@link ValidateBlackboard} is updated appropriately.
   * Operations are performed until a {@linkplain
   * ValidatePrms#getEntryOperationsDuration given number of seconds}
   * have passed.
   *
   * @see ValidatePrms#getEntryOperation
   */
  public static void doEntryOperations() throws Exception {
    long duration = ValidatePrms.getEntryOperationsDuration() * 1000;
    long start = System.currentTimeMillis();

    while (System.currentTimeMillis() - start < duration) {
      String op = ValidatePrms.getEntryOperation();
      if (DEBUG) {
        logger.info("Performing operation \"" + op + "\"");
      }

      if (op.equals("create")) {
        create();

      } else if (op.equals("load")) {
        load();

      } else if (op.equals("get")) {
        get();

      } else if (op.equals("update")) {
        update();

      } else if (op.equals("invalidate")) {
        invalidate();

      } else if (op.equals("destroy")) {
        destroy();

      } else {
        String s = "Unknown entry operation: " + op;
        throw new HydraConfigException(s);
      }
    }
  }

  /**
   * A CLOSE task that validates the contents of the region.
   */
  public static void validateRegion() throws Exception {
    for (Iterator iter = region.keys().iterator(); iter.hasNext(); ) {
      Object key = iter.next();

      // Remember that every task thread will execute this method, so
      // there is bound to be some contention.  Thus, the call to
      // get() may return false.
      get(key);
    }
  }

  /**
   * An END task that examines the contents of the {@link
   * ValidateBlackboard} to make sure that there no entry is still "in
   * use".  If so, then there was a problem with the test.
   */
  public static void ensureNotInUse() throws Exception {
    bb = ValidateBlackboard.getInstance();
    Map map = bb.getSharedMap().getMap();
    StringBuffer sb = new StringBuffer();

    for (Iterator iter = map.entrySet().iterator();
         iter.hasNext(); ) {
      Map.Entry entry = (Map.Entry) iter.next();
      if (entry.getValue() instanceof InUse) {
        sb.append("  Key ");
        sb.append(entry.getKey());
        sb.append(" is still in use");
        sb.append("\n");
      }
    }

    if (sb.length() > 0) {
      sb.insert(0, "At the end of the test, some keys were still in use\n");
      throw new TestException(sb.toString());
    }
  }

  //////////////////////  Static Methods  //////////////////////

  /**
   * Creates a new entry in the region
   */
  private static void create() throws Exception {
    Object key = NameFactory.getNextPositiveObjectName();

    if (DEBUG) {
      logger.info("Creating " + key);
    }

    Value oldValue = bb.get(key);
    Assert.assertTrue(oldValue == null);
    Assert.assertTrue(bb.replace(key, oldValue, IN_USE));

    String objectType = ValidatePrms.getObjectType();
    long l = NameFactory.getCounterForName(key);
    Object value = ObjectHelper.createObject(objectType, (int) l);

    if (DEBUG) {
      logger.info("Created " + key + " -> " + value);
    }

    region.create(key, value);

    ObjectValue newValue = new ObjectValue(value);
    Assert.assertTrue(bb.replace(key, IN_USE, newValue));
  }

  /**
   * Chooses a random key and {@link #get(Object) gets} it.
   *
   * @returns whether or not the validation actually took place
   */
  private static boolean get() throws Exception {
    Object key = bb.getRandomKey();
    //Thread.yield(); // bruce - allow things like invalidations to be processed in the cache
    return get(key);
  }

  /**
   * Validates that the value in the region for the given
   * <code>key</code> matches the value in the blackboard.  Takes into
   * account various region configurations.  Note that if the entry is
   * currently "in use", then the validation will not occur.
   *
   * @throws TestException
   *         Value in region is not what we expect
   *
   * @returns Whether or not the validation actually took place
   */
  private static boolean get(Object key) throws Exception {
    Value expected = bb.get(key);
    if (expected == null) {
      String s = "No value in blackboard for key " + key;
      Assert.assertTrue(expected != null, s);
    }

    if (expected instanceof InUse) {
      if (DEBUG) {
        logger.info("Not getting " + key + " because it is in use");
      }
      return false;
    }

    // Make sure nobody else can change the entry while we're
    // validating it.
    if (!bb.replace(key, expected, IN_USE)) {
      if (DEBUG) {
        logger.info("Not getting " + key + " because we could not " +
                    "put it in use");
      }
      return false;
    }

    if (expected instanceof DestroyedValue) {
      if (region.getEntry(key) != null) {
        String s = "Expected entry " + key + " to be destroyed";
        throw new TestException(s);
      }

    } else if (expected instanceof InvalidValue) {
      Region.Entry entry = region.getEntry(key);
      if (entry == null) {
        if (DEBUG) {
          logger.info("Invalid entry for " + key +
                      " does not exist in this VM");
        }

      } else {
        Object value = entry.getValue();
        if (value != null) {
          String s = "Expected entry " + key + " to be invalid, " +
            "but it has value " + value;
          throw new TestException(s);
        }
      }

    } else if (expected instanceof ObjectValue) {
      // Do a get() so that the value is loaded/searched for
      Object expectedValue = ((ObjectValue) expected).getValue();
      Object actualValue = region.get(key);
      if (!expectedValue.equals(actualValue)) {
        if (System.currentTimeMillis() >
            (expected.getTimestamp() + latency)) {
          String s = "Expected entry " + key + " to have value " +
            expectedValue + " from " + expected.formatTimestamp() + 
            ", but it had value " + actualValue;
          throw new TestException(s);

        } else {
          if (DEBUG) {
            logger.info("Have not received latest value of " + key +
                        " (" + expected + ")");
          }
        }
      }

    } else {
      Assert.assertTrue(false, "Don't know how to handle a " +
                        expected.getClass().getName());
    }

    Assert.assertTrue(bb.replace(key, IN_USE, expected));

    if (DEBUG) {
      logger.info("Successfully got " + key);
    }
    return true;
  }

  /**
   * Creates a new key and gets it from the cache.
   *
   * @see ValidateLoader
   */
  private static void load() throws Exception {
    Object key = NameFactory.getNextPositiveObjectName();

    if (DEBUG) {
      logger.info("Loading value for " + key);
    }

    region.get(key);
  }

  /**
   * Chooses a random key in the region and assigns it a new value.
   *
   * @returns Whether or not the update actually took place
   */
  private static boolean update() throws Exception {
    Object key = bb.getRandomKey();

    if (DEBUG) {
      logger.info("Updating " + key);
    }

    Value oldValue = bb.get(key);
    if (oldValue == null) {
      String s = "No value in blackboard for key " + key;
      Assert.assertTrue(oldValue != null, s);
    }

    if (oldValue instanceof InUse) {
      if (DEBUG) {
        logger.info("Not updating " + key + " because it is in use");
      }
      return false;
    }

    // Make sure nobody else can change the entry while we're
    // validating it.
    if (!bb.replace(key, oldValue, IN_USE)) {
      if (DEBUG) {
        logger.info("Not updating " + key + " because we could not " +
                    "put it in use");
      }
      return false;
    }

    String objectType = ValidatePrms.getObjectType();
    long l = NameFactory.getCounterForName(key);
    Object value = ObjectHelper.createObject(objectType, (int) l);

    if (DEBUG) {
      if (oldValue instanceof ObjectValue) {
        logger.info("Updated " + key + " -> (" + 
                    ((ObjectValue) oldValue).getValue() + " -> " +
                    value);

      } else {
        logger.info("Updated " + key + " -> " + value);
      }
    }

    region.put(key, value);

    ObjectValue newValue = new ObjectValue(value);
    Assert.assertTrue(bb.replace(key, IN_USE, newValue));
    return true;
  }

  /**
   * Chooses a random key in the region and (distributed) invalidates
   * it. 
   *
   * @returns Whether or not the update actually took place
   */
  private static boolean invalidate() throws Exception {
    Object key = bb.getRandomKey();

    if (DEBUG) {
      logger.info("Invalidating " + key);
    }

    Value oldValue = bb.get(key);
    if (oldValue == null) {
      String s = "No value in blackboard for key " + key;
      Assert.assertTrue(oldValue != null, s);
    }

    if (oldValue instanceof InUse) {
      if (DEBUG) {
        logger.info("Not invalidating " + key + " because it is in use");
      }
      return false;
    }

    // Make sure nobody else can change the entry while we're
    // validating it.
    if (!bb.replace(key, oldValue, IN_USE)) {
      if (DEBUG) {
        logger.info("Not invalidating " + key + " because we could not " +
                    "put it in use");
      }
      return false;
    }

    if ((oldValue instanceof InvalidValue) ||
        (oldValue instanceof DestroyedValue)) {
      if (DEBUG) {
        logger.info("Not invalidating " + key + " because it is not "
                    + "valid: " + oldValue);
      }

      Assert.assertTrue(bb.replace(key, IN_USE, oldValue));
      return false;

    } else {
      // Get the value into this VM before invalidating it.  Note that
      // this should NOT invoke the loader because the value is valid
      // is some region in the distributed system.
      region.get(key);
      region.invalidate(key);

      InvalidValue newValue = new InvalidValue();
      Assert.assertTrue(bb.replace(key, IN_USE, newValue));
      return true;
    }
  }

  /**
   * Chooses a random key in the region and (distributed) destroyes
   * it. 
   *
   * @returns Whether or not the update actually took place
   */
  private static boolean destroy() throws Exception {
    Object key = bb.getRandomKey();

    if (DEBUG) {
      logger.info("Destroying " + key);
    }

    Value oldValue = bb.get(key);
    if (oldValue == null) {
      String s = "No value in blackboard for key " + key;
      Assert.assertTrue(oldValue != null, s);
    }

    if (oldValue instanceof InUse) {
      if (DEBUG) {
        logger.info("Not destroying " + key + " because it is in use");
      }
      return false;
    }

    // Make sure nobody else can change the entry while we're
    // validating it.
    if (!bb.replace(key, oldValue, IN_USE)) {
      if (DEBUG) {
        logger.info("Not invalidating " + key + " because we could not " +
                    "put it in use");
      }
      return false;
    }

    if (oldValue instanceof DestroyedValue) {
      if (DEBUG) {
        logger.info("Not destroying " + key + " because it is already "
                    + "destroyed: " + oldValue);
      }

      Assert.assertTrue(bb.replace(key, IN_USE, oldValue));
      return false;
    } 

    if (oldValue instanceof ObjectValue) {
      // Get the value into this VM before invalidating it.  Note that
      // this should NOT invoke the loader because the value is valid
      // is some region in the distributed system.
      region.get(key);

    } else if (oldValue instanceof InvalidValue &&
               region.getEntry(key) == null) {
      if (DEBUG) {
        logger.info("Not destroying " + key + " because it is " +
                    "invalid in some other VM");
      }
      
      Assert.assertTrue(bb.replace(key, IN_USE, oldValue));
      return false;
    }

    // Entry is in this VM
    Assert.assertTrue(region.getEntry(key) != null);
    region.destroy(key);

    DestroyedValue newValue = new DestroyedValue();
    Assert.assertTrue(bb.replace(key, IN_USE, newValue));
    return true;
  }

}
