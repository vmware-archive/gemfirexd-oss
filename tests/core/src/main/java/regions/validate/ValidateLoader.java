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
import objects.ObjectHelper;
import util.NameFactory;

/**
 * A <code>CacheLoader</code> that creates a new value for the region
 * and updates the value in the {@link ValidateBlackboard}
 * appropriately. 
 *
 * @author David Whitlock
 * @since 3.5
 */
public class ValidateLoader implements CacheLoader {

  /** The "in use" token */
  private static InUse IN_USE = InUse.singleton;

  //////////////////////  Constructors  //////////////////////

  /**
   * Creates a new <code>ValidateLoader</code>
   */
  public ValidateLoader() {

  }

  ////////////////////  Instance Methods  ////////////////////

  /**
   * Creates a new value for the key and places it in the blackboard
   */
  public Object load(LoaderHelper helper)
    throws CacheLoaderException {

    Object key = helper.getKey();

    // Look for the value loaded in another cache
    Object value;

    if (helper.getRegion().getAttributes().getScope().isDistributed()) {
      if (true) {
        String s = "netSearch not working.  See bug 30895";
        throw new UnsupportedOperationException(s);
      }

      try {
        value = helper.netSearch(false /* doNetLoad */);

      } catch (TimeoutException ex) {
        String s = "Timed out while loading " + key;
        throw new CacheLoaderException(s, ex);
      }

      if (value != null) {
        return value;
      }
    }

    LogWriter logger = hydra.Log.getLogWriter();
    boolean DEBUG = ValidatePrms.isDebug();

    if (DEBUG) {
      logger.info("Loading " + key);
    }

    ValidateBlackboard bb = ValidateBlackboard.getInstance();

    while (true) {
      Value oldValue = bb.get(key);
      if (!bb.replace(key, oldValue, IN_USE)) {
        try {
          Thread.sleep(200);
          if (DEBUG) {
            logger.info("Waiting to use " + key);
          }

        } catch (InterruptedException ex) {
          logger.info("Interrupted while waiting to load " + key);
          return null;
        }
      }

      if (oldValue instanceof ObjectValue) {
        // Someone else has already loaded it
        return ((ObjectValue) oldValue).getValue();

      } else {
        String objectType = ValidatePrms.getObjectType();
        long l = NameFactory.getCounterForName(key);
        value = ObjectHelper.createObject(objectType, (int) l);

        if (DEBUG) {
          logger.info("Loaded " + key + " -> " + value);
        }
        
        ObjectValue newValue = new ObjectValue(value);
        Assert.assertTrue(bb.replace(key, IN_USE, newValue));
        return value;
      }
    }
  }

  public void close() {

  }

}
