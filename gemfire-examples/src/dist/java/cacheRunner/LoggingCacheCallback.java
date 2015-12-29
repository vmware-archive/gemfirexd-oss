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
package cacheRunner;

import com.gemstone.gemfire.cache.*;
import java.util.*;

/**
 * The abstract superclass of various GemFire
 * <code>CacheCallback</code>s that log information about the events
 * that they receieve.  Because this class implements the
 * <code>Declarable</code> interface, it may be used in a
 * <code>cache.xml</code> file.
 *
 * @author GemStone Systems, Inc.
 * @since 4.0
 */
public abstract class LoggingCacheCallback 
  implements CacheCallback, Declarable {

  /** Should this callback log to the GemFire logger as opposed to
   * standard out? */
  private boolean useGemFireLogger;

  //////////////////////  Constructors  //////////////////////

  /**
   * Creates a new <code>LoggingCacheCallback</code> that logs to
   * standard out.
   */
  protected LoggingCacheCallback() {
    this.useGemFireLogger = false;
  }

  /////////////////////  Instance Methods  /////////////////////

  /**
   * Initializes this cache callback using data configured in a
   * <code>cache.xml</code> file.  This callback recognizes the
   * <code>useGemFireLogger</code> property.
   */
  public void init(Properties props) {
    Enumeration<?> names = props.propertyNames();
    while (names.hasMoreElements()) {
      String name = (String) names.nextElement();
      if (name.equals("useGemFireLogger")) {
        String prop = props.getProperty(name);
        if (prop != null) {
          this.useGemFireLogger = Boolean.valueOf(prop).booleanValue();
        }

      } else {
        String s = "Unknown configuration property: " + name;
        throw new IllegalArgumentException(s);
      }
    }
  }

  /**
   * No logging is performed when a callback is closed
   */
  public void close() {

  }

  /**
   * Logs a message to either standard out or to the GemFire logger
   * obtained from the given <code>GemFireCache</code>.
   */
  protected void log(String message, GemFireCache cache) {
    if (this.useGemFireLogger) {
      cache.getLogger().info(message);
    } else {
      System.out.println(message);
    }
  }
  
  /**
   * Logs a message to either standard out or to the GemFire logger
   * obtained from the given <code>Region</code>.
   */
  protected void log(String message, Region region) {
    if (region.getRegionService() instanceof GemFireCache) {
      log(message, (GemFireCache)region.getRegionService());
    } else {
      System.out.println(message);
    }
  }

  /**
   * Formats information about a <code>CacheEvent</code>
   */
  private String format(CacheEvent event) {
    Operation operation = event.getOperation();

    StringBuffer sb = new StringBuffer();
    sb.append("Event on region ");
    sb.append(event.getRegion().getFullPath());

    sb.append("\n  [");
    if (operation.isDistributed()) {
      sb.append("distributed");

    } else {
      sb.append("local");
    }

    sb.append(", ");
    if (operation.isExpiration()) {
      sb.append("expiration");
    } else {
      sb.append("not expiration");
    }

    sb.append(", ");
    if (event.isOriginRemote()) {
      sb.append("remote origin");
    } else {
      sb.append("local origin");
    }
    
    sb.append("]\n");
    Object argument = event.getCallbackArgument();
    if (argument != null) {
      sb.append("Callback argument: ");
      sb.append(argument);
      sb.append("\n");
    }

    return sb.toString();
  }

  /**
   * Formats an arbitrary object into a string
   */
  protected String format(Object obj) {
    if (obj instanceof byte[]) {
      return new String((byte[]) obj);

    } else if (obj instanceof ExampleObject) {
      ExampleObject example = (ExampleObject) obj;
      StringBuffer sb = new StringBuffer();

      sb.append("\"");
      sb.append(example.getDoubleField());
      sb.append("\"(double)");

      sb.append(" \"");
      sb.append(example.getLongField());
      sb.append("\"(long)");

      sb.append(" \"");
      sb.append(example.getFloatField());
      sb.append("\"(float)");

      sb.append(" \"");
      sb.append(example.getIntField());
      sb.append("\"(int)");

      sb.append(" \"");
      sb.append(example.getShortField());
      sb.append("\"(short)");

      sb.append(" \"");
      sb.append(example.getStringField());
      sb.append("\"(String)");

      return sb.toString();

    } else {
      return String.valueOf(obj);
    }
  }

  /**
   * Formats an <code>EntryEvent</code> into a string
   */
  protected String format(EntryEvent event) {
    Operation operation = event.getOperation();

    StringBuffer sb = new StringBuffer();
    sb.append("Entry");
    sb.append(format((CacheEvent) event));
    
    sb.append("  [");
    if (operation.isLoad()) {
      if (operation.isLocalLoad()) {
        sb.append("local load");

      } else {
        sb.append("distributed load");
      }

      if (operation.isNetLoad()) {
        sb.append(", net load");
      }

    } else {
      sb.append("not load");
    }

    if (operation.isNetSearch()) {
      sb.append(", net search");
    }
    sb.append("]\n");

    if (event.getTransactionId() != null) {
      sb.append("  Transaction: ");
      sb.append(event.getTransactionId());
      sb.append("\n");
    }

    sb.append("  Key: ");
    sb.append(format(event.getKey()));
    sb.append("\n");

    sb.append("  Old value: ");
    sb.append(format(event.getOldValue()));
    sb.append("\n");

    sb.append("  New value: ");
    sb.append(format(event.getNewValue()));
    sb.append("\n");

    return sb.toString();
  }

  /**
   * Logs information about an <code>EntryEvent</code>
   *
   * @param kind
   *        The kind (update, destroy, etc.) of event to be logged
   */
  protected void log(String kind, EntryEvent event) {
    StringBuffer sb = new StringBuffer();
    sb.append(kind);
    sb.append(" ");
    sb.append(format(event));

    log(sb.toString(), event.getRegion());
  }

  /**
   * Logs information about a <code>RegionEvent</code>
   * @param kind
   *        The kind (destroy, etc.) of event to be logged
   */
  protected void log(String kind, RegionEvent event) {
    StringBuffer sb = new StringBuffer();
    sb.append(kind);
    sb.append(" ");
    sb.append("Region");
    sb.append(format(event));

    log(sb.toString(), event.getRegion());
  }
  
}
